%pyspark
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import col, udf
from pyspark.sql import Window
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import json
import datetime
import time
from collections import defaultdict
from dateutil.parser import parse
import argparse
"""
按照车辆电池总压单体电压不一致算法产生预警数据
:return: 电池总压单体电压不一致预警结果存入Hive表
"""
# 创建SparkSession, 作为读取数据、处理源数据、配置回话和管理集群资源的入口
spark = SparkSession.builder \
    .appName("AnalysingBatteryAlarms") \
    .config("spark.debug.maxToStringFields", "1000") \
    .config("spark.network.timeout", "1000") \
    .config("spark.shuffle.memoryFraction", "0.6") \
    .config("spark.default.parallelism", "500") \
    .config("spark.sql.shuffle.partitions", "500") \
    .enableHiveSupport() \
    .getOrCreate()  # prod

# 读取满足总压采样异常场景的数据
source_table = "saas_battery.d_i_battery_block_features"
dim_table = "saas_battery.ods_dim_battery_model"
dict_params = algorithm.dict_params
battery_types = dict_params.keys()
battery_types_str = ', '.join(["'" + i + "'" for i in battery_types])
ap13_block_msg_counter_limit = 1
ap13_delta_cell_pack_volt_threshold = 20

# 1. 一次性读取所有数据，只做基本过滤
sql_read = f"""
    SELECT t1.*, t2.series_num, 'tsp' as source
    FROM
    (SELECT *, size(max_volt_sn_count) as max_volt_count, size(min_volt_sn_count) as min_volt_count, size(max_temp_sn_count) as max_temp_count, size(min_temp_sn_count) as min_temp_count
    FROM {source_table}
    WHERE dt = '20260501'
    and battery_type in ({battery_types_str})
    ) t1
    LEFT JOIN 
        (SELECT  cell_number AS series_num, battery_model
        FROM {dim_table}
        WHERE dt = '20260501'
        ) t2
    ON t1.battery_type = t2.battery_model
    WHERE (t1.max_volt_count > 1 or t1.min_volt_count > 1 or max_temp_count > 1 or min_temp_count > 1)
    """
df_detail = spark.sql(sql_read)

# 2. 计算总压与单体电压之差，有三种计算方式
df_detail = df_detail.withColumn("delta_cell_pack_volt_1",
                                 (df_detail.avg_high_cell_volt + df_detail.avg_low_cell_volt) / 2
                                 * df_detail.series_num / 1000 - df_detail.avg_pack_voltage)
df_detail = df_detail.withColumn("delta_cell_pack_volt_2",
                                 (df_detail.max_high_cell_volt + df_detail.min_low_cell_volt) / 2
                                 * df_detail.series_num / 1000 - df_detail.avg_pack_voltage)
df_detail = df_detail.withColumn("delta_cell_pack_volt_3",
                                 (df_detail.min_high_cell_volt + df_detail.max_low_cell_volt) / 2
                                 * df_detail.series_num / 1000 - df_detail.avg_pack_voltage)
df_detail = df_detail.withColumn("delta_cell_pack_volt_1_abs", functions.abs(df_detail.delta_cell_pack_volt_1))
df_detail = df_detail.withColumn("delta_cell_pack_volt_2_abs", functions.abs(df_detail.delta_cell_pack_volt_2))
df_detail = df_detail.withColumn("delta_cell_pack_volt_3_abs", functions.abs(df_detail.delta_cell_pack_volt_3))

# 3. 根据绝对值找出最小的电压偏差
df_detail = df_detail.withColumn("delta_cell_pack_volt", functions.when(
    (functions.when(df_detail.delta_cell_pack_volt_1_abs < df_detail.delta_cell_pack_volt_2_abs,
                    df_detail.delta_cell_pack_volt_1_abs).otherwise(df_detail.delta_cell_pack_volt_2_abs))
    > df_detail.delta_cell_pack_volt_3_abs, df_detail.delta_cell_pack_volt_3).otherwise(
    functions.when(df_detail.delta_cell_pack_volt_1_abs < df_detail.delta_cell_pack_volt_2_abs,
                   df_detail.delta_cell_pack_volt_1).otherwise(df_detail.delta_cell_pack_volt_2)))
df_detail = df_detail.withColumn("delta_cell_pack_volt_abs", functions.abs(df_detail.delta_cell_pack_volt))

# 4. 按照battery_type分组处理
processed_dfs = []
column = [
    "battery_id", "battery_type", "device_id", "device_name", "process_id",  "event_type",
    "start_time", "end_time", "start_user_soc", "start_current", "avg_pack_voltage",
    "avg_insu_resis", "avg_high_cell_volt", "avg_low_cell_volt", "delta_cell_pack_volt", "source"
]

for battery_type in battery_types:
    # 获取对应battery_type的参数
    params = dict_params.get(battery_type)
    if not params:
        continue
    
    # 提取参数
    ap13_block_msg_counter_limit = int(params["ap13_block_msg_counter_limit"])
    ap13_delta_cell_pack_volt_threshold = int(params["ap13_delta_cell_pack_volt_threshold"])
    
    # 5. 过滤当前battery_type的数据
    filtered_df = df_detail.filter(df_detail["battery_type"] == battery_type)\
        .filter(f"block_msg_count > {ap13_block_msg_counter_limit}")\
        .filter(f"delta_cell_pack_volt > {ap13_delta_cell_pack_volt_threshold}")
    
    # 6. 筛选每一块电池压差最大的最早触发时刻
    w1 = Window.partitionBy("battery_id").orderBy(functions.desc("delta_cell_pack_volt_abs"),
                                                  functions.asc("start_time"))
    filtered_df = filtered_df.withColumn("row_number", functions.row_number().over(w1))\
        .filter(functions.col("row_number") == 1)
    
    filtered_df = filtered_df.select(column)
    processed_dfs.append(filtered_df)

# 7. 合并所有处理后的DataFrame
if processed_dfs:
    df_detail = processed_dfs[0]
    for i in range(1, len(processed_dfs)):
        df_detail = df_detail.union(processed_dfs[i])
else:
    # 如果没有数据，创建一个空DataFrame
    schema = StructType([
        StructField("battery_id", StringType(), True),
        StructField("battery_type", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("device_name", StringType(), True),
        StructField("process_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
        StructField("start_user_soc", DoubleType(), True),
        StructField("start_current", DoubleType(), True),
        StructField("avg_pack_voltage", DoubleType(), True),
        StructField("avg_insu_resis", DoubleType(), True),
        StructField("avg_high_cell_volt", DoubleType(), True),
        StructField("avg_low_cell_volt", DoubleType(), True),
        StructField("delta_cell_pack_volt", DoubleType(), True),
        StructField("source", StringType(), True)
    ])
    df_detail = spark.createDataFrame([], schema)

df_detail = df_detail\
    .withColumn("start_time", functions.unix_timestamp(df_detail["start_time"]).cast(IntegerType()))

print(df_detail.count())
df_detail.show(20, truncate=False)
