%pyspark
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
按照车辆电池单体温差离群算法产生预警数据
:return: 电池单体温度离群预警结果存入Hive表
"""
# 创建SparkSession, 作为读取数据、处理源数据、配置回话和管理集群资源的入口
spark = SparkSession.builder \
    .appName("AnalysingBatteryAlarms") \
    .config("spark.debug.maxToStringFields", "1000") \
    .config("spark.network.timeout", "1000") \
    .config("spark.shuffle.memoryFraction", "0.6") \
    .config("spark.default.parallelism", "6000") \
    .config("spark.sql.shuffle.partitions", "8000") \
    .enableHiveSupport() \
    .getOrCreate()  # prod

# 读取满足单体温差离群场景的数据
source_table = "saas_battery.d_i_battery_block_features"
dict_params = algorithm.dict_params
battery_types = dict_params.keys()
battery_types_str = ', '.join(["'" + i + "'" for i in battery_types])

# 1. 一次性读取所有数据，只做基本过滤
sql_read = f"""
SELECT *, 'tsp' as source FROM {source_table}
WHERE dt = '{input_date}'
AND block_duration >= 30
and battery_type in ({battery_types_str})
"""
df_all = spark.sql(sql_read)

# 2. 按照battery_type分组处理
processed_dfs = []

column = [
    "battery_id", "battery_type", "process_id", "device_id", "device_name", "cell_type", "start_time", "end_time",
    "avg_temp_diff", "max_temp_diff", "max_high_probe_temp", "min_high_probe_temp", "min_low_probe_temp",
    "max_temp_diff_temp_entropy", "event_type", "source", "brand"
]

for battery_type in battery_types:
    # 获取对应battery_type的参数
    params = dict_params.get(battery_type)
    if not params:
        continue

    # 3. 过滤当前battery_type的数据
    # 3.1 过滤battery_type
    filtered_df = df_all.filter(df_all["battery_type"] == battery_type)

    # 3.2 应用当前battery_type的参数进行过滤
    ap9_temp_diff_entropy_threshold = params["ap9_temp_diff_entropy_threshold"]
    ap9_low_probe_temp_range = list(params["ap9_low_probe_temp_range"])
    ap9_high_probe_temp_range = list(params["ap9_high_probe_temp_range"])
    ap9_low_probe_temp_threshold = int(params["ap9_low_probe_temp_threshold"])
    ap9_high_probe_temp_threshold = int(params["ap9_high_probe_temp_threshold"])
    ap9_temp_diff_abparking_threshold = int(params["ap9_temp_diff_abparking_threshold"])
    ap9_temp_diff_parking_threshold = int(params["ap9_temp_diff_parking_threshold"])
    ap9_temp_diff_max_threshold = int(params["ap9_temp_diff_max_threshold"])
    ap9_temp_diff_avg_threshold = int(params["ap9_temp_diff_avg_threshold"])
    ap9_temp_diff_chgorprk_threshold = int(params["ap9_temp_diff_chgorprk_threshold"])
    ap9_operating_condition = params.get("ap9_operating_condition")
    ap9_cell_type = params.get("ap9_cell_type")

    if ap9_operating_condition == "null" or ap9_operating_condition == "" or ap9_operating_condition is None:
        scene_query_str = ""
    else:
        scene_type = list(ap9_operating_condition.split(","))
        scene_query_list = list()
        for scene in scene_type:
            scene_query_list.append(f"""event_type like '%{scene.strip()}%'""")
        scene_query_str = "AND (" + " OR ".join(scene_query_list) + ")"

    if ap9_cell_type == "null" or ap9_cell_type == "" or ap9_cell_type is None:
        cell_type_query_str = ""
    else:
        cell_type_type = list(ap9_cell_type.split(","))
        cell_type_query_list = list()
        for cell_type in cell_type_type:
            cell_type_query_list.append(f"""cell_type = '{cell_type.strip()}'""")
        cell_type_query_str = "AND (" + " OR ".join(cell_type_query_list) + ")"

    # 3.3 应用过滤条件
    filter_condition = f"""
    max_temp_diff_temp_entropy >= {ap9_temp_diff_entropy_threshold}
    {scene_query_str}
    {cell_type_query_str}
    AND (
        (avg_temp_diff >= (case when event_type='parking' then {ap9_temp_diff_parking_threshold} else {ap9_temp_diff_abparking_threshold} end)
        AND min_low_probe_temp < {ap9_low_probe_temp_range[1]}
        AND min_low_probe_temp >= {ap9_low_probe_temp_range[0]}
        AND min_high_probe_temp >= {ap9_high_probe_temp_threshold}
        ) OR (
            max_temp_diff >= {ap9_temp_diff_max_threshold}
            AND avg_temp_diff >= {ap9_temp_diff_avg_threshold}
        ) OR (
            event_type IN ( 'charge', 'parking' )
            AND max_temp_diff >= {ap9_temp_diff_chgorprk_threshold}
            AND min_low_probe_temp >= {ap9_low_probe_temp_threshold}
            AND max_high_probe_temp < {ap9_high_probe_temp_range[1]}
            AND max_high_probe_temp >= {ap9_high_probe_temp_range[0]}
        )
    )
    """
    filtered_df = filtered_df.filter(filter_condition)

    if filtered_df.count() == 0:
        continue

    # 3.4 筛选每一块电池最早触发时刻
    w = Window.partitionBy("battery_id").orderBy(F.asc("start_time"))
    filtered_df = filtered_df.withColumn("asc_time_num", F.row_number().over(w)).where("asc_time_num = 1")
    filtered_df = filtered_df.select(column)

    processed_dfs.append(filtered_df)

# 4. 合并所有处理后的DataFrame
if processed_dfs:
    df_detail = processed_dfs[0]
    for i in range(1, len(processed_dfs)):
        df_detail = df_detail.union(processed_dfs[i])
else:
    # 如果没有数据，创建一个空DataFrame
    schema = StructType([
        StructField("battery_id", StringType(), True),
        StructField("battery_type", StringType(), True),
        StructField("process_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("device_name", StringType(), True),
        StructField("cell_type", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
        StructField("avg_temp_diff", DoubleType(), True),
        StructField("max_temp_diff", DoubleType(), True),
        StructField("max_high_probe_temp", DoubleType(), True),
        StructField("min_high_probe_temp", DoubleType(), True),
        StructField("min_low_probe_temp", DoubleType(), True),
        StructField("max_temp_diff_temp_entropy", DoubleType(), True),
        StructField("event_type", StringType(), True),
        StructField("source", StringType(), True),
        StructField("brand", StringType(), True)
    ])
    df_detail = spark.createDataFrame([], schema)

df_detail = df_detail \
    .withColumn("start_time", F.unix_timestamp(df_detail["start_time"]).cast(IntegerType()))

print(df_detail.count())
df_detail.show(20, truncate=False)
