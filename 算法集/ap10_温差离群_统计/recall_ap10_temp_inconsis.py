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
# from py_saas_algorithm.alarm import gen_alarm_id, gen_hash_code
from collections import defaultdict
from dateutil.parser import parse
# from common.tool import save_alarm_result
import argparse
"""
按照电池温差离群统计模型产生预警数据
:return: 电池温差离群预警结果存入Hive表
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

# 读取满足温差场景的数据
source_table = "saas_battery.d_i_battery_block_features"
dict_params = algorithm.dict_params
ap10_block_msg_counter_limit = 10
ap10_temp_entropy_n_std = 1
ap10_temp_diff_experince = 20
ap10_max_temp_diff_temp_entropy_threshold = 1.3
ap10_temp_diff_day_threshold = 4
ap10_temp_diff_limit = 8
ap10_min_low_probe_temp_0 = 0
ap10_n_std = 4

# 读取当天数据
sql_read = f"""
    select * from  {source_table}
    where dt = '20260501'
    and max_temp_diff_temp_entropy > 0
    and min_low_probe_temp != {ap10_min_low_probe_temp_0}
    and block_msg_count > {ap10_block_msg_counter_limit}
    """
df_detail = spark.sql(sql_read)

processed_dfs = []

# 按电池类型分组处理
for battery_type, params in dict_params.items():
    # 过滤当前battery_type的数据
    filtered_df = df_detail.filter(df_detail["battery_type"] == battery_type)
    
    # 按battery_id聚合，计算当日平均温差
    df_avg_temp_diff_day = filtered_df.groupBy("battery_id").agg(
        functions.round(functions.avg("max_temp_diff"), 2).alias("avg_temp_diff_day"))
    
    # 按device_type聚合，计算均值和标准差
    df_device_type = filtered_df.groupBy("device_type").agg(
        functions.round(functions.avg("max_temp_diff"), 2).alias("label_mean"),
        functions.round(functions.stddev("max_temp_diff"), 2).alias("label_std"))
    
    # Join回主表
    filtered_df = filtered_df.join(df_avg_temp_diff_day, on="battery_id", how="left")
    filtered_df = filtered_df.join(df_device_type, on="device_type", how="left")
    
    # 过滤：保留温差显著偏离均值的记录
    filtered_df = filtered_df.filter(f"max_temp_diff > label_mean + {ap10_n_std} * label_std")
    
    # 计算温度熵的均值和标准差
    df_temp_entropy = filtered_df.groupBy("device_type").agg(
        functions.round(functions.avg("max_temp_diff_temp_entropy"), 2).alias("temp_entropy_mean"),
        functions.round(functions.stddev("max_temp_diff_temp_entropy"), 2).alias("temp_entropy_std"))
    filtered_df = filtered_df.join(df_temp_entropy, on="device_type", how="left")
    
    # 综合判定（全部满足）
    filtered_df = filtered_df.filter(f"""
        (max_temp_diff_temp_entropy < temp_entropy_mean - {ap10_temp_entropy_n_std} * temp_entropy_std
         or max_temp_diff > {ap10_temp_diff_experince})
        and (max_temp_diff > {ap10_temp_diff_experince}
             or max_temp_diff_temp_entropy < {ap10_max_temp_diff_temp_entropy_threshold})
        and (battery_type != '280'
             or (battery_type = '280' and avg_temp_diff_day > {ap10_temp_diff_day_threshold}))
        and avg_temp_diff_day > {ap10_temp_diff_limit}
    """)
    
    # 去重：按battery_id取温差最大的一条
    w = Window.partitionBy("battery_id").orderBy(functions.desc("max_temp_diff"))
    filtered_df = filtered_df.withColumn("row_number", functions.row_number().over(w)) \
        .filter("row_number = 1")
    
    processed_dfs.append(filtered_df)

# 合并所有处理后的DataFrame
if processed_dfs:
    df_detail = processed_dfs[0]
    for i in range(1, len(processed_dfs)):
        df_detail = df_detail.union(processed_dfs[i])
else:
    df_detail = spark.createDataFrame([], schema=StructType([]))

print(df_detail.count())
df_detail.show(20, truncate=False)
