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
按照电池采样异常专家模型产生预警数据
:return: 电池采样异常预警结果存入Hive表
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

# 读取满足采样异常场景的数据
source_table = "saas_battery.d_i_battery_block_features"
dict_params = algorithm.dict_params
# 默认参数（按 battery_type 配置）
default_params = {
    "ap12_total_msg_count_threshold": "120",
    "ap12_volt_init_rate_threshold": "0.7",
    "ap12_temp_init_rate_threshold": "0.7",
    "ap12_resis_init_rate_threshold": "0.9",
    "ap12_init_msg_count_threshold": "20"
}
battery_types = dict_params.keys()
battery_types_str = ', '.join(["'" + i + "'" for i in battery_types])

# 1. 读取明细数据，计算每条的初始化异常帧数
sql_read = f"""select *, 'tsp' as source,
    COALESCE(cell_volt_init_count['5094.0'], 0) +
    COALESCE(cell_volt_init_count['4096.0'], 0) +
    COALESCE(cell_volt_init_count['65535.0'], 0) AS volt_init_count,

    COALESCE(probe_temp_init_count['-50.0'], 0) +
    COALESCE(probe_temp_init_count['-40.0'], 0) +
    COALESCE(probe_temp_init_count['255.0'], 0) AS temp_init_count,

    COALESCE(insu_resis_init_count['5000.0'], 0) +
    COALESCE(insu_resis_init_count['10000.0'], 0) +
    COALESCE(insu_resis_init_count['60000.0'], 0) AS resis_init_count
    from {source_table}
    where dt = '20260501'
    and battery_type in ({battery_types_str})
    """
df_detail = spark.sql(sql_read)

# 2. 按battery_id汇总全天初始化异常帧数
sql_init_all = f"""
SELECT  battery_id,
        Sum(block_msg_count)  AS total_msg_count,
        Sum(volt_init_count)  AS total_volt_init_count,
        Sum(temp_init_count)  AS total_temp_init_count,
        Sum(resis_init_count) AS total_resis_init_count
  FROM (
        SELECT battery_id,
            block_msg_count,
            COALESCE(cell_volt_init_count['5094.0'], 0) +
            COALESCE(cell_volt_init_count['4096.0'], 0) +
            COALESCE(cell_volt_init_count['65535.0'], 0) AS volt_init_count,

            COALESCE(probe_temp_init_count['-50.0'], 0) +
            COALESCE(probe_temp_init_count['-40.0'], 0) +
            COALESCE(probe_temp_init_count['255.0'], 0) AS temp_init_count,

            COALESCE(insu_resis_init_count['5000.0'], 0) +
            COALESCE(insu_resis_init_count['10000.0'], 0) +
            COALESCE(insu_resis_init_count['60000.0'], 0) AS resis_init_count

        FROM {source_table}
        WHERE dt = '20260501'
        and battery_type in ({battery_types_str})
        )
GROUP BY battery_id
"""
df_init_all = spark.sql(sql_init_all)

# 3. 按照battery_type分组处理
processed_dfs = []

column = [
    "battery_id", "battery_type", "device_id", "device_name", "process_id", "event_type",
    "start_time", "end_time", "start_real_soc", "start_current", "avg_pack_voltage",
    "avg_insu_resis", "avg_high_cell_volt", "avg_low_cell_volt", "source",
    "total_msg_count", "total_volt_init_count", "total_temp_init_count", "total_resis_init_count"
]

for battery_type in battery_types:
    # 获取对应battery_type的参数，无配置时使用默认值
    params = dict_params.get(battery_type, default_params)

    # 提取参数
    ap12_total_msg_count_threshold = int(params["ap12_total_msg_count_threshold"])
    ap12_volt_init_rate_threshold = float(params["ap12_volt_init_rate_threshold"])
    ap12_temp_init_rate_threshold = float(params["ap12_temp_init_rate_threshold"])
    ap12_resis_init_rate_threshold = float(params["ap12_resis_init_rate_threshold"])
    ap12_init_msg_count_threshold = int(params["ap12_init_msg_count_threshold"])

    # 4. 过滤当前battery_type的数据
    filtered_df = df_detail.filter(df_detail["battery_type"] == battery_type)

    # 过滤：单条初始化异常帧数大于阈值
    filtered_df = filtered_df.filter(
        f"(volt_init_count > {ap12_init_msg_count_threshold} or temp_init_count > {ap12_init_msg_count_threshold} or resis_init_count > {ap12_init_msg_count_threshold})"
    )

    if filtered_df.count() == 0:
        continue

    # 过滤汇总数据：总消息帧数 > 阈值 且 初始化频率 > 阈值
    df_init = df_init_all.filter(
        f"total_msg_count > {ap12_total_msg_count_threshold} AND "
        f"(total_volt_init_count / total_msg_count > {ap12_volt_init_rate_threshold} OR "
        f"total_temp_init_count / total_msg_count > {ap12_temp_init_rate_threshold} OR "
        f"total_resis_init_count / total_msg_count > {ap12_resis_init_rate_threshold})"
    )

    # 去重：按battery_id取start_time最早的一条
    w = Window.partitionBy("battery_id").orderBy("start_time")
    filtered_df = filtered_df.withColumn("asc_time_num", functions.row_number().over(w)).where("asc_time_num = 1")

    # 合并汇总数据
    filtered_df_final = filtered_df.join(df_init, on=["battery_id"], how="inner")

    if filtered_df_final.count() == 0:
        continue

    filtered_df_final = filtered_df_final.select(column)

    processed_dfs.append(filtered_df_final)

# 5. 合并所有处理后的DataFrame
if processed_dfs:
    df_detail = processed_dfs[0]
    for i in range(1, len(processed_dfs)):
        df_detail = df_detail.union(processed_dfs[i])
else:
    df_detail = spark.createDataFrame([], schema=StructType([]))

print(df_detail.count())
df_detail.show(20, truncate=False)
