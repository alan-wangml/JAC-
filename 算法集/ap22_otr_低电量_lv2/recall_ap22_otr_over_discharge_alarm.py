%pyspark
import sys
import math
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import col
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
静置过放告警检测，筛选全天parking状态的电池，按电芯类型判定低电量阈值，产生二级告警
:return: 低电量告警结果存入Hive表
"""
# 创建SparkSession, 作为读取数据、处理源数据、配置回话和管理集群资源的入口
spark = SparkSession.builder \
    .appName("AnalysingOverDischargeAlarms") \
    .config("spark.debug.maxToStringFields", "1000") \
    .config("spark.network.timeout", "1000") \
    .config("spark.shuffle.memoryFraction", "0.6") \
    .config("spark.default.parallelism", "6000") \
    .config("spark.sql.shuffle.partitions", "8000") \
    .enableHiveSupport() \
    .getOrCreate()  # prod

# 参数定义
dict_params = algorithm.dict_params
ap22_lfp_overdch_voltage_threshold = 2940
ap22_ncm_overdch_voltage_threshold = 3000

# 读取当天数据
source_table = "saas_battery.d_i_battery_block_features"
input_date = "20260501"

sql_read = f"""
WITH parking_only AS (
    SELECT battery_id, SUM(not_parking) AS not_parking_sum
    FROM (
        SELECT battery_id, IF(event_type = 'parking', 0, 1) AS not_parking
        FROM {source_table}
        WHERE dt = '{input_date}'
        GROUP BY battery_id, IF(event_type = 'parking', 0, 1)
    )
    GROUP BY battery_id
    HAVING not_parking_sum = 0
)
SELECT a.*
FROM (
    SELECT a.*,
        ROW_NUMBER() OVER (
            PARTITION BY a.battery_id, a.cell_type
            ORDER BY min_low_cell_volt DESC
        ) AS min_low_cell_volt_rk
    FROM {source_table} a
    JOIN parking_only b ON a.battery_id = b.battery_id
    WHERE a.dt = '{input_date}'
        AND a.min_low_cell_volt > 0
) a
WHERE min_low_cell_volt_rk = 1
"""
base_filter_str = """
    battery_id is not null and device_id is not null
    and start_time is not null and event_type is not null
    and min_low_cell_volt is not null and cell_type is not null
"""
df_detail = spark.sql(sql_read).filter(base_filter_str)

# 按电芯类型过滤电压阈值
# LFP电芯：min_low_cell_volt <= ap22_lfp_overdch_voltage_threshold
# NCM电芯：min_low_cell_volt <= ap22_ncm_overdch_voltage_threshold
df_detail = df_detail.filter(
    ((col("cell_type") == "LFP") & (col("min_low_cell_volt") <= ap22_lfp_overdch_voltage_threshold)) |
    ((col("cell_type") == "NCM") & (col("min_low_cell_volt") <= ap22_ncm_overdch_voltage_threshold))
)

# 生成告警明细
df_alarm = df_detail.select(
    col("battery_id"),
    col("battery_type"),
    col("device_id"),
    col("device_name"),
    col("process_id"),
    col("event_type").alias("vehicle_state"),
    col("start_time").cast("timestamp").alias("sample_time"),
    col("start_user_soc").alias("user_soc"),
    col("start_current").alias("current"),
    col("avg_pack_voltage"),
    col("avg_insu_resis").alias("insulation_resistance"),
    col("avg_high_cell_volt").alias("max_cell_voltage"),
    col("avg_low_cell_volt").alias("min_cell_voltage"),
    col("min_low_cell_volt"),
    col("cell_type")
).withColumn("alarm_level", functions.lit(2)) \
 .withColumn("diagnosis_code", functions.lit("P0502")) \
 .withColumn("alarm_type", functions.lit("over_discharge_lv2"))

print(df_alarm.count())
df_alarm.show(20, truncate=False)