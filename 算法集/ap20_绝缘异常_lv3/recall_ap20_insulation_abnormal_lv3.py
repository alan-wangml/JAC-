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
实时检测电池绝缘异常，判定绝缘阻值低、继电器状态、滑动窗口条件，产生三级告警
:return: 绝缘异常三级告警结果存入Hive表
"""
# 创建SparkSession, 作为读取数据、处理源数据、配置回话和管理集群资源的入口
spark = SparkSession.builder \
    .appName("AnalysingInsulationAlarmsLv3") \
    .config("spark.debug.maxToStringFields", "1000") \
    .config("spark.network.timeout", "1000") \
    .config("spark.shuffle.memoryFraction", "0.6") \
    .config("spark.default.parallelism", "6000") \
    .config("spark.sql.shuffle.partitions", "8000") \
    .enableHiveSupport() \
    .getOrCreate()  # prod

# 参数定义
dict_params = algorithm.dict_params
ap20_insulation_resistance_threshold = [8000, 40]  # kΩ
ap20_soc_charge_start_array = [1, 2, 4]
ap20_Realtime_continually_TSP_threshold = [60, 60]  # 帧
ap20_Realtime_frame_TSP_threshold = 3  # 帧
ap20_current_threshold = 1  # A

ap20_insulation_threshold_1 = float(ap20_insulation_resistance_threshold[0])
ap20_insulation_threshold_2 = float(ap20_insulation_resistance_threshold[1])
ap20_continue_threshold_1 = int(ap20_Realtime_continually_TSP_threshold[0])
ap20_continue_threshold_2 = int(ap20_Realtime_continually_TSP_threshold[1])
ap20_frame_threshold = int(ap20_Realtime_frame_TSP_threshold)

# 读取当天数据
source_table_detail = "saas_battery.ods_battery_detail_h_i"
input_date = "20260501"

sql_read = f"""
SELECT 
    battery_id,
    battery_model as battery_type,
    '' AS vin,
    IF(device_id IS NULL, '', device_id) AS vehicle_id,
    IF(device_type IS NULL, '', device_type) AS vehicle_type,
    cast(sample_time DIV 1000 as timestamp) sample_time,
    'tsp' as data_type,
    sample_time AS sample_timestamp,
    case 
        when battery_state = 1 then 'periodical_charge_update' 
        when battery_state = 3 then 'periodical_journey_update' 
        else 'periodical_parking_update' end as vehicle_state,
    process_id,
    CAST(charge_state AS INT) AS charge_state,
    CAST(bms_cntctr_sts AS INT) AS bms_cntctr_sts,
    CAST(insulation_resistance AS DOUBLE) AS insulation_resistance,
    CAST(voltage AS DOUBLE) AS voltage,
    CAST(current AS DOUBLE) AS current,
    CAST(user_soc AS DOUBLE) AS user_soc,
    CAST(max_cell_voltage AS DOUBLE) AS max_cell_voltage,
    CAST(min_cell_voltage AS DOUBLE) AS min_cell_voltage,
    CAST(max_probe_temperature AS DOUBLE) AS max_probe_temperature,
    CAST(min_probe_temperature AS DOUBLE) AS min_probe_temperature,
    pack_cell_voltage,
    pack_probe_temperature
FROM {source_table_detail}
WHERE dt = '{input_date}' AND sample_time IS NOT NULL AND battery_id IS NOT NULL
"""
base_filter_str = """
    battery_id is not null and vehicle_id is not null and vin is not null
    and sample_time is not null and vehicle_state is not null
    and insulation_resistance is not null and voltage is not null
    and pack_cell_voltage is not null and pack_probe_temperature is not null
"""
df_detail = spark.sql(sql_read).filter(base_filter_str)

# Step1: 数据初筛 - 过滤绝缘阻值 < 8000 kΩ 的数据
df_detail = df_detail.filter(f"insulation_resistance < {ap20_insulation_threshold_1}")

# Step2: 充电状态与继电器判定
# 排除充电连接状态 (charge_state in [1,2])
df_detail = df_detail.filter(
    ~col("charge_state").isin(ap20_soc_charge_start_array)
)

# 排除高压继电器闭合状态 (bms_cntctr_sts = 1)
# 但若 bms_cntctr_sts 为 null 且 abs(current) <= 1A，则保留
df_detail = df_detail.filter(
    (col("bms_cntctr_sts") != 1) |
    (col("bms_cntctr_sts").isNull() & (functions.abs(col("current")) <= ap20_current_threshold))
)

# Step3: 滑动窗口判定 - 连续4帧，存在3帧及以上非相同值
w1 = Window.partitionBy("battery_id").orderBy("sample_timestamp")

# 计算相邻帧绝缘阻值是否相同
df_detail = df_detail.withColumn("prev_insulation", functions.lag("insulation_resistance", 1).over(w1))
df_detail = df_detail.withColumn("is_diff_value", functions.expr(
    "CASE WHEN prev_insulation IS NULL OR insulation_resistance != prev_insulation THEN 1 ELSE 0 END"
))

# 计算连续4帧滑动窗口内非相同值的数量
df_detail = df_detail.withColumn("diff_count_4", functions.sum("is_diff_value").over(
    Window.partitionBy("battery_id").orderBy("sample_timestamp").rowsBetween(-3, 0)
))

# 计算连续4帧滑动窗口内绝缘阻值 < 8000 的帧数
df_detail = df_detail.withColumn("insulation_low_count_4", functions.sum(
    functions.expr(f"CASE WHEN insulation_resistance < {ap20_insulation_threshold_1} THEN 1 ELSE 0 END")
).over(Window.partitionBy("battery_id").orderBy("sample_timestamp").rowsBetween(-3, 0)))

# 计算连续4帧滑动窗口的总帧数
df_detail = df_detail.withColumn("frame_count_4", functions.count("battery_id").over(
    Window.partitionBy("battery_id").orderBy("sample_timestamp").rowsBetween(-3, 0)
))

# 窗口条件：4帧完整，存在 >= 3 帧非相同值，所有帧绝缘 < 8000
df_detail = df_detail.withColumn("window_pass", functions.expr(f"""
    CASE WHEN frame_count_4 >= {ap20_continue_threshold_1}
        AND diff_count_4 >= {ap20_frame_threshold}
        AND insulation_low_count_4 >= {ap20_continue_threshold_1}
    THEN 1 ELSE 0 END
"""))

# Step4: 严重绝缘异常判定 - 绝缘 < 40 kΩ 且持续4帧以上（三级告警条件）
df_detail = df_detail.withColumn("insulation_severe_count_4", functions.sum(
    functions.expr(f"CASE WHEN insulation_resistance < {ap20_insulation_threshold_2} THEN 1 ELSE 0 END")
).over(Window.partitionBy("battery_id").orderBy("sample_timestamp").rowsBetween(-3, 0)))

df_detail = df_detail.withColumn("is_alarm", functions.expr(f"""
    CASE WHEN window_pass = 1
        AND insulation_severe_count_4 >= {ap20_continue_threshold_2}
    THEN 1 ELSE 0 END
"""))

# 过滤告警数据
df_alarm = df_detail.filter("is_alarm = 1")

# 生成告警明细
df_alarm = df_alarm.select(
    "battery_id", "battery_type", "vin", "vehicle_id", "vehicle_type",
    "sample_time", "data_type", "sample_timestamp", "vehicle_state",
    "process_id", "charge_state", "bms_cntctr_sts",
    "insulation_resistance", "voltage", "current", "user_soc",
    "max_cell_voltage", "min_cell_voltage",
    "max_probe_temperature", "min_probe_temperature",
    "pack_cell_voltage", "pack_probe_temperature",
    "diff_count_4", "insulation_low_count_4", "insulation_severe_count_4"
).withColumn("alarm_level", functions.lit(3)) \
 .withColumn("alarm_type", functions.lit("insulation_abnormal_lv3"))

print(df_alarm.count())
df_alarm.show(20, truncate=False)