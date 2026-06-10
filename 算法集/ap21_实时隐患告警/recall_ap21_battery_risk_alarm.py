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
实时检测电池隐患，判定单体过压、欠压、压差、过温、温差、绝缘异常，加权综合评分产生综合告警
:return: 电池隐患告警结果存入Hive表
"""
# 创建SparkSession, 作为读取数据、处理源数据、配置回话和管理集群资源的入口
spark = SparkSession.builder \
    .appName("AnalysingBatteryRiskAlarms") \
    .config("spark.debug.maxToStringFields", "1000") \
    .config("spark.network.timeout", "1000") \
    .config("spark.shuffle.memoryFraction", "0.6") \
    .config("spark.default.parallelism", "6000") \
    .config("spark.sql.shuffle.partitions", "8000") \
    .enableHiveSupport() \
    .getOrCreate()  # prod

# 参数定义
dict_params = algorithm.dict_params

# 异常因子权重 (volt_over, volt_under, vdiff, temp_over, tdiff, iso)
ap21_factor_weight_array = [1, 1, 1, 1, 1, 1]

# NCM电压异常阈值 (过压上限, 欠压下限) mV
ap21_volt_threshold_ncm_array = [4350, 2500]
# LFP电压异常阈值 (过压上限, 欠压下限) mV
ap21_volt_threshold_lfp_array = [3850, 2000]

# 压差异常阈值 mV
ap21_vdiff_threshold = 50
# LFP压差电压范围 (上限, 下限) mV
ap21_lfp_vmax_threshold_array = [3350, 3000]

# 过温异常阈值 ℃
ap21_temp_over_threshold = 61
# 温差异常阈值 ℃
ap21_tdiff_threshold = 20

# 绝缘异常阈值组 (iso_cur_lowlimit, iso_cur_toplimit, iso_lvl1, iso_lvl2)
ap21_iso_threshold_array = [1000, -20, 2000, 1000]

# 综合因子告警阈值 (volt_over, volt_under, vdiff, temp_over, tdiff, iso, combine)
ap21_factor_fault_array = [1, 1, 1, 1, 1, 2, 2]

# 滑动窗口参数
ap21_window_set = 5  # 窗口长度（帧）
ap21_interval_set = 3  # 中断帧数上限
ap21_window_valid_set = 2  # 有效帧数阈值
ap21_abnormal_restart_set = 3  # 异常重启次数阈值
ap21_abnormal_interval_set = 600  # 单帧中断时长阈值（秒）
ap21_cd_set = 3600  # 预警冷却时间（秒）

# 产品组
ap21_battery_model_ncm_lst = ['GX-1P108S']
ap21_battery_model_lfp_lst = ['21011E3C1','2101ZHM116']

# 初始化值过滤列表
insulation_excludes = [10000, 20000, 65535]
voltage_excludes = [0, 65535]
temperature_excludes = [-40, 214, 215, 254, 255]

# 提取参数
volt_over_weight = ap21_factor_weight_array[0]
volt_under_weight = ap21_factor_weight_array[1]
vdiff_weight = ap21_factor_weight_array[2]
temp_over_weight = ap21_factor_weight_array[3]
tdiff_weight = ap21_factor_weight_array[4]
iso_weight = ap21_factor_weight_array[5]

volt_over_num_threshold = ap21_factor_fault_array[0]
volt_under_num_threshold = ap21_factor_fault_array[1]
vdiff_num_threshold = ap21_factor_fault_array[2]
temp_over_num_threshold = ap21_factor_fault_array[3]
tdiff_num_threshold = ap21_factor_fault_array[4]
iso_num_threshold = ap21_factor_fault_array[5]
combine_num_threshold = ap21_factor_fault_array[6]

iso_cur_lowlimit = ap21_iso_threshold_array[0]
iso_cur_toplimit = ap21_iso_threshold_array[1]
iso_lvl1 = ap21_iso_threshold_array[2]
iso_lvl2 = ap21_iso_threshold_array[3]

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
    IF(device_name IS NULL, '', device_name) AS device_name,
    cast(sample_time DIV 1000 as timestamp) sample_time,
    'tsp' as data_type,
    sample_time AS sample_timestamp,
    case 
        when battery_state = 1 then 'periodical_charge_update' 
        when battery_state = 3 then 'periodical_journey_update' 
        else 'periodical_parking_update' end as vehicle_state,
    process_id,
    CAST(battery_state AS INT) AS battery_state,
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
    and max_cell_voltage is not null and min_cell_voltage is not null
    and max_probe_temperature is not null and min_probe_temperature is not null
"""
df_detail = spark.sql(sql_read).filter(base_filter_str)

# Step1: 数据初筛 - 过滤初始化值
# 过滤绝缘初始化值
df_detail = df_detail.filter(
    ~col("insulation_resistance").isin(insulation_excludes)
)
# 过滤电压初始化值
df_detail = df_detail.filter(
    ~col("max_cell_voltage").isin(voltage_excludes) &
    ~col("min_cell_voltage").isin(voltage_excludes)
)
# 过滤温度初始化值
df_detail = df_detail.filter(
    ~col("max_probe_temperature").isin(temperature_excludes) &
    ~col("min_probe_temperature").isin(temperature_excludes)
)

# Step2: 计算压差和温差
df_detail = df_detail.withColumn("vdiff", col("max_cell_voltage") - col("min_cell_voltage"))
df_detail = df_detail.withColumn("tdiff", col("max_probe_temperature") - col("min_probe_temperature"))

# Step3: 异常现象识别（逐帧判定）
# 判断电池类型（NCM 或 LFP）
df_detail = df_detail.withColumn("is_ncm", col("battery_type").isin(ap21_battery_model_ncm_lst) if ap21_battery_model_ncm_lst else functions.lit(False))
df_detail = df_detail.withColumn("is_lfp", col("battery_type").isin(ap21_battery_model_lfp_lst) if ap21_battery_model_lfp_lst else functions.lit(False))

# 单体过压异常
df_detail = df_detail.withColumn("volt_over_abnormal", functions.expr(f"""
    CASE WHEN is_ncm = true AND max_cell_voltage > {ap21_volt_threshold_ncm_array[0]} THEN 1
         WHEN is_lfp = true AND max_cell_voltage > {ap21_volt_threshold_lfp_array[0]} THEN 1
         ELSE 0 END
"""))

# 单体欠压异常
df_detail = df_detail.withColumn("volt_under_abnormal", functions.expr(f"""
    CASE WHEN is_ncm = true AND min_cell_voltage < {ap21_volt_threshold_ncm_array[1]} THEN 1
         WHEN is_lfp = true AND min_cell_voltage < {ap21_volt_threshold_lfp_array[1]} THEN 1
         ELSE 0 END
"""))

# 压差异常
df_detail = df_detail.withColumn("vdiff_abnormal", functions.expr(f"""
    CASE WHEN is_ncm = true AND vdiff > {ap21_vdiff_threshold} THEN 1
         WHEN is_lfp = true AND vdiff > {ap21_vdiff_threshold}
              AND max_cell_voltage >= {ap21_lfp_vmax_threshold_array[1]}
              AND max_cell_voltage <= {ap21_lfp_vmax_threshold_array[0]} THEN 1
         ELSE 0 END
"""))

# 过温异常
df_detail = df_detail.withColumn("temp_over_abnormal", functions.expr(f"""
    CASE WHEN max_probe_temperature > {ap21_temp_over_threshold} THEN 1 ELSE 0 END
"""))

# 温差异常
df_detail = df_detail.withColumn("tdiff_abnormal", functions.expr(f"""
    CASE WHEN tdiff > {ap21_tdiff_threshold} THEN 1 ELSE 0 END
"""))

# 绝缘异常（电流在范围内且绝缘值低于阈值）
df_detail = df_detail.withColumn("iso_abnormal", functions.expr(f"""
    CASE WHEN current >= {iso_cur_toplimit} AND current <= {iso_cur_lowlimit}
              AND insulation_resistance IS NOT NULL
         THEN CASE WHEN insulation_resistance < {iso_lvl2} THEN 2
                   WHEN insulation_resistance < {iso_lvl1} THEN 1
                   ELSE 0 END
         ELSE 0 END
"""))

# Step4: 滑动窗口统计
w1 = Window.partitionBy("battery_id").orderBy("sample_timestamp") \
    .rowsBetween(-(ap21_window_set - 1), 0)

# 窗口内各类异常帧数统计
df_detail = df_detail.withColumn("sin_overvolt", functions.sum("volt_over_abnormal").over(w1))
df_detail = df_detail.withColumn("sin_undervolt", functions.sum("volt_under_abnormal").over(w1))
df_detail = df_detail.withColumn("sin_vdiff", functions.sum("vdiff_abnormal").over(w1))
df_detail = df_detail.withColumn("sin_overtemp", functions.sum("temp_over_abnormal").over(w1))
df_detail = df_detail.withColumn("sin_tdiff", functions.sum("tdiff_abnormal").over(w1))
df_detail = df_detail.withColumn("sin_iso", functions.sum("iso_abnormal").over(w1))

# 窗口内有效帧数
df_detail = df_detail.withColumn("window_frame_count", functions.count("battery_id").over(w1))

# 加权综合得分
df_detail = df_detail.withColumn("combine_val", functions.expr(f"""
    sin_overvolt * {volt_over_weight} + sin_undervolt * {volt_under_weight} +
    sin_vdiff * {vdiff_weight} + sin_overtemp * {temp_over_weight} +
    sin_tdiff * {tdiff_weight} + sin_iso * {iso_weight}
"""))

# Step5: 告警判定
# 窗口有效帧数 >= 阈值才计算
df_detail = df_detail.withColumn("window_valid", functions.expr(f"""
    CASE WHEN window_frame_count >= {ap21_window_valid_set} THEN 1 ELSE 0 END
"""))

# 单项告警判定
df_detail = df_detail.withColumn("alarm_volt_over", functions.expr(f"""
    CASE WHEN window_valid = 1 AND sin_overvolt > {volt_over_num_threshold} THEN 1 ELSE 0 END
"""))
df_detail = df_detail.withColumn("alarm_volt_under", functions.expr(f"""
    CASE WHEN window_valid = 1 AND sin_undervolt > {volt_under_num_threshold} THEN 1 ELSE 0 END
"""))
df_detail = df_detail.withColumn("alarm_vdiff", functions.expr(f"""
    CASE WHEN window_valid = 1 AND sin_vdiff > {vdiff_num_threshold} THEN 1 ELSE 0 END
"""))
df_detail = df_detail.withColumn("alarm_temp_over", functions.expr(f"""
    CASE WHEN window_valid = 1 AND sin_overtemp > {temp_over_num_threshold} THEN 1 ELSE 0 END
"""))
df_detail = df_detail.withColumn("alarm_tdiff", functions.expr(f"""
    CASE WHEN window_valid = 1 AND sin_tdiff > {tdiff_num_threshold} THEN 1 ELSE 0 END
"""))
df_detail = df_detail.withColumn("alarm_iso", functions.expr(f"""
    CASE WHEN window_valid = 1 AND sin_iso > {iso_num_threshold} THEN 1 ELSE 0 END
"""))
df_detail = df_detail.withColumn("alarm_combine", functions.expr(f"""
    CASE WHEN window_valid = 1 AND combine_val > {combine_num_threshold} THEN 1 ELSE 0 END
"""))

# 任意告警触发
df_detail = df_detail.withColumn("is_alarm", functions.expr("""
    CASE WHEN alarm_volt_over = 1 OR alarm_volt_under = 1 OR alarm_vdiff = 1
         OR alarm_temp_over = 1 OR alarm_tdiff = 1 OR alarm_iso = 1
         OR alarm_combine = 1 THEN 1 ELSE 0 END
"""))

# 告警类型
df_detail = df_detail.withColumn("alarm_type", functions.expr(f"""
    CASE WHEN alarm_volt_over = 1 THEN '单体过压异常'
         WHEN alarm_volt_under = 1 THEN '单体欠压异常'
         WHEN alarm_vdiff = 1 THEN '压差异常'
         WHEN alarm_temp_over = 1 THEN '过温异常'
         WHEN alarm_tdiff = 1 THEN '温差异常'
         WHEN alarm_iso = 1 THEN '绝缘异常'
         WHEN alarm_combine = 1 THEN '电池隐患综合告警'
         ELSE '' END
"""))

# 过滤告警数据
df_alarm = df_detail.filter("is_alarm = 1")

# 生成告警明细
df_alarm = df_alarm.select(
    "battery_id", "battery_type", "vin", "vehicle_id", "vehicle_type",
    "device_name", "sample_time", "data_type", "sample_timestamp",
    "vehicle_state", "process_id", "battery_state",
    "insulation_resistance", "voltage", "current", "user_soc",
    "max_cell_voltage", "min_cell_voltage",
    "max_probe_temperature", "min_probe_temperature",
    "vdiff", "tdiff",
    "pack_cell_voltage", "pack_probe_temperature",
    "sin_overvolt", "sin_undervolt", "sin_vdiff",
    "sin_overtemp", "sin_tdiff", "sin_iso",
    "combine_val", "alarm_type"
).withColumn("alarm_level", functions.lit(3))

print(df_alarm.count())
df_alarm.show(20, truncate=False)