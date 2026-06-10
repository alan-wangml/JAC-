%pyspark
import sys
import math
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import col, udf
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
实时检测电池热失控，基于电压/温度采样异常检测与5分支组合判定，产生一级告警
:return: 热失控告警结果存入Hive表
"""
# 创建SparkSession, 作为读取数据、处理源数据、配置回话和管理集群资源的入口
spark = SparkSession.builder \
    .appName("AnalysingThermalRunaway") \
    .config("spark.debug.maxToStringFields", "1000") \
    .config("spark.network.timeout", "1000") \
    .config("spark.shuffle.memoryFraction", "0.6") \
    .config("spark.default.parallelism", "6000") \
    .config("spark.sql.shuffle.partitions", "8000") \
    .enableHiveSupport() \
    .getOrCreate()  # prod

# 参数定义
dict_params = algorithm.dict_params
ap16_sample_volt_bin = 10
ap16_sample_volt_rate_threshold = 0.7
ap16_sample_volt_rate_threshold2 = 0.5
ap16_sample_volt_rate_threshold3 = 0.9
ap16_sample_volt_threshold = 30  # mV
ap16_sample_temp_bin = 5
ap16_sample_temp_rate_threshold = 0.5
ap16_sample_temp_threshold = 7  # ℃
ap16_consis_threshold = 0.5

ap16_max_probe_temperature_threshold = 60  # ℃
ap16_delta_temperature_threshold1 = 45  # ℃
ap16_delta_temperature_threshold2 = 30  # ℃
ap16_min_probe_temperature_threshold = 10  # ℃
ap16_max_cell_voltage_threshold = 4500  # mV
ap16_min_cell_voltage_threshold = 1300  # mV
ap16_min_cell_voltage_threshold2 = 3800  # mV
ap16_delta_voltage_threshold = 120  # mV
ap16_insulation_resistance_threshold1 = 80  # kΩ

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

# Step1: 数据初筛 - 去除总压=6553.5V且总电流=5553.5A的异常数据帧
df_detail = df_detail.filter(
    ~((col("voltage") == 6553.5) & (col("current") == 5553.5))
)

# ── UDF：归一化与分箱工具函数 ──────────────────────────────────────────

def normalize_list(data):
    """归一化数据到 [0, 1] 区间"""
    if not data or len(data) == 0:
        return []
    min_val, max_val = min(data), max(data)
    if max_val == min_val:
        return [0.0] * len(data)
    return [(x - min_val) / (max_val - min_val) for x in data]

def bin_data(normalized_data, num_bins):
    """将归一化数据等间距分箱（左闭右开，末箱含1.0）"""
    if not normalized_data or len(normalized_data) == 0:
        return [[] for _ in range(num_bins)]
    bins = [[] for _ in range(num_bins)]
    bin_edges = np.linspace(0, 1, num_bins + 1)
    for value in normalized_data:
        value = max(0.0, min(1.0, value))
        bin_idx = np.digitize(value, bin_edges, right=False) - 1
        if bin_idx >= num_bins:
            bin_idx = num_bins - 1
        bins[bin_idx].append(value)
    return bins

def calc_bin_percentage(bins, bin_indices):
    """计算指定分箱的元素个数占总量的比例"""
    total = sum(len(b) for b in bins)
    if total == 0:
        return 0.0
    selected = sum(len(bins[i]) for i in bin_indices if 0 <= i < len(bins))
    return selected / total

# ── UDF：Step2 采样异常检测 ────────────────────────────────────────────

def detect_sample_abnormal(voltage_list, temperature_list):
    """
    Step2：电压/温度数据过滤及采样质量检测。
    返回: (voltage_init_abnormal, temperature_init_abnormal,
           voltage_sample_abnormal, temperature_sample_abnormal,
           filtered_voltage, filtered_temperature)
    """
    if voltage_list is None:
        voltage_list = []
    if temperature_list is None:
        temperature_list = []

    voltage_init_abnormal = False
    temperature_init_abnormal = False
    voltage_sample_abnormal = False
    temperature_sample_abnormal = False

    # Step2.1 有效值过滤
    # 电压：(1000, 5094] mV
    filtered_voltage = [v for v in voltage_list if v is not None and 1000 < v <= 5094]
    # 温度：(-40,0)∪(0,150] ℃，且为 0.5 的整数倍
    filtered_temperature = [
        t for t in temperature_list
        if t is not None and ((-40 < t < 0) or (0 < t <= 150)) and (t * 2) % 1 == 0
    ]

    # 初始化异常检测
    if len(filtered_voltage) == 0 or len(set(filtered_voltage)) == 1:
        voltage_init_abnormal = True
    if len(filtered_temperature) == 0 or len(set(filtered_temperature)) == 1:
        temperature_init_abnormal = True

    # 电压采样异常检测（仅在非初始化异常时执行）
    if not voltage_init_abnormal and len(filtered_voltage) > 1:
        normalized = normalize_list(filtered_voltage)
        bins = bin_data(normalized, ap16_sample_volt_bin)
        voltage_diff = max(filtered_voltage) - min(filtered_voltage)
        for i, bin_data in enumerate(bins):
            pct = len(bin_data) / len(filtered_voltage)
            # 条件1：中间集聚异常
            if (pct > ap16_sample_volt_rate_threshold
                    and len(bins[0]) != 1
                    and calc_bin_percentage(bins, [0, 1, -1]) < ap16_sample_volt_rate_threshold2
                    and voltage_diff > ap16_sample_volt_threshold):
                voltage_sample_abnormal = True
                break
            # 条件2：单体高离群（bin0 > 90%）
            if i == 0 and pct > ap16_sample_volt_rate_threshold3:
                voltage_sample_abnormal = True
                break

    # 温度采样异常检测（仅在非初始化异常时执行）
    if not temperature_init_abnormal and len(filtered_temperature) > 1:
        normalized = normalize_list(filtered_temperature)
        bins = bin_data(normalized, ap16_sample_temp_bin)
        temp_diff = max(filtered_temperature) - min(filtered_temperature)
        for bin_data in bins:
            pct = len(bin_data) / len(filtered_temperature)
            if (pct > ap16_sample_temp_rate_threshold
                    and len(bins[0]) <= 2
                    and temp_diff > ap16_sample_temp_threshold):
                temperature_sample_abnormal = True
                break

    # 二次过滤（仅在电压无异常时执行）
    if not voltage_init_abnormal and not voltage_sample_abnormal and len(filtered_voltage) > 1:
        normalized = normalize_list(filtered_voltage)
        bins = bin_data(normalized, ap16_sample_volt_bin)
        first_bin_count = len(bins[0])
        if (first_bin_count != 1
                and calc_bin_percentage(bins, [0, 1, -1]) >= ap16_sample_volt_rate_threshold2):
            filtered_voltage = [v for v in filtered_voltage if v >= ap16_min_cell_voltage_threshold2]

    return (
        voltage_init_abnormal,
        temperature_init_abnormal,
        voltage_sample_abnormal,
        temperature_sample_abnormal,
        filtered_voltage,
        filtered_temperature,
    )

sample_abnormal_schema = StructType([
    StructField("voltage_init_abnormal", BooleanType(), True),
    StructField("temperature_init_abnormal", BooleanType(), True),
    StructField("voltage_sample_abnormal", BooleanType(), True),
    StructField("temperature_sample_abnormal", BooleanType(), True),
    StructField("filtered_voltage", ArrayType(DoubleType()), True),
    StructField("filtered_temperature", ArrayType(DoubleType()), True),
])

detect_sample_abnormal_udf = udf(detect_sample_abnormal, sample_abnormal_schema)

# 应用采样异常检测UDF
df_detail = df_detail.withColumn(
    "sample_result",
    detect_sample_abnormal_udf(col("pack_cell_voltage"), col("pack_probe_temperature"))
)

# 展开采样检测结果
df_detail = df_detail.select(
    "*",
    col("sample_result.voltage_init_abnormal").alias("voltage_init_abnormal"),
    col("sample_result.temperature_init_abnormal").alias("temperature_init_abnormal"),
    col("sample_result.voltage_sample_abnormal").alias("voltage_sample_abnormal"),
    col("sample_result.temperature_sample_abnormal").alias("temperature_sample_abnormal"),
    col("sample_result.filtered_voltage").alias("filtered_voltage"),
    col("sample_result.filtered_temperature").alias("filtered_temperature"),
).drop("sample_result")

# Step3: 分支条件评估
# 电压/温度异常标记（影响分支可用性）
df_detail = df_detail.withColumn("voltage_abnormal", functions.expr(
    "voltage_init_abnormal OR voltage_sample_abnormal"
))
df_detail = df_detail.withColumn("temperature_abnormal", functions.expr(
    "temperature_init_abnormal OR temperature_sample_abnormal"
))

# 分支1：最大温度过高（温度异常时禁用）
df_detail = df_detail.withColumn("branch1", functions.expr(f"""
    CASE WHEN NOT temperature_abnormal 
        AND SIZE(filtered_temperature) > 0
        AND array_max(filtered_temperature) >= {ap16_max_probe_temperature_threshold}
    THEN TRUE ELSE FALSE END
"""))

# 分支2：温差异常（温度异常时禁用）
df_detail = df_detail.withColumn("branch2", functions.expr(f"""
    CASE WHEN NOT temperature_abnormal 
        AND SIZE(filtered_temperature) > 1
        AND (
            array_max(filtered_temperature) - array_min(filtered_temperature) >= {ap16_delta_temperature_threshold1}
            OR (
                array_max(filtered_temperature) - array_min(filtered_temperature) >= {ap16_delta_temperature_threshold2}
                AND array_min(filtered_temperature) >= {ap16_min_probe_temperature_threshold}
            )
        )
    THEN TRUE ELSE FALSE END
"""))

# 分支3：电压跌落（电压异常时禁用）
df_detail = df_detail.withColumn("branch3", functions.expr(f"""
    CASE WHEN NOT voltage_abnormal 
        AND SIZE(filtered_voltage) > 0
        AND (
            array_max(filtered_voltage) >= {ap16_max_cell_voltage_threshold}
            OR array_min(filtered_voltage) <= {ap16_min_cell_voltage_threshold}
        )
    THEN TRUE ELSE FALSE END
"""))

# 分支4：电压压差（电压异常时禁用）
df_detail = df_detail.withColumn("branch4", functions.expr(f"""
    CASE WHEN NOT voltage_abnormal 
        AND SIZE(filtered_voltage) > 1
        AND array_max(filtered_voltage) - array_min(filtered_voltage) >= {ap16_delta_voltage_threshold}
        AND array_max(filtered_voltage) >= {ap16_min_cell_voltage_threshold2}
    THEN TRUE ELSE FALSE END
"""))

# 分支5：绝缘异常（始终评估）
df_detail = df_detail.withColumn("branch5", functions.expr(f"""
    CASE WHEN insulation_resistance <= {ap16_insulation_resistance_threshold1}
    THEN TRUE ELSE FALSE END
"""))

# Step4: 组合判定与告警
# 组合1（温度+电压）：(B1 OR B2) AND (B3 OR B4)
df_detail = df_detail.withColumn("combination1", functions.expr("""
    (branch1 OR branch2) AND (branch3 OR branch4)
"""))

# 组合2（主分支+绝缘）：(B1 OR B2 OR B3 OR B4) AND B5
df_detail = df_detail.withColumn("combination2", functions.expr("""
    (branch1 OR branch2 OR branch3 OR branch4) AND branch5
"""))

# 组合3（任一+采样异常）：(B1~B5任一) AND (电压采样异常 OR 温度采样异常) AND 绝缘值 ≠ 0
df_detail = df_detail.withColumn("combination3", functions.expr("""
    (branch1 OR branch2 OR branch3 OR branch4 OR branch5)
    AND (voltage_sample_abnormal OR temperature_sample_abnormal)
    AND insulation_resistance != 0
"""))

# 告警判定：任一组合满足即触发
df_detail = df_detail.withColumn("is_alarm", functions.expr(
    "combination1 OR combination2 OR combination3"
))

# 过滤告警数据
df_alarm = df_detail.filter("is_alarm = TRUE")

# 生成告警明细
df_alarm = df_alarm.select(
    "battery_id", "battery_type", "vin", "vehicle_id", "vehicle_type",
    "sample_time", "data_type", "sample_timestamp", "vehicle_state",
    "process_id",
    "insulation_resistance", "voltage", "current", "user_soc",
    "max_cell_voltage", "min_cell_voltage",
    "max_probe_temperature", "min_probe_temperature",
    "pack_cell_voltage", "pack_probe_temperature",
    "filtered_voltage", "filtered_temperature",
    "voltage_init_abnormal", "temperature_init_abnormal",
    "voltage_sample_abnormal", "temperature_sample_abnormal",
    "branch1", "branch2", "branch3", "branch4", "branch5",
    "combination1", "combination2", "combination3"
).withColumn("alarm_level", functions.lit(1)) \
 .withColumn("diagnosis_code", functions.lit("P0501")) \
 .withColumn("alarm_type", functions.lit("thermal_runaway"))

print(df_alarm.count())
df_alarm.show(20, truncate=False)