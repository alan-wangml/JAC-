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
按照箱线图指标判断电池压差离群状态产生预警数据
:return: 电池压差离群预警结果存入Hive表
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

# 参数定义
dict_params = algorithm.dict_params
ap8_cell_voltage = [2500, 4500]
ap8_sample_interval = [5.0, 3.0, 30.0]
ap8_min_probe_temp_threshold = 10
ap8_vdiff_threshold = 1
ap8_soc_threshold = [0.1, 0.85]
ap8_factor_threshold = [0.015, 0.002, 1, 0.4]
ap8_volt_ratio = 1000

ap8_cell_volt_lower_bound = float(ap8_cell_voltage[0])
ap8_cell_volt_upper_bound = float(ap8_cell_voltage[1])
ap8_standard_sample_interval = float(ap8_sample_interval[0])
ap8_sample_interval_lower_bound = float(ap8_sample_interval[1])
ap8_sample_interval_upper_bound = float(ap8_sample_interval[2])
ap8_current_factor = float(ap8_factor_threshold[0])
ap8_soc_factor = float(ap8_factor_threshold[1])
ap8_vdiff_factor = float(ap8_factor_threshold[2])
ap8_temp_factor = float(ap8_factor_threshold[3])

# 读取当天数据
source_table_detail = "saas_battery.ods_battery_detail_h_i"
source_table_dim = "saas_battery.ods_dim_battery_model"
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
    'NCM' AS cell_type,
    sample_time AS sample_timestamp,
    case 
        when battery_state = 1 then 'periodical_charge_update' 
        when battery_state = 3 then 'periodical_journey_update' 
        else 'periodical_parking_update' end as vehicle_state,
    process_id,
    CAST(insulation_resistance AS DOUBLE) AS vehicle_insulation_resistance,
    CAST(voltage AS DOUBLE) AS voltage,
    CAST(current AS DOUBLE) AS current,
    CAST(user_soc AS DOUBLE) AS user_soc,
    CASE 
        WHEN user_soc >= 55 AND user_soc <= 65 THEN 'range1'
        ELSE 'range2'
    END AS soc_tag,
    CAST(extremumhistvoltsinglbtrysn AS INT) AS max_cell_voltage_sn,
    CAST(extremumsinbtryhistvolt AS DOUBLE) AS max_cell_voltage,
    CAST(extremumLwstVoltSinglBtrySn AS INT) AS min_cell_voltage_sn,
    CAST(extremumsinbtrylwstvolt AS DOUBLE) AS min_cell_voltage,
    CAST(extremumhisttempprbsn AS INT) AS max_probe_temperature_sn,
    CAST(extremumhighesttemp AS DOUBLE) AS max_probe_temperature,
    CAST(extremumlwsttempprbsn AS INT) AS min_probe_temperature_sn,
    CAST(extremumlowesttemp AS DOUBLE) AS min_probe_temperature,
    CAST((extremumhighesttemp + extremumlowesttemp) / 2 AS DOUBLE) AS avg_probe_temperature,
    CAST((extremumsinbtryhistvolt - extremumsinbtrylwstvolt) * 1000 AS DOUBLE) AS cell_volt_diff
FROM (
    SELECT 
        battery_id, battery_model, device_id, device_type,
        sample_time, battery_state, process_id,
        insulation_resistance, voltage, current, user_soc,
        max_cell_voltage_sn as extremumhistvoltsinglbtrysn,
        max_cell_voltage as extremumsinbtryhistvolt,
        min_cell_voltage_sn as extremumLwstVoltSinglBtrySn,
        min_cell_voltage as extremumsinbtrylwstvolt,
        max_probe_temperature_sn as extremumhisttempprbsn,
        max_probe_temperature as extremumhighesttemp, 
        min_probe_temperature_sn as extremumlwsttempprbsn,
        min_probe_temperature as extremumlowesttemp
    FROM {source_table_detail}
    WHERE dt = '{input_date}' AND sample_time IS NOT NULL AND battery_id IS NOT NULL
) base
"""
base_filter_str = """
    battery_id is not null and vehicle_id is not null and vin is not null
    and sample_time is not null and vehicle_state is not null and vehicle_insulation_resistance is not null
    and voltage is not null and current is not null and user_soc is not null
    and max_cell_voltage is not null and min_cell_voltage is not null
"""
df_detail = spark.sql(sql_read).filter(base_filter_str)

# 合并电池静态信息
sql_static = f"""
select a.battery_id as battery_id_static, 
    b.battery_capacity, b.battery_energy from
    (select id as battery_id, battery_model from {source_table_dim} where dt = '{input_date}') a
    left join
    (select battery_model, battery_capacity as battery_energy, battery_volume as battery_capacity,
     cell_number as cell_quantity from {source_table_dim} where dt = '{input_date}') b
    on a.battery_model = b.battery_model
    where b.battery_model is not null
"""
df_battery_type = functions.broadcast(spark.sql(sql_static))

# 过滤当前battery_type的数据
battery_type = "GX-1P108S"
sql_filter = f"""
    (vehicle_state = 'periodical_parking_update' or vehicle_state = 'periodical_charge_update')
    and max_cell_voltage > {ap8_cell_volt_lower_bound}
    and max_cell_voltage < {ap8_cell_volt_upper_bound}
    and min_cell_voltage > {ap8_cell_volt_lower_bound}
    and min_cell_voltage < {ap8_cell_volt_upper_bound}
"""
df_detail = df_detail.filter(f"battery_type = '{battery_type}'") \
    .withColumn('min_cell_voltage', col('min_cell_voltage') * ap8_volt_ratio) \
    .withColumn('max_cell_voltage', col('max_cell_voltage') * ap8_volt_ratio) \
    .withColumn("cell_volt_diff", col("cell_volt_diff") * ap8_volt_ratio) \
    .filter(f"cell_volt_diff > {ap8_vdiff_threshold}").filter(sql_filter)
df_detail = df_detail.join(
    df_battery_type,
    df_detail["battery_id"] == df_battery_type["battery_id_static"], "left_outer").drop("battery_id_static")

# 计算报文之间的时间间隔
w1 = Window.partitionBy("battery_id", "battery_type", "cell_type", "vehicle_state").orderBy("sample_timestamp")
df_detail = df_detail.withColumn("next_timestamp", functions.lead("sample_timestamp").over(w1)) \
    .withColumn("next_high_volt", functions.lead("max_cell_voltage").over(w1)) \
    .withColumn("next_low_volt", functions.lead("min_cell_voltage").over(w1))
df_detail = df_detail.withColumn("msg_time_interval", functions.expr(
    f"""case when next_timestamp - sample_timestamp > {ap8_sample_interval_upper_bound}
       then {ap8_standard_sample_interval} else next_timestamp - sample_timestamp end"""))
df_detail = df_detail.filter(f"""msg_time_interval >= {ap8_sample_interval_lower_bound}
    or next_high_volt != max_cell_voltage or next_low_volt != min_cell_voltage""")

# 多因子空间投影计算加权压差
df_detail = df_detail \
    .withColumn("soc_rate", functions.atan(
        ap8_soc_factor * functions.pow(df_detail["user_soc"], 3)) * 2 / functions.lit(math.pi)) \
    .withColumn("current_rate", functions.exp(-ap8_current_factor * functions.abs(df_detail["current"]))) \
    .withColumn("vdiff_rate", functions.when(
        (df_detail["min_cell_voltage"] > 0) & (df_detail["max_cell_voltage"] < 3600),
        1 / (functions.exp(-5 / df_detail["cell_volt_diff"]) + 1)).when(
        (((df_detail["user_soc"] >= 55) & (df_detail["user_soc"] <= 65)) | (df_detail["user_soc"] <= 25))
        & (df_detail["cell_volt_diff"] < 40), 1 - 0.01 * df_detail["cell_volt_diff"]).when(
        (((df_detail["user_soc"] >= 55) & (df_detail["user_soc"] <= 65)) | (df_detail["user_soc"] <= 25))
        & (df_detail["cell_volt_diff"] >= 40), 0.85).otherwise(
        1 / (functions.pow(ap8_vdiff_factor, df_detail["cell_volt_diff"]) + 1))) \
    .withColumn("temp_rate", 1 / (functions.exp(-ap8_temp_factor * df_detail["avg_probe_temperature"]) + 1))
df_detail = df_detail.withColumn(
    "weighted_vdiff_level", functions.expr(
        """round(soc_rate * current_rate * vdiff_rate * temp_rate * cell_volt_diff, 2)"""))

# 持久化
df_detail.persist(storageLevel=StorageLevel.DISK_ONLY)

# 单电池箱线图指标
df_single = df_detail.groupBy("battery_id", "battery_type", "cell_type", "vehicle_state", "soc_tag").agg(
    functions.expr("percentile_approx(weighted_vdiff_level, array(0.05, 0.25, 0.5, 0.75, 0.95), 999)").alias(
        "sin_vdiff_quartiles"),
    functions.expr("round(percentile(weighted_vdiff_level, 0.05), 2)").alias("sin_5_vdiff_quartiles"),
    functions.expr("round(percentile(weighted_vdiff_level, 0.25), 2)").alias("sin_25_vdiff_quartiles"),
    functions.expr("round(percentile(weighted_vdiff_level, 0.50), 2)").alias("sin_50_vdiff_quartiles"),
    functions.expr("round(percentile(weighted_vdiff_level, 0.75), 2)").alias("sin_75_vdiff_quartiles"),
    functions.expr("round(percentile(weighted_vdiff_level, 0.95), 2)").alias("sin_95_vdiff_quartiles"))

# 全量电池箱线图指标
df_all = df_detail.groupBy("battery_type", "cell_type", "vehicle_state", "soc_tag").agg(
    functions.expr("percentile_approx(weighted_vdiff_level, array(0.05, 0.25, 0.5, 0.75, 0.95), 999)").alias(
        "all_vdiff_quartiles"),
    functions.expr("round(percentile(weighted_vdiff_level, 0.05), 2)").alias("all_5_vdiff_quartiles"),
    functions.expr("round(percentile(weighted_vdiff_level, 0.25), 2)").alias("all_25_vdiff_quartiles"),
    functions.expr("round(percentile(weighted_vdiff_level, 0.50), 2)").alias("all_50_vdiff_quartiles"),
    functions.expr("round(percentile(weighted_vdiff_level, 0.75), 2)").alias("all_75_vdiff_quartiles"),
    functions.expr("round(percentile(weighted_vdiff_level, 0.95), 2)").alias("all_95_vdiff_quartiles"))
df_all = df_all.withColumn("all_vdiff_upper_bound", functions.expr(
    "round(2.5 * all_75_vdiff_quartiles - 1.5 * all_25_vdiff_quartiles, 2)")).filter("battery_type is not null")
df_all.persist(storageLevel=StorageLevel.MEMORY_ONLY)

# 合并单电池和全量箱线图
df_single = df_single.join(df_all, on=["battery_type", "cell_type", "vehicle_state", "soc_tag"], how="left_outer")

# 触发预警
df_single = df_single.withColumn("is_alarm", functions.expr(f"""
    case when sin_5_vdiff_quartiles > all_vdiff_upper_bound
        and sin_95_vdiff_quartiles - sin_5_vdiff_quartiles > all_95_vdiff_quartiles - all_5_vdiff_quartiles
        and vehicle_state = 'periodical_parking_update'
    then 1
    when sin_95_vdiff_quartiles > 4 * (all_95_vdiff_quartiles - all_5_vdiff_quartiles)
        and sin_75_vdiff_quartiles > all_vdiff_upper_bound and soc_tag = 'range2'
    then 2 else 0 end"""))
df_alarm = df_single.filter("is_alarm = 1 or is_alarm = 2")

# 获取单块电池压差最大时刻的打点数据
w2 = Window.partitionBy("battery_id", "battery_type", "cell_type", "vehicle_state", "soc_tag") \
    .orderBy(functions.desc("cell_volt_diff"))
df_max_vdiff = df_detail.withColumn("desc_vdiff_num", functions.row_number().over(w2)).where("desc_vdiff_num = 1")

df_alarm = df_alarm.join(
    df_max_vdiff, on=["battery_id", "battery_type", "cell_type", "vehicle_state", "soc_tag"], how="left_outer")

print(df_alarm.count())
df_alarm.show(20, truncate=False)
