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
按照车辆电池单体过压算法产生预警数据
:return: 电池单体电压离群预警结果存入Hive表
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

# 读取满足压差场景的数据
source_table = "saas_battery.d_i_battery_block_features"
dict_params = algorithm.dict_params
ap2_entropy_lfp_threshold = 1
ap2_min_low_probe_temp_lfp_threshold = 5
ap2_min_low_cell_volt_lfp_threshold = 3200
ap2_block_msg_count_threshold = 3
ap2_max_high_cell_volt_lfp_threshold =3450
ap2_volt_diff_lfp_without_threshold =7
ap2_volt_diff_lfp_within_threshold = 20
ap2_offset_lfp_shreshold = 0.02
ap2_operating_condition = ""
ap2_cell_type = 'LFP'
# 基于场景筛选数据
if ap2_operating_condition == "null" or ap2_operating_condition == "":
    scene_query_str = ""
else:
    scene_type = list(ap2_operating_condition.split(","))  # 算法场景
    scene_query_list = list()
    for scene in scene_type:
        scene_query_list.append(f"""event_type like '%{scene}%'""")
    scene_query_str = "AND (" + " OR ".join(scene_query_list) + ")"

# 按照电池类型生成sql语句
if ap2_cell_type == "null" or ap2_cell_type == "":
    cell_type_query_str = ""
else:
    cell_type_type = list(ap2_cell_type.split(","))  # 算法场景
    cell_type_query_list = list()
    for cell_type in cell_type_type:
        cell_type_query_list.append(f"""cell_type = '{cell_type}'""")
    cell_type_query_str = "AND (" + " OR ".join(cell_type_query_list) + ")"

# 读取当天数据
sql_read = f"""
    select * from  {source_table}
    where dt = '20260501'
    and battery_type in ('GX-1P108S','21011E3C1','2101ZHM116')
    {scene_query_str}
    {cell_type_query_str}
    and max_volt_diff_volt_entropy < {ap2_entropy_lfp_threshold}
    and min_low_probe_temp >= {ap2_min_low_probe_temp_lfp_threshold}
    and max_volt_diff_min_cell_voltage >= {ap2_min_low_cell_volt_lfp_threshold}
    and max_volt_diff_max_cell_voltage <= {ap2_max_high_cell_volt_lfp_threshold}
    """
df_detail = spark.sql(sql_read)
df_detail = df_detail.drop('max_volt_diff_volt_mean')

# 定义中位数UDF
def array_median(arr):
    try:
        # 过滤数组中的None值
        clean_arr = [x for x in arr if x is not None]
        if not clean_arr:  # 过滤后为空数组
            return None  # 返回浮点型，避免类型不一致
        return float(np.median(arr))
    except:
        return None
# 定义均值UDF（处理空数组）
def array_avg_udf(arr):
    try:
        # 过滤数组中的None值
        clean_arr = [x for x in arr if x is not None]
        if not clean_arr:  # 过滤后为空数组
            return None  # 返回浮点型，避免类型不一致
        return float(np.mean(arr))
    except:
        return None
avg_udf = udf(array_avg_udf, DoubleType())
median_udf = udf(array_median, DoubleType())

df_detail = df_detail.withColumn("max_volt_diff_volt_median", median_udf(col("max_volt_diff_cell_voltage")))
df_detail = df_detail.withColumn("max_volt_diff_volt_mean", avg_udf(col("max_volt_diff_cell_voltage")))

# key filter
df_detail = df_detail.filter(f"""max_volt_diff_volt_median - max_volt_diff_volt_mean 
                                    >= {ap2_offset_lfp_shreshold}
                                and
                                max_volt_diff >=
                                    case
                                        when soc_tag > 1 and soc_tag < 17
                                            then {ap2_volt_diff_lfp_within_threshold}
                                        else {ap2_volt_diff_lfp_without_threshold}
                                    end
                            """)
column = [
    "battery_id", "battery_type", "device_id", "device_name", "process_id", "event_type", "max_volt_diff", 
    "start_time", "end_time", "start_real_soc", "start_current", "avg_pack_voltage",
    "avg_insu_resis", "avg_high_cell_volt", "avg_low_cell_volt", 
    "max_volt_diff_volt_mean", "max_volt_diff_volt_median"
]
df_detail = df_detail.select(column)
df_detail_p = df_detail \
    .withColumn("start_time", functions.unix_timestamp(df_detail["start_time"]).cast(IntegerType()))

print(df_detail_p.count())

w = Window.partitionBy("battery_id").orderBy(col("max_volt_diff").desc())
df_top = (
    df_detail_p
    .withColumn("rn", functions.row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
    )
print(df_top.count())
df_top.show(20, truncate=False)
