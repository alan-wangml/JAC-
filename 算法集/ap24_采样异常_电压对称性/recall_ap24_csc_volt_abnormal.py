%pyspark
import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions
from pyspark.sql.functions import col
from pyspark.sql.types import *
import numpy as np
import datetime
import time
from dateutil.parser import parse
import argparse
"""
按照车辆电池CSC电压采样异常专家模型产生预警数据
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
    "ap24_volt_diff_threshold": "100",
    "ap24_max_volt_diff_volt_entropy_threshold": "0.1",
    "ap24_uppervolt_outlier_threshold": "50",
    "ap24_lowervolt_outlier_threshold": "50"
}
battery_types = dict_params.keys()
battery_types_str = ', '.join(["'" + i + "'" for i in battery_types])

# 1. 一次性读取所有数据，只做基本过滤
sql_read = f"""
SELECT *, explode(min_volt_sn_count) as (min_sn,min_value) FROM(
SELECT *, explode(max_volt_sn_count) as (max_sn,max_value) FROM(
SELECT battery_id,battery_type,event_type,device_id,device_name,process_id,start_time,end_time,start_user_soc,
start_current,avg_pack_voltage,avg_insu_resis,avg_high_cell_volt,avg_low_cell_volt, max_volt_diff,
volt_diff_quartiles[75] as volt_diff, volt_diff_quartiles,max_volt_diff_volt_entropy,max_volt_sn_count, 
min_volt_sn_count,size(max_volt_sn_count) as max_count, size(min_volt_sn_count) as min_count,
max_volt_diff_max_cell_voltage, max_volt_diff_min_cell_voltage,max_volt_diff_cell_voltage, 'tsp' as source  
FROM {source_table}
WHERE dt = '20260501'
and battery_type in ({battery_types_str})
HAVING (max_count = 1 and min_count = 1)
ORDER BY start_time))
"""
df_detail = spark.sql(sql_read)
df_detail.cache()

# 2. 按照battery_type分组处理
processed_dfs = []

for row in battery_types:
    battery_type = row
    # 获取对应battery_type的参数，无配置时使用默认值
    params = dict_params.get(battery_type, default_params)

    # 提取参数
    ap24_volt_diff_threshold = int(params["ap24_volt_diff_threshold"])
    ap24_max_volt_diff_volt_entropy_threshold = float(params["ap24_max_volt_diff_volt_entropy_threshold"])
    ap24_uppervolt_outlier_threshold = int(params["ap24_uppervolt_outlier_threshold"])
    ap24_lowervolt_outlier_threshold = int(params["ap24_lowervolt_outlier_threshold"])
    ap24_operating_condition = 'parking,charge'

    # 基于场景筛选数据
    if ap24_operating_condition == "null" or ap24_operating_condition == "":
        scene_query_str = ""
    else:
        scene_type = list(ap24_operating_condition.split(","))  # 算法场景
        scene_query_list = list()
        for scene in scene_type:
            scene_query_list.append(f"event_type like '%{scene.strip()}%'")
        scene_query_str = " OR ".join(scene_query_list)

    # 3. 过滤当前battery_type的数据
    filtered_df = df_detail.filter(df_detail["battery_type"] == battery_type)

    # 基于场景筛选
    if scene_query_str:
        filtered_df = filtered_df.filter(scene_query_str)

    # 最高最低电芯相邻
    filtered_df = filtered_df.withColumn("sn_diff", functions.abs(filtered_df["max_sn"] - filtered_df["min_sn"]))
    filtered_df = filtered_df.filter(f"max_volt_diff > {ap24_volt_diff_threshold}  and max_volt_diff_volt_entropy > {ap24_max_volt_diff_volt_entropy_threshold}")
    filtered_df = filtered_df.filter("sn_diff = 1")

    if filtered_df.count() == 0:
        continue

    # 展开电芯列表，计算电芯列表分位数
    df_alarm_detail = filtered_df.withColumn("cell_volt", functions.explode("max_volt_diff_cell_voltage"))
    df_detail_group_info = df_alarm_detail.groupby("battery_id", "start_time") \
        .agg(functions.expr(f"percentile_approx(cell_volt, array(0.05,0.25,0.5,0.75,0.95), 999)")
             .alias("cell_volt_quartiles"))

    # 合并
    filtered_df = filtered_df.join(df_detail_group_info, on=["battery_id", "start_time"], how="left") \
        .selectExpr("battery_id", "battery_type", "event_type", "device_id", "device_name", "start_time", "max_volt_diff", 
                    "process_id", "end_time", "start_user_soc", "start_current", "avg_pack_voltage", "avg_insu_resis",
                    "avg_high_cell_volt", "avg_low_cell_volt", "volt_diff", "volt_diff_quartiles",
                    "max_volt_diff_volt_entropy", "max_volt_sn_count", "min_volt_sn_count", "max_count", "min_count",
                    "max_volt_diff_max_cell_voltage", "max_volt_diff_min_cell_voltage", "source",
                    "max_volt_diff_cell_voltage", "cell_volt_quartiles", "cell_volt_quartiles[0] as cell_volt_5",
                    "cell_volt_quartiles[1] as cell_volt_25", "cell_volt_quartiles[2] as cell_volt_50",
                    "cell_volt_quartiles[3] as cell_volt_75", "cell_volt_quartiles[4] as cell_volt_95")

    filtered_df = filtered_df.withColumn("iqr", filtered_df["cell_volt_75"] - filtered_df["cell_volt_25"])
    filtered_df = filtered_df.withColumn("upper_limit", filtered_df["cell_volt_75"] + 1.5*filtered_df["iqr"])
    filtered_df = filtered_df.withColumn("lower_limit", filtered_df["cell_volt_25"] - 1.5*filtered_df["iqr"])
    filtered_df = filtered_df \
        .withColumn("is_high_outliter",
                    functions.when(filtered_df["max_volt_diff_max_cell_voltage"] > filtered_df["upper_limit"], 1)
                    .otherwise(0))
    filtered_df = filtered_df \
        .withColumn("is_low_outliter",
                    functions.when(filtered_df["max_volt_diff_min_cell_voltage"] < filtered_df["lower_limit"], 1)
                    .otherwise(0))
    filtered_df = filtered_df.withColumn("is_csc_volt_fault",
                                     functions.when((filtered_df["is_high_outliter"] == 1)
                                                    & (filtered_df["is_low_outliter"] == 1), 1).otherwise(0))
    filtered_df = filtered_df.filter("is_csc_volt_fault = 1")

    # 计算最高最低偏离程度
    filtered_df = filtered_df \
        .withColumn("high_delt", filtered_df["max_volt_diff_max_cell_voltage"] - filtered_df["cell_volt_50"])
    filtered_df = filtered_df \
        .withColumn("low_delt", filtered_df["cell_volt_50"] - filtered_df["max_volt_diff_min_cell_voltage"])

    filtered_df = filtered_df.filter(f"high_delt > {ap24_uppervolt_outlier_threshold} and low_delt > {ap24_lowervolt_outlier_threshold}")

    # 创建row_num，按battery_id去重取volt_diff最大的一条
    w1 = Window.partitionBy("battery_id").orderBy(functions.desc("volt_diff"), functions.asc("start_time"))
    filtered_df = filtered_df.withColumn("row_num", functions.row_number().over(w1)).where("row_num = 1")

    column = [
        "battery_id", "battery_type", "device_id", "device_name", "process_id",
        "event_type", "start_time", "end_time", "start_user_soc", "start_current","max_volt_diff",
        "max_volt_sn_count", "min_volt_sn_count", "volt_diff_quartiles", "max_volt_diff_volt_entropy",
        "max_volt_diff_max_cell_voltage", "max_volt_diff_min_cell_voltage",  "source"
    ]

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
        StructField("device_id", StringType(), True),
        StructField("device_name", StringType(), True),
        StructField("process_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
        StructField("start_user_soc", DoubleType(), True),
        StructField("start_current", DoubleType(), True),
        StructField("max_volt_diff", DoubleType(), True),
        StructField("max_volt_sn_count", MapType(StringType(), DoubleType()), True),
        StructField("min_volt_sn_count", MapType(StringType(), DoubleType()), True),
        StructField("volt_diff_quartiles", ArrayType(DoubleType()), True),
        StructField("max_volt_diff_volt_entropy", DoubleType(), True),
        StructField("max_volt_diff_max_cell_voltage", DoubleType(), True),
        StructField("max_volt_diff_min_cell_voltage", DoubleType(), True),
        StructField("source", StringType(), True)
    ])
    df_detail = spark.createDataFrame([], schema)

df_detail = df_detail \
    .withColumn("start_time", functions.unix_timestamp(df_detail["start_time"]).cast(IntegerType()))

print(df_detail.count())
df_detail.show(20, truncate=False)
