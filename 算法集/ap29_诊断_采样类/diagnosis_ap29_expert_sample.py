%pyspark
import sys
import uuid
import math
import numpy as np
from pyspark.sql import SparkSession, functions, Window, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import sort_array, col, expr
from pyspark.sql.types import *
import pandas as pd
import json
from dateutil.parser import parse
import datetime
"""
诊断层--预警层采样异常诊断
:return: 智能诊断结果存入Hive表
"""
# 创建SparkSession, 作为读取数据、处理源数据、配置回话和管理集群资源的入口
spark = SparkSession.builder \
    .appName("diagnosis_ap29_expert_sample") \
    .config("spark.debug.maxToStringFields", "1000") \
    .config("spark.network.timeout", "1000") \
    .config("spark.default.parallelism", "500") \
    .config("spark.sql.shuffle.partitions", "800") \
    .config("spark.yarn.executor.memoryOverhead", "4096") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
    .enableHiveSupport() \
    .getOrCreate()  # prod

# 读取满足采样异常诊断场景的数据
block_table = "saas_battery.d_i_battery_block_features"
alarm_table = "saas_battery.d_i_alarm_results"
dict_params = algorithm.dict_params
# 默认参数（按 battery_type 配置）
default_params = {
    "ap29_date_back": 1,
    "ap29_volt_bounce_up_count_limit": 2,
    "ap29_volt_bounce_down_count_limit": 2,
    "ap29_high_outlier_sigma_coeff": 4.5,
    "ap29_low_outlier_sigma_coeff": 4.5,
    "ap29_max_volt_diff_limit": [100, 100]
}
input_date = "20260501"
enterprise_id = "JAC"

algorithm_config = [
    ["Algorithm_ap24", 24, 'JAC', 'SAMPLE'],
    ["Algorithm_ap13", 13, 'JAC', 'SAMPLE'],
    ["Algorithm_ap12", 12, 'JAC', 'SAMPLE'],
    ["Algorithm_ap22", 22, 'JAC', 'SAMPLE'],
]
sample_reall_algorithms = ["Algorithm_ap24", "Algorithm_ap13", "Algorithm_ap12"]
sample_reall_algorithms_str = ",".join([f"'{x}'" for x in sample_reall_algorithms])

pd_algorithm_config = pd.DataFrame(algorithm_config, columns=["algorithm_id", "model_id", "enterprise_id", "model_type"])
df_algorithm_config = spark.createDataFrame(pd_algorithm_config)

# 1. 读取告警数据
alarm_sql_read = f"""
SELECT 
     a.algorithm_id, a.battery_id as object_id, a.hash_code
     , algorithm_instance
     , a.result_create_time as alarm_time
     , CAST(unix_timestamp(a.result_create_time, 'yyyy-MM-dd HH:mm:ss') AS INT) AS result_timestamp
     , a.additional_data['msg_type'] as vehicle_state
     , a.result_create_time
     , a.additional_data['device_id'] as device_id
     , a.additional_data['process_id'] as process_id_tag
     , a.additional_data['process_id'] as alarm_process_id
     , a.additional_data['msg_type'] as alarm_msg_type
     , a.additional_data['data_type'] as alarm_data_source, a.alarm_data as result_data 
  FROM {alarm_table} a
    WHERE a.dt = '{input_date}'
    AND a.tenant_id = '{enterprise_id}' 
    AND a.algorithm_instance in ({sample_reall_algorithms_str})
    """
df_alarm_detail_ = spark.sql(alarm_sql_read)

df_alarm_detail_ = df_alarm_detail_.join(df_algorithm_config, how='inner', on='algorithm_id')
df_alarm_detail_ = df_alarm_detail_.withColumn("vehicle_state",
                                             functions.when(col("vehicle_state").like("%parking%"), "parking")
                                             .when(col("vehicle_state").like("%charge%"), "charge")
                                             .when(col("vehicle_state").like("%journey%"), "journey")
                                             .when(col("vehicle_state").like("%discharge%"), "discharge")
                                             .otherwise(col("vehicle_state")))

# 获取告警车辆vin
def get_device_name(alarm_detail):
    try:
        res = json.loads(alarm_detail)["device_name"]
    except:
        res = None
    return res

func_udf_1 = functions.udf(get_device_name, StringType())
df_alarm_detail_ = df_alarm_detail_.withColumn("alarm_msg_type", col("vehicle_state")).withColumn(
    "device_name", func_udf_1("result_data"))
df_alarm_detail_ = functions.broadcast(df_alarm_detail_)
df_alarm_bid = df_alarm_detail_.select("object_id").distinct()

# 2. 读取block_feature数据 - 从所有battery_type中获取最大回溯天数
max_back_days = max([params.get("ap29_date_back", 1) for params in dict_params.values()])
end_date = parse(str(input_date))
start_date_m = end_date - datetime.timedelta(days=max_back_days)
start_date_m = start_date_m.strftime("%Y%m%d")
end_date_ = end_date.strftime("%Y-%m-%d")

sql_read = f"""SELECT battery_id , device_id , event_type , battery_type
    , cell_type , max_volt_diff_cell_voltage  , max_volt_diff_volt_stddev  , max_volt_diff,
    process_id , start_time , end_time, max_volt_diff_max_cell_voltage, max_volt_diff_min_cell_voltage
    FROM {block_table}
    WHERE dt >= '{start_date_m}' AND dt <= '{input_date}' 
    AND start_time is not null 
    AND soc_tag is not null
    AND cell_type is not null 
    """
df_base = spark.sql(sql_read).drop('device_id', 'device_name')
df_block_detail = df_base.join(df_alarm_bid, df_alarm_bid["object_id"] == df_base["battery_id"])
df_block_detail = df_block_detail.withColumn("start_time_timestamp",
                                            functions.unix_timestamp(col("start_time"), 'yyyy-MM-dd HH:mm:ss')
                                ).withColumn("end_time_timestamp",
                                        functions.unix_timestamp(col("end_time"), 'yyyy-MM-dd HH:mm:ss'))

# 3. 离群判断 - UDF定义
voltage_stats_schema = StructType([
    StructField("max_volt_diff_volt_mean", FloatType(), False),
    StructField("max_volt_diff_volt_stddev", FloatType(), False),
    StructField("max_minus_rest_mean", FloatType(), False)
])

def compute_voltage_mean_std_tuple(volt_list):
    if (volt_list is None) or (len(volt_list) <= 1) or (None in volt_list):
        return (0.0, 0.0, 0.0)
    
    volts = [float(v) for v in volt_list]
    N = len(volts)
    total = sum(volts)
    max_v = max(volts)
    
    fe_mean = total / N
    fe_std = float(np.std(volts))
    max_minus_rest_mean = max_v - (total - max_v) / (N - 1)
    
    return (fe_mean, fe_std, max_minus_rest_mean)

udf_compute_voltage_stats = F.udf(compute_voltage_mean_std_tuple, voltage_stats_schema)

# 4. 按battery_type分组处理
battery_types = dict_params.keys()
processed_dfs = []

for battery_type in battery_types:
    params = dict_params.get(battery_type, default_params)

    ap29_date_back = params.get("ap29_date_back", 1)
    ap29_high_outlier_sigma_coeff = params.get("ap29_high_outlier_sigma_coeff")
    ap29_low_outlier_sigma_coeff = params.get("ap29_low_outlier_sigma_coeff")
    ap29_volt_bounce_up_count_limit = params.get("ap29_volt_bounce_up_count_limit")
    ap29_volt_bounce_down_count_limit = params.get("ap29_volt_bounce_down_count_limit")
    ap29_max_volt_diff_limit = params.get("ap29_max_volt_diff_limit")

    start_date = end_date - datetime.timedelta(days=ap29_date_back)
    start_date_ = start_date.strftime("%Y-%m-%d")

    # 过滤当前battery_type和时间范围的数据
    df_detail = df_block_detail.filter(f"battery_type = '{battery_type}'")
    df_detail = df_detail.filter(expr(f"""end_time between '{start_date_} 00:00:00' and '{end_date_} 23:59:59' """))

    if df_detail.count() == 0:
        continue

    df_max_vdiff_block = df_detail.dropna(how="any", subset=["max_volt_diff_cell_voltage"])

    df_outlier = df_max_vdiff_block.withColumn("outlier_info",
                                               udf_compute_voltage_stats(df_max_vdiff_block["max_volt_diff_cell_voltage"]))
    
    # Block-高离群（block_volt_hg_out）
    df_outlier = df_outlier.withColumn("high_outlier_limit",
                                       F.col("outlier_info.max_volt_diff_volt_mean") + ap29_high_outlier_sigma_coeff * F.col("outlier_info.max_volt_diff_volt_stddev"))
    df_outlier = df_outlier.withColumn("is_high_outlier_alarm", functions.when(
        col("max_volt_diff_max_cell_voltage") > ap29_max_volt_diff_limit[0], 1).otherwise(0))
    
    # Block-低离群（block_volt_low_out）
    df_outlier = df_outlier.withColumn("low_outlier_limit",
                                       F.col("outlier_info.max_volt_diff_volt_mean") - ap29_low_outlier_sigma_coeff * F.col("outlier_info.max_volt_diff_volt_stddev"))
    df_outlier = df_outlier.withColumn("is_low_outlier_alarm", functions.when(
        col("max_volt_diff_min_cell_voltage") < ap29_max_volt_diff_limit[1], 1).otherwise(0))
    
    window = Window.partitionBy("battery_id").orderBy(col("start_time"))
    df_outlier = df_outlier.withColumn("next_is_high_outlier_alarm", F.lead("is_high_outlier_alarm").over(window)).\
        withColumn("next_is_low_outlier_alarm", F.lead("is_low_outlier_alarm").over(window))
    
    df_outlier = df_outlier.withColumn("volt_bounce_up_count", functions.when(
    (col("is_high_outlier_alarm") == 1) & (col("next_is_high_outlier_alarm") == 0), 1).otherwise(0))\
     .withColumn("volt_bounce_down_count", functions.when(
    (col("is_low_outlier_alarm") == 1) & (col("next_is_low_outlier_alarm") == 0), 1).otherwise(0))
     
    df_outlier = df_outlier.groupBy("battery_id", 'battery_type').agg(
        F.sum("volt_bounce_up_count").alias("volt_bounce_up_count"),
        F.sum("volt_bounce_down_count").alias("volt_bounce_down_count")
    ).withColumn("volt_bounce_up_count", F.when(F.col("volt_bounce_up_count") >= ap29_volt_bounce_up_count_limit, 1).otherwise(0)).\
        withColumn("volt_bounce_down_count", F.when(F.col("volt_bounce_down_count") >= ap29_volt_bounce_down_count_limit, 1).otherwise(0))

    if df_outlier.count() == 0:
        continue

    processed_dfs.append(df_outlier)

# 5. 合并所有处理后的DataFrame
if processed_dfs:
    df_outlier = processed_dfs[0]
    for i in range(1, len(processed_dfs)):
        df_outlier = df_outlier.unionByName(processed_dfs[i])
else:
    # 如果没有数据，创建空DataFrame
    schema = StructType([
        StructField("battery_id", StringType(), True),
        StructField("battery_type", StringType(), True),
        StructField("volt_bounce_up_count", IntegerType(), True),
        StructField("volt_bounce_down_count", IntegerType(), True)
    ])
    df_outlier = spark.createDataFrame([], schema)

df_alarm_detail = df_alarm_detail_.join(df_outlier, df_outlier["battery_id"] == df_alarm_detail_["object_id"], "left")

df_alarm_detail = df_alarm_detail.withColumn('diagnosis_code', functions.when( (F.col('algorithm_id') == 'Algorithm_ap24') & 
                                                                              ((F.col('volt_bounce_up_count') == 1) | (F.col('volt_bounce_down_count') == 1)), 'P0301')
                                                                    .when(F.col('algorithm_id').isin('Algorithm_ap12', 'Algorithm_ap14'), 'P0303')
                                                                    .otherwise(F.lit('P0308'))                                      
                                                    )

df_alarm_detail = df_alarm_detail.withColumn("diagnosis_features", functions.lit("null"))

# 生成报告
df_res = df_alarm_detail.select(
    df_alarm_detail["hash_code"].cast(StringType()),
    df_alarm_detail["object_id"].alias("battery_id").cast(StringType()),
    df_alarm_detail["battery_type"].cast(StringType()),
    df_alarm_detail["device_id"].cast(StringType()),
    df_alarm_detail["device_name"].cast(StringType()),
    df_alarm_detail["algorithm_id"].cast(StringType()),
    df_alarm_detail["model_id"].alias("algorithm_model").cast(StringType()),
    df_alarm_detail["model_type"].alias("algorithm_model_type").cast(StringType()),
    df_alarm_detail["alarm_time"].cast(TimestampType()),
    df_alarm_detail["alarm_process_id"].cast(StringType()),
    df_alarm_detail["alarm_msg_type"].cast(StringType()),
    df_alarm_detail["alarm_data_source"].cast(StringType()),
    functions.lit("Algorithm_ap29").alias("diagnosis_model_id").cast(StringType()),
    functions.lit("Algorithm_ap29_params").alias("diagnosis_params_id").cast(StringType()),
    functions.lit("").alias("diagnosis_instance").cast(StringType()),
    df_alarm_detail["diagnosis_code"].cast(StringType()),
    functions.lit(1).alias("diagnosis_prob").cast(DoubleType()),
    df_alarm_detail["diagnosis_features"].cast(StringType())) \
    .withColumn("update_time", functions.current_timestamp().cast(TimestampType())) \
    .withColumn("dw_etl_time", functions.current_timestamp().cast(TimestampType())) \
    .withColumn("diagnosis_model_type", functions.lit("expert_model"))

df_res = df_res.select(
    df_res["hash_code"].cast(StringType()),
    df_res["battery_id"].cast(StringType()),
    df_res["battery_type"].cast(StringType()),
    df_res["device_id"].cast(StringType()),
    df_res["device_name"].cast(StringType()),
    df_res["algorithm_id"].cast(StringType()),
    df_res["algorithm_model"].cast(StringType()),
    df_res["algorithm_model_type"].cast(StringType()),
    df_res["alarm_time"].cast(TimestampType()),
    df_res["alarm_process_id"].cast(StringType()),
    df_res["alarm_msg_type"].cast(StringType()),
    df_res["alarm_data_source"].cast(StringType()),
    df_res["diagnosis_model_id"].cast(StringType()),
    df_res["diagnosis_model_type"].cast(StringType()),
    df_res["diagnosis_params_id"].cast(StringType()),
    df_res["diagnosis_instance"].cast(StringType()),
    df_res["diagnosis_code"].cast(StringType()),
    df_res["diagnosis_prob"].cast(DoubleType()),
    df_res["diagnosis_features"].cast(StringType()),
    df_res["update_time"].cast(TimestampType()),
    df_res["dw_etl_time"].cast(TimestampType())
)

df_res = df_res.select(
    df_res["hash_code"].cast(StringType()),
    df_res["battery_id"].cast(StringType()),
    df_res["battery_type"].cast(StringType()),
    df_res["device_id"].cast(StringType()),
    df_res["device_name"].cast(StringType()),
    df_res["algorithm_id"].cast(StringType()),
    df_res["algorithm_model"].cast(StringType()),
    df_res["algorithm_model_type"].cast(StringType()),
    df_res["alarm_time"].cast(TimestampType()),
    df_res["alarm_process_id"].cast(StringType()),
    df_res["alarm_msg_type"].cast(StringType()),
    df_res["alarm_data_source"].cast(StringType()),
    df_res["diagnosis_model_id"].cast(StringType()),
    df_res["diagnosis_model_type"].cast(StringType()),
    df_res["diagnosis_params_id"].cast(StringType()),
    df_res["diagnosis_instance"].cast(StringType()),
    df_res["diagnosis_code"].cast(StringType()),
    df_res["diagnosis_prob"].cast(DoubleType()),
    df_res["diagnosis_features"].cast(StringType()),
    functions.lit(enterprise_id).alias("tenant_id").cast(StringType()),
    df_res["update_time"].cast(TimestampType()),
    df_res["dw_etl_time"].cast(TimestampType())
)

df_res.show(10, False)
print(df_res.count())