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
诊断层--预警层温度异常诊断
:return: 智能诊断结果存入Hive表
"""
# 创建SparkSession, 作为读取数据、处理源数据、配置回话和管理集群资源的入口
spark = SparkSession.builder \
    .appName("diagnosis_ap27_expert_temperature") \
    .config("spark.debug.maxToStringFields", "1000") \
    .config("spark.network.timeout", "1000") \
    .config("spark.shuffle.memoryFraction", "0.6") \
    .config("spark.default.parallelism", "6000") \
    .config("spark.sql.shuffle.partitions", "500") \
    .config("spark.yarn.executor.memoryOverhead", "4096") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
    .enableHiveSupport() \
    .getOrCreate()  # prod

# 读取满足温度异常诊断场景的数据
block_table = "saas_battery.d_i_battery_block_features"
alarm_table = "saas_battery.d_i_alarm_results"
dict_params = algorithm.dict_params
# 默认参数（按 battery_type 配置）
default_params = {
    "ap27_date_back_days": 7,
    "ap27_max_temp_diff_limit": 7,
    "ap27_temp_diff_lasts_limit": 24,
    "ap27_min_low_temp_threshold": 0,
    "ap27_min_low_temp_ratio": 0.8,
    "ap27_min_low_temp_range": 10.0,
    "ap27_min_low_probe_temp_last_threshold": 0,
    "ap27_negative_temp_slope_ratio_threshold": 0.2
}
input_date = "20260501"
enterprise_id = "JAC"
N_partition = 500

algorithm_config = [
    ["Algorithm_ap10", 10, 'JAC', 'TEMP'],
    ["Algorithm_ap11", 11, 'JAC', 'TEMP'],
    ["Algorithm_ap9", 9, 'JAC', 'TEMP'],
]
temperature_reall_algorithms = ["Algorithm_ap10", "Algorithm_ap11", "Algorithm_ap9"]
temperature_reall_algorithms_str = ",".join([f"'{x}'" for x in temperature_reall_algorithms])

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
    AND upper(a.tenant_id) = '{enterprise_id}'
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
    "device_name_alarm", func_udf_1("result_data"))
df_alarm_detail_ = functions.broadcast(df_alarm_detail_)
df_alarm_bid = df_alarm_detail_.select("object_id").distinct()

# 2. 读取block_feature数据 - 读取所有告警电池的数据
# 从所有battery_type中获取最大回溯天数
max_back_days = max([params.get("ap27_date_back_days", 7) for params in dict_params.values()])

end_date = parse(str(input_date))
start_date_m = (end_date - datetime.timedelta(days=max_back_days))
start_date_m = start_date_m.strftime("%Y%m%d")
end_date_ = end_date.strftime("%Y-%m-%d")

sql_read = f"""SELECT * FROM {block_table}
    WHERE dt >= '{start_date_m}' AND dt <= '{input_date}' 
    AND start_time is not null 
    AND soc_tag is not null
    AND cell_type is not null 
    """
df_base = spark.sql(sql_read).drop('device_id', 'device_name').repartition(N_partition, 'battery_id')
df_block_detail = df_base.join(df_alarm_bid, df_alarm_bid["object_id"] == df_base["battery_id"])
df_block_detail = df_block_detail.withColumn("start_time_timestamp",
                                            functions.unix_timestamp(col("start_time"), 'yyyy-MM-dd HH:mm:ss')
                                ).withColumn("end_time_timestamp",
                                        functions.unix_timestamp(col("end_time"), 'yyyy-MM-dd HH:mm:ss'))
df_block_detail.cache()

# 定义输出 Schema
output_schema = StructType([
    StructField("battery_id", StringType(), True),
    StructField("battery_type", StringType(), True),
    StructField("diagnosis_code", StringType(), True),
    StructField("diagnosis_features", StringType(), True)
])

needed_columns = [
    "battery_id", 
    "battery_type", 
    "start_time", 
    "end_time", 
    "max_temp_diff", 
    "min_low_probe_temp"
]

# 3. 定义 UDF - 接收参数字典
def create_diagnosis_udf(params):
    ap27_max_temp_diff_limit = params.get("ap27_max_temp_diff_limit")
    ap27_temp_diff_lasts_limit = params.get("ap27_temp_diff_lasts_limit")
    ap27_min_low_temp_threshold = params.get("ap27_min_low_temp_threshold")
    ap27_min_low_temp_ratio = params.get("ap27_min_low_temp_ratio")
    ap27_min_low_temp_range = params.get("ap27_min_low_temp_range")
    ap27_min_low_probe_temp_last_threshold = params.get("ap27_min_low_probe_temp_last_threshold")
    ap27_negative_temp_slope_ratio_threshold = params.get("ap27_negative_temp_slope_ratio_threshold")
    
    def diagnosis_logic_native(rows):
        cols = needed_columns
        pdf = pd.DataFrame(rows, columns=cols)
        if pdf.empty:
            return None

        # --- 1. 预处理 ---
        pdf = pdf.sort_values("start_time").reset_index(drop=True)
        limit = ap27_max_temp_diff_limit

        # --- Step 1: 分组切片 ---
        pdf['flag'] = pdf['max_temp_diff'] >= limit
        pdf['group_id'] = (pdf['flag'] != pdf['flag'].shift()).cumsum()

        # --- Step 2: 压差持续时间判断 ---
        pre_diagnosis_code = "P0308"
        last_suspect_gid = None

        suspicious_pdf = pdf[pdf['flag'] == True]
        if not suspicious_pdf.empty:
            for gid, group in suspicious_pdf.groupby('group_id'):
                duration = (pd.to_datetime(group['end_time']).max() - 
                            pd.to_datetime(group['start_time']).min()).total_seconds()

                low_temp_ratio_check = (group['min_low_probe_temp'] > ap27_min_low_temp_threshold).mean()

                if duration >= ap27_temp_diff_lasts_limit * 3600 and \
                low_temp_ratio_check >= ap27_min_low_temp_ratio:
                    pre_diagnosis_code = "P0302"
                    last_suspect_gid = gid

        if last_suspect_gid is None:
            return {
                "battery_id": str(pdf['battery_id'].iloc[0]),
                "battery_type": str(pdf['battery_type'].iloc[0]),
                "diagnosis_code": pre_diagnosis_code,
                "diagnosis_features": None
            }

        # --- Step 3: 恢复判断与特征提取 ---
        final_code = pre_diagnosis_code
        target_group = pdf[pdf['group_id'] == last_suspect_gid].reset_index(drop=True)

        last_max_tdiff = target_group['max_temp_diff'].iloc[-1]
        last_duration = (pd.to_datetime(target_group['end_time']).max() - 
                        pd.to_datetime(target_group['start_time']).min()).total_seconds()
        delta_low_temp = target_group['min_low_probe_temp'].iloc[-1] - target_group['min_low_probe_temp'].iloc[0]
        low_temp_last = target_group['min_low_probe_temp'].iloc[-1]

        temp_slopes = target_group['min_low_probe_temp'].diff().dropna()
        neg_slope_ratio = (temp_slopes < 0).mean() if not temp_slopes.empty else 0.0

        feature_dict = {
            "last_suspect_max_temp_diff": f"{last_max_tdiff}C",
            "last_suspect_last_duration": f"{last_duration/3600:.2f}h",
            "last_suspect_delta_low_temp": f"{delta_low_temp:.2f}",
            "last_suspect_min_low_probe_temp_last": f"{low_temp_last:.2f}",
            "last_suspect_negative_temp_slope_ratio": f"{neg_slope_ratio:.4f}"
        }

        if pre_diagnosis_code == "P0302" and len(target_group) >= 3:
            c1 = delta_low_temp >= ap27_min_low_temp_range
            c2 = low_temp_last > ap27_min_low_probe_temp_last_threshold
            c3 = neg_slope_ratio < ap27_negative_temp_slope_ratio_threshold

            if c1 and c2 and c3:
                final_code = "P0308"

        return {
            "battery_id": str(pdf['battery_id'].iloc[0]),
            "battery_type": str(pdf['battery_type'].iloc[0]),
            "diagnosis_code": final_code,
            "diagnosis_features": json.dumps(feature_dict, ensure_ascii=False)
        }
    
    return F.udf(diagnosis_logic_native, output_schema)

# 4. 按battery_type分组处理
battery_types = dict_params.keys()
processed_dfs = []

for battery_type in battery_types:
    # 获取对应battery_type的参数，无配置时使用默认值
    params = dict_params.get(battery_type, default_params)
    
    back_days = params.get("ap27_date_back_days", 7)

    start_date = (end_date - datetime.timedelta(days=back_days))
    start_date_ = start_date.strftime("%Y-%m-%d")

    df_detail = df_block_detail.filter(expr(f"""end_time between '{start_date_} 00:00:00' and '{end_date_} 23:59:59' """))

    # 创建当前battery_type的诊断UDF
    diagnosis_udf = create_diagnosis_udf(params)
    
    # 过滤当前battery_type的数据
    df_detail_p = df_detail.filter(f"battery_type = '{battery_type}'")
    
    if df_detail_p.count() == 0:
        continue
    
    # 应用诊断UDF
    df_detail_part = df_detail_p.select(*needed_columns)
    df_res_p = df_detail_part.withColumn("all_data", F.struct([df_detail_part[c] for c in df_detail_part.columns])) \
        .groupBy("battery_id") \
        .agg(F.collect_list("all_data").alias("rows_list")) \
        .withColumn("res_struct", diagnosis_udf("rows_list")) \
        .select("res_struct.*")
    
    if df_res_p.count() == 0:
        continue
    
    processed_dfs.append(df_res_p)

# 5. 合并所有处理后的DataFrame
if processed_dfs:
    df_res = processed_dfs[0]
    for i in range(1, len(processed_dfs)):
        df_res = df_res.union(processed_dfs[i])
else:
    # 如果没有数据，创建一个空DataFrame
    schema = StructType([
        StructField("battery_id", StringType(), True),
        StructField("battery_type", StringType(), True),
        StructField("diagnosis_code", StringType(), True),
        StructField("diagnosis_features", StringType(), True)
    ])
    df_res = spark.createDataFrame([], schema)

df_res.show(100, truncate=False)

df_alarm_detail = df_alarm_detail_.join(df_res, df_alarm_detail_["object_id"] == df_res["battery_id"])

# 生成报告
df_res = df_alarm_detail.select(
    df_alarm_detail["hash_code"].cast(StringType()),
    df_alarm_detail["object_id"].alias("battery_id").cast(StringType()),
    df_alarm_detail["battery_type"].cast(StringType()),   
    df_alarm_detail["device_id"].cast(StringType()),
    df_alarm_detail["device_name_alarm"].alias('device_name').cast(StringType()),
    df_alarm_detail["algorithm_id"].cast(StringType()),
    df_alarm_detail["model_id"].alias("algorithm_model").cast(StringType()),
    df_alarm_detail["model_type"].alias("algorithm_model_type").cast(StringType()),
    df_alarm_detail["alarm_time"].cast(TimestampType()),
    df_alarm_detail["alarm_process_id"].cast(StringType()),
    df_alarm_detail["alarm_msg_type"].cast(StringType()),
    df_alarm_detail["alarm_data_source"].cast(StringType()),
    functions.lit("Algorithm_ap27").alias("diagnosis_model_id").cast(StringType()),
    functions.lit("Algorithm_ap27_params").alias("diagnosis_params_id").cast(StringType()),
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