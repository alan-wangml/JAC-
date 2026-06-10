%pyspark
import sys
import math
import json
import datetime
import time
import uuid
import argparse
import requests
from urllib.parse import urljoin
from typing import Dict
from collections import defaultdict
from dateutil.parser import parse
import pandas as pd
import numpy as np
from scipy.interpolate import interp1d
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr, udf, sort_array
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
"""
电压智能诊断模块 - 专家模型
按照电池压差离群预警结果进行诊断，产生诊断数据
:return: 电池电压类诊断结果存入Hive表
"""
# 创建SparkSession, 作为读取数据、处理源数据、配置回话和管理集群资源的入口
spark = SparkSession.builder \
    .appName("jac_diagnosis_expert_voltage") \
    .config("spark.debug.maxToStringFields", "1000") \
    .config("spark.network.timeout", "1000") \
    .config("spark.shuffle.memoryFraction", "0.6") \
    .config("spark.sql.shuffle.partitions", "500") \
    .config("spark.driver.maxResultSize", "3g") \
    .config("spark.yarn.executor.memoryOverhead", "6144") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
    .enableHiveSupport() \
    .getOrCreate()  # prod

# 参数定义
dict_params = algorithm.dict_params
algorithm_list = algorithm.config_params["algorithm_config"]
enterprise_id = algorithm.enterprise_id
alarm_id = gen_alarm_id(algorithm.algorithm_id, algorithm.algorithm_params_id, input_date)
N_partition = 500

# 读取告警数据
input_date = "20260501"
etl_date = parse(str(input_date)) + datetime.timedelta(days=1)
etl_date = etl_date.strftime("%Y%m%d")

pd_algorithm_config = pd.DataFrame(algorithm_list, columns=["algorithm_id", "model_id", "enterprise_id", "model_type"])
df_algorithm_config = spark.createDataFrame(pd_algorithm_config)

alarm_table = "saas_battery.d_i_alarm_results"
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
  WHERE a.dt = '{input_date}' AND a.tenant_id = '{enterprise_id}'
"""
recalled_alarms_raw = spark.sql(alarm_sql_read)
window_spec = Window.partitionBy("object_id", "algorithm_instance").orderBy(col("result_timestamp").desc())
recalled_alarms = recalled_alarms_raw.join(df_algorithm_config, how='inner', on='algorithm_id')
recalled_alarms = recalled_alarms.withColumn("vehicle_state",
    F.when(col("vehicle_state").like("%parking%"), "parking")
     .when(col("vehicle_state").like("%charge%"), "charge")
     .when(col("vehicle_state").like("%journey%"), "journey")
     .when(col("vehicle_state").like("%discharge%"), "discharge")
     .otherwise(col("vehicle_state")))

# 解析设备名称
def get_device_name(alarm_detail):
    try:
        res = json.loads(alarm_detail)["device_name"]
    except:
        res = None
    return res
udf_get_device_name = udf(get_device_name, StringType())
recalled_alarms = recalled_alarms.withColumn("alarm_msg_type", col("vehicle_state")).withColumn(
    "device_name", udf_get_device_name("result_data"))
recalled_alarms = F.broadcast(recalled_alarms)

# 读取block数据
block_table = "saas_battery.d_i_battery_block_features"
max_back_days = max([params.get("ap28_date_back", 7) for params in dict_params.values()])
end_date = parse(str(input_date))
start_date = end_date - datetime.timedelta(days=max_back_days)
start_date = start_date.strftime("%Y%m%d")
end_date_ = end_date.strftime("%Y-%m-%d")

sql_read = f"""SELECT * FROM {block_table}
    WHERE dt >= '{start_date}' AND dt <= '{input_date}' 
    AND start_time is not null 
    AND soc_tag is not null
    AND cell_type is not null
"""
df_base = spark.sql(sql_read).drop('device_id', 'device_name').repartition(N_partition, 'battery_id')
df_detail = df_base.join(recalled_alarms, recalled_alarms["object_id"] == df_base["battery_id"])
df_detail = df_detail.withColumn("start_time_timestamp", F.unix_timestamp(col("start_time"), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("end_time_timestamp", F.unix_timestamp(col("end_time"), 'yyyy-MM-dd HH:mm:ss'))
df_detail.persist(StorageLevel.DISK_ONLY)

# ====== 工具函数 ======
def compute_regression(df):
    """计算线性回归斜率"""
    n_points = params.get('ap28_slope_n_points', 3)
    r2_threshold = params.get('ap28_ncm_r2_threshold', 0.8)
    window_spec = Window.partitionBy("battery_id").orderBy(F.desc("start_time_timestamp"))
    df = df.withColumn("rn", F.row_number().over(window_spec)).filter(F.col("rn") <= n_points)
    stats = df.groupBy("battery_id").agg(
        F.count("socgap").alias("n"),
        F.sum("socgap").alias("sum_x"),
        F.sum("start_time_timestamp").alias("sum_y"),
        F.sum(F.col("socgap") * F.col("start_time_timestamp")).alias("sum_xy"),
        F.sum(F.col("socgap") * F.col("socgap")).alias("sum_x2"),
        F.sum(F.col("start_time_timestamp") * F.col("start_time_timestamp")).alias("sum_y2"))
    stats = stats.filter(F.col("n") >= 2).withColumn("slope", F.expr(
        "(n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)"))
    stats = stats.withColumn("r_squared", F.expr(
        """case when (n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y) > 0
           then ((n * sum_xy - sum_x * sum_y) * (n * sum_xy - sum_x * sum_y)) 
                / ((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y))
           else 0 end"""))
    df_max_time = df.groupBy("battery_id").agg(F.max("end_time").alias("end_time"))
    return stats.join(df_max_time, "battery_id")

def v_diff_time_slope(input_df):
    """计算压差时间斜率"""
    back_days = params.get("ap28_date_back", 7)
    start_date_slope = (end_date - datetime.timedelta(days=back_days)).strftime("%Y-%m-%d")
    df_slope = input_df.filter(F.expr(f"end_time between '{start_date_slope} 00:00:00' and '{end_date_} 23:59:59'"))
    window_spec = Window.partitionBy("battery_id").orderBy(F.desc("end_time"))
    df_slope = df_slope.withColumn("rn", F.row_number().over(window_spec)).filter(F.col("rn") <= params.get('ap28_slope_n_points', 3))
    stats = df_slope.groupBy("battery_id").agg(
        F.count("max_volt_diff").alias("n"),
        F.sum("max_volt_diff").alias("sum_x"),
        F.sum("end_time_timestamp").alias("sum_y"),
        F.sum(F.col("max_volt_diff") * F.col("end_time_timestamp")).alias("sum_xy"),
        F.sum(F.col("max_volt_diff") * F.col("max_volt_diff")).alias("sum_x2"))
    stats = stats.filter(F.col("n") >= 2).withColumn("max_slope", F.expr(
        "(n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)"))
    return stats.select("battery_id", "max_slope")

# ====== 自放电判定 ======
def self_discharge_for_lfp_cell_type(input_df):
    """LFP电池自放电判定（基于SOC-OCV插值+回归slope）"""
    cell_type = 'lfp'
    vmax_thres_array = params.get(f"ap28_{cell_type}_batt_last_highest_cell_voltage_threshold_array")
    vgap_thres_array = params.get(f"ap28_{cell_type}_batt_volt_diff_maxlowest_threshold_array")
    vgap_2nd_thres_array = params.get(f"ap28_{cell_type}_batt_volt_diff_maxlower_threshold_array")
    duration_threshold = params.get("ap28_parking_event_duration_threshold")

    window_batt = Window.partitionBy("battery_id").orderBy("end_time")
    input_df = input_df.withColumn("prev_event_type", F.lag("event_type").over(window_batt))
    parking_df = input_df.filter(col('event_type') == 'parking')
    parking_df = parking_df.withColumn("is_new_group",
        F.when((col("prev_event_type").isNull()) | (col("prev_event_type") != "parking"), 1).otherwise(0))
    parking_df = parking_df.withColumn("group_id", F.sum("is_new_group").over(window_batt))
    data_detail = parking_df
    group_window = Window.partitionBy("battery_id", "group_id")
    time_desc_window = group_window.orderBy(F.desc("start_time"))
    data_with_total = data_detail.withColumn("group_start_time", F.min('start_time_timestamp').over(group_window))
    data_with_total = data_with_total.withColumn("group_end_time", F.max('end_time_timestamp').over(group_window))
    data_with_total = data_with_total.withColumn("group_last_time", (col("group_end_time") - col("group_start_time")))
    filtered_data = data_with_total.filter(
        (col("group_last_time") >= duration_threshold) &
        (col("last_pack_cell_voltage").isNotNull() & col("max_high_cell_volt").isNotNull())
    ).withColumn("filtered_voltage",
        udf(lambda arr: [x for x in arr if x != 0], ArrayType(IntegerType()))(F.col("last_pack_cell_voltage"))
    ).withColumn("sorted_voltage", sort_array(col("filtered_voltage"))).withColumn(
        'vmax', col("sorted_voltage").getItem(F.size(col("sorted_voltage")) - 1)).withColumn(
        "vmin", col("sorted_voltage").getItem(0)).withColumn(
        "vmin_2nd", col("sorted_voltage").getItem(1)).withColumn(
        "vgap", col("vmax") - col("vmin")).withColumn(
        "vgap_2nd", col("vmax") - col("vmin_2nd"))
    ranked_data = filtered_data.withColumn("rank", F.row_number().over(time_desc_window))
    parking_last_frame = ranked_data.filter(F.col("rank") == 1)
    parking_last_frame = parking_last_frame.withColumn("self_discharge", expr(f"""
        case when vmax between {vmax_thres_array[0]} and {vmax_thres_array[1]} and vgap >={vgap_thres_array[0]} and vgap_2nd <={vgap_2nd_thres_array[0]} then 'P0201'
             when vmax between {vmax_thres_array[1]} and {vmax_thres_array[2]} and vgap >={vgap_thres_array[1]} and vgap_2nd <={vgap_2nd_thres_array[1]} then 'P0201'
             when vmax between {vmax_thres_array[2]} and {vmax_thres_array[3]} and vgap >={vgap_thres_array[2]} and vgap_2nd <={vgap_2nd_thres_array[2]} then 'P0201'
             when vmax between {vmax_thres_array[3]} and {vmax_thres_array[4]} and vgap >={vgap_thres_array[3]} and vgap_2nd <={vgap_2nd_thres_array[3]} then 'P0201'
             when vmax between {vmax_thres_array[4]} and {vmax_thres_array[5]} and vgap >={vgap_thres_array[4]} and vgap_2nd <={vgap_2nd_thres_array[4]} then 'P0201'
        else NULL end
    """))
    valid_bid = parking_last_frame.filter(F.col("self_discharge") == 'P0201').select('battery_id').distinct()
    if valid_bid.rdd.isEmpty():
        return spark.createDataFrame([], schema=StructType([
            StructField("battery_id", StringType()), StructField("self_discharge", StringType()),
            StructField("cell_type", StringType()), StructField("slope", DoubleType()),
            StructField("end_time", StringType()), StructField("parking_time_intvl", StringType())]))
    parking_last_frame_step3 = filtered_data.join(valid_bid, on='battery_id', how='inner').filter(
        (col('start_time_timestamp') - col('group_start_time') >= duration_threshold) &
        ((col('vmax') >= params.get("ap28_lfp_socrate_volt_threshold_array")[0]) &
         (col('vmax') <= params.get("ap28_lfp_socrate_volt_threshold_array")[1])))
    lfp_soc_ocv_map = {k: v for v, k in params.get("ap28_lfp_soc_ocv_list")}
    x, y = list(lfp_soc_ocv_map.keys()), list(lfp_soc_ocv_map.values())
    interp_func = interp1d(x, y, kind='linear', fill_value='extrapolate')
    @udf(DoubleType())
    def map_soc_to_ocv_lfp(voltage):
        if voltage is None: return None
        if voltage in lfp_soc_ocv_map: return float(lfp_soc_ocv_map[voltage])
        return float(interp_func(voltage))
    parking_last_frame_step3 = parking_last_frame_step3.withColumn('socmax', map_soc_to_ocv_lfp(col('vmax'))).withColumn(
        'socmin', map_soc_to_ocv_lfp(col('vmin'))).withColumn('socgap', col('socmax') - col('socmin'))
    df_regression = compute_regression(parking_last_frame_step3).withColumn('self_discharge',
        F.when(((F.col('slope') >= params.get("ap28_lfp_socrate_threshold")[1]) &
                (F.col('r_squared') >= params.get("ap28_lfp_r2_threshold"))) |
               (F.col('slope') >= params.get("ap28_lfp_socrate_threshold")[0]), F.lit('P0201')
        ).otherwise(F.lit(None))).select('battery_id', 'slope', 'self_discharge', 'end_time')
    df_res = df_regression
    windowSpecMax = Window.partitionBy("battery_id").orderBy(F.col("start_time_timestamp").desc())
    df_with_max = parking_last_frame_step3.withColumn(
        "lfp_parking_time_1", F.first("start_time_timestamp").over(windowSpecMax)).withColumn(
        "Vmax1", F.first("vmax").over(windowSpecMax)).withColumn("is_candidate",
        F.when((F.abs(F.col("vmax") - F.col("Vmax1")) < 10), F.col("start_time_timestamp")).otherwise(None)
    ).withColumn("lfp_parking_time_2", F.min("is_candidate").over(Window.partitionBy("battery_id")))
    df_with_max = df_with_max.filter(F.col("start_time_timestamp") == F.col("lfp_parking_time_1")).withColumn(
        'parking_time_intvl', F.to_json(F.create_map(
            F.lit('lfp_parking_time_1'), F.date_format(F.col('lfp_parking_time_1').cast('timestamp'), "yyyyMMdd HH:mm:ss"),
            F.lit('lfp_parking_time_2'), F.date_format(F.col('lfp_parking_time_2').cast('timestamp'), "yyyyMMdd HH:mm:ss")
        ))).select('battery_id', 'parking_time_intvl')
    df_res = df_res.join(df_with_max, "battery_id", how="left").withColumn("cell_type", F.lit('LFP')) \
        .select('battery_id', 'self_discharge', 'cell_type', 'slope', 'end_time', 'parking_time_intvl')
    return df_res

def self_discharge_for_ncm_cell_type(input_df):
    """NCM电池自放电判定（基于SOC-OCV插值+回归slope）"""
    cell_type = 'ncm'
    vmax_thres_array = params.get(f"ap28_{cell_type}_batt_last_highest_cell_voltage_threshold_array")
    vgap_thres_array = params.get(f"ap28_{cell_type}_batt_volt_diff_maxlowest_threshold_array")
    vgap_2nd_thres_array = params.get(f"ap28_{cell_type}_batt_volt_diff_maxlower_threshold_array")
    duration_threshold = params.get("ap28_parking_event_duration_threshold")
    vgap_lowest_2_thres_array = params.get("ap28_ncm_batt_volt_diff_lowerlowest_threshold_array")

    window_batt = Window.partitionBy("battery_id").orderBy("end_time")
    input_df = input_df.withColumn("prev_event_type", F.lag("event_type").over(window_batt))
    parking_df = input_df.filter(col('event_type') == 'parking')
    parking_df = parking_df.withColumn("is_new_group",
        F.when((col("prev_event_type").isNull()) | (col("prev_event_type") != "parking"), 1).otherwise(0))
    parking_df = parking_df.withColumn("group_id", F.sum("is_new_group").over(window_batt))
    data_detail = parking_df
    group_window = Window.partitionBy("battery_id", "group_id")
    time_desc_window = group_window.orderBy(F.desc("start_time"))
    data_with_total = data_detail.withColumn("group_start_time", F.min('start_time_timestamp').over(group_window))
    data_with_total = data_with_total.withColumn("group_end_time", F.max('end_time_timestamp').over(group_window))
    data_with_total = data_with_total.withColumn("group_last_time", (col("group_end_time") - col("group_start_time")))
    filtered_data = data_with_total.filter(
        (col("group_last_time") >= duration_threshold) &
        (col("last_pack_cell_voltage").isNotNull() & col("max_high_cell_volt").isNotNull())
    ).withColumn('filtered_voltage',
        udf(lambda arr: [x for x in arr if x != 0], ArrayType(IntegerType()))(F.col("last_pack_cell_voltage"))
    ).withColumn("sorted_voltage", sort_array(col("filtered_voltage"))).withColumn(
        'vmax', col("sorted_voltage").getItem(F.size(col("sorted_voltage")) - 1)).withColumn(
        "vmin", col("sorted_voltage").getItem(0)).withColumn(
        "vmin_2nd", col("sorted_voltage").getItem(1)).withColumn(
        "vgap", col("vmax") - col("vmin")).withColumn(
        "vgap_2nd", col("vmax") - col("vmin_2nd"))
    ranked_data = filtered_data.withColumn("rank", F.row_number().over(time_desc_window))
    parking_last_frame = ranked_data.filter(F.col("rank") == 1)
    parking_last_frame = parking_last_frame.withColumn("vgap_lowest_2", col("vmin_2nd") - col("vmin")).withColumn(
        "self_discharge", expr(f"""
            case when vmax between {vmax_thres_array[0]} and {vmax_thres_array[1]} and vgap >={vgap_thres_array[0]} 
                    and (vgap_2nd <={vgap_2nd_thres_array[0]} or vgap_lowest_2>={vgap_lowest_2_thres_array[0]}) then 'P0201'
                 when vmax between {vmax_thres_array[2]} and {vmax_thres_array[3]} and vgap >={vgap_thres_array[1]} 
                    and (vgap_2nd <={vgap_2nd_thres_array[1]} or vgap_lowest_2>={vgap_lowest_2_thres_array[1]}) then 'P0201'
            else NULL end
        """))
    valid_bid_1 = parking_last_frame.filter(F.col("self_discharge") == 'P0201').select('battery_id').distinct()
    valid_bid_0 = parking_last_frame.withColumn('rk', F.row_number().over(
        Window.partitionBy("battery_id").orderBy(F.desc("end_time")))).filter(
        (F.col('rk') == 1) & (col('avg_volt_diff') < params.get("ap28_ncm_last_volt_diff_threshold_array")[1])
    ).withColumn('end_time', F.max('end_time').over(Window.partitionBy("battery_id"))).select(
        'battery_id', 'end_time', F.lit(None).alias('self_discharge')).distinct()
    valid_bid = valid_bid_1.join(valid_bid_0, on='battery_id', how='left_anti')
    parking_last_frame_step3 = filtered_data.join(valid_bid, on='battery_id', how='inner').filter(
        ((col('vmax') >= params.get("ap28_ncm_filter_volt_threshold_array")[0]) &
         (col('vmax') <= params.get("ap28_ncm_filter_volt_threshold_array")[1])) &
        ((col('start_time_timestamp') - col('group_start_time') >= duration_threshold)))
    parking_last_frame_step2 = valid_bid_0
    if not parking_last_frame_step3.head(1) and not parking_last_frame_step2.head(1):
        return spark.createDataFrame([], schema=StructType([
            StructField("battery_id", StringType()), StructField("self_discharge", StringType()),
            StructField("cell_type", StringType()), StructField("slope", DoubleType()),
            StructField("end_time", StringType()), StructField("parking_time_intvl", StringType())]))
    ncm_soc_ocv_map = {k: v for v, k in params.get("ap28_ncm_soc_ocv_list")}
    x, y = list(ncm_soc_ocv_map.keys()), list(ncm_soc_ocv_map.values())
    interp_func = interp1d(x, y, kind='linear', fill_value='extrapolate')
    @udf(DoubleType())
    def map_soc_to_ocv_ncm(voltage):
        if voltage is None: return None
        if voltage in ncm_soc_ocv_map: return float(ncm_soc_ocv_map[voltage])
        return float(interp_func(voltage))
    parking_last_frame_step3 = parking_last_frame_step3.withColumn('socmax', map_soc_to_ocv_ncm(col('vmax'))).withColumn(
        'socmin', map_soc_to_ocv_ncm(col('vmin'))).withColumn('socgap', col('socmax') - col('socmin'))
    df_regression = compute_regression(parking_last_frame_step3).withColumn('self_discharge',
        F.when(F.col('slope') >= params.get("ap28_ncm_socrate_threshold"), F.lit('P0201')
        ).otherwise(F.lit(None))).select('battery_id', 'slope', 'self_discharge', 'end_time')
    df_res = parking_last_frame_step2.unionByName(df_regression, allowMissingColumns=True)
    windowSpecMax = Window.partitionBy("battery_id").orderBy(F.col("start_time_timestamp").desc())
    df_with_max = parking_last_frame_step3.withColumn(
        "ncm_parking_time_1", F.first("start_time_timestamp").over(windowSpecMax)).withColumn(
        "Vmax1", F.first("vmax").over(windowSpecMax)).withColumn("is_candidate",
        F.when((F.abs(F.col("vmax") - F.col("Vmax1")) < 25), F.col("start_time_timestamp")).otherwise(None)
    ).withColumn("ncm_parking_time_2", F.min("is_candidate").over(Window.partitionBy("battery_id")))
    df_with_max = df_with_max.filter(F.col("start_time_timestamp") == F.col("ncm_parking_time_1")).withColumn(
        'parking_time_intvl', F.to_json(F.create_map(
            F.lit('ncm_parking_time_1'), F.date_format(F.col('ncm_parking_time_1').cast('timestamp'), "yyyyMMdd HH:mm:ss"),
            F.lit('ncm_parking_time_2'), F.date_format(F.col('ncm_parking_time_2').cast('timestamp'), "yyyyMMdd HH:mm:ss")
        ))).select('battery_id', 'parking_time_intvl')
    df_res = df_res.join(df_with_max, "battery_id", how="left").withColumn("cell_type", F.lit('NCM')) \
        .select('battery_id', 'self_discharge', 'cell_type', 'slope', 'end_time', 'parking_time_intvl')
    return df_res

# ====== 通用组诊断 ======
def generate_volt_alarm_general(alarms_df, df_alarm_event, v_diff_time_slope_df, self_discharge_df):
    """通用组诊断：15个特征标签匹配9类诊断代码"""
    max_volt_diff_threshold_array = params.get("ap28_max_volt_diff_threshold_array")
    avg_volt_diff_threshold = params.get("ap28_avg_volt_diff_threshold")
    soc_corr_threshold = params.get("ap28_event_volt_diff_soc_corr_threshold")
    current_corr_threshold = params.get("ap28_event_volt_diff_current_corr_threshold")
    entropy_threshold = params.get("ap28_event_volt_diff_entropy_threshold")
    m_sn_count = params.get("ap28_m_sn_count")
    sn_rate = params.get("ap28_sn_rate")
    sc_current_threshold = params.get("ap28_sc_current_threshold")

    # 提取告警事件特征
    event_cols = ["object_id", "process_id", "event_type", "cell_type"]
    event_window = Window.partitionBy(event_cols)

    df_event = df_alarm_event.groupBy(event_cols).agg(
        F.corr("max_volt_diff", "start_real_soc").alias("event_volt_diff_soc_pcorrelation"),
        F.corr("max_volt_diff", "start_current").alias("event_volt_diff_current_pcorrelation"),
        F.expr("approx_percentile(max_volt_diff_volt_entropy, 0.5, 100)").alias("event_volt_diff_entropy"),
        F.max("is_high_outlier_alarm").alias("is_high_outlier_alarm"),
        F.max("is_low_outlier_alarm").alias("is_low_outlier_alarm"),
        F.max("vmax_to_mean_rest_diff").alias("vmax_to_mean_rest_diff"),
        F.max("is_extre_unstable").alias("is_extre_unstable"),
        F.max("hg_sn_is_busbar").alias("hg_sn_is_busbar"),
        F.max("is_module_imbalance").alias("is_module_imbalance"),
        F.max("is_loop_volt_low_outlier").alias("is_loop_volt_low_outlier"),
        F.max("is_circ").alias("is_circ"),
        F.max("is_selfdch").alias("is_selfdch"),
        F.avg("avg_volt_diff").alias("block_avg_volt_diff"),
        F.max("max_volt_diff").alias("block_max_volt_diff"),
        F.max("end_time").alias("end_time"))

    # 合并斜率
    df_event = df_event.join(v_diff_time_slope_df, df_event["object_id"] == v_diff_time_slope_df["battery_id"], "left")

    # 特征标签判定
    diagnosis_code_pre = df_event.withColumn("is_event_volt_diff_soc_pcorrelation",
        F.when(F.abs(col("event_volt_diff_soc_pcorrelation")) > soc_corr_threshold, 1).otherwise(0)
    ).withColumn("is_event_volt_diff_current_pcorrelation",
        F.when(F.abs(col("event_volt_diff_current_pcorrelation")) > current_corr_threshold, 1).otherwise(0)
    ).withColumn("is_event_volt_diff_entropy",
        F.when(col("event_volt_diff_entropy") > entropy_threshold, 1).otherwise(0))

    # 诊断代码匹配
    diagnosis_code_pre = diagnosis_code_pre.withColumn("diagnosis_code_pre",
        F.when((col("is_event_volt_diff_current_pcorrelation") == 1) &
               (col("is_high_outlier_alarm") == 1) & (col("is_extre_unstable") == 1), "P0202")
         .when((col("is_event_volt_diff_soc_pcorrelation") == 1) &
               (col("is_high_outlier_alarm") == 1) & (col("is_extre_unstable") == 1), "P0203")
         .when((col("hg_sn_is_busbar") == 1) & (col("is_module_imbalance") == 1), "P0204")
         .when((col("hg_sn_is_busbar") == 1) & (col("is_loop_volt_low_outlier") == 1), "P0205")
         .when((col("is_low_outlier_alarm") == 1) & (col("is_extre_unstable") == 1), "P0206")
         .when(col("is_circ") == 1, "P0207")
         .when(col("is_event_volt_diff_entropy") == 1, "P0208")
         .when((col("is_high_outlier_alarm") == 1) & (col("is_low_outlier_alarm") == 1) &
               (col("is_extre_unstable") == 1), "P0301")
         .when((col("is_event_volt_diff_soc_pcorrelation") == 1) &
               (col("is_low_outlier_alarm") == 1), "P0309")
         .otherwise("P0001"))

    # 融合自放电结果
    diagnosis_code_pre = diagnosis_code_pre.join(
        self_discharge_df.selectExpr("battery_id as object_id", "self_discharge", "parking_time_intvl", "slope"),
        on="object_id", how="left")
    diagnosis_code_pre = diagnosis_code_pre.withColumn("diagnosis_code_pre",
        F.when(col("self_discharge").isNotNull(), "P0201").otherwise(col("diagnosis_code_pre")))

    # 计算事件压差信息
    df_event_info = df_alarm_event.selectExpr("battery_id", "event_type", "process_id", "cell_type", "battery_type",
        "start_time", "max_volt_diff", "volt_diff_quartiles[75] as volt_diff_75")
    window_time_aes = event_window.orderBy(F.asc("start_time"))
    window_time_desc = event_window.orderBy(F.desc("start_time"))
    df_event_info = df_event_info.withColumn("row_asc", F.row_number().over(window_time_aes)).withColumn(
        "row_desc", F.row_number().over(window_time_desc))
    df_event_info = df_event_info.withColumn("min_event_volt_diff",
        F.first(F.when(F.col("row_asc") == 1, F.col("volt_diff_75")), ignorenulls=True).over(window_time_aes)
    ).withColumn("max_event_volt_diff",
        F.first(F.when(F.col("row_desc") == 1, F.col("volt_diff_75")), ignorenulls=True).over(window_time_desc))
    df_event_info = df_event_info.selectExpr(*event_cols, "battery_type", "start_time", "volt_diff_75",
        "max_event_volt_diff", "min_event_volt_diff")
    df_event_info = df_event_info.withColumn("cal_start_time", F.min(col("start_time")).over(event_window)) \
        .withColumn("cal_end_time", F.max(col("start_time")).over(event_window))
    df_event_info = df_event_info.withColumn("delt_time",
        (col("cal_end_time").cast(DoubleType()) - col("cal_start_time").cast(DoubleType())) / (3600 * 24))
    df_event_info = df_event_info.withColumn("volt_diff_exp_rate",
        F.when(col("delt_time") > 0, (col("max_event_volt_diff") - col("min_event_volt_diff")) / col("delt_time")).otherwise(0))
    window_volt_diff_75_desc = event_window.orderBy(F.desc("volt_diff_75"))
    df_event_info = df_event_info.withColumn("row_num", F.row_number().over(window_volt_diff_75_desc))
    df_event_info = df_event_info.filter("row_num = 1").withColumnRenamed("cell_type", "event_cell_type")

    # 合并判断依据
    diagnosis_code_pre = diagnosis_code_pre.join(df_event_info
        .withColumnRenamed("battery_id", "object_id")
        .withColumnRenamed("process_id", "alarm_process_id")
        .withColumnRenamed("event_cell_type", "cell_type")
        .withColumnRenamed("event_type", "vehicle_state"),
        on=["object_id", "alarm_process_id", "cell_type", "vehicle_state"], how="left")

    # 诊断结果与降级
    diagnosis_res = diagnosis_code_pre.withColumn("diagnosis_code",
        F.when((col("diagnosis_code_pre") == "P0201") & (col("block_avg_volt_diff") <= avg_volt_diff_threshold), "P0211")
         .when(col("diagnosis_code_pre") == "P0201", "P0201")
         .when((col("diagnosis_code_pre") == "P0204") & (col("vmax_to_mean_rest_diff") <= max_volt_diff_threshold_array[3]), "P0212")
         .when((col("diagnosis_code_pre") == "P0204") & ((col("battery_type") == "102") | (col("battery_type") == "102-X")), "P0204")
         .when(col("diagnosis_code_pre") == "P0204", "P0212")
         .when((col("diagnosis_code_pre") == "P0205") & ((col("battery_type") == "102") | (col("battery_type") == "102-X")), "P0205")
         .when(col("diagnosis_code_pre") == "P0205", "P0001")
         .when((col("diagnosis_code_pre") == "P0202") & (col("vmax_to_mean_rest_diff") <= max_volt_diff_threshold_array[0]), "P0212")
         .when(col("diagnosis_code_pre") == "P0202", "P0202")
         .when((col("diagnosis_code_pre") == "P0203") & (col("vmax_to_mean_rest_diff") <= max_volt_diff_threshold_array[1]), "P0212")
         .when(col("diagnosis_code_pre") == "P0203", "P0203")
         .when((col("diagnosis_code_pre") == "P0309") & (col("block_max_volt_diff") <= max_volt_diff_threshold_array[2]), "P0212")
         .when(col("diagnosis_code_pre") == "P0309", "P0309")
         .when(col("diagnosis_code_pre") == "P0206", "P0206")
         .when(col("diagnosis_code_pre") == "P0207", "P0207")
         .when((col("diagnosis_code_pre") == "P0301") & (col("block_max_volt_diff") <= max_volt_diff_threshold_array[4]), "P0211")
         .when(col("diagnosis_code_pre") == "P0301", "P0301")
         .otherwise(col("diagnosis_code_pre")))

    # 75A电池优先保留P0201
    window_battery = Window.partitionBy("hash_code").orderBy(
        F.when(F.col("diagnosis_code") == "P0201", 1).otherwise(2))
    df_res_0 = diagnosis_res.filter(F.col('battery_type') == "195").withColumn(
        "rn", F.row_number().over(window_battery)).filter(F.col("rn") == 1).drop("rn")
    df_res_1 = diagnosis_res.filter(F.col('battery_type') != "195")
    diagnosis_res = df_res_0.unionByName(df_res_1)

    # 生成诊断特征JSON
    def alarm_feature(row):
        features = {}
        for field in ["event_volt_diff_soc_pcorrelation", "event_volt_diff_current_pcorrelation",
                      "event_volt_diff_entropy", "is_high_outlier_alarm", "is_low_outlier_alarm",
                      "vmax_to_mean_rest_diff", "is_extre_unstable", "hg_sn_is_busbar",
                      "is_module_imbalance", "is_loop_volt_low_outlier", "is_circ", "max_slope",
                      "is_selfdch", "block_avg_volt_diff", "block_max_volt_diff",
                      "max_event_volt_diff", "volt_diff_exp_rate"]:
            features[field] = row[field] if field in row and row[field] is not None else None
        return (row["hash_code"], row["cell_type"], json.dumps(features, ensure_ascii=False))

    schema = StructType([
        StructField("hash_code", StringType(), True),
        StructField("cell_type", StringType(), True),
        StructField("diagnosis_features", StringType(), True)])
    diagnosis_res_feature = diagnosis_res.rdd.map(alarm_feature)
    if diagnosis_res_feature.isEmpty():
        df_diagnosis_feature = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    else:
        df_diagnosis_feature = spark.createDataFrame(diagnosis_res_feature, schema)

    diagnosis_res = diagnosis_res.join(df_diagnosis_feature, on=["hash_code", "cell_type"], how="left")

    df_res = diagnosis_res.select(
        diagnosis_res["hash_code"].cast(StringType()),
        diagnosis_res["object_id"].alias("battery_id").cast(StringType()),
        diagnosis_res["battery_type"].cast(StringType()),
        diagnosis_res["device_id"].cast(StringType()),
        diagnosis_res["device_name"].cast(StringType()),
        diagnosis_res["algorithm_id"].cast(StringType()),
        diagnosis_res["algorithm_model"].cast(StringType()),
        diagnosis_res["algorithm_model_type"].cast(StringType()),
        diagnosis_res["alarm_time"].cast(TimestampType()),
        diagnosis_res["alarm_process_id"].cast(StringType()),
        diagnosis_res["alarm_msg_type"].cast(StringType()),
        diagnosis_res["alarm_data_source"].cast(StringType()),
        F.lit(algorithm.algorithm_id).alias("diagnosis_model_id").cast(StringType()),
        F.lit("expert_model").alias("diagnosis_model_type").cast(StringType()),
        F.lit(algorithm.algorithm_params_id).alias("diagnosis_params_id").cast(StringType()),
        F.lit(alarm_id).alias("diagnosis_instance").cast(StringType()),
        diagnosis_res["diagnosis_code"].cast(StringType()),
        F.lit(1).alias("diagnosis_prob").cast(DoubleType()),
        diagnosis_res["diagnosis_features"].cast(StringType()),
        F.current_timestamp().alias("update_time").cast(TimestampType()),
        F.current_timestamp().alias("dw_etl_time").cast(TimestampType()))
    return df_res

# ====== 主流程：按battery_type分组处理 ======
output_schema = StructType([
    StructField("hash_code", StringType(), True), StructField("battery_id", StringType(), True),
    StructField("battery_type", StringType(), True), StructField("device_id", StringType(), True),
    StructField("device_name", StringType(), True), StructField("algorithm_id", StringType(), True),
    StructField("algorithm_model", StringType(), True), StructField("algorithm_model_type", StringType(), True),
    StructField("alarm_time", TimestampType(), True), StructField("alarm_process_id", StringType(), True),
    StructField("alarm_msg_type", StringType(), True), StructField("alarm_data_source", StringType(), True),
    StructField("diagnosis_model_id", StringType(), True), StructField("diagnosis_model_type", StringType(), True),
    StructField("diagnosis_params_id", StringType(), True), StructField("diagnosis_instance", StringType(), True),
    StructField("diagnosis_code", StringType(), True), StructField("diagnosis_prob", DoubleType(), True),
    StructField("diagnosis_features", StringType(), True), StructField("update_time", TimestampType(), True),
    StructField("dw_etl_time", TimestampType(), True)])

battery_types = dict_params.keys()
processed_dfs = []

for battery_type in battery_types:
    params = dict_params.get(battery_type)
    if not params:
        continue
    back_days = params.get("ap28_date_back", 7)
    start_date_bt = (end_date - datetime.timedelta(days=back_days)).strftime("%Y-%m-%d")

    df_detail_p = df_detail.filter(expr(f"""battery_type = '{battery_type}' 
        and end_time between '{start_date_bt} 00:00:00' and '{end_date_} 23:59:59'"""))
    if df_detail_p.count() == 0:
        continue

    # 压差时间斜率
    v_diff_time_slope_df = v_diff_time_slope(input_df=df_detail_p)

    # LFP自放电
    self_discharge_res_lfp = self_discharge_for_lfp_cell_type(input_df=df_detail_p.filter(col('cell_type') == 'LFP'))
    self_discharge_res_lfp.persist(StorageLevel.MEMORY_AND_DISK_SER)

    # NCM自放电
    self_discharge_res_ncm = self_discharge_for_ncm_cell_type(input_df=df_detail_p.filter(col('cell_type') == 'NCM'))
    self_discharge_res_ncm.persist(StorageLevel.MEMORY_AND_DISK_SER)

    # 合并自放电结果
    union_df = self_discharge_res_lfp.union(self_discharge_res_ncm)
    window_spec_sd = Window.partitionBy("battery_id").orderBy(F.desc("slope"))
    mixed_df = union_df.withColumn("rn", F.row_number().over(window_spec_sd)).filter("rn = 1").drop("rn").filter(col('battery_id') != '')
    self_discharge = mixed_df.select("battery_id", "parking_time_intvl", "self_discharge", "end_time", "cell_type")
    slope_df = mixed_df.select("battery_id", "slope")
    v_diff_time_slope_df = v_diff_time_slope_df.join(slope_df, on="battery_id", how="outer").filter(col('battery_id') != '')

    df_alarm_event = df_detail_p.filter((col("process_id_tag") == col("process_id")) & (col("alarm_msg_type") == col("event_type")))
    df_alarm_event.persist(StorageLevel.MEMORY_AND_DISK_SER)

    df_res_p = generate_volt_alarm_general(
        alarms_df=recalled_alarms,
        df_alarm_event=df_alarm_event,
        v_diff_time_slope_df=v_diff_time_slope_df,
        self_discharge_df=self_discharge)
    if df_res_p.count() == 0:
        continue
    processed_dfs.append(df_res_p)

# 合并结果
if processed_dfs:
    df_res = processed_dfs[0]
    for i in range(1, len(processed_dfs)):
        df_res = df_res.union(processed_dfs[i])
else:
    df_res = spark.createDataFrame([], output_schema)

df_detail.unpersist()

# 写入目标表
target_table = "saas_battery.d_i_battery_diagnosis_results"
df_res = df_res.select(
    df_res["hash_code"].cast(StringType()), df_res["battery_id"].cast(StringType()),
    df_res["battery_type"].cast(StringType()), df_res["device_id"].cast(StringType()),
    df_res["device_name"].cast(StringType()), df_res["algorithm_id"].cast(StringType()),
    df_res["algorithm_model"].cast(StringType()), df_res["algorithm_model_type"].cast(StringType()),
    df_res["alarm_time"].cast(TimestampType()), df_res["alarm_process_id"].cast(StringType()),
    df_res["alarm_msg_type"].cast(StringType()), df_res["alarm_data_source"].cast(StringType()),
    df_res["diagnosis_model_id"].cast(StringType()), df_res["diagnosis_model_type"].cast(StringType()),
    df_res["diagnosis_params_id"].cast(StringType()), df_res["diagnosis_instance"].cast(StringType()),
    df_res["diagnosis_code"].cast(StringType()), df_res["diagnosis_prob"].cast(DoubleType()),
    df_res["diagnosis_features"].cast(StringType()),
    F.lit(algorithm.enterprise_id).alias("tenant_id").cast(StringType()),
    df_res["update_time"].cast(TimestampType()), df_res["dw_etl_time"].cast(TimestampType()))

tmp_name = "tmp_table_" + str(uuid.uuid4())[:8]
df_res.createOrReplaceTempView(tmp_name)
sql_overwrite = f"""
    INSERT OVERWRITE TABLE {target_table} PARTITION(dt='{input_date}', register_instance='{algorithm.algorithm_id}')
    SELECT * FROM {tmp_name}
"""
spark.sql(sql_overwrite)

print(df_res.count())
df_res.show(20, truncate=False)
