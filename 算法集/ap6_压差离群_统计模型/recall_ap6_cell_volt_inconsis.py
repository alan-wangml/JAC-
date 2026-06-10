%pyspark
import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions
from pyspark.sql.functions import col
from pyspark.sql.types import *
import numpy as np
"""
按照车辆电池压差离群预警（统计模型）算法产生预警数据
:return: 电池压差离群预警结果存入Hive表
"""
# 创建SparkSession
spark = SparkSession.builder \
    .appName("AnalysingBatteryAlarms") \
    .config("spark.debug.maxToStringFields", "1000") \
    .config("spark.network.timeout", "1000") \
    .config("spark.shuffle.memoryFraction", "0.6") \
    .config("spark.default.parallelism", "6000") \
    .config("spark.sql.shuffle.partitions", "8000") \
    .enableHiveSupport() \
    .getOrCreate()

# 读取满足压差统计模型场景的数据
source_table = "saas_battery.d_i_battery_block_features"
dict_params = algorithm.dict_params
ap6_min_low_probe_temp_threshold = 10
ap6_block_msg_count_threshold = 3
ap6_soc_tag_threshold = [2, 17]
ap6_volt_quartiles_outlier_threshold = 12
ap6_volt_diff_75quartiles_threshold = [100, 100, 25, 25, 14, 14, 10]
ap6_real_soc_interval_threshold = [30, 50, 65]
ap6_volt_diff_threshold = 10
ap6_entropy_outlier = 0.3
ap6_operating_condition = ""
ap6_cell_type = ""

# 处理场景条件
if ap6_operating_condition == "null" or ap6_operating_condition == "":
    scene_query_str = ""
else:
    scene_type = list(ap6_operating_condition.split(","))
    scene_query_list = list()
    for scene in scene_type:
        scene_query_list.append(f"""event_type like '%{scene}%'""")
    scene_query_str = "AND (" + " OR ".join(scene_query_list) + ")"

# 处理电池类型条件
if ap6_cell_type == "null" or ap6_cell_type == "":
    cell_type_query_str = ""
else:
    cell_type_type = list(ap6_cell_type.split(","))
    cell_type_query_list = list()
    for cell_type in cell_type_type:
        cell_type_query_list.append(f"""cell_type = '{cell_type}'""")
    cell_type_query_str = "AND (" + " OR ".join(cell_type_query_list) + ")"

# 读取当天数据
sql_read = f"""
    select *, 'tsp' as source from {source_table}
    where dt = '20260501'
    and battery_type in ('GX-1P108S','21011E3C1','2101ZHM116')
    {scene_query_str}
    {cell_type_query_str}
    and max_volt_diff_volt_entropy IS NOT NULL
    and min_low_probe_temp >= {ap6_min_low_probe_temp_threshold}
    and block_msg_count > {ap6_block_msg_count_threshold}
    and soc_tag >= {ap6_soc_tag_threshold[0]}
    and (cell_type != 'LFP' or (cell_type = 'LFP' and soc_tag < {ap6_soc_tag_threshold[1]}))
    """
df_detail = spark.sql(sql_read)

# 计算block最大电流
df_detail = df_detail.na.fill({'max_discharge_current': 0, 'max_charge_current': 0})
df_detail = df_detail.withColumn("max_current",
                                 functions.when(df_detail["max_discharge_current"] > -df_detail["max_charge_current"],
                                                df_detail["max_discharge_current"])
                                 .otherwise(-df_detail["max_charge_current"]))
curr_tag_interval = 18
df_detail = df_detail.withColumn("curr_tag",
                                 functions.when(col("max_current") > curr_tag_interval, "hg_curr")
                                 .otherwise("lw_curr"))
df_detail = df_detail.fillna("null", subset=["battery_type", "event_type", "curr_tag"])

# 生成label标签
df_detail = df_detail.withColumn("label",
                                 functions.concat_ws("|", df_detail["battery_type"], df_detail["event_type"],
                                                     df_detail["soc_tag"], df_detail["curr_tag"], df_detail["cell_type"]))

# 计算每一个子空间的75分位数平均值和标准差
w1 = Window.partitionBy("label")
df_detail = df_detail.withColumn("volt_diff_pct_75", df_detail["volt_diff_quartiles"][75])
df_detail = df_detail.withColumn("label_mean", functions.mean(df_detail["volt_diff_pct_75"]).over(w1))
df_detail = df_detail.withColumn("label_std", functions.stddev(df_detail["volt_diff_pct_75"]).over(w1))

# 压差最大时刻的温度熵值离群判断
df_detail = df_detail.withColumn("is_select", functions.lit("1"))
w2 = Window.partitionBy("is_select")
df_detail = df_detail.withColumn("volt_entropy_mean",
                                 functions.mean(df_detail["max_volt_diff_volt_entropy"]).over(w2))
df_detail = df_detail.withColumn("volt_entropy_std",
                                 functions.stddev(df_detail["max_volt_diff_volt_entropy"]).over(w2))

# 异常筛出
df_detail = df_detail.filter(
    ((df_detail["volt_diff_pct_75"] > df_detail["label_mean"] + ap6_volt_quartiles_outlier_threshold * df_detail["label_std"])
     & (df_detail["max_volt_diff_volt_entropy"] < df_detail["volt_entropy_mean"] - ap6_entropy_outlier * df_detail["volt_entropy_std"]))
    | (df_detail["volt_diff_pct_75"] > ap6_volt_diff_75quartiles_threshold[0])
)

# 筛选每一块电池（取max_volt_diff最大且最早的一条）
w1 = Window.partitionBy("battery_id").orderBy(functions.desc("max_volt_diff"), functions.asc("start_time"))
df_detail = df_detail.withColumn("row_number", functions.row_number().over(w1))
df_detail = df_detail.filter(df_detail["row_number"] == 1)
df_detail = df_detail.withColumn("volt_diff_quartiles_75", df_detail["volt_diff_quartiles"][75])

df_detail = df_detail.select(
    df_detail["battery_id"], df_detail["battery_type"], df_detail["device_id"], df_detail["device_name"],
    df_detail["process_id"], df_detail["event_type"], df_detail["start_time"], df_detail["end_time"],
    df_detail["start_real_soc"], df_detail["start_current"], df_detail["avg_pack_voltage"],
    df_detail["avg_insu_resis"], df_detail["avg_high_cell_volt"], df_detail["avg_low_cell_volt"],
    df_detail["volt_diff_quartiles_75"], df_detail["max_volt_diff_volt_entropy"], df_detail["label_mean"],
    df_detail["label_std"], df_detail["label"], df_detail["source"], df_detail["volt_entropy_mean"],
    df_detail["volt_entropy_std"], df_detail["max_volt_diff"], df_detail["min_volt_diff"])

# 异常确认 - SOC区间过滤（条件1）
df_detail = df_detail.filter(f"""(start_real_soc >= {ap6_real_soc_interval_threshold[1]}
                                    and start_real_soc <= {ap6_real_soc_interval_threshold[2]}
                                    and volt_diff_quartiles_75 >= {ap6_volt_diff_75quartiles_threshold[2]}) 
                                or (start_real_soc >= {ap6_real_soc_interval_threshold[2]}
                                    and volt_diff_quartiles_75 > {ap6_volt_diff_75quartiles_threshold[4]}) 
                                or (start_real_soc <= {ap6_real_soc_interval_threshold[1]}
                                    and volt_diff_quartiles_75 > {ap6_volt_diff_75quartiles_threshold[5]})""")
# 异常确认 - SOC区间过滤（条件2）
df_detail = df_detail.filter(f"""(start_real_soc < {ap6_real_soc_interval_threshold[0]}
                                    and volt_diff_quartiles_75 >= {ap6_volt_diff_75quartiles_threshold[3]}) 
                                or (start_real_soc >= {ap6_real_soc_interval_threshold[0]}
                                    and volt_diff_quartiles_75 > {ap6_volt_diff_75quartiles_threshold[6]})""")
# 异常确认 - 事件类型过滤（条件3）
df_detail = df_detail.filter(f"""(event_type = 'parking'
                                    and (max_volt_diff - min_volt_diff <= {ap6_volt_diff_threshold}
                                         or volt_diff_quartiles_75 > {ap6_volt_diff_75quartiles_threshold[1]}))
                                or (event_type != 'parking')""")

column = [
    "battery_id", "battery_type", "device_id", "device_name", "process_id", "event_type",
    "start_time", "end_time", "start_real_soc", "start_current", "avg_pack_voltage",
    "avg_insu_resis", "avg_high_cell_volt", "avg_low_cell_volt", "volt_diff_quartiles_75",
    "max_volt_diff_volt_entropy", "label_mean", "label_std", "label", "source",
    "volt_entropy_mean", "volt_entropy_std", "max_volt_diff", "min_volt_diff"
]
df_detail = df_detail.select(column)
df_detail = df_detail \
    .withColumn("start_time", functions.unix_timestamp(df_detail["start_time"]).cast(IntegerType()))

print(df_detail.count())
