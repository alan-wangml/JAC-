%pyspark
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql import Window
from pyspark.sql.types import *
"""
按照车辆电池压差离群预警（压差偏高）算法产生预警数据
:return: 电池压差偏高预警结果存入Hive表
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

# 读取满足压差偏高场景的数据
source_table = "saas_battery.d_i_battery_block_features"
dict_params = algorithm.dict_params
ap4_msg_count_limit = 20
ap4_soc_tag_limit = 1
ap4_lfp_soc_tag_upper_limit = 17
ap4_high_volt_diff_count_limit = 1
ap4_operating_condition = ""
ap4_cell_type = ""

# 处理场景条件
if ap4_operating_condition == "null" or ap4_operating_condition == "":
    scene_query_str = ""
else:
    scene_type = list(ap4_operating_condition.split(","))
    scene_query_list = list()
    for scene in scene_type:
        scene_query_list.append(f"""event_type like '%{scene}%'""")
    scene_query_str = "AND (" + " OR ".join(scene_query_list) + ")"

# 处理电池类型条件
if ap4_cell_type == "null" or ap4_cell_type == "":
    cell_type_query_str = ""
else:
    cell_type_type = list(ap4_cell_type.split(","))
    cell_type_query_list = list()
    for cell_type in cell_type_type:
        cell_type_query_list.append(f"""battery_type = '{cell_type}'""")
    cell_type_query_str = "AND (" + " OR ".join(cell_type_query_list) + ")"

# 读取当天数据
sql_read = f"""
    select *, 'tsp' as source from {source_table}
    where dt = '20260501'
    and battery_type in ('GX-1P108S','21011E3C1','2101ZHM116')
    {scene_query_str}
    {cell_type_query_str}
    and (cell_type != 'LFP' or soc_tag < {ap4_lfp_soc_tag_upper_limit})
    and effect_msg_count > {ap4_msg_count_limit}
    and soc_tag > {ap4_soc_tag_limit}
    and high_volt_diff_count > {ap4_high_volt_diff_count_limit}
    """
df_detail = spark.sql(sql_read)

# 按battery_id分组，筛选每一块电池最高压差记录
w = Window.partitionBy("battery_id").orderBy(F.desc("high_volt_diff_count"))
df_detail = df_detail.withColumn("desc_count_num", F.row_number().over(w)).where("desc_count_num = 1")

column = [
    "battery_id", "battery_type", "process_id", "device_id", "device_name", "cell_type", "start_time",
    "start_real_soc", "start_current", "avg_pack_voltage", "avg_high_cell_volt", "avg_low_cell_volt",
    "avg_insu_resis", "event_type", "soc_tag", "effect_msg_count", "high_volt_diff_count", "max_volt_diff",
    "max_volt_diff_max_cell_voltage", "max_volt_diff_max_cell_voltage_sn",
    "source"
]
df_detail = df_detail.select(column)
df_detail = df_detail \
    .withColumn("start_time", F.unix_timestamp(df_detail["start_time"]).cast(IntegerType()))

print(df_detail.count())