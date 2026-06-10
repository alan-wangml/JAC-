#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# @Time    : 2025/08/22
# @Author  : Roc.han
"""
# name: ap22_otr_over_discharge_alarm.py
# from: saas_battery.d_i_battery_block_features
# to: saas_battery.d_i_alarm_results
# comments: 静置过放告警 AP22

import sys
import uuid
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
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
from typing import Dict, Optional
from urllib.parse import urljoin
import requests

global input_date, algorithm_id, algorithm_env, algorithm, algorithm_params_id, alarm_id


# 预警特征schema
alarmSchema = StructType([
    StructField("algorithm_id", StringType(), True),
    StructField("algorithm_params_id", StringType(), True),
    StructField("battery_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("process_id", StringType(), True),
    StructField("msg_type", StringType(), True),
    StructField("alarm_data", StringType(), True),
    StructField("hash_code", StringType(), True),
    StructField("result_create_time", LongType(), True),
    StructField("create_time", LongType(), True),
    StructField("update_time", LongType(), True),
    StructField("alarm_id", StringType(), True),
    StructField("data_source", StringType(), True),
    StructField("data_type", StringType(), True)
])


class NpEncoder(json.JSONEncoder):
    """
    转换numpy数据类型为python3数据类型
    """
    def default(self, obj):
        """
        数据类型转换
        :param obj: numpy 数据
        :return: Python3 数据
        """
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)


def custom_log(*args):
    """
    打印带有当前时间的日志信息。
    
    :param message: 要记录的日志信息字符串
    """
    now = datetime.datetime.now()
    formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
    message = " ".join(map(str, args))
    print(f"{formatted_time} - {message}")


def gen_alarm_id(alg_id, alg_params_id, create_date):
    """
    生成告警ID
    :param alg_id: 算法ID
    :param alg_params_id: 算法配置ID
    :param create_date: 告警的实际发生时间，格式如：20190101
    :return:
    """
    return '-'.join([str(uuid.uuid3(uuid.NAMESPACE_DNS, f'{alg_id}.{alg_params_id}.{create_date}'))])


def gen_hash_code(alarm_id, battery_id, device_id, raise_timestamp, *args):
    """
    生成单条告警的唯一hash code值
    :param alarm_id: algorithm_id, algorithm_params_id, date(eg. 20190101)
    :param battery_id: 电池ID
    :param device_id: 设备ID
    :param raise_timestamp: 时间戳（毫秒）
    :param args: 其他参数
    :return:
    """
    string = '.'.join([str(i) for i in ([alarm_id, battery_id, device_id, raise_timestamp] + list(args))])
    return str(uuid.uuid3(uuid.NAMESPACE_DNS, string)).replace('-', '')


def print_query(query, keyword="query"):
    print("####"*5+f"{keyword} starts"+"####"*5)
    print(query)
    print("####"*5+f"{keyword} ends"+"####"*5)
    print()


# 生成告警明细
def get_battery_alarm(alarm_group):
    """
    :param alarm_group: 初步过滤出满足条件的预警数据
    :return: res: 进一步过滤出的有效预警数据
    """
    res = pd.DataFrame(
        columns=(
            "algorithm_id", "algorithm_params_id", "battery_id", "device_id", "process_id", "msg_type", "alarm_data",
            "hash_code", "result_create_time", "create_time", "update_time", "alarm_id", "data_source", "data_type"
        )
    )

    column = [
            "battery_id", "battery_type", "device_id", "device_name", "process_id",
            "event_type", "start_time", "end_time", "start_user_soc", "start_current",
            "avg_pack_voltage", "avg_insu_resis", "avg_high_cell_volt", "avg_low_cell_volt",
            "min_low_cell_volt", "cell_type", "source"
        ]
    alarm_group = pd.DataFrame(alarm_group, columns=column)
    alarm_group = alarm_group.where(alarm_group.notnull(), None)
    alarm_group = alarm_group.sort_values("start_time")
    alarm_group = alarm_group.reset_index(drop=True)

    alarm_info = defaultdict(list)
    if alarm_group.shape[0] > 0:
        for i in range(alarm_group.shape[0]):
            battery_id = alarm_group.loc[i, "battery_id"]
            device_id = alarm_group.loc[i, "device_id"]
            process_id = alarm_group.loc[i, "process_id"]
            vehicle_state = alarm_group.loc[i, "event_type"]
            raise_timestamp = alarm_group.loc[i, "start_time"] * 1000
            dict_alarm_info = {
                "battery_id": battery_id,  # 电池编号
                "min_low_cell_volt": alarm_group.loc[i, "min_low_cell_volt"],  # 最小最低单体电压
                "cell_type": alarm_group.loc[i, "cell_type"],
                "device_id": device_id,  # 设备编号
                "device_name": alarm_group.loc[i, "device_name"],  # 设备名称
                "time_stamp": raise_timestamp,  # 采样时间
                "soc": alarm_group.loc[i, "start_user_soc"],  # soc
                "start_current": alarm_group.loc[i, "start_current"],  # 起始电流
                "avg_pack_voltage": alarm_group.loc[i, "avg_pack_voltage"],  # 平均总压
                "avg_high_cell_volt": alarm_group.loc[i, "avg_high_cell_volt"],  # 平均最高单体电压
                "avg_low_cell_volt": alarm_group.loc[i, "avg_low_cell_volt"],  # 平均最低单体电压
            }
            hash_code = gen_hash_code(alarm_id, battery_id, device_id, raise_timestamp)
            alarm_info["algorithm_id"].append(algorithm_id)
            alarm_info["algorithm_params_id"].append(algorithm_params_id)
            alarm_info["battery_id"].append(battery_id)
            alarm_info["device_id"].append(device_id)
            alarm_info["process_id"].append(process_id)
            alarm_info["msg_type"].append(vehicle_state)
            alarm_info["alarm_data"].append(json.dumps(dict_alarm_info, cls=NpEncoder))
            alarm_info["hash_code"].append(hash_code)
            alarm_info["result_create_time"].append(int(raise_timestamp / 1000))
            etl_time = int(time.time())
            alarm_info["create_time"].append(etl_time)
            alarm_info["update_time"].append(etl_time)
            alarm_info["alarm_id"].append(alarm_id)
            alarm_info["data_source"].append(alarm_group.loc[i, "source"].lower())
            alarm_info["data_type"].append(alarm_group.loc[i, "source"].upper())

        res = res.append(pd.DataFrame(alarm_info), ignore_index=True, sort=False)
        res = res.where(res.notnull(), None)
        if res.shape[0] == 0:
            return [[None] * 14]
        else:
            list_alarms = np.array(res).tolist()
            return list_alarms


def generate_battery_alarm_info(source_table, target_table):
    """
    产生电池过放预警结果并存入Hive表
    按battery_type分组处理，每个类型使用各自的参数阈值
    """
    # 读取算法参数
    dict_params = algorithm.dict_params
    battery_types = dict_params.keys()
    battery_types_str = ', '.join(["'" + i + "'" for i in battery_types])    
    # 1. 读取基础数据 - 筛选全天都是parking的电池
    sql_read = f"""
    WITH parking_only as (
        SELECT battery_id, sum(not_parking) as not_parkin_sum
        FROM (
            SELECT battery_id, if(event_type='parking',0,1) as not_parking 
            FROM {source_table}
            WHERE dt = '{input_date}' 
                    and battery_type in ({battery_types_str})
            GROUP by battery_id, if(event_type='parking',0,1) 
        )
        GROUP by battery_id
       HAVING not_parkin_sum=0
    )
    SELECT a.*, 'tsp' as source
    FROM (    
        SELECT a.*
            , row_number() over(partition by a.battery_id, a.cell_type order by min_low_cell_volt desc) as min_low_cell_volt_rk
          FROM {source_table} a
          JOIN parking_only  as b
            ON a.battery_id=b.battery_id
         WHERE a.dt = '{input_date}' 
            and battery_type in ({battery_types_str})
           AND a.min_low_cell_volt>0
    ) a
    WHERE min_low_cell_volt_rk=1 
    """
    
    print_query(sql_read, "base_query")
    df_base = spark.sql(sql_read)
    df_base.cache()
    custom_log(f"Base data count: {df_base.count()}")
    
    # 2. 按battery_type分组处理
    processed_dfs = []
    column = [
            "battery_id", "battery_type", "device_id", "device_name", "process_id",
            "event_type", "start_time", "end_time", "start_user_soc", "start_current",
            "avg_pack_voltage", "avg_insu_resis", "avg_high_cell_volt", "avg_low_cell_volt",
            "min_low_cell_volt", "cell_type", "source"
        ]
    
    for battery_type in battery_types:
        # 获取对应battery_type的参数
        params = dict_params.get(battery_type)
        if not params:
            continue
        
        custom_log(f"Processing battery_type: {battery_type}")
        
        # 提取参数
        ap22_lfp_overdch_voltage_threshold = int(params["ap22_lfp_overdch_voltage_threshold"])
        ap22_ncm_overdch_voltage_threshold = int(params["ap22_ncm_overdch_voltage_threshold"])
        
        # 过滤当前battery_type的数据
        df_detail = df_base.filter(f"battery_type = '{battery_type}'")
        
        # 过滤电压阈值
        df_detail = df_detail.filter(f"""(cell_type='LFP' and min_low_cell_volt<={ap22_lfp_overdch_voltage_threshold})
                                  or (cell_type='NCM' and min_low_cell_volt<={ap22_ncm_overdch_voltage_threshold})
                                 """)
        
        if df_detail.count() == 0:
            custom_log(f"No data found for battery_type: {battery_type}")
            continue
        
        df_detail = df_detail.select(column)
        processed_dfs.append(df_detail)
        custom_log(f"Finished processing battery_type: {battery_type}, record count: {df_detail.count()}")
    
    # 3. 合并所有处理后的DataFrame
    if processed_dfs:
        df_detail = processed_dfs[0]
        for i in range(1, len(processed_dfs)):
            df_detail = df_detail.union(processed_dfs[i])
        custom_log(f"Total processed records: {df_detail.count()}")
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
            StructField("avg_pack_voltage", DoubleType(), True),
            StructField("avg_insu_resis", DoubleType(), True),
            StructField("avg_high_cell_volt", DoubleType(), True),
            StructField("avg_low_cell_volt", DoubleType(), True),
            StructField("min_low_cell_volt", DoubleType(), True),
            StructField("cell_type", StringType(), True),
            StructField("source", StringType(), True)
        ])
        df_detail = spark.createDataFrame([], schema)

    df_detail = df_detail.withColumn("start_time", F.unix_timestamp(df_detail["start_time"]).cast(IntegerType()))

    def map_alarms(alarms):
        """
        车辆电池单体过压预警映射函数
        :param alarms: 计算得到的电池预警列表
        :return: 预警结果
        """
        if isinstance(alarms[1][0], list):
            return alarms[1]
        else:
            list_alarm = [[]]
            for alarm in alarms[1]:
                list_alarm[0].append(alarm)
            return list_alarm

    rdd_res = df_detail.select(column).rdd \
        .map(lambda x: (x["battery_id"], x)) \
        .groupByKey() \
        .mapValues(get_battery_alarm) \
        .flatMap(map_alarms)
    
    # 将生成的事件特征转化为spark DataFrame
    df_alarm = spark.createDataFrame(rdd_res, schema=alarmSchema)

    # 丢弃所有元素均为null的行, 并且将告警结果转化为指定的数据类型（对齐AP10/AP6）
    task_time = datetime.datetime.now()
    df_alarm = df_alarm.na.drop(how="all").filter(df_alarm["alarm_id"].isNotNull()) \
        .select(
        df_alarm["alarm_id"].cast(StringType()),
        F.lit(task_time).alias("task_time").cast(TimestampType()),
        F.lit(input_date).alias("data_time").cast(StringType()),
        df_alarm["data_source"].cast(StringType()),
        df_alarm["algorithm_id"].cast(StringType()),
        df_alarm["algorithm_params_id"].cast(StringType()),
        F.lit(algorithm.enterprise_id).alias("tenant_id").cast(StringType()),
        df_alarm["battery_id"].alias("object_id").cast(StringType()),
        df_alarm["hash_code"].cast(StringType()),
        df_alarm["result_create_time"].cast(TimestampType()),
        F.struct("device_id", "process_id", "msg_type", "data_type").alias("additional_data"),
        df_alarm["alarm_data"].cast(StringType()),
        F.lit(0).alias("notify_status").cast(IntegerType()),
        df_alarm["create_time"].cast(TimestampType()),
        df_alarm["update_time"].cast(TimestampType()),
        F.lit(F.current_timestamp()).cast(TimestampType()).alias("dw_etl_time")
    )

    # 将电池告警数据写入Hive数据表
    now_date = datetime.datetime.now().strftime('%Y-%m-%d').replace("-", "")
    now_date = parse(str(input_date)) + datetime.timedelta(days=1)
    now_date = now_date.strftime("%Y%m%d")
    
    df_alarm.show(10, False)
    df_alarm = df_alarm.repartition(1)

    # 写入alarm表
    tmp_name = "tmp_table_"+str(uuid.uuid4())[:8] 
    df_alarm.createOrReplaceTempView(tmp_name)
    sql_overwrite = f"""
        INSERT OVERWRITE TABLE {target_table} PARTITION(etl_date='{now_date}', dt='{input_date}', algorithm_instance='{algorithm_id}')
        SELECT * FROM {tmp_name}
    """
    print(sql_overwrite)
    spark.sql(sql_overwrite)
    custom_log("DONE")


def parse_rule(algorithm_params_) -> dict:
    """
    解析pandas DataFrame的规则数据，生成{ruleCode: 解析后ruleValue}的字典
    :param df: 输入DataFrame，必须包含ruleCode, ruleValue, ruleType三个字段
    :return: 解析后的规则字典
    """
    # 初始化结果字典
    rule_result = {}
    
    # 遍历DataFrame每一行处理
    for row in algorithm_params_:
        # 提取字段值，处理空值（None/NaN）
        rule_code = row.get("ruleCode")
        rule_value = row.get("ruleValue")
        rule_type = row.get("ruleType")
        
        # 跳过ruleCode为空的行（无键则无法存入字典）
        if pd.isna(rule_code) or rule_code == "":
            continue
        
        # 统一处理ruleValue空值
        if pd.isna(rule_value):
            rule_result[str(rule_code)] = 'null'
            continue
        
        # 统一转换为字符串（避免原始值为数字/其他类型的干扰）
        rule_value_str = str(rule_value).strip()
        # 统一转换为小写（兼容RuleType/INT等不规范写法）
        rule_type = str(rule_type).strip().lower() if not pd.isna(rule_type) else "string"

        # 1. 处理int类型：直接转换为整数
        if rule_type == "int":
            try:
                parsed_val = int(rule_value_str)
            except (ValueError, TypeError):
                parsed_val = None
        
        # 2. 处理double类型：转换为浮点数
        elif rule_type == "double":
            try:
                parsed_val = float(rule_value_str)
            except (ValueError, TypeError):
                parsed_val = None
        # 3. 处理不支持的类型：默认按string规则解析
        else:
            try:
                parsed_val = json.loads(rule_value_str)
            except (json.JSONDecodeError, TypeError):
                parsed_val = rule_value_str
        
        # 将解析结果存入字典
        rule_result[str(rule_code)] = parsed_val
    
    return rule_result


def call_algorithm_api(host: str, algorithm_id: str) -> Dict:
    """
    调用GET接口获取算法默认参数JSON（基于Host动态拼接接口地址）
    :param host: 接口服务Host地址（含协议+域名/IP+端口
    :param algorithm_id: 算法ID（必传）
    :return: 接口返回的默认参数字典
    :raises: requests.RequestException(网络错误)、ValueError(接口返回非200/非JSON)
    """
    # 接口固定路径
    API_PATH = "/gateway/openapiservice/api/basic/ruleTargetConfigNio/getList"
    # 动态拼接完整接口地址
    full_api_url = urljoin(host, API_PATH)
    # 构造GET请求参数
    params = {"algorithmId": algorithm_id}
    try:
        # 发送GET请求，设置超时时间
        response = requests.get(full_api_url, params=params, timeout=10)
        response.raise_for_status()  # 非200状态码抛出异常
        default_params = response.json()  # 解析JSON响应
        data = default_params.get('data')
        algorithm_params_dict = {}
        for part_param in data:
            if part_param.get('batteryModel'):
                algorithm_params_ = part_param.get('params')
                algorithm_params = parse_rule(algorithm_params_)
                algorithm_params_dict[part_param.get('batteryModel')] = algorithm_params
        if not algorithm_params_dict:
            raise ValueError(f"未找到batteryModel的参数模板，请检查。")
        return algorithm_params_dict  # 成功获取并返回参数
    except Exception as e:
        custom_log(f"从接口获取算法{algorithm_id} 参数失败：{str(e)}")
        return {}


def create_algorithm(algorithm_id, algorithm_params):
    """
    创建算法实例
    :param algorithm_id: 算法ID
    :param algorithm_params: 算法参数字典 {battery_type: {param: value}}
    :return: 算法实例
    """
    class MyClass:
        def __init__(self, algorithm_id, algorithm_params):
            # 实例属性（每个实例独立拥有）
            self.algorithm_id = algorithm_id
            self.dict_params = algorithm_params
            self.algorithm_params_id = algorithm_id + "_params"
            self.enterprise_id = "JAC"
            # 必要参数校验
            self.necessary_params = {"ap22_lfp_overdch_voltage_threshold", "ap22_ncm_overdch_voltage_threshold"}
            self.validate_necessary_params()

        def validate_necessary_params(self):
            for battery_model in list(self.dict_params.keys()):
                if not self.necessary_params.issubset(self.dict_params.get(battery_model, {}).keys()):
                    print(f"necessary_params must be a subset of default_params for battery_model {battery_model}")
                    self.dict_params.pop(battery_model)
            if self.dict_params == {}:
                raise ValueError("No valid battery_model with necessary_params")
    
    # 创建实例
    algorithm = MyClass(algorithm_id, algorithm_params)
    return algorithm


def parse_args():
    parser = argparse.ArgumentParser(description="Parse command line arguments and call algorithm API (with Host).")

    # 原有位置参数（必须提供）
    parser.add_argument("date", type=str, help="The date in YYYYMMDD format")
    parser.add_argument("hh", type=str, help="The hour in HH format")

    # 核心参数：新增Host地址 + 必选算法ID
    parser.add_argument("-H", "--host", 
                        type=str, 
                        default="http://10.231.7.200:34433",
                        help="API service host (include protocol, domain/IP, port)")
    parser.add_argument("-a", "--algorithm_id", 
                        type=str, 
                        help="Algorithm_ID identifier (required)")
    
    # 原有环境参数
    parser.add_argument("--algorithm_env", type=str, default="prod")
    parser.add_argument("--prod", action="store_true", help="Production mode")
    parser.add_argument("--test", action="store_true", help="Test mode")
    parser.add_argument("--dev", action="store_true", help="Development mode")

    args = parser.parse_args()

    # 环境模式校验：必须且只能选其一
    env_flags = [args.prod, args.test, args.dev]
    if sum(env_flags) == 0:
        raise ValueError("You must specify either --prod or --test or --dev")
    if sum(env_flags) > 1:
        raise ValueError("You cannot specify multiple modes at the same time")
    
    return args


if __name__ == "__main__":
    """
    入口函数 - 支持按battery_type分组处理
    """
    # 获取算法输入参数
    custom_log(f"Starting {__file__} with arguments: {sys.argv[1:]}")
    start_time = time.time()
    # sys.argv = ["", "20260303", "", "--host", "http://10.231.7.200:34433", "--algorithm_id", "Algorithm_ap22", "--test"]
    args = parse_args()
    ALGORITHM_ID = "Algorithm_ap22"

    input_date = args.date
    test_flag = args.test
    algorithm_id = args.algorithm_id or ALGORITHM_ID
    algorithm_env = args.algorithm_env

    # 从API获取参数
    default_params = {}
    try:
        default_params = call_algorithm_api(
            host=args.host,
            algorithm_id=algorithm_id
        )
        if default_params:
            print(f"成功从Host【{args.host}】获取算法{algorithm_id}的参数模板：\n{default_params}")
    except ValueError as e:
        raise SystemExit(f"脚本执行失败：{str(e)}")

    algorithm_id = args.algorithm_id or ALGORITHM_ID
    algorithm_env = args.algorithm_env
    algorithm = create_algorithm(algorithm_id, default_params)
    algorithm_params_id = algorithm.algorithm_params_id
    alarm_id = gen_alarm_id(algorithm_id, algorithm_params_id, input_date)

    source_table = "saas_battery.d_i_battery_block_features"
    target_table = "saas_battery.d_i_alarm_results"
    # 创建SparkSession, 作为读取数据、处理源数据、配置回话和管理集群资源的入口
    spark = SparkSession.builder \
        .appName("AnalysingBatteryAlarms") \
        .config("spark.debug.maxToStringFields", "1000") \
        .config("spark.network.timeout", "1000") \
        .config("spark.shuffle.memoryFraction", "0.6") \
        .config("spark.default.parallelism", "6000") \
        .config("spark.sql.shuffle.partitions", "8000") \
        .enableHiveSupport() \
        .getOrCreate()

    generate_battery_alarm_info(source_table, target_table)
    end_time = time.time()
    custom_log(f"Finished {sys.argv[0].split('/')[-1]} with arguments: {sys.argv[1:]}, total time: {(end_time - start_time)//60} minutes")
