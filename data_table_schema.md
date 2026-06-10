# 数据表结构信息

## 表1: dw_battery.h_i_pt_tsp_battery_detail
### 表说明
车辆实时数据表，上传频率为5s，分区字段date、hour，保留近30天数据

### 字段列表

| col_name | data_type | comment |
|---------|-----------|---------|
| alarmCommon | map<tinyint,tinyint> | 通用报警 |
| batteryId | string | 电池id |
| bmsAvgCellVolt | decimal(38,18) | 单体平均电压 |
| bmsAvgTemp | decimal(38,18) | 电池包平均温度 |
| bmsChrgPwrLmt | decimal(38,18) | 动态可充功率限值 |
| bmsDischrgPwrLmt | decimal(38,18) | 动态可放功率限值 |
| bmsHealthStatus | decimal(38,18) | 电池健康状态 |
| bmsInCoolantTemp | decimal(38,18) | 进水温度 |
| bmsIsolationLevel | smallint | 电池绝缘等级 |
| bmsOutCoolantTemp | decimal(38,18) | 出水温度 |
| btryPakSn | smallint | 电池包序列号 |
| btryReEncoding | string | 法规电池包编码 |
| city | string | 市 |
| drivingAclrtnPedalPosn | decimal(38,18) | 加速器踏板位置 |
| drivingAverageSpeed | decimal(38,18) | 一个上传区间内的平均车速 |
| drivingBrkPedalState | smallint | 制动踏板踩踏状态NOT_PRESSED = 0;PRESSED = 1;RESERVERD = 2;ERROR = 3; |
| drivingBrkPedalValid | smallint | 制动踏板有效状态BRK_PED_VALID = 0;BRK_PED_INVALID = 1; |
| drivingMaxSpeed | decimal(38,18) | 一个上传区间内的最高车速 |
| drivingMinSpeed | decimal(38,18) | 一个上传区间内的最低车速 |
| drivingSteerWhlRotnAg | decimal(38,18) | 方向盘转角 |
| drivingSteerWhlRotnSpd | smallint | 方向盘转动速度 |
| drivingVehDrvgMod | smallint | 驾驶模式0 = automatic mode, 1 = economy mode, 2= comfort mode, 3 = sport mode, 7 = invalid |
| extremumHighestTemp | decimal(38,18) | 最高温度值 |
| extremumHistTempBtrySbsysSn | smallint | 最高温度子系统号 |
| extremumHistTempPrbSn | smallint | 最高温度探针单体代号 |
| extremumHistVoltBtrySbsysSn | smallint | 高电压电池子系统号 |
| extremumHistVoltSinglBtrySn | smallint | 最高电压电池单体代号 |
| extremumLowestTemp | decimal(38,18) | 最低温度值 |
| extremumLwstTempBtrySbsysSn | smallint | 最低温度子系统号 |
| extremumLwstTempPrbSn | smallint | 最低温度探针单体代号 |
| extremumLwstVoltBtrySbsysSn | smallint | 最低电压电池子系 |
| extremumLwstVoltSinglBtrySn | smallint | 最低电压电池单体代号 |
| extremumSinBtryHistVolt | decimal(38,18) | 电池单体电压最高值 |
| extremumSinBtryLwstVolt | decimal(38,18) | 电池单体电压最低值 |
| hvacAirConOn | boolean | 空调运行状态 |
| hvacAmbTempC | float | 车内温度 |
| hvacOutsideTempC | float | 车外温度 |
| hvacPm2p5Cabin | smallint | 车内PM 2.5 |
| hvacPm2p5FilterActive | boolean | PM2.5净化器开启状态 |
| iccId | string | SIM卡ICCID号 |
| local_info | string | 城市 |
| msgType | string | 事件类型 |
| processId | string | 过程id |
| province | string | 省 |
| sampleTs | timestamp | 采样时间 |
| socBtryCap | decimal(38,18) | 电池能力,动力电池标称总能量 |
| socBtryPakCurnt | decimal(38,18) | 可充电储能装置电流 |
| socBtryPakHistTemp | decimal(38,18) | 电池包最高温度值 |
| socBtryPakLwstTemp | decimal(38,18) | 电池包最低温度值 |
| socBtryPakSn | smallint | 动力蓄电池包序号(SH)/ 可充电储能子系统号(GB) |
| socBtryPakSum | smallint | 电池包数 |
| socBtryPakVoltage | decimal(38,18) | 可充电储能装置电压 |
| socBtryQualActvtn | boolean | 实时电耗 |
| socChrgFinalSoc | smallint | 充电截止soc |
| socChrgState | tinyint | 充电状态0 = no charging, 1 = charge processing, 2 = charge complete, 3 = charge fault |
| socDumpEnrgy | decimal(38,18) | 剩余电量 |
| socFrmStartBtrySn | int | 本帧起始电池序号 |
| socHivoltBtryCurnt | decimal(38,18) | 高压电池电流 |
| socPrbTempLst | array<smallint> | 可充电储能子系统各温度探针检测到的温度值列表 |
| socRealtimePowerConsumption | int | 电池均衡激活 |
| socRemainingRange | decimal(38,18) | 剩余里程 |
| socSinBtryHistTemp | smallint | 单体最高温度 |
| socSinBtryLwstTemp | smallint | 单体最低温度 |
| socSinBtryQuntyOfFrm | smallint | 本帧单体电池总数 |
| socSinBtryQuntyOfPak | int | 单体电池总数 |
| socSinBtryVoltLst | array<decimal(38,18)> | 单体电池电压 |
| socSoc | decimal(38,18) | 电池剩余电量百分比 |
| socTempPrbQunty | int | 可充电储能温度探针个数 |
| vehicleAclrtnPedalPosn | decimal(38,18) | 加速器踏板位置 |
| vehicleBrkPedalState | smallint | 制动踏板踩踏状态 |
| vehicleBrkPedalValid | smallint | 制动踏板有效状态 |
| vehicleChrgState | smallint | 充电状态1 = parking charging, 2 = driving charging, 3 = not charging, 4 = charge complete, 254 = abnormal, 255 = invalid |
| vehicleDcDcSts | smallint | DC-DC状态1 = working, 2 = disconnected, 254 = abnormal, 255 = invalid |
| vehicleGear | tinyint | 挡位 |
| vehicleId | string | 车ID |
| vehicleInsulatnResis | int | 绝缘电阻 |
| vehicleMileage | int | 累计里程 |
| vehicleOprtnMode | smallint | 运行模式1 = pure electric, 2 = hydride, 3 = fuel, 254 = abnormal, 255 = invalid |
| vehicleSoc | decimal(38,18) | soc |
| vehicleSpeed | decimal(38,18) | 车速 |
| vehicleUrgtPrwShtdwn | boolean | 紧急下电请求 |
| vehicleVehlState | smallint | 车辆状态1= driving, 2 = parked vehicle, 3 = driver present, 4 = sw update, 254 = abnormal, 255 = invalid |
| vehicleVehlTotlCurnt | decimal(38,18) | 总电流 |
| vehicleVehlTotlVolt | decimal(38,18) | 总电压 |
| vin | string | 车辆VIN码(17位) |
| chargingEstimateChrgTime | int | 预估充满电剩余时间 |
| chargingChargerType | tinyint | 充电类型NO_REQUEST = 0;NORMAL = 1;AC = 2;DC = 3;POWER_EXPRESS = 4;INVALID = 5; |
| chargingInVoltAc | decimal(38,18) | 充电交流输入电压 |
| chargingInVoltDc | decimal(38,18) | 充电直流输入电压 |
| bmsBatteryPackCap | double | 锁电类型 |
| vcuActvDchaCmdPeuf | double | 前PEU的相关信息 |
| vcuActvDchaCmdPeur | double | 后PEU的相关信息 |
| bmsSoc | double | 电池真实soc |
| obcmChgStatus | double | OBCM充电状态 |
| ptcTotActPwr | double | PTC开启功率 |
| comprActPwr | double | 空调压缩机开启功率 |
| lvBattU | double | 低压电池电压 |
| lvBattSoc | double | 低压电池soc |
| bmsCellVoltgMaxB | double | BMS(2nd package) Maximum Cell Voltage |
| bmsCellVoltgMinB | double | BMS(2nd package) Minimum Cell Voltage |
| bmsMaximumCellVoltageNumberB | double | b电芯最大电压对应单体编号 |
| bmsMinimumCellVoltageNumberB | double | b电芯最小电压对应单体编号 |
| bmsMinimumTempNumberB | double | b电芯最小温度对应单体编号 |
| bmsTempMaxB | double | ESS Inlet Temperature Maximum (2nd package) |
| bmsTempMinB | double | ESS Inlet Temperature Minimum (2nd package) |
| bmsMaximumTempNumberB | double | b电芯最大温度对应单体编号 |
| bmsEssInletTemp | double | 车端冷却液进水温度 |
| bmsEssOutletTemp | double | 车端冷却液出水温度 |
| platformType | int | 平台区分标识，0=nt1,1=nt2 |
| bmsCntctrSts | double | 电池包内继电器信号 |
| regionCode | string | 地区编码 |
| vehicleVersion | string | 车机版本号 |
| vehicleVersionLarge | string | 车机大版本号 |
| date | string | 日期分区 |
| hour | string | 小时分区 |

---

## 表2: dw_battery.h_i_oss_battery
### 表说明
换电站实时数据表，上传频率为10s，分区字段date、hour，保留近30天数据
### 字段列表

| col_name | data_type | comment |
|---------|-----------|---------|
| avg_battery_pack_temperature | float | 电池包平均温度 |
| avg_cell_voltage | float | 电池包平均电压 |
| battery_id | string | 电池id |
| battery_status | tinyint | 电池状态 |
| bms_battery_rated_capacity | float | 电池真实容量 |
| bms_charge_power_limit_dynamic | float | 动态可充功率限值 |
| bms_discharge_power_limit_dynamic | float | 动态可放功率限值 |
| bms_insulation_resistance_value | int | 绝缘阻值 |
| bms_normal_capacity | float | BMS标准容量 |
| bms_pack_energy_available | float | 电池包可用能量 |
| bms_request_charge_current | float | BMS请求充电电流 |
| bms_version | string | BMS版本号 |
| charging_end_time | bigint | 充电结束时间 |
| charging_start_time | bigint | 充电开始时间 |
| current | float | 电流 |
| customer_usage_soc | float | 用户使用SOC |
| cycle_ix | bigint | 循环次数索引 |
| device_id | string | 设备ID |
| device_type | string | 设备类型 |
| expected_charging_ready_time | bigint | 预计充电就绪时间 |
| fault_level | tinyint | 故障等级 |
| hardware_version | string | 硬件版本 |
| inlet_water_temperature | float | 进水温度 |
| is_current_error_eq_0 | tinyint | 电流错误是否为0 |
| is_soc_jumping | tinyint | SOC是否跳变 |
| is_soc_jumping_slot | tinyint | SOC跳变时间段 |
| is_temp_gt_50 | tinyint | 温度是否大于50度 |
| is_temp_lt_minus_30 | tinyint | 温度是否小于-30度 |
| is_voltage_eq_0 | tinyint | 电压是否为0 |
| is_voltage_gt_410 | tinyint | 电压是否大于410V |
| is_voltage_jumping | tinyint | 电压是否跳变 |
| is_voltage_jumping_slot | tinyint | 电压跳变时间段 |
| is_voltage_lt_250 | tinyint | 电压是否小于250V |
| isolation_level | tinyint | 绝缘等级 |
| max_battery_pack_temperature | float | 电池包最高温度 |
| max_battery_pack_temperature_number | smallint | 电池包最高温度编号 |
| max_cell_voltage | float | 单体最高电压 |
| max_cell_voltage_number | smallint | 单体最高电压编号 |
| min_battery_pack_temperature | float | 电池包最低温度 |
| min_battery_pack_temperature_number | smallint | 电池包最低温度编号 |
| min_cell_voltage | float | 单体最低电压 |
| min_cell_voltage_number | smallint | 单体最低电压编号 |
| outlet_water_temperature | float | 出水温度 |
| power | float | 功率 |
| slot_id | string | 仓位号 |
| soc | float | 荷电状态 |
| soh | float | 健康状态 |
| status | int | 状态 |
| target_soc | float | 目标SOC |
| time | string | 时间 |
| time_stamp | bigint | 时间戳 |
| vehicle_plate_number | string | 车牌号 |
| voltage | float | 电压 |
| is_chargeable | boolean | 是否可充电 |
| is_high_voltage_locked | boolean | 是否高压互锁 |
| is_balanced | boolean | 是否开启均衡 |
| battery_bms_state | int | 电池BMS状态 |
| battery_charge_state | smallint | 电池充电状态 |
| battery_charge_mode | smallint | 电池充电模式 |
| is_disconnect_high_voltage_relay_requested | boolean | 是否请求断开高压继电器 |
| is_battery_charge_ready | boolean | 电池是否准备充电 |
| is_battery_charge_permit | boolean | 电池是否允许充电 |
| is_battery_reach_target_soc | boolean | 电池是否达到目标SOC |
| is_battery_reach_target_voltage | boolean | 电池是否达到目标电压 |
| is_battery_beach_target_cell_voltage | boolean | 电池是否达到目标单体电压 |
| bms_battery_rated_voltage | float | BMS电池额定电压 |
| bms_battery_nominal_energy | float | BMS电池标称能量 |
| bms_battery_max_permitted_voltage | float | BMS电池最大允许电压 |
| bms_battery_max_permitted_current | float | BMS电池最大允许电流 |
| bms_battery_max_permitted_cell_voltage | float | BMS电池最大允许单体电压 |
| bms_battery_max_permitted_temperature | float | BMS电池最大允许温度 |
| bms_battery_min_permitted_voltage | float | BMS电池最小允许电压 |
| bms_request_charge_voltage | float | BMS请求充电电压 |
| bms_charge_current_limit | float | BMS充电电流限值 |
| chargeable_energy | float | 可充电能量 |
| charged_energy | float | 已充电能量 |
| discharged_energy | float | 已放电能量 |
| bms_cell_temperature_standard_deviation | int | BMS电芯温度标准差 |
| discharging_start_timestamp | bigint | 放电开始时间戳 |
| battery_captured_fault | int | 电池捕获故障 |
| battery_abort_controls_reason | int | 电池中止控制原因 |
| bms_battery_dynamic_discharging_power_limit | float | BMS电池动态放电功率限值 |
| bms_battery_long_time_discharging_power_limit | float | BMS电池长时间放电功率限值 |
| bms_battery_short_time_discharging_power_limit | float | BMS电池短时间放电功率限值 |
| bms_battery_dynamic_charging_power_limit | float | BMS电池动态充电功率限值 |
| bms_battery_long_time_charging_power_limit | float | BMS电池长时间充电功率限值 |
| bms_battery_short_time_charging_power_limit | float | BMS电池短时间充电功率限值 |
| bms_battery_pack_capacity | int | BMS电池包容量 |
| bms_battery_dc_charging_voltagemax | float | BMS电池直流充电最大电压 |
| bms_battery_dc_total_voltagemax | float | BMS电池直流总最大电压 |
| bms_battery_inner_high_voltage_connector_status | int | BMS电池内部高压连接器状态 |
| bms_battery_stop_charging_soc | float | BMS电池停止充电SOC |
| bms_connection_state | smallint | BMS连接状态 |
| temperature_sensor_data_index | array<string> | 温度传感器数据索引 |
| temperature_sensor_data | array<float> | 温度传感器数据 |
| cell_voltage_sensor_data_index | array<string> | 单体电压传感器数据索引 |
| cell_voltage_sensor_data | array<float> | 单体电压传感器数据 |
| bmscgw274 | array<int> | BMS CGW 274数据 |
| software_version | string | 软件版本 |
| battery_type | string | 电池类型 |
| max_cell_voltage_b | float | 单体最高电压B组 |
| min_cell_voltage_b | float | 单体最低电压B组 |
| max_cell_voltage_number_b | smallint | 单体最高电压编号B组 |
| min_cell_voltage_number_b | smallint | 单体最低电压编号B组 |
| max_battery_pack_temperature_b | float | 电池包最高温度B组 |
| min_battery_pack_temperature_b | float | 电池包最低温度B组 |
| max_battery_pack_temperature_number_b | smallint | 电池包最高温度编号B组 |
| min_battery_pack_temperature_number_b | smallint | 电池包最低温度编号B组 |
| bms_pack_capacity_type | int | BMS电池包容量类型 |
| bms_battery_capacity_type | int | BMS电池容量类型 |
| bms_battery_usable_capacity_type | int | BMS电池可用容量类型 |
| bms_pack_capacity_type_description | string | BMS电池包容量类型描述 |
| bms_cgw | string | null |
| source | string | 数据来源 |
| date | string | date partition |
| hour | string | hour partition |

---

## 表3: dm_battery_compass.d_i_battery_block_features

### 表说明
电池block特征表，天级更新，分区字段为date,source,brand

### 字段列表

| 列名 | 注释 | 类型 | 需求字段 | 含义 |
|------|------|------|----------|------|
| battery_id | 电池编号 | string | 车 | Block窗口对应的电池BID编号 |
| cell_type | 电芯类型 | string | 静态信息 | Block窗口对应的电池的电芯型号。LFP，NCM，SIB，NULL1，NULL2，NULL3 |
| battery_type | 电池类型 | string | 静态信息 | 电池型号 |
| battery_capacity | 电池容量 | double | 静态信息 | battery_type对应的电池标称容量 |
| battery_energy | 电池电量 | double | 静态信息 | battery_type对应的电池标称能量 |
| real_energy | 真实电量 | double | 车 | battery_id对应电池的实际可用电池电量 |
| device_name | 设备名称 | string | 车 | Block窗口对应的车辆vin，和swap的设备ID |
| device_id | 设备编号 | string | 车 | Block窗口对应的设备编号 |
| device_type | 设备类型 | string | 车 | Block窗口对应的设备类型 |
| is_test | 是否测试设备 | string | na | 电池所在设备状态。0-用户车, 1-普通测试车, 4-租赁车,NULL为非车辆上 |
| process_id | 过程编号 | string | 车 | Block窗口对应的时间编号，根据event_type并按照时间顺序生成 |
| bms_version | bms版本 | string | 车 | Block窗口对应的bms版本 |
| device_version | 设备版本 | string | 车 | Block窗口对应的设备版本 |
| event_type | 事件类型 | string | 车 | Block窗口对应的事件类型 |
| event_id | 事件编号 | string | 车 | 根据process_id，vin，battery_id，event_type生成唯一的事件编号 |
| hour_tag | 时段标签 | int | 车 | Block窗口对应的时段标签。每2h tag+1，24h有12个tag |
| soc_tag | soc标签 | int | 国标 | Block窗口对应的soc标签。每5% user_SOC tag+1，100%SOC有20个tag |
| date_tag | 日期标签 | string | 车 | Block窗口对应的日期标签 |
| curr_tag | 电流标签 | int | 国标 | Block窗口对应的电流标签 |
| block_msg_count | block报文总量 | int | 车 | 统计block内报文接收总量 |
| effect_msg_count | 有效报文总量 | int | 车 | 统计block内去除异常采样、重复采样、采样错位数据后有效报文接收总量 |
| fault_msg_count | 无效报文总量 | int | 车 | 统计事件无效报文接收数量 |
| cell_volt_init_count | 单体电压初始值计数 | map<double,int> | 静态信息 | 统计block内最高单体电压分别为4.096或5.094或65535的报文总量 |
| probe_temp_init_count | 单体温度初始值计数 | map<double,int> | 静态信息 | 统计block内最低单体温度分别为-40或-50或255的报文总量 |
| bms_temp_init_count | 单体温度初始值计数_-40 | map<double,int> | 静态信息 | 统计block内最高或者最低单体温度为-40报文接收总量 |
| insu_resis_init_count | 绝缘初始值计数 | map<double,int> | 静态信息 | 统计block内绝缘分别为5000或10000或60000的报文总量 |
| first_pack_cell_voltage | 第一帧单体电压列表 | array<double> | 国标 | Block窗口中以第一帧报文的所有电芯电压列表值 |
| first_pack_probe_temp | 第一帧温度列表 | array<double> | 国标 | Block窗口中以第一帧报文的所有电芯电压列表值 |
| last_pack_cell_voltage | 最后一帧单体电压列表 | array<double> | 国标 | Block窗口中以最后一帧报文的所有电芯电压列表值 |
| last_pack_probe_temp | 最后一帧温度列表 | array<double> | 国标 | Block窗口中以最后一帧报文的所有电芯温度列表值 |
| start_time | 开始时间 | timestamp | 车 | Block窗口中以第一帧有效报文的采样时间作为起始时间 |
| end_time | 结束时间 | timestamp | 车 | Block窗口中以最后一帧有效报文的采样时间作为截止时间 |
| block_duration | block时长(s) | bigint | 车 | Block时长是窗口中所有相邻报文时间差△t求和 |
| start_user_soc | 起始用户soc | double | 国标 | Block窗口中第一帧有效报文的user_soc |
| end_user_soc | 结束用户soc | double | 国标 | Block窗口中最后一帧有效报文的user_soc |
| start_real_soc | 真实起始soc | double | 车 | Block窗口中第一帧有效报文的real_soc |
| end_real_soc | 真实结束soc | double | 车 | Block窗口中最后一帧有效报文的real_soc |
| start_soh | 起始soh | double | 车 | Block窗口中第一帧有效报文的soh |
| end_soh | 结束soh | double | 车 | Block窗口中最后一帧有效报文的soh |
| start_current | 起始电流 | double | 国标 | Block窗口中第一帧有效报文的电流 |
| end_current | 结束电流 | double | 国标 | Block窗口中最后一帧有效报文的电流 |
| max_discharge_current | 最大放电电流 | double | 国标 | Block窗口中放电电流的最大值 |
| min_discharge_current | 最小放电电流 | double | 国标 | Block窗口中放电电流的最小值 |
| avg_discharge_current | 平均放电电流 | double | 国标 | Block窗口中放电电流的平均值 |
| max_charge_current | 最大充电电流 | double | 国标 | Block窗口中充电电流的最大值 |
| min_charge_current | 最小充电电流 | double | 国标 | Block窗口中充电电流的最小值 |
| avg_charge_current | 平均充电电流 | double | 国标 | Block窗口中充电电流的平均值 |
| max_pack_voltage | 最大总电压 | double | 国标 | Block窗口中总电压的最大值 |
| min_pack_voltage | 最小总电压 | double | 国标 | Block窗口中总电压的最小值 |
| avg_pack_voltage | 平均总电压 | double | 国标 | Block窗口中总电压的最小值 |
| max_high_cell_volt | 最大最高单体电压 | double | 2016国标 | Block窗口中最高单体电压的最大值 |
| min_high_cell_volt | 最小最高单体电压 | double | 2016国标 | Block窗口中最高单体电压的最小值 |
| avg_high_cell_volt | 平均最高单体电压 | double | 2016国标 | Block窗口中最高单体电压的平均值 |
| max_low_cell_volt | 最大最低单体电压 | double | 2016国标 | Block窗口中最低单体电压的最大值 |
| min_low_cell_volt | 最小最低单体电压 | double | 2016国标 | Block窗口中最低单体电压的最小值 |
| avg_low_cell_volt | 平均最低单体电压 | double | 2016国标 | Block窗口中最低单体电压的平均值 |
| max_avg_cell_volt | 最大平均单体电压 | double | 2016国标 | Block窗口中平均单体电压的最大值 |
| min_avg_cell_volt | 最小平均单体电压 | double | 2016国标 | Block窗口中平均单体电压的最小值 |
| avg_avg_cell_volt | 平均平均单体电压 | double | 2016国标 | Block窗口中平均单体电压的平均值 |
| max_cell_volt_quartiles | 最大单体电压分位数 | map<double,double> | 2016国标 | Block窗中最大单体电压时刻的电压分位数 |
| max_high_probe_temp | 最大最高单体温度 | double | 2016国标 | Block窗口中最高单体温度的最大值 |
| min_high_probe_temp | 最小最高单体温度 | double | 2016国标 | Block窗口中最高单体温度的最小值 |
| avg_high_probe_temp | 平均最高单体温度 | double | 2016国标 | Block窗口中最高单体温度的平均值 |
| max_low_probe_temp | 最大最低单体温度 | double | 2016国标 | Block窗口中最低单体温度的最大值 |
| min_low_probe_temp | 最小最低单体温度 | double | 2016国标 | Block窗口中最低单体温度的最小值 |
| avg_low_probe_temp | 平均最低单体温度 | double | 2016国标 | Block窗口中最低单体温度的平均值 |
| max_volt_skewness | 最大电压偏离程度 | double | 2016国标 | Block窗口中单体电压偏离程度的最大值 |
| min_volt_skewness | 最小电压偏离程度 | double | 2016国标 | Block窗口中单体电压偏离程度的最小值 |
| avg_volt_skewness | 平均电压偏离程度 | double | 2016国标 | Block窗口中单体电压偏离程度的平均值 |
| volt_skewness_quartiles | 电压偏离程度分位数 | map<double,double> | 2016国标 | Block窗口中单体电压偏离程度的分位数 |
| max_volt_diff | 最大压差 | double | 2016国标 | Block窗口中单体压差的最大值 |
| min_volt_diff | 最小压差 | double | 2016国标 | Block窗口中单体压差的最小值 |
| avg_volt_diff | 平均压差 | double | 2016国标 | Block窗口中单体压差的平均值 |
| high_volt_diff_count | 偏高压差次数 | int | 2016国标 | block窗口中偏高压差次数 |
| volt_diff_quartiles | 压差分位数 | map<double,double> | 2016国标 | Block窗口中单体压差的分位数 |
| sp_after_chg_volt_diff0 | 换电站充电截止后静置第一帧压差 | double | na | 换电站内，电芯充电截止后，开始静置最初t_0时刻的电芯电压数据 |
| sp_after_chg_volt_diff1 | 换电站充电截止后静置300s压差 | double | na | 电芯充电截止后，开始静置300s的t_1时刻电芯电压数据 |
| spafchg_volt_drop_rate | 换电站充电截止后静置压降速率 | double | na | t_0到t_1时间内的压降速率 |
| spafchg_outlier_rate | 换电站充电截止后静置电芯低压离群程度 | double | na | 对t_1时刻电芯电压进行排序，识别出最低电芯电压Vmin_1和倒数第4低电芯电压Vmin_4, 计算离群程度 |
| max_temp_diff | 最大温差 | double | 2016国标 | Block窗口中单体温差的最大值 |
| min_temp_diff | 最小温差 | double | 2016国标 | Block窗口中单体温差的最小值 |
| avg_temp_diff | 平均温差 | double | 2016国标 | Block窗口中单体温差的平均值 |
| max_volt_sn_count | 最高单体电压编号计数 | map<int,int> | 2016国标 | 统计block内过滤异常数据后最高单体电压编号出现的值和数量 |
| min_volt_sn_count | 最低单体电压编号计数 | map<int,int> | 2016国标 | 统计block内过滤异常数据后最低单体电压编号出现的值和数量 |
| max_temp_sn_count | 最高温度编号计数 | map<int,int> | 2016国标 | 统计block内过滤异常数据后最高单体温度编号出现的值和数量 |
| min_temp_sn_count | 最低温度编号计数 | map<int,int> | 2016国标 | 统计block内过滤异常数据后最低单体温度编号出现的值和数量 |
| volt_rank_mean | 电压排位对应均值 | array<double> | 国标 | 统计block内过滤异常数据后单体电压列表对应位置单体排位的均值 |
| volt_rank_stddev | 电压排位对应标准差 | array<double> | 国标 | 统计block内过滤异常数据后单体电压列表对应位置单体排位的标准差 |
| temp_rank_mean | 温度排位对应均值 | array<double> | 国标 | 统计block内过滤异常数据后单体温度列表对应位置单体排位的均值 |
| charge_cap | 充电容量(A*s) | double | 车 | 计算窗口中，取充电电流（即电流小于0A的数值）计算充电容量 |
| charge_energy | 充电电量(W*h) | double | 车 | 计算窗口中，取充电电流（即电流小于0A）计算充电电量 |
| discharge_cap | 放电容量(A*s) | double | 车 | 计算窗口中，取放电电流（即电流大于0A）计算放电容量 |
| discharge_energy | 放电电量(W*h) | double | 车 | 计算窗口中，取放电电流（即电流大于0A）计算放电电量 |
| max_insu_resis | 最高绝缘 | double | 国标 | Block窗口中绝缘阻值的最大值 |
| min_insu_resis | 最低绝缘 | double | 国标 | Block窗口中绝缘阻值的最小值 |
| avg_insu_resis | 平均绝缘 | double | 国标 | Block窗口中绝缘阻值的平均值 |
| insu_resis_quartiles | 绝缘分位数 | map<double,double> | 国标 | Block窗口中绝缘阻值的分位数 |
| balance_count | 均衡次数 | int | 车 | Block窗口中均衡开启的总次数 |
| balance_duration | 均衡时长(s) | bigint | 车 | Block窗口中均衡开启的总时长 |
| volt_diff_time_corr | 压差时间相关系数 | double | 2016国标 | Block窗口中单体压差与时间的Pearson相关系数 |
| volt_diff_soc_corr | 压差soc相关系数 | double | 2016国标 | Block窗口中单体压差与SOC的Pearson相关系数 |
| volt_diff_current_corr | 压差电流相关系数 | double | 2016国标 | Block窗口中单体压差与电流的Pearson相关系数 |
| temp_diff_current_corr | 温差电流相关系数 | double | 2016国标 | Block窗口中单体温差与电流的Pearson相关系数 |
| max_volt_diff_time | 最大压差时刻时间 | timestamp | 2016国标 | Block窗口中压差最大时刻对应的时间 |
| max_volt_diff_soc | 最大压差时刻soc | double | 2016国标 | Block窗口中压差最大时刻对应的SOC |
| max_volt_diff_current | 最大压差时刻电流 | double | 2016国标 | Block窗口中压差最大时刻对应的电流 |
| max_volt_diff_max_cell_voltage | 最大压差时刻最高单体电压 | double | 2016国标 | Block窗口中压差最大时刻对应的最高单体电压 |
| max_volt_diff_max_cell_voltage_sn | 最大压差时刻最高单体电压编号 | int | 2016国标 | Block窗口中压差最大时刻对应的最高单体电压编号 |
| max_volt_diff_min_cell_voltage | 最大压差时刻最低单体电压 | double | 2016国标 | Block窗口中压差最大时刻对应的最低单体电压 |
| max_volt_diff_min_cell_voltage_sn | 最大压差时刻最低单体电压编号 | int | 2016国标 | Block窗口中压差最大时刻对应的最低单体电压编号 |
| max_volt_diff_volt_mean | 最大压差时刻电芯电压平均值 | double | 2016国标 |  |
| max_volt_diff_cell_voltage | 最大压差时刻单体电压列表 | array<double> | 2016国标 | Block窗口中压差最大时刻对应的单体电压列表 |
| max_volt_diff_volt_entropy | 最大压差时刻单体电压列表香农熵 | double | 2016国标 | Block窗口中压差最大时刻对应的单体电压列表的香农熵 |
| max_volt_diff_volt_rms | 最大压差时刻单体压差有效值 | double | 2016国标 | Block窗口中压差最大时刻对应的单体电压差的有效值 |
| max_volt_diff_volt_arv | 最大压差时刻单体压差平均值 | double | 2016国标 | Block窗口中压差最大时刻对应的单体电压差的平均值 |
| max_volt_diff_volt_stddev | 最大压差时刻单体压差均方差 | double | 2016国标 | Block窗口中压差最大时刻对应的单体电压差的均方差 |
| max_volt_diff_volt_margin | 最大压差时刻单体压差波形因子 | double | 2016国标 | Block窗口中压差最大时刻对应的单体电压差的波形因子 |
| max_volt_diff_volt_skewness | 最大压差时刻单体压差斜度因子 | double | 2016国标 | Block窗口中压差最大时刻对应的单体电压差的斜度因子 |
| max_volt_diff_volt_kurtosis | 最大压差时刻单体压差峭度因子 | double | 2016国标 | Block窗口中压差最大时刻对应的单体电压差的峭度因子 |
| max_temp_diff_time | 最大温差时刻时间 | timestamp | 2016国标 | Block窗口中温差最大时刻对应的时间 |
| max_temp_diff_soc | 最大温差时刻soc | double | 2016国标 | Block窗口中温差最大时刻对应的SOC |
| max_temp_diff_current | 最大温差时刻电流 | double | 2016国标 | Block窗口中温差最大时刻对应的电流 |
| max_temp_diff_max_probe_temp | 最大温差时刻最高温度 | double | 2016国标 | Block窗口中温差最大时刻对应的最高单体温度 |
| max_temp_diff_max_probe_temp_sn | 最大温差时刻最高温度编号 | int | 2016国标 | Block窗口中温差最大时刻对应的最高单体温度编号 |
| max_temp_diff_min_probe_temp | 最大温差时刻最低温度 | double | 2016国标 | Block窗口中温差最大时刻对应的最低单体温度 |
| max_temp_diff_min_probe_temp_sn | 最大温差时刻最低温度编号 | int | 2016国标 | Block窗口中温差最大时刻对应的最低单体温度编号 |
| max_temp_diff_probe_temp | 最大温差时刻温度列表 | array<double> | 2016国标 | Block窗口中温差最大时刻对应的单体温度列表 |
| max_temp_diff_temp_entropy | 最大温差时刻温度列表香农熵 | double | 2016国标 | Block窗口中温差最大时刻对应的单体温度列表香农熵 |
| max_mileage | 最大(结束)里程 | int | 车 | Block窗口中选取车辆行驶里程的最大值 |
| min_mileage | 最小(起始)里程 | int | 车 | Block窗口中选取车辆行驶里程的最小值 |
| max_speed | 最大车速 | double | 车 | Block窗口中选取车辆速度的最大值 |
| min_speed | 最小车速 | double | 车 | Block窗口中选取车辆速度的最小值 |
| avg_speed | 平均车速 | double | 车 | Block窗口中选取车辆速度的平均值 |
| exp_time | 截止时间 | timestamp | na | 计算时段所在的最晚时间为准 |
| dw_etl_time | etl时间 | timestamp | na | 特征的提取时间 |
| exp_date | 截止日期 | string | na | 计算时段所在的日期 |
| date | string | date partition |
| source | string | source partition |
| brand | string | brand partition |