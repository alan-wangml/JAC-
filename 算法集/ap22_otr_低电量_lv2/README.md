# ap22 低电量异常识别预警_lv2

**算法ID**：Algorithm_ap22  
**层级**：预警层（离线预警，7×24h监控）  
**数据源**：TSP（saas_battery.d_i_battery_block_features）  
**适用范围**：全系电芯（LFP / NCM）  
**状态**：运行中

---

## 核心逻辑

**Step1 数据提取**
- 根据预警对象、场景条件提取相应数据帧。数据源 dm_battery_profile.d_i_battery_block_features
- block中直接引用的数据：battery_id、cell_type、device_type、min_low_cell_volt、event_type

**Step2 数据过滤**
- 回溯前一天的所有block，若所有block的event_type都为parking，记为有效事件
- 过滤掉min_low_cell_volt = 0或null的数据

**Step3 预警报出**
- 对于有效事件，分为2个条件报警，分别对应铁锂和三元两种电池，两个条件关系为or，满足1个则报警
- 条件1（cell_type == lfp的情况）：所有block的min_low_cell_volt < ap22_lfp_overdch_voltage_threshold（2940）
- 条件2（cell_type == ncm的情况）：所有block的min_low_cell_volt < ap22_ncm_overdch_voltage_threshold（3000）

---

## 关键参数

| 参数 | 变量名 | V1.0 | 单位 |
|------|--------|------|------|
| LFP过放电压阈值 | ap22_lfp_overdch_voltage_threshold | 2940 | mV |
| NCM过放电压阈值 | ap22_ncm_overdch_voltage_threshold | 3000 | mV |

---

## 上游数据表输入字段

**数据源表**：`saas_battery.d_i_battery_block_features`（电池块特征聚合数据）

| 字段名 | 类型 | 说明 |
|--------|------|------|
| battery_id | STRING | 电池ID |
| battery_type | STRING | 电池型号 |
| device_id | STRING | 设备ID |
| device_name | STRING | 设备名称 |
| process_id | STRING | 报文ID |
| event_type | STRING | 事件类型（parking/charging/journey） |
| start_time | TIMESTAMP | 开始时间 |
| end_time | TIMESTAMP | 结束时间 |
| start_user_soc | DOUBLE | 起始SOC（%） |
| start_current | DOUBLE | 起始电流（A） |
| avg_pack_voltage | DOUBLE | 平均总压（V） |
| avg_insu_resis | DOUBLE | 平均绝缘阻值（kΩ） |
| avg_high_cell_volt | DOUBLE | 平均最高单体电压（mV） |
| avg_low_cell_volt | DOUBLE | 平均最低单体电压（mV） |
| min_low_cell_volt | DOUBLE | 最小最低单体电压（mV） |
| cell_type | STRING | 电芯类型（LFP / NCM） |

## 调优记录

_暂无_