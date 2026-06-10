# ap24 电池采样异常预警_专家模型_电压对称性抖动

**算法ID**：Algorithm_21ffd66732  
**层级**：预警层（离线预警，7×24h监控）  
**数据源**：block（d_i_battery_block_features）  
**适用范围**：全系电池  
**状态**：运行中（V1.0）

---

## 上游数据表输入字段

| 字段名 | 类型 | 说明 |
|--------|------|------|
| battery_id | string | 电池ID |
| battery_type | string | 电池类型 |
| event_type | string | 事件类型（parking/charge等） |
| device_id | string | 设备ID |
| device_name | string | 设备名称 |
| process_id | string | 过程ID |
| start_time | string | 开始时间 |
| end_time | string | 结束时间 |
| start_user_soc | double | 起始用户SOC |
| start_current | double | 起始电流（A） |
| avg_pack_voltage | double | 平均总压（V） |
| avg_insu_resis | double | 平均绝缘电阻（kΩ） |
| avg_high_cell_volt | double | 平均最高单体电压（mV） |
| avg_low_cell_volt | double | 平均最低单体电压（mV） |
| max_volt_diff | double | 最大压差（mV） |
| volt_diff_quartiles | array | 压差分位数（P25/P50/P75等） |
| max_volt_diff_volt_entropy | double | 电压香农熵 |
| max_volt_sn_count | array | 最高单体电压探头编号及计数 |
| min_volt_sn_count | array | 最低单体电压探头编号及计数 |
| max_volt_diff_max_cell_voltage | double | 最大压差对应的最高单体电压（mV） |
| max_volt_diff_min_cell_voltage | double | 最大压差对应的最低单体电压（mV） |
| max_volt_diff_cell_voltage | array | 最大压差时的所有电芯电压列表 |

---

## 核心逻辑

**Step1 数据初筛**
- 按 battery_type 分组处理
- event_type in (parking, charge)（场景筛选）
- max_volt_sn_count 和 min_volt_sn_count 各只有一个电芯（max_count = 1 且 min_count = 1）
- max_volt_diff > ap24_volt_diff_threshold（压差大于阈值）
- max_volt_diff_volt_entropy > ap24_max_volt_diff_volt_entropy_threshold（电压香农熵大于阈值）

**Step2 相邻电芯筛选**
- abs(max_sn - min_sn) = 1（最高最低电芯编号相邻）

**Step3 对称性抖动检测**
- 展开电芯列表（max_volt_diff_cell_voltage），按 battery_id + start_time 分组计算分位数（P5, P25, P50, P75, P95）
- IQR = P75 - P25
- upper_limit = P75 + 1.5 × IQR
- lower_limit = P25 - 1.5 × IQR
- is_high_outlier：max_volt_diff_max_cell_voltage > upper_limit → 1
- is_low_outlier：max_volt_diff_min_cell_voltage < lower_limit → 1
- is_csc_volt_fault：is_high_outlier = 1 且 is_low_outlier = 1

**Step4 偏离程度判定**
- high_delt = max_volt_diff_max_cell_voltage - P50
- low_delt = P50 - max_volt_diff_min_cell_voltage
- high_delt > ap24_uppervolt_outlier_threshold 且 low_delt > ap24_lowervolt_outlier_threshold

**Step5 去重**：按 battery_id 分组，取 volt_diff 最大且 start_time 最早的一条

---

## 关键参数

| 参数 | 变量名 | 值 | 单位 |
|------|--------|-----|------|
| 压差阈值 | ap24_volt_diff_threshold | 100 | mV |
| 电压香农熵阈值 | ap24_max_volt_diff_volt_entropy_threshold | 0.1 | — |
| 最高电压偏离阈值 | ap24_uppervolt_outlier_threshold | 50 | mV |
| 最低电压偏离阈值 | ap24_lowervolt_outlier_threshold | 50 | mV |

---

## 调优记录

_暂无_