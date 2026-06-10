# ap21 电池实时隐患告警

**算法ID**：Algorithm_ap21  
**层级**：实时算法（实时 Kafka 消息触发）  
**数据源**：OSS / TSP（saas_battery.ods_battery_detail_h_i）  
**适用范围**：全系电芯（LFP / NCM）  
**告警等级**：综合因子告警  
**状态**：运行中

---

## 核心逻辑

**Step1 数据提取与过滤**
- 从 Kafka 实时帧数据中提取 battery_id、device_id、device_type、device_name、battery_state、sample_time
- 聚合计算：vdiff = max_cell_voltage - min_cell_voltage，tdiff = max_probe_temperature - min_probe_temperature
- 过滤初始化值：
    - 绝缘值 insulation_resistance ∈ {10000, 20000, 65535} → 丢弃
    - 单体电压 max_cell_voltage / min_cell_voltage ∈ {0, 65535} → 丢弃
    - 温度 max_probe_temperature / min_probe_temperature ∈ {-40, 214, 215, 254, 255} → 丢弃

**Step2 滑动窗口定义**
- 以长度为 ap21_window_set（100）帧的滑动窗口进行异常分析
- 数据中断帧数 > ap21_interval_set（5）则窗口重新创建
- 窗口内有效帧数 > ap21_window_valid_set（45）才计算，否则丢弃窗口
- 窗口异常重启次数 abnormal_restart_count > ap21_abnormal_restart_set（11）或单帧中断时长 > ap21_abnormal_interval_set（600s）→ 触发 CGW 采样时间异常告警

**Step3 异常现象识别（逐帧判定）**
- 按 battery_model 区分 NCM / LFP 产品组，使用各自参数阈值
- 单体过压异常（volt_over_abnormal）：max_cell_voltage > volt_over_boundary 且不为初始值 → 1
- 单体欠压异常（volt_under_abnormal）：min_cell_voltage < volt_under_boundary 且不为初始值 → 1
- 压差异常（vdiff_abnormal）：
    - NCM：vdiff > ap21_vdiff_threshold（50mV）→ 1
    - LFP：vdiff > ap21_vdiff_threshold 且 max_cell_voltage ∈ [lfp_vmax_lower_threshold, lfp_vmax_upper_threshold] → 1
- 过温异常（temp_over_abnormal）：max_probe_temperature > ap21_temp_over_threshold（61℃）且不为初始值 → 1
- 温差异常（tdiff_abnormal）：tdiff > ap21_tdiff_threshold（20℃）→ 1
- 绝缘异常（iso_abnormal）：电流 ∈ [iso_cur_lowlimit, iso_cur_toplimit] 且不为初始值时：
    - insulation_resistance < iso_lvl2（1000kΩ）→ 2
    - insulation_resistance < iso_lvl1（2000kΩ）→ 1

**Step4 窗口统计与告警判定**
- 统计窗口内各类异常帧数：sin_overvolt、sin_undervolt、sin_vdiff、sin_overtemp、sin_tdiff、sin_iso
- 加权综合得分：combine_val = Σ(sin_i × weight_i)
- 单项告警：各类异常帧数 > 对应阈值（默认1）→ 触发对应单项告警
- 综合告警：combine_val > ap21_factor_fault_array 综合阈值（默认10）→ 触发电池隐患综合告警

---

## 关键参数

| 参数 | 变量名 | V1.0 | 单位 |
|------|--------|------|------|
| 异常因子权重 | ap21_factor_weight_array | (1, 1, 1, 1, 1, 1) | / |
| NCM电压异常阈值 | ap21_volt_threshold_ncm_array | (4350, 2500) | mV |
| LFP电压异常阈值 | ap21_volt_threshold_lfp_array | (3850, 2000) | mV |
| 压差异常阈值 | ap21_vdiff_threshold | 50 | mV |
| LFP压差电压范围 | ap21_lfp_vmax_threshold_array | (3350, 3000) | mV |
| 过温异常阈值 | ap21_temp_over_threshold | 61 | ℃ |
| 温差异常阈值 | ap21_tdiff_threshold | 20 | ℃ |
| 绝缘异常阈值组 | ap21_iso_threshold_array | (1000, -20, 2000, 1000) | A/A/kΩ/kΩ |
| 综合因子告警阈值 | ap21_factor_fault_array | (1, 1, 1, 1, 1, 2, 2) | / |
| 滑动窗口长度 | ap21_window_set | 5 | 帧 |
| 窗口重启间隔上限 | ap21_interval_set | 3 | 帧 |
| 有效帧数阈值 | ap21_window_valid_set | 2 | 帧 |
| 窗口异常重启阈值 | ap21_abnormal_restart_set | 3 | 次 |
| 单帧中断时长阈值 | ap21_abnormal_interval_set | 600 | s |
| 预警冷却时间 | ap21_cd_set | 3600 | s |
| NCM产品组 | ap21_battery_model_ncm_lst | (XXX, XXX, XXX) | / |
| LFP产品组 | ap21_battery_model_lfp_lst | (XXX, XXX, XXX) | / |

---

## 上游数据表输入字段

**数据源表**：`saas_battery.ods_battery_detail_h_i`（TSP/OSS 实时 Kafka 明细数据）

| 字段名 | 类型 | 说明 |
|--------|------|------|
| battery_id | STRING | 电池ID |
| battery_model | STRING | 电池型号 |
| device_id | STRING | 设备ID |
| device_type | STRING | 设备类型 |
| device_name | STRING | 设备名称 |
| sample_time | BIGINT | 采样时间戳（毫秒） |
| battery_state | INT | 电池状态（1=充电, 3=行驶, 其他=停放） |
| process_id | STRING | 报文ID |
| insulation_resistance | DOUBLE | 绝缘阻值（kΩ） |
| voltage | DOUBLE | 总压（V） |
| current | DOUBLE | 电流（A） |
| user_soc | DOUBLE | 用户SOC（%） |
| max_cell_voltage | DOUBLE | 最高单体电压（mV） |
| min_cell_voltage | DOUBLE | 最低单体电压（mV） |
| max_probe_temperature | DOUBLE | 最高探针温度（℃） |
| min_probe_temperature | DOUBLE | 最低探针温度（℃） |
| pack_cell_voltage | ARRAY<DOUBLE> | 单体电压列表 |
| pack_probe_temperature | ARRAY<DOUBLE> | 探针温度列表 |

---

## 调优记录

_暂无_