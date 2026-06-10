# ap8 电池压差离群预警_数理模型

**算法ID**：Algorithm_103eebf143  
**层级**：预警层（离线预警，7×24h监控）  
**数据源**：TSP 和 OSS（ods_battery_detail_h_i / ods_dim_battery_model）  
**适用范围**：NCM 电芯  
**状态**：运行中

---

## 核心逻辑

**Step1 数据初筛**
- cell_type = 'NCM'，battery_type 按配置过滤（如 GX-1P108S）
- vehicle_state = 'periodical_parking_update' 或 'periodical_charge_update'
- max_cell_voltage / min_cell_voltage 在 (ap8_cell_volt_lower_bound, ap8_cell_volt_upper_bound) 范围内
- cell_volt_diff > ap8_vdiff_threshold
- 电压值 × ap8_volt_ratio 转换为 mV 单位
- 合并电池静态信息（battery_capacity, battery_energy）
- 计算报文时间间隔：lead 取下一条 sample_timestamp，间隔 > ap8_sample_interval_upper_bound 时按 ap8_standard_sample_interval 处理
- 过滤重复采样：msg_time_interval ≥ ap8_sample_interval_lower_bound，或相邻报文电压值发生变化

**Step2 多因子空间投影（计算加权压差）**
- 电流映射 current_rate：e^(-ap8_current_factor × |current|)
- SOC映射 soc_rate：atan(ap8_soc_factor × soc³) × 2/π
- 温度映射 temp_rate：1 / (1 + e^(-ap8_temp_factor × avg_temp))
- 压差映射 vdiff_rate（分4场景）：
    - 场景1（低压）：min_cell_voltage > 0 且 max_cell_voltage < 3600mV → 1/(e^(-5/cell_volt_diff) + 1)
    - 场景2（充电平台小压差）：(soc 55-65 或 soc ≤ 25) 且 cell_volt_diff < 40mV → 1 - 0.01 × cell_volt_diff
    - 场景3（充电平台大压差）：(soc 55-65 或 soc ≤ 25) 且 cell_volt_diff ≥ 40mV → 0.85
    - 场景4（其他）：1/(ap8_vdiff_factor^cell_volt_diff + 1)
- 加权压差 weighted_vdiff_level = round(soc_rate × current_rate × vdiff_rate × temp_rate × cell_volt_diff, 2)

**Step3 箱线图统计与离群判定**
- soc_tag 定义：user_soc ∈ [55, 65] → 'range1'，否则 → 'range2'
- 按 battery_id / battery_type / cell_type / vehicle_state / soc_tag 分组，计算单电池加权压差的 5%/25%/50%/75%/95% 分位数
- 按 battery_type / cell_type / vehicle_state / soc_tag 分组，计算全量电池的箱线图指标
- 计算全量上边缘：all_vdiff_upper_bound = 2.5 × all_75_vdiff_quartiles - 1.5 × all_25_vdiff_quartiles
- 单电池与全量箱线图 left_outer join
- 触发条件：
    - 类型1（parking场景）：sin_5_vdiff_quartiles > all_vdiff_upper_bound AND sin_95-sin_5 > all_95-all_5 AND vehicle_state = 'periodical_parking_update'
    - 类型2（非平台SOC）：sin_95_vdiff_quartiles > 4×(all_95-all_5) AND sin_75 > all_vdiff_upper_bound AND soc_tag = 'range2'
- 取单电池压差最大时刻（cell_volt_diff 降序第一条）的打点数据作为告警明细

---

## 关键参数

| 参数 | 变量名 | V1.0 | 单位 |
|------|--------|------|------|
| ap8_单体电压范围 | ap8_cell_voltage | (2500, 4500) | mV |
| ap8_采样间隔 | ap8_sample_interval | [5.0, 3.0, 30.0] | s |
| ap8_压差阈值 | ap8_vdiff_threshold | 1 | mV |
| ap8_投影因子 | ap8_factor_threshold | [0.015, 0.002, 1, 0.4] | / |
| ap8_电压比率系数 | ap8_volt_ratio | 1000 | / |

---

## 上游数据表输入字段

**数据源表1**：`saas_battery.ods_battery_detail_h_i`（TSP/OSS 明细数据）

| 字段名 | 类型 | 说明 |
|--------|------|------|
| battery_id | STRING | 电池ID |
| battery_model | STRING | 电池型号 |
| device_id | STRING | 设备ID |
| device_type | STRING | 设备类型 |
| sample_time | BIGINT | 采样时间戳（毫秒） |
| battery_state | INT | 电池状态（1=充电, 3=行驶, 其他=停放） |
| process_id | STRING | 报文ID |
| insulation_resistance | DOUBLE | 绝缘阻值（kΩ） |
| voltage | DOUBLE | 总压（V） |
| current | DOUBLE | 电流（A） |
| user_soc | DOUBLE | 用户SOC（%） |
| max_cell_voltage_sn | INT | 最高单体电压序号 |
| max_cell_voltage | DOUBLE | 最高单体电压（V） |
| min_cell_voltage_sn | INT | 最低单体电压序号 |
| min_cell_voltage | DOUBLE | 最低单体电压（V） |
| max_probe_temperature_sn | INT | 最高温度探针序号 |
| max_probe_temperature | DOUBLE | 最高探针温度（℃） |
| min_probe_temperature_sn | INT | 最低温度探针序号 |
| min_probe_temperature | DOUBLE | 最低探针温度（℃） |

**数据源表2**：`saas_battery.ods_dim_battery_model`（电池型号维度表）

| 字段名 | 类型 | 说明 |
|--------|------|------|
| battery_model | STRING | 电池型号 |
| battery_capacity | DOUBLE | 电池容量（Ah） |
| battery_energy | DOUBLE | 电池能量（kWh） |

## 调优记录

_暂无_
