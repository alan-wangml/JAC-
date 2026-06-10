# ap6 电池压差离群预警_统计模型

**算法ID**：Algorithm_f0a173ec73  
**层级**：预警层（离线预警，7×24h监控）  
**数据源**：block  
**适用范围**：NCM / LFP 电芯  
**状态**：运行中

---

## 核心逻辑

**Step1 数据初筛**
- min_low_probe_temp ≥ ap6_min_low_probe_temp_threshold
- block_msg_count > ap6_block_msg_count_threshold
- soc_tag ≥ ap6_soc_tag_threshold[0]
- cell_type != 'LFP' 或 (cell_type = 'LFP' and soc_tag < ap6_soc_tag_threshold[1])
- max_volt_diff_volt_entropy IS NOT NULL

**Step2 宏观统计（分组统计）**
- 按 battery_type / event_type / soc_tag / curr_tag / cell_type 分组
- 计算各组 volt_diff_quartiles 的 75% 分位数均值（label_mean）和标准差（label_std）
- 计算全量 max_volt_diff_volt_entropy 的均值（volt_entropy_mean）和标准差（volt_entropy_std）

**Step3 异常筛出**
- 每块电池取 max_volt_diff 最大且最早的一条记录
- 聚合分组统计数据，计算异常条件：
  - (volt_diff_quartiles_75 > label_mean + ap6_volt_quartiles_outlier_threshold × label_std) AND (max_volt_diff_volt_entropy < volt_entropy_mean - ap6_entropy_outlier × volt_entropy_std)
  - OR volt_diff_quartiles_75 > ap6_volt_diff_75quartiles_threshold[0]

**Step4 异常确认（SOC区间过滤）**
- 条件1（SOC区间）：
  - (SOC ∈ [50, 65] AND volt_diff_quartiles_75 ≥ 25)
  - OR (SOC ≥ 65 AND volt_diff_quartiles_75 > 14)
  - OR (SOC ≤ 50 AND volt_diff_quartiles_75 > 14)
- 条件2（SOC区间）：
  - (SOC < 30 AND volt_diff_quartiles_75 ≥ 25)
  - OR (SOC ≥ 30 AND volt_diff_quartiles_75 > 10)
- 条件3（事件类型）：
  - (event_type = 'parking' AND (max_volt_diff - min_volt_diff ≤ 10 OR volt_diff_quartiles_75 > 100))
  - OR event_type ≠ 'parking'

---

## 关键参数

| 参数 | 变量名 | V1.0 | 单位 |
|------|--------|------|------|
| ap6_最低温度阈值 | ap6_min_low_probe_temp_threshold | 10 | °C |
| ap6_有效报文总量阈值 | ap6_block_msg_count_threshold | 3 | / |
| ap6_soc标签阈值 | ap6_soc_tag_threshold | [2, 17] | / |
| ap6_分位数离群阈值 | ap6_volt_quartiles_outlier_threshold | 12 | / |
| ap6_75分位数硬阈值 | ap6_volt_diff_75quartiles_threshold | [100, 100, 25, 25, 14, 14, 10] | mV |
| ap6_实际soc区间阈值 | ap6_real_soc_interval_threshold | [30, 50, 65] | % |
| ap6_压差波动阈值 | ap6_volt_diff_threshold | 10 | mV |
| ap6_熵值离群系数 | ap6_entropy_outlier | 0.3 | / |

---

## 上游数据表输入字段

**数据源表**：`saas_battery.d_i_battery_block_features`

| 字段名 | 类型 | 说明 |
|--------|------|------|
| battery_id | STRING | 电池ID |
| battery_type | STRING | 电池型号 |
| device_id | STRING | 设备ID |
| device_name | STRING | 设备名称 |
| process_id | STRING | 报文ID |
| event_type | STRING | 事件类型（charge/parking等） |
| cell_type | STRING | 电芯类型（LFP/NCM） |
| soc_tag | INT | SOC分档标签（1-20） |
| start_time | TIMESTAMP | 窗口起始时间 |
| end_time | TIMESTAMP | 窗口结束时间 |
| start_real_soc | DOUBLE | 窗口起始真实SOC |
| start_current | DOUBLE | 窗口起始电流 |
| avg_pack_voltage | DOUBLE | 窗口平均总压 |
| avg_insu_resis | DOUBLE | 窗口平均绝缘阻值 |
| avg_high_cell_volt | DOUBLE | 窗口平均最高单体电压 |
| avg_low_cell_volt | DOUBLE | 窗口平均最低单体电压 |
| block_msg_count | INT | 窗口报文总数 |
| max_discharge_current | DOUBLE | 窗口最大放电电流 |
| max_charge_current | DOUBLE | 窗口最大充电电流 |
| max_volt_diff | DOUBLE | 窗口最大压差 |
| min_volt_diff | DOUBLE | 窗口最小压差 |
| volt_diff_quartiles | MAP<INT, DOUBLE> | 窗口压差四分位数（key: 25/50/75/100） |
| max_volt_diff_volt_entropy | DOUBLE | 压差最大时刻电压列表熵值 |
| min_low_probe_temp | DOUBLE | 窗口最低温度探针值 |

## 调优记录

_暂无_
