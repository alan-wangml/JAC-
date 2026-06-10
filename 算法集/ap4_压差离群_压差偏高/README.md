# ap8 电池压差离群预警_专家模型2_压差偏高

**算法ID**：Algorithm_d62367ab45  
**层级**：预警层（离线预警，7×24h监控）  
**数据源**：block  
**状态**：运行中

---

## 核心逻辑

**Step1 数据初筛**
- cell_type != 'LFP' 或 soc_tag ≤ ap4_lfp_soc_tag_upper_limit
- effect_msg_count > ap4_msg_count_limit
- soc_tag ≥ ap4_soc_tag_limit

**Step2 高压差次数统计**
- high_volt_diff_count：压差 > 100mV 的次数

**Step3 综合判定**
- high_volt_diff_count > ap4_high_volt_diff_count_limit

---

## 关键参数

| 参数 | 变量名 | V1.0 | 单位 |
|------|--------|------|------|
| ap4_有效报文总量阈值 | ap4_msg_count_limit | 20 | / |
| ap4_soc标签阈值 | ap4_soc_tag_limit | 1 | / |
| ap4_铁锂soc上限阈值 | ap4_lfp_soc_tag_upper_limit | 17 | / |
| ap4_偏高压差次数阈值 | ap4_high_volt_diff_count_limit | 1 | / |

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
| start_real_soc | DOUBLE | 窗口起始真实SOC |
| start_current | DOUBLE | 窗口起始电流 |
| avg_pack_voltage | DOUBLE | 窗口平均总压 |
| avg_insu_resis | DOUBLE | 窗口平均绝缘阻值 |
| avg_high_cell_volt | DOUBLE | 窗口平均最高单体电压 |
| avg_low_cell_volt | DOUBLE | 窗口平均最低单体电压 |
| effect_msg_count | INT | 窗口有效报文数 |
| high_volt_diff_count | INT | 窗口高压差报文数 |
| max_volt_diff | DOUBLE | 窗口最大压差 |
| max_volt_diff_max_cell_voltage | DOUBLE | 压差最大时刻最高单体电压 |
| max_volt_diff_max_cell_voltage_sn | INT | 压差最大时刻最高单体电压序号 |

## 调优记录

_暂无_
