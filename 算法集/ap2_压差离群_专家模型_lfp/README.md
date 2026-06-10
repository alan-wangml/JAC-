# ap2 电池压差离群预警_专家模型1_LFP

**算法ID**：Algorithm_ap2_vdiff_outlier_empirical_lfp  
**层级**：预警层（离线预警，7×24h监控）  
**数据源**：block  
**适用范围**：LFP 电芯  
**状态**：运行中

---

## 核心逻辑

**Step1 数据初筛**
- cell_type = 'LFP'
- max_volt_diff_volt_entropy < ap2_entropy_lfp_threshold
- 静态条件：min_low_probe_temp ≥ ap2_min_low_probe_temp_lfp_threshold，max_volt_diff_min_cell_voltage ≥ ap2_min_low_cell_volt_lfp_threshold，max_volt_diff_max_cell_voltage ≤ ap2_max_high_cell_volt_lfp_threshold
- 动态条件：
    - soc_tag＞1 and soc_tag＜17，lfp组的电芯压差 max_volt_diff ≥ ap2_volt_diff_lfp_within_threshold
    - soc_tag≤ 1 or soc_tag≥17，lfp组的电芯压差 max_volt_diff ≥ ap2_volt_diff_lfp_without_threshold

**Step2 中间值计算**
- max_volt_diff_volt_median：max_volt_diff_cell_voltage 列表的中位数电压
- max_volt_diff_volt_mean：max_volt_diff_cell_voltage 列表的均值电压

**Step3 综合判定**
- max_volt_diff_volt_median - max_volt_diff_volt_mean ≥ ap2_offset_lfp_shreshold

---

## 关键参数

| 参数 | 变量名 | V1.0 | 单位 |
|------|--------|------|------|
| ap2_电压列表熵阈值_lfp | ap2_entropy_lfp_threshold | 1 | / |
| ap2_最小最低温度阈值_lfp | ap2_min_low_probe_temp_lfp_threshold | 5 | °C |
| ap2_最小最低电压阈值_lfp | ap2_min_low_cell_volt_lfp_threshold | 3200 | mV |
| ap2_最大最高电压阈值_lfp | ap2_max_high_cell_volt_lfp_threshold | 3450 | mV |
| ap2_SOC区间内压差阈值_lfp | ap2_volt_diff_lfp_within_threshold | 20 | mV |
| ap2_SOC区间外压差阈值_lfp | ap2_volt_diff_lfp_without_threshold | 7 | mV |
| ap2_压差偏离阈值_lfp | ap2_offset_lfp_shreshold | 0.02 | mV |

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
| max_volt_diff | DOUBLE | 窗口最大压差 |
| max_volt_diff_volt_entropy | DOUBLE | 压差最大时刻电压列表熵值 |
| max_volt_diff_cell_voltage | ARRAY<DOUBLE> | 压差最大时刻电芯电压列表 |
| max_volt_diff_min_cell_voltage | DOUBLE | 压差最大时刻最低单体电压 |
| max_volt_diff_max_cell_voltage | DOUBLE | 压差最大时刻最高单体电压 |
| min_low_probe_temp | DOUBLE | 窗口最低温度探针值 |

## 调优记录

_暂无_
