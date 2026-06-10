# ap3 电池压差离群预警_专家模型1_NCM

**算法ID**：Algorithm_ap3_vdiff_outlier_empirical_ncm  
**层级**：预警层（离线预警，7×24h监控）  
**数据源**：block  
**适用范围**：NCM 电芯  
**状态**：运行中

---

## 核心逻辑

**Step1 数据初筛**
- cell_type = 'NCM'
- max_volt_diff_volt_entropy < ap3_entry_ncm_threshold
- max_volt_diff ≥ ap3_volt_diff_ncm_sta_threshold
- min_low_probe_temp ≥ ap3_min_low_probe_temp_ncm_threshold，min_low_cell_volt ≥ ap3_min_low_cell_volt_ncm_threshold，max_discharge_current ≤ ap3_max_discharge_current_ncm_threshold 或 abs(max_charge_current) ≤ ap3_max_charge_current_ncm_threshold

**Step2 中间值计算**
- max_volt_diff_volt_middle：NCM 电芯中位数电压

**Step3 综合判定**
- max_volt_diff_volt_mean < max_volt_diff_volt_middle - ap3_offset_ncm_shreshold

---

## 关键参数

| 参数 | 变量名 | V1.0 | 单位 |
|------|--------|------|------|
| ap3_电压列表熵阈值_ncm | ap3_entry_ncm_threshold | 1 | / |
| ap3_最小最低温度阈值_ncm | ap3_min_low_probe_temp_ncm_threshold | 10 | °C |
| ap3_最小最低电压阈值_ncm | ap3_min_low_cell_volt_ncm_threshold | 3500 | mV |
| ap3_最大充电电流阈值_ncm | ap3_max_charge_current_ncm_threshold | 20 | A |
| ap3_最大放电电流阈值_ncm | ap3_max_discharge_current_ncm_threshold | 20 | A |
| ap3_静态工况压差阈值_ncm | ap3_volt_diff_ncm_sta_threshold | 20 | mV |
| ap3_压差偏离阈值_ncm | ap3_offset_ncm_shreshold | 0.05 | mV |

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
| max_discharge_current | DOUBLE | 窗口最大放电电流 |
| max_charge_current | DOUBLE | 窗口最大充电电流 |
| min_low_probe_temp | DOUBLE | 窗口最低温度探针值 |

## 调优记录

_暂无_
