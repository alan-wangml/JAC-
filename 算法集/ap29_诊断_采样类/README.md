# ap29 诊断层_专家模型_采样类

**算法ID**：Algorithm_ap29  
**层级**：诊断层（离线，按天运行）  
**数据源**：block（d_i_battery_block_features）、alarm（d_i_alarm_results）  
**上游算法**：ap12（综合）、ap13（总压）、ap24（电压对称性）  
**诊断代码**：P0301（CSC电压采样误差大）/ P0303（CSC功耗异常）/ P0308（采样可靠性劣化）  
**状态**：运行中（V1.0）

---

## 上游数据表输入字段

### d_i_alarm_results（告警数据）

| 字段名 | 类型 | 说明 |
|--------|------|------|
| algorithm_id | string | 算法ID（Algorithm_ap12/ap13/ap24） |
| battery_id | string | 电池ID（告警对象） |
| result_create_time | string | 告警创建时间 |
| additional_data | map | 附加数据（含 device_id/process_id/msg_type/data_type） |
| alarm_data | string | 告警数据 |

### d_i_battery_block_features（block特征数据）

| 字段名 | 类型 | 说明 |
|--------|------|------|
| battery_id | string | 电池ID |
| battery_type | string | 电池类型 |
| start_time | string | 开始时间 |
| max_volt_diff | double | 最大压差（mV） |
| max_volt_diff_max_cell_voltage | double | 最大压差对应的最高单体电压（mV） |
| max_volt_diff_min_cell_voltage | double | 最大压差对应的最低单体电压（mV） |
| max_volt_diff_cell_voltage | array | 最大压差时的所有电芯电压列表 |

---

## 核心逻辑

**Step1 数据读取与预处理**
- 从 d_i_alarm_results 读取当天 ap12/ap13/ap24 告警数据
- 从 d_i_battery_block_features 读取告警电池的 block 特征数据（回溯 ap29_date_back 天）
- 按 battery_type 分组处理

**Step2 电压统计量计算**
- 对 max_volt_diff_cell_voltage 电芯列表计算均值（fe_mean）、标准差（fe_std）、max_minus_rest_mean
- 若列表为空或长度 ≤ 1，返回 (0.0, 0.0, 0.0)

**Step3 离群判断**
- 高离群上限：high_outlier_limit = fe_mean + ap29_high_outlier_sigma_coeff × fe_std
- 低离群下限：low_outlier_limit = fe_mean - ap29_low_outlier_sigma_coeff × fe_std
- is_high_outlier_alarm：max_volt_diff_max_cell_voltage > ap29_max_volt_diff_limit[0] → 1
- is_low_outlier_alarm：max_volt_diff_min_cell_voltage < ap29_max_volt_diff_limit[1] → 1

**Step4 电压跳变计数**
- 按 battery_id 时间排序，使用 lead 窗口函数获取下一帧的离群标记
- volt_bounce_up_count：当前帧高离群=1 且下一帧=0 的帧数
- volt_bounce_down_count：当前帧低离群=1 且下一帧=0 的帧数
- 汇总后：volt_bounce_up_count ≥ ap29_volt_bounce_up_count_limit → 1；volt_bounce_down_count ≥ ap29_volt_bounce_down_count_limit → 1

**Step5 融合诊断**
- algorithm_id = Algorithm_ap24 且 (volt_bounce_up_count = 1 或 volt_bounce_down_count = 1) → P0301
- algorithm_id ∈ (Algorithm_ap12, Algorithm_ap14) → P0303
- 其他 → P0308

---

## 关键参数

| 参数 | 变量名 | 值 | 单位 |
|------|--------|-----|------|
| 回溯天数 | ap29_date_back | 1 | 天 |
| 高离群 sigma 系数 | ap29_high_outlier_sigma_coeff | 4.5 | — |
| 低离群 sigma 系数 | ap29_low_outlier_sigma_coeff | 4.5 | — |
| 电压跳变上限计数阈值 | ap29_volt_bounce_up_count_limit | 2 | 次 |
| 电压跳变下限计数阈值 | ap29_volt_bounce_down_count_limit | 2 | 次 |
| 最大压差限制 | ap29_max_volt_diff_limit | [100, 100] | mV |

---

## 调优记录

_暂无_