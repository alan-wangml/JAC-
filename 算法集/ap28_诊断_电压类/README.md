# ap28 诊断层_专家模型_电压类

**算法ID**：Algorithm_ap28  
**层级**：诊断层（离线，按天运行）  
**数据源**：block（d_i_battery_block_features）+ alarm（d_i_alarm_results）  
**适用范围**：LFP / NCM 电芯  
**状态**：运行中  
**上游算法**：ap2、ap3、ap8、alphaQ、统计模型、数学模型

---

## 上游数据表输入字段

### d_i_alarm_results（告警数据）

| 字段名 | 类型 | 说明 |
|--------|------|------|
| algorithm_id | string | 算法ID（ap2/ap3/ap4/ap6/ap8等） |
| battery_id | string | 电池ID（告警对象） |
| result_create_time | string | 告警创建时间 |
| additional_data | map | 附加数据（含 device_id/process_id/msg_type/data_type） |
| alarm_data | string | 告警数据 |

### d_i_battery_block_features（block特征数据）

| 字段名 | 类型 | 说明 |
|--------|------|------|
| battery_id | string | 电池ID |
| battery_type | string | 电池类型 |
| event_type | string | 事件类型（parking/charge/driving等） |
| start_time | string | 开始时间 |
| end_time | string | 结束时间 |
| start_user_soc | double | 起始用户SOC |
| start_current | double | 起始电流（A） |
| avg_pack_voltage | double | 平均总压（V） |
| avg_high_cell_volt | double | 平均最高单体电压（mV） |
| avg_low_cell_volt | double | 平均最低单体电压（mV） |
| max_volt_diff | double | 最大压差（mV） |
| max_volt_diff_max_cell_voltage | double | 最大压差对应的最高单体电压（mV） |
| max_volt_diff_min_cell_voltage | double | 最大压差对应的最低单体电压（mV） |
| max_volt_diff_cell_voltage | array | 最大压差时的所有电芯电压列表 |
| max_volt_sn_count | array | 最高单体电压探头编号及计数 |
| min_volt_sn_count | array | 最低单体电压探头编号及计数 |
| max_volt_diff_volt_entropy | double | 电压香农熵 |
| volt_diff_quartiles | array | 压差分位数 |
| last_pack_cell_voltage | array | 最后帧所有电芯电压列表 |
| block_avg_volt_diff | double | block平均压差（mV） |
| block_max_volt_diff | double | block最大压差（mV） |

---

## 诊断代码映射

| 代码 | 含义 |
|------|------|
| P0201 | 自放电大 |
| P0202 | 欧姆内阻大 |
| P0203 | 极化内阻大 |
| P0204 | 串联 busbar 异常 |
| P0205 | 并联 busbar 异常 |
| P0206 | 电芯低容 |
| P0207 | CSC 功耗异常 |
| P0208 | EMC 干扰 |
| P0301 | CSC 电压采样误差大 |
| P0309 | SOC 偏差大 |
| P0211 | 压差偏低（降级） |
| P0212 | 压差偏低（降级） |
| P0001 | 无诊断结论 |

---

## 核心逻辑

**Step1 数据准备与特征提取**
- 读取当天告警数据（d_i_alarm_results），筛选上游电压类算法（ap2/ap3/ap4/ap6/ap8）的预警结果
- 回溯 ap28_date_back 天的 block 特征数据，按 battery_type 分组处理
- 提取15个特征标签：
    - event_volt_diff_soc_pcorrelation：压差与SOC的皮尔逊相关系数
    - event_volt_diff_current_pcorrelation：压差与电流的皮尔逊相关系数
    - event_volt_diff_entropy：压差熵值
    - is_high_outlier_alarm：高离群告警标记
    - is_low_outlier_alarm：低离群告警标记
    - vmax_to_mean_rest_diff：最高电压偏离其余电芯均值的程度
    - is_extre_unstable：极值不稳定性
    - hg_sn_is_busbar：最高压电芯是否为 busbar
    - is_module_imbalance：模组不均衡
    - is_loop_volt_low_outlier：回路电压低离群
    - is_circ：环流标记
    - max_slope：压差时间斜率
    - is_selfdch：自放电标记
    - block_avg_volt_diff：block 平均压差
    - block_max_volt_diff：block 最大压差

**Step2 自放电专项判定（LFP组 / NCM组）**
- 回溯 parking 事件，按连续 parking 分组（group_id），过滤 group_last_time > ap28_parking_event_duration_threshold
- 取每组最后一帧的 last_pack_cell_voltage，排序后计算 vmax、vmin、vmin_2nd、vgap、vgap_2nd
- LFP组：按 vmax 分6档区间，分别判定 vgap ≥ 阈值 且 vgap_2nd ≤ 阈值 → P0201
- NCM组：按 vmax 分2档区间，判定 vgap ≥ 阈值 且（vgap_2nd ≤ 阈值 或 vgap_lowest_2 ≥ 阈值）→ P0201
- NCM组额外过滤：max_slope > ap28_max_slope_threshold 才保留
- 对候选电池，通过 SOC-OCV 插值将电压转为 SOC，线性回归计算自放电斜率（slope）
    - LFP：slope ≥ ap28_lfp_socrate_threshold[0] 或（slope ≥ ap28_lfp_socrate_threshold[1] 且 r² ≥ ap28_lfp_r2_threshold）→ P0201
    - NCM：slope ≥ ap28_ncm_socrate_threshold → P0201

**Step3 通用组诊断与融合**
- 15个特征标签按规则匹配诊断代码：
    - P0202（欧姆内阻大）：压差-电流相关性高 + 高离群 + 极值不稳定
    - P0203（极化内阻大）：压差-SOC相关性高 + 高离群 + 极值不稳定
    - P0204（串联busbar异常）：最高压电芯为busbar + 模组不均衡
    - P0205（并联busbar异常）：最高压电芯为busbar + 回路电压低离群
    - P0206（电芯低容）：低离群 + 极值不稳定
    - P0207（CSC功耗异常）：环流标记
    - P0208（EMC干扰）：压差熵值高
    - P0301（CSC采样误差大）：高离群 + 低离群 + 极值不稳定
    - P0309（SOC偏差大）：压差-SOC相关性高 + 低离群
- 融合规则（从上往下匹配，命中即退出）：
    1. LFP组自放电 P0201 → 输出 P0201
    2. NCM组自放电 P0201 → 输出 P0201
    3. 通用组 ≠ P0201 → 输出通用结果
    4. 通用组 = P0201 → 输出 P0201
- 压差偏低降级：诊断码为 P0201/P0202/P0203/P0204/P0301/P0309 时，若对应压差指标低于阈值，降级为 P0211/P0212/P0001
- 取告警事件中压差最大时刻的打点数据作为诊断明细

---

## 关键参数

| 参数 | 变量名 | V1.0 | 单位 |
|------|--------|------|------|
| ap28_回溯天数 | ap28_date_back | 7 | 天 |
| ap28_等待时间 | ap28_wait_time | 7200 | s |
| ap28_斜率计算点数 | ap28_slope_n_points | 3 | / |
| ap28_短路电流阈值 | ap28_sc_current_threshold | 18 | A |
| ap28_最大斜率阈值 | ap28_max_slope_threshold | 0.11 | / |
| ap28_母线电芯数量 | ap28_m_sn_count | 50 | / |
| ap28_母线电芯比例 | ap28_sn_rate | 0.8 | / |
| ap28_压差SOC相关性阈值 | ap28_event_volt_diff_soc_corr_threshold | 0.1 | / |
| ap28_压差电流相关性阈值 | ap28_event_volt_diff_current_corr_threshold | 0.5 | / |
| ap28_压差熵值阈值 | ap28_event_volt_diff_entropy_threshold | 0.5 | / |
| ap28_平均压差阈值 | ap28_avg_volt_diff_threshold | 20 | mV |
| ap28_最大压差阈值数组 | ap28_max_volt_diff_threshold_array | [280, 380, 15, 250, 100] | mV |
| ap28_parking事件持续阈值 | ap28_parking_event_duration_threshold | 7200 | s |
| ap28_LFP最高电压分档阈值 | ap28_lfp_batt_last_highest_cell_voltage_threshold_array | [3000, 3205, 3255, 3270, 3290, 3295] | mV |
| ap28_LFP压差阈值 | ap28_lfp_batt_volt_diff_maxlowest_threshold_array | [550, 49, 44, 37, 35] | mV |
| ap28_LFP次低压差阈值 | ap28_lfp_batt_volt_diff_maxlower_threshold_array | [22, 14, 9, 6, 4] | mV |
| ap28_NCM最高电压分档阈值 | ap28_ncm_batt_last_highest_cell_voltage_threshold_array | [3650, 4200, 3650, 4200] | mV |
| ap28_NCM压差阈值 | ap28_ncm_batt_volt_diff_maxlowest_threshold_array | [36, 36] | mV |
| ap28_NCM次低压差阈值 | ap28_ncm_batt_volt_diff_maxlower_threshold_array | [15, 15] | mV |
| ap28_NCM最低两电芯压差阈值 | ap28_ncm_batt_volt_diff_lowerlowest_threshold_array | [20, 20] | mV |
| ap28_NCM末次压差阈值 | ap28_ncm_last_volt_diff_threshold_array | [28, 28] | mV |
| ap28_NCM过滤电压阈值 | ap28_ncm_filter_volt_threshold_array | [3650, 4200] | mV |
| ap28_LFP自放电率电压阈值 | ap28_lfp_socrate_volt_threshold_array | [3200, 3289] | mV |
| ap28_LFP自放电率阈值 | ap28_lfp_socrate_threshold | [0.75, 0.3] | / |
| ap28_LFP R²阈值 | ap28_lfp_r2_threshold | 0.16 | / |
| ap28_NCM R²阈值 | ap28_ncm_r2_threshold | 0.8 | / |
| ap28_NCM自放电率阈值 | ap28_ncm_socrate_threshold | 0.22 | / |
| ap28_高压差阈值 | ap28_vdiff_high_threshold | 40 | mV |

---

## 调优记录

_暂无_