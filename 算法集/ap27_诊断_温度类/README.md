# ap27 诊断层_专家模型_温度类

**算法ID**：Algorithm_ap27  
**层级**：诊断层（离线，按天运行）  
**数据源**：block（d_i_battery_block_features）、alarm（d_i_alarm_results）  
**上游算法**：ap9、ap10、ap11  
**诊断代码**：P0302（CSC温度采样误差大）/ P0308（采样可靠性劣化）  
**状态**：运行中（V1.0）

---

## 上游数据表输入字段

### d_i_alarm_results（告警数据）

| 字段名 | 类型 | 说明 |
|--------|------|------|
| algorithm_id | string | 算法ID（Algorithm_ap9/ap10/ap11） |
| battery_id | string | 电池ID（告警对象） |
| hash_code | string | 哈希码 |
| algorithm_instance | string | 算法实例 |
| result_create_time | string | 告警创建时间 |
| additional_data | map | 附加数据（含 device_id/process_id/msg_type/data_type） |
| alarm_data | string | 告警数据 |

### d_i_battery_block_features（block特征数据）

| 字段名 | 类型 | 说明 |
|--------|------|------|
| battery_id | string | 电池ID |
| battery_type | string | 电池类型 |
| start_time | string | 开始时间 |
| max_temp_diff | double | 最大温差（℃） |
| min_low_probe_temp | double | 最低探头温度（℃） |

---

## 核心逻辑

**Step1 数据读取与预处理**
- 从 d_i_alarm_results 读取当天 ap9/ap10/ap11 告警数据
- 从 d_i_battery_block_features 读取告警电池的 block 特征数据（回溯 ap27_date_back_days 天）
- 按 battery_type 分组处理

**Step2 分组切片**
- 按 battery_id 回溯时间窗口内每帧，计算 Delta_temp = max_temp_diff
- 与 ap27_max_temp_diff_limit 比较，分为疑似组（≥阈值）和正常组
- 使用 group_id 标记连续疑似片段

**Step3 持续时间判断**
- 疑似组持续时长 duration ≥ ap27_temp_diff_lasts_limit × 3600 秒
- 疑似组内 min_low_probe_temp > ap27_min_low_temp_threshold 的帧占比 ≥ ap27_min_low_temp_ratio
- 满足 → pre_diagnosis = P0302；否则 → P0308

**Step4 恢复判断（仅 P0302 进入）**
- 计算最后疑似组的最低温度变化幅度 delta_low_temp、最低温度终值 low_temp_last、负斜率占比 neg_slope_ratio
- 同时满足以下三条则认为恢复（输出 P0308），否则保持 P0302：
  1. delta_low_temp ≥ ap27_min_low_temp_range
  2. low_temp_last > ap27_min_low_probe_temp_last_threshold
  3. neg_slope_ratio < ap27_negative_temp_slope_ratio_threshold

**Step5 特征提取**：输出 last_suspect_max_temp_diff、last_suspect_last_duration、last_suspect_delta_low_temp、last_suspect_min_low_probe_temp_last、last_suspect_negative_temp_slope_ratio

---

## 关键参数

| 参数 | 变量名 | 值 | 单位 |
|------|--------|-----|------|
| 回溯天数 | ap27_date_back_days | 7 | 天 |
| 最大温差阈值 | ap27_max_temp_diff_limit | 7 | ℃ |
| 温差持续时间阈值 | ap27_temp_diff_lasts_limit | 24 | 小时 |
| 最低温度阈值 | ap27_min_low_temp_threshold | 0 | ℃ |
| 最低温度比例阈值 | ap27_min_low_temp_ratio | 0.8 | — |
| 温度变化幅度阈值 | ap27_min_low_temp_range | 10.0 | ℃ |
| 最低温度终值阈值 | ap27_min_low_probe_temp_last_threshold | 0 | ℃ |
| 负斜率占比阈值 | ap27_negative_temp_slope_ratio_threshold | 0.2 | — |

---

## 调优记录

_暂无_