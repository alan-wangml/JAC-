# ap10 电池温差离群预警_统计模型

**算法ID**：Algorithm_ec97501447  
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
| device_type | string | 设备类型 |
| block_msg_count | int | 报文计数 |
| max_temp_diff | double | 最大温差（℃） |
| max_temp_diff_temp_entropy | double | 温度香农熵 |
| min_low_probe_temp | double | 最低探头温度（℃） |

---

## 核心逻辑

**Step1 数据初筛**
- max_temp_diff_temp_entropy > 0（温度香农熵大于0，即存在温度不一致性）
- min_low_probe_temp ≠ ap10_min_low_probe_temp_0（最低探头温度不为0）
- block_msg_count > ap10_block_msg_counter_limit（报文计数大于下限）

**Step2 温差统计特征计算**
- 按 battery_id 分组，计算 avg_temp_diff_day = avg(max_temp_diff)（当日平均温差）
- 按 device_type 分组，计算 label_mean = avg(max_temp_diff)、label_std = stddev(max_temp_diff)（温差均值和标准差）
- 过滤保留 max_temp_diff > label_mean + ap10_n_std × label_std 的记录（温差显著偏离均值）
- 在过滤后的数据上，按 device_type 分组，计算 temp_entropy_mean = avg(max_temp_diff_temp_entropy)、temp_entropy_std = stddev(max_temp_diff_temp_entropy)（温度熵的均值和标准差）

**Step3 综合判定（全部满足）**
1. max_temp_diff_temp_entropy < temp_entropy_mean - ap10_temp_entropy_n_std × temp_entropy_std 或 max_temp_diff > ap10_temp_diff_experince
2. max_temp_diff > ap10_temp_diff_experince 或 max_temp_diff_temp_entropy < ap10_max_temp_diff_temp_entropy_threshold
3. battery_type ≠ '280' 或 (battery_type = '280' 且 avg_temp_diff_day > ap10_temp_diff_day_threshold)
4. avg_temp_diff_day > ap10_temp_diff_limit

**Step4 去重**：按 battery_id 分组，取 max_temp_diff 最大的一条记录

---

## 关键参数

| 参数 | 变量名 | 值 | 单位 |
|------|--------|-----|------|
| 报文计数下限 | ap10_block_msg_counter_limit | 10 | — |
| 温度熵标准差倍数 | ap10_temp_entropy_n_std | 1 | — |
| 经验温差阈值 | ap10_temp_diff_experince | 20 | ℃ |
| 温度香农熵阈值 | ap10_max_temp_diff_temp_entropy_threshold | 1.3 | — |
| 日均温差阈值（280型）| ap10_temp_diff_day_threshold | 4 | ℃ |
| 日均温差阈值（通用）| ap10_temp_diff_limit | 8 | ℃ |
| 最低探头温度下限 | ap10_min_low_probe_temp_0 | 0 | ℃ |
| 温差标准差倍数 | ap10_n_std | 4 | — |

---

## 调优记录

_暂无_