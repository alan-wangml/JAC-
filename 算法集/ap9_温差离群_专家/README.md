# ap9 电池温差离群预警_专家模型

**算法ID**：Algorithm_c1bb911a64  
**层级**：预警层（离线预警，7×24h监控）  
**数据源**：block（d_i_battery_block_features）  
**适用范围**：全系电池  
**诊断代码**：P0401  
**状态**：运行中（V2.0）

---

## 上游数据表输入字段

| 字段名 | 类型 | 说明 |
|--------|------|------|
| battery_id | string | 电池ID |
| battery_type | string | 电池类型 |
| process_id | string | 过程ID |
| device_id | string | 设备ID |
| device_name | string | 设备名称 |
| cell_type | string | 电芯类型 |
| start_time | string | 开始时间 |
| end_time | string | 结束时间 |
| block_duration | double | 块持续时间（秒） |
| event_type | string | 事件类型（parking/charge/driving等） |
| avg_temp_diff | double | 平均温差（℃） |
| max_temp_diff | double | 最大温差（℃） |
| max_high_probe_temp | double | 最高探头温度（℃） |
| min_high_probe_temp | double | 最低高温探头温度（℃） |
| min_low_probe_temp | double | 最低探头温度（℃） |
| max_temp_diff_temp_entropy | double | 温度香农熵 |
| brand | string | 品牌 |

---

## 核心逻辑

**Step1 数据初筛**
- block_duration ≥ 30（块持续时间至少30秒）
- max_temp_diff_temp_entropy ≥ 0（温度香农熵非负，即存在温度不一致性）
- 可选：按 ap9_operating_condition 过滤 event_type（如"charge,driving"）
- 可选：按 ap9_cell_type 过滤电芯类型

**Step2 综合判定（三场景，任一满足即可触发告警）**

| 场景 | 条件 |
|------|------|
| 场景1 | avg_temp_diff ≥ (parking ? 17 : 15)℃；min_low_probe_temp ∈ [-5, 60)℃；min_high_probe_temp ≥ 10℃ |
| 场景2 | max_temp_diff ≥ 30℃ 且 avg_temp_diff ≥ 7.5℃ |
| 场景3 | event_type ∈ (charge, parking)；max_temp_diff ≥ 20℃；min_low_probe_temp ≥ 5℃；max_high_probe_temp ∈ [10, 120)℃ |

**Step3 去重**：按 battery_id 分组，取 start_time 最早的一条记录

---

## 关键参数

| 参数 | 变量名 | 值 | 单位 |
|------|--------|-----|------|
| 温度香农熵阈值 | ap9_temp_diff_entropy_threshold | 0 | - |
| 最低温度范围 | ap9_low_probe_temp_range | [-5, 60) | ℃ |
| 最高温度范围 | ap9_high_probe_temp_range | [10, 120) | ℃ |
| 探头最低温度阈值（场景3）| ap9_low_probe_temp_threshold | 5 | ℃ |
| 最低高温阈值（场景1）| ap9_high_probe_temp_threshold | 10 | ℃ |
| 非parking平均温差阈值 | ap9_temp_diff_abparking_threshold | 15 | ℃ |
| parking平均温差阈值 | ap9_temp_diff_parking_threshold | 17 | ℃ |
| 大温差阈值（场景2）| ap9_temp_diff_max_threshold | 30 | ℃ |
| 大温差场景平均温差阈值 | ap9_temp_diff_avg_threshold | 7.5 | ℃ |
| 充停场景大温差阈值（场景3）| ap9_temp_diff_chgorprk_threshold | 20 | ℃ |
| 工况过滤（可选）| ap9_operating_condition | null | - |
| 电芯类型过滤（可选）| ap9_cell_type | "" | - |

---

## 调优记录

_暂无_