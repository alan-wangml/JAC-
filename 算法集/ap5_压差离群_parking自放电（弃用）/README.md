# 压差离群预警_专家模型3_parking自放电

**算法ID**：—  
**层级**：预警层（离线预警，7×24h监控）  
**数据源**：TSP 和 OSS  
**状态**：运行中（V2.0）

---

## 核心逻辑

**Step1 数据初筛**
- event_type = 'parking'
- max_volt_diff 前后两帧变化 ≤ 5mV（不满足则丢弃）
- event_duration ≥ 200min

**Step2 自放电特征计算**（去极化）
- t1：事件起始 + 70min 后第一帧
- t2：事件最后一帧
- vdiff_list[n] = volt_list[n]_t1 - volt_list[n]_t2
- vdiff_avg = avg(vdiff_list)

**Step3 综合判定（全部满足）**
- vdiff_avg ≤ 10mV
- max(vdiff_list) - vdiff_avg ≥ 4mV
- avg(volt_t2) - min(volt) ≥ 10mV

---

## 关键参数

| 参数 | 值 |
|------|---|
| 事件时长下限 | 200 min |
| 去极化等待时长 | 70 min |

---

## 调优记录

_暂无_
