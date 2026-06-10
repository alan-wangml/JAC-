# ap7 压差离群预警_统计模型_AlphaQ

**算法ID**：Algorithm_4e90fc8f12  
**层级**：预警层（离线预警，7×24h监控）  
**数据源**：block（d_i_battery_block_features）  
**适用范围**：仅 70kWh 电池（battery_type in ('102', '102-X')）  
**诊断代码**：P0201（自放电异常）  
**状态**：运行中（V1.0）

---

## 核心逻辑

**Step1 数据筛选**
- 筛选 70kWh 电池（battery_type in ('102', '102-X')）
- 慢充事件起始 SOC ≤ 30%（scg_start_soc = 30.0）
- 当前事件 SOC 跨度 ≥ 18%（soc_gap = 18.0），回溯事件 SOC 跨度 ≥ 19%（back_event_soc_gap = 19.0）
- 回溯天数 ≤ 30 天（back_date_limit = 30）
- block 消息数 ≥ 3（block_msg_counter_limit = 3）

**Step2 充电排名计算**
- 对 SOC ∈ [60%, 80%] 区间内的所有 block 帧，计算每个电芯的充电排名（按电压高低排序）
- 归一化排名 volt_rank_mean ∈ [0, 1]，1 = 排名最差（电压最低），0 = 排名最好
- 同时获取上一次同充电类型事件的 volt_rank_mean_lag

**Step3 排名突降检测（逐电芯扫描 1~192）**
- 当前事件排名 ≥ cell_rank_mean_limit（0.99）→ 当前排名极差
- 回溯事件排名 < cell_rank_mean_lag_limit（0.49）→ 回溯期间排名曾正常
- 两条件同时满足 → 该电芯为疑似问题电芯

**Step4 支路结构检查**
- 确定疑似电芯所在组（0~7）和支路（1 或 2）
- 计算组内 24 芯排名变化（current - lag）：正值 = 恶化，负值 = 改善
- 问题支路中排名改善（变化值 < 0）的电芯数 ≥ n_drop（8）
- 对侧支路中排名恶化（变化值 > 0）的电芯数 ≥ n_raise（10）
- 两条件同时满足 → 触发告警，输出首个满足条件的电芯

**Step5 输出**
- 告警触发时输出问题电芯编号、所在组/支路、排名变化量、裕度信息
- 未触发时输出全局排名分布和候选电芯列表（供漏报分析）
- 诊断层若 algorithm_id = alphaQ，则 diagnosis_code 直接赋值 P0201

---

## 关键参数

| 参数 | 变量名 | 值 | 单位 |
|------|--------|-----|------|
| 当前事件排名阈值 | cell_rank_mean_limit | 0.99 | — |
| 回溯事件排名阈值 | cell_rank_mean_lag_limit | 0.49 | — |
| 同支路改善电芯数下限 | n_drop | 8 | 个 |
| 对侧支路恶化电芯数下限 | n_raise | 10 | 个 |
| SOC采样窗口下限 | soc_low | 60.0 | % |
| SOC采样窗口上限 | soc_high | 80.0 | % |
| 当前事件最小SOC跨度 | soc_gap | 18.0 | % |
| 回溯事件最小SOC跨度 | back_event_soc_gap | 19.0 | % |
| 慢充起始SOC上限 | scg_start_soc | 30.0 | % |
| 回溯天数 | back_date_limit | 30 | 天 |
| block消息数下限 | block_msg_counter_limit | 3 | 条 |
| 快/慢充分界电流 | current_limit | 200.0 | A |

---

## 上游数据表输入字段

**数据源表**：`saas_battery.d_i_battery_block_features`

| 字段名 | 类型 | 说明 |
|--------|------|------|
| battery_id | STRING | 电池ID |
| battery_type | STRING | 电池型号 |
| device_id | STRING | 设备ID |
| process_id | STRING | 报文ID |
| process_id_lag | STRING | 回溯事件报文ID |
| charge_mode_tag | INT | 充电模式标签（2=慢充, 3=快充） |
| start_time | TIMESTAMP | 窗口起始时间 |
| start_time_lag | TIMESTAMP | 回溯事件窗口起始时间 |
| volt_rank_mean | ARRAY<DOUBLE> | 当前事件192个电芯平均排名（归一化到[0,1]） |
| volt_rank_mean_lag | ARRAY<DOUBLE> | 回溯事件192个电芯平均排名（归一化到[0,1]） |
| volt_diff_mean | DOUBLE | 当前事件平均压差 |
| volt_diff_mean_lag | DOUBLE | 回溯事件平均压差 |
| block_msg_count | INT | 窗口报文总数 |

## 调优记录

_暂无_
