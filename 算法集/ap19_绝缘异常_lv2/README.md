# ap19 绝缘异常报警_专家模型_lv2

**算法ID**：Algorithm_InsulationAnormalyLv2  
**层级**：实时算法（实时 Kafka 消息触发）  
**数据源**：OSS / TSP（saas_battery.ods_battery_detail_h_i）  
**适用范围**：全系电芯  
**告警等级**：二级  
**状态**：运行中  
**诊断代码**：P0101 / P0501

---

## 核心逻辑

**Step1 数据初筛**
- 去除绝缘值为 NULL 的数据帧（电压温度列表为 NULL 同样过滤）
- 检测到电池绝缘阻值 insulation_resistance < ap19_insulation_resistance_threshold[0]（8000 kΩ）

**Step2 充电状态与继电器判定**
- 判断充电状态 charge_state 是否 in ap19_soc_charge_start_array（[1,2]），in 则为充电连接状态，丢弃数据
- 若不为充电连接状态，判断高压继电器状态 bms_cntctr_sts 是否为闭合状态（bms_cntctr_sts = 1），若为闭合状态则丢弃数据
- 若 insulation_resistance 不为 null 且 bms_cntctr_sts 为 null，判断 abs(current) <= ap19_current_threshold（1A），若是则保留数据

**Step3 滑动窗口判定**
- 若继电器不为闭合状态，判断绝缘阻值 insulation_resistance < ap19_insulation_resistance_threshold[0]（8000）
- 持续 ap19_Realtime_continually_TSP_threshold[0]（4）帧以上
- 且存在大于 ap19_Realtime_frame_TSP_threshold（3）帧非相同值
- 不满足则丢弃数据

**Step4 严重绝缘异常判定**
- 绝缘阻值 insulation_resistance < ap19_insulation_resistance_threshold[1]（40 kΩ）
- 且持续 ap19_Realtime_continually_TSP_threshold[1]（4）帧以上
- 满足则发起【二级响应】，诊断代码 P0101

---

## 关键参数

| 参数 | 变量名 | V1.0 | 单位 |
|------|--------|------|------|
| 电池绝缘阈值 | ap19_insulation_resistance_threshold | [8000, 40] | kΩ |
| TSP充电状态 | ap19_soc_charge_start_array | [1, 2] | — |
| TSP绝缘低持续时间阈值 | ap19_Realtime_continually_TSP_threshold | [4, 4] | 帧 |
| TSP绝缘低数据帧阈值 | ap19_Realtime_frame_TSP_threshold | 3 | 帧 |
| 电流阈值 | ap19_current_threshold | 1 | A |
| 采样分箱阈值 | ap19_sample_bin_threshold | [10, 5] | — |
| 采样异常阈值 | ap19_sample_abnormal_threshold | [30, 0.7, 0.5, 0.9, 7, 0.5] | — |

---

## 上游数据表输入字段

**数据源表**：`saas_battery.ods_battery_detail_h_i`（TSP/OSS 实时 Kafka 明细数据）

| 字段名 | 类型 | 说明 |
|--------|------|------|
| battery_id | STRING | 电池ID |
| battery_model | STRING | 电池型号 |
| device_id | STRING | 设备ID |
| device_type | STRING | 设备类型 |
| sample_time | BIGINT | 采样时间戳（毫秒） |
| battery_state | INT | 电池状态（1=充电, 3=行驶, 其他=停放） |
| charge_state | INT | 充电状态 |
| bms_cntctr_sts | INT | 高压继电器状态（1=闭合） |
| process_id | STRING | 报文ID |
| insulation_resistance | DOUBLE | 绝缘阻值（kΩ） |
| voltage | DOUBLE | 总压（V） |
| current | DOUBLE | 电流（A） |
| user_soc | DOUBLE | 用户SOC（%） |
| max_cell_voltage | DOUBLE | 最高单体电压（V） |
| min_cell_voltage | DOUBLE | 最低单体电压（V） |
| max_probe_temperature | DOUBLE | 最高探针温度（℃） |
| min_probe_temperature | DOUBLE | 最低探针温度（℃） |
| pack_cell_voltage | ARRAY<DOUBLE> | 单体电压列表 |
| pack_probe_temperature | ARRAY<DOUBLE> | 探针温度列表 |

---

## 调优记录

_暂无_