# ap13 电池采样异常预警_专家模型_总压采样异常

**算法ID**：Algorithm_5cc8826925  
**层级**：预警层（离线预警，7×24h监控）  
**数据源**：block（d_i_battery_block_features）+ dim（ods_dim_battery_model）  
**适用范围**：全系电池  
**诊断代码**：P0305  
**状态**：运行中（V1.2）  
**常见误报**：电压数据不更新（总压数据正常更新时）

---

## 上游数据表输入字段

### d_i_battery_block_features

| 字段名 | 类型 | 说明 |
|--------|------|------|
| battery_id | string | 电池ID |
| battery_type | string | 电池类型 |
| device_id | string | 设备ID |
| device_name | string | 设备名称 |
| process_id | string | 过程ID |
| event_type | string | 事件类型 |
| start_time | string | 开始时间 |
| end_time | string | 结束时间 |
| start_user_soc | double | 起始用户SOC |
| start_current | double | 起始电流（A） |
| avg_pack_voltage | double | 平均总压（V） |
| avg_insu_resis | double | 平均绝缘电阻（kΩ） |
| avg_high_cell_volt | double | 平均最高单体电压（mV） |
| avg_low_cell_volt | double | 平均最低单体电压（mV） |
| max_high_cell_volt | double | 最大最高单体电压（mV） |
| min_low_cell_volt | double | 最小最低单体电压（mV） |
| min_high_cell_volt | double | 最小最高单体电压（mV） |
| max_low_cell_volt | double | 最大最低单体电压（mV） |
| max_volt_sn_count | array | 最高单体电压探头编号及计数 |
| min_volt_sn_count | array | 最低单体电压探头编号及计数 |
| max_temp_sn_count | array | 最高温度探头编号及计数 |
| min_temp_sn_count | array | 最低温度探头编号及计数 |
| block_msg_count | int | 报文计数 |

### ods_dim_battery_model

| 字段名 | 类型 | 说明 |
|--------|------|------|
| battery_model | string | 电池型号（关联 battery_type） |
| cell_number | int | 串联数（series_num） |

---

## 核心逻辑

**Step1 数据初筛**
- 按 battery_type 过滤（从参数配置读取）
- size(max_volt_sn_count) > 1 或 size(min_volt_sn_count) > 1 或 size(max_temp_sn_count) > 1 或 size(min_temp_sn_count) > 1
- 即：最高/最低单体电压探头数量或最高/最低温度探头数量中任一大于1
- LEFT JOIN ods_dim_battery_model 获取 series_num（串联数）

**Step2 总压一致性计算（3种方式取最优）**
- 方式1：delta_cell_pack_volt_1 = (avg_high_cell_volt + avg_low_cell_volt) / 2 × series_num / 1000 - avg_pack_voltage
- 方式2：delta_cell_pack_volt_2 = (max_high_cell_volt + min_low_cell_volt) / 2 × series_num / 1000 - avg_pack_voltage
- 方式3：delta_cell_pack_volt_3 = (min_high_cell_volt + max_low_cell_volt) / 2 × series_num / 1000 - avg_pack_voltage
- 取三者中绝对值最小的作为 delta_cell_pack_volt（即最优估计）
- 过滤条件：block_msg_count > ap13_block_msg_counter_limit

**Step3 综合判定**
- delta_cell_pack_volt > ap13_delta_cell_pack_volt_threshold（默认 20V）→ 触发告警
- 按 battery_id 分组，取 delta_cell_pack_volt 绝对值最大且 start_time 最早的记录作为告警明细
- 告警诊断代码：P0305（总压采样异常）

---

## 关键参数

| 参数 | 变量名 | V1.0 | 单位 |
|------|--------|------|------|
| 报文计数下限 | ap13_block_msg_counter_limit | 1 | — |
| 总压偏差阈值 | ap13_delta_cell_pack_volt_threshold | 20 | V |

---

## 调优记录

_暂无_