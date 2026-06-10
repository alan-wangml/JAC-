# ap12 电池采样异常预警_专家模型_综合

**算法ID**：Algorithm_3b0ecc5289  
**层级**：预警层（离线预警，7×24h监控）  
**数据源**：block（d_i_battery_block_features）  
**适用范围**：全系电池  
**诊断代码**：P0303  
**状态**：运行中（V1.0）

---

## 上游数据表输入字段

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
| start_real_soc | double | 起始真实SOC |
| start_current | double | 起始电流（A） |
| avg_pack_voltage | double | 平均总压（V） |
| avg_insu_resis | double | 平均绝缘电阻（kΩ） |
| avg_high_cell_volt | double | 平均最高单体电压（mV） |
| avg_low_cell_volt | double | 平均最低单体电压（mV） |
| block_msg_count | int | 报文计数 |
| cell_volt_init_count | map | 单体电压初始化值计数（含4096/5094/65535） |
| probe_temp_init_count | map | 探头温度初始化值计数（含-50/-40/255） |
| insu_resis_init_count | map | 绝缘电阻初始化值计数（含5000/10000/60000） |

---

## 核心逻辑

**Step1 初始化异常帧数计算**
- 电压初始值：4096、5094、65535 mV
- 温度初始值：-50、-40、255 ℃
- 绝缘初始值：5000、10000、60000 kΩ
- 每条记录分别计算 volt_init_count、temp_init_count、resis_init_count（对应初始值出现的帧数之和）

**Step2 单条过滤**
- 按 battery_type 分组处理
- 过滤条件：volt_init_count > ap12_init_msg_count_threshold 或 temp_init_count > ap12_init_msg_count_threshold 或 resis_init_count > ap12_init_msg_count_threshold

**Step3 全天汇总过滤**
- 按 battery_id 汇总：total_msg_count = Sum(block_msg_count)，total_volt_init_count = Sum(volt_init_count)，total_temp_init_count = Sum(temp_init_count)，total_resis_init_count = Sum(resis_init_count)
- 过滤条件（全部满足）：
  - total_msg_count > ap12_total_msg_count_threshold
  - 以下任一满足：
    - total_volt_init_count / total_msg_count > ap12_volt_init_rate_threshold
    - total_temp_init_count / total_msg_count > ap12_temp_init_rate_threshold
    - total_resis_init_count / total_msg_count > ap12_resis_init_rate_threshold

**Step4 去重**：按 battery_id 分组，取 start_time 最早的一条记录

---

## 关键参数

| 参数 | 变量名 | 值 | 单位 |
|------|--------|-----|------|
| 总消息帧数阈值 | ap12_total_msg_count_threshold | 120 | 帧 |
| 电压初始化频率阈值 | ap12_volt_init_rate_threshold | 0.7 | — |
| 温度初始化频率阈值 | ap12_temp_init_rate_threshold | 0.7 | — |
| 绝缘初始化频率阈值 | ap12_resis_init_rate_threshold | 0.9 | — |
| 单条初始化帧数阈值 | ap12_init_msg_count_threshold | 20 | 帧 |

---

## 调优记录

_暂无_