# ap16 热失控实时告警

**算法ID**：Algorithm_ThermalRunaway  
**层级**：实时算法（实时 Kafka 消息触发）  
**数据源**：OSS / TSP（saas_battery.ods_battery_detail_h_i）  
**适用范围**：全系电芯  
**告警等级**：一级（最高）  
**状态**：运行中  
**诊断代码**：P0501

---

## 核心逻辑

**Step1 数据提取与处理**
- 提取字段：battery_id、device_id、insulation_resistance、pack_cell_voltage、pack_probe_temperature
- 从 pack_probe_temperature 列表取 max / min（null 值默认为 0），单位 ℃
- 从 pack_cell_voltage 列表取 max / min（若原始单位为 V 则 ×1000 放大），单位 mV（null 值默认为 0）
- 过滤：去除总压 voltage = 6553.5V 且总电流 current = 5553.5A（同时满足）的数据帧

**Step2 采样异常识别**
- 电压有效值过滤：仅保留 (1000, 5094] mV 内的值
- 温度有效值过滤：仅保留 (-40, 0) ∪ (0, 150] ℃ 内的值，且各电芯温度值均为 0.5 的整数倍
- 初始化异常：过滤后列表为空 或 列表均为相同值 → 判为电压/温度初始化异常
- 电压一致性异常：consis_index = (q_90 - q_10) / (Vmax - Vmin) > ap16_consis_threshold（0.5）
- 电压采样异常（归一化分箱数 ap16_sample_volt_bin = 10）：
    - 条件1：任一分箱元素占比 > ap16_sample_volt_rate_threshold（0.7）AND bin0 个数 ≠ 1 AND (bin0+bin1+bin_last) 占比 < ap16_sample_volt_rate_threshold2（0.5）AND 压差 > ap16_sample_volt_threshold（30mV）
    - 条件2：bin0 占比 > ap16_sample_volt_rate_threshold3（0.9）
- 温度采样异常（归一化分箱数 ap16_sample_temp_bin = 5）：
    - 任一分箱元素占比 > ap16_sample_temp_rate_threshold（0.5）AND bin0 个数 ≤ 2 AND 温差 > ap16_sample_temp_threshold（7℃）
- 二次过滤：若 bin0 个数 ≠ 1 且 (bin0+bin1+bin_last) 占比 ≥ 0.5，仅保留 ≥ 3800mV 的电压值

**Step3 分支条件评估**
- 分支1（最大温度）：max_probe_temperature ≥ max_probe_temperature_threshold（60℃）
- 分支2（温度温差）：(max_t - min_t ≥ delta_temperature_threshold1（45℃）) OR (max_t - min_t ≥ delta_temperature_threshold2（30℃）AND min_t ≥ min_probe_temperature_threshold（10℃）)
- 分支3（电压跌落）：max_cell_voltage ≥ max_cell_voltage_threshold（4500mV）OR min_cell_voltage ≤ min_cell_voltage_threshold（1300mV）
- 分支4（电压压差）：max_cell_voltage - min_cell_voltage ≥ delta_voltage_threshold（120mV）AND max_cell_voltage ≥ min_cell_voltage_threshold2（3800mV）
- 分支5（绝缘异常）：insulation_resistance ≤ insulation_resistance_threshold1（80 kΩ）
- 采样异常影响：电压初始化/采样异常 → 分支3、4禁用；温度初始化/采样异常 → 分支1、2禁用

**Step4 组合判定与告警**
- 组合1（温度+电压）：(分支1 OR 分支2) AND (分支3 OR 分支4)
- 组合2（主分支+绝缘）：(分支1 OR 分支2 OR 分支3 OR 分支4) AND 分支5
- 组合3（任一+采样异常）：(分支1~5任一) AND (电压采样异常 OR 温度采样异常) AND 绝缘值 ≠ 0
- 以上组合为 OR 关系，任一满足即告警：【电池热失控，P0501】

---

## 关键参数

| 参数 | 变量名 | V1.0 | 单位 |
|------|--------|------|------|
| 温度阈值 | ap16_temperature_threshold | [60, 45, 30, 10] | ℃ |
| 电压阈值 | ap16_voltage_threshold | [4500, 1300, 3800, 120] | mV |
| 绝缘阈值 | ap16_insulation_resistance_threshold | [80, 0] | kΩ |
| 采样异常阈值 | ap16_sample_abnormal_threshold | [30, 0.7, 0.5, 0.9, 7, 0.5] | — |
| 采样分箱阈值 | ap16_sample_bin_threshold | [10, 5] | — |
| 一致性阈值 | ap16_consis_threshold | 0.5 | — |

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

## 案例数据

| 文件 | 说明 |
|------|------|
| 工作簿2.xlsx | 热失控案例数据（含电压/温度/绝缘等明细） |
| 误报分析报告_7FP批量_20260526.md | 7FP 批量误报分析报告（2026-05-26） |
| 热失控实时算法设计文档.md | 算法详细设计文档 V1.1 |

---

## 调优记录

_暂无_