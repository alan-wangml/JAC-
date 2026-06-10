%pyspark
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

"""
压差离群预警_AlphaQ 算法模型
算法ID：Algorithm_4e90fc8f12
适用范围：仅 70kWh 电池（battery_type in ('102', '102-X')）
"""

# 默认参数（按 battery_type 配置）
default_params = {
    "cell_rank_mean_limit": 0.99,
    "cell_rank_mean_lag_limit": 0.49,
    "n_drop": 8,
    "n_raise": 10,
    "n_cells_total": 192,
    "n_cells_per_group": 24,
    "n_cells_per_branch": 12,
    "soc_low": 60.0,
    "soc_high": 80.0,
    "soc_gap": 18.0,
    "back_event_soc_gap": 19.0,
    "scg_start_soc": 30.0,
    "back_date_limit": 30,
    "block_msg_counter_limit": 3,
    "current_limit": 200.0,
}
dict_params = {}  # 由外部注入: algorithm.dict_params

# ════════════════════════════════════════════════════════════
# 1. 参数配置（AlphaQ V1.0）
# ════════════════════════════════════════════════════════════

@dataclass
class AlphaQConfig:
    """
    AlphaQ 参数配置（来自《模型参数文档》）
    可在实例化时覆盖任意参数，便于调参验证。
    """

    # ─ 核心判定参数 ──────────────────────────────────────────
    cell_rank_mean_limit: float = 0.99
    """当前事件：电芯平均排名 ≥ 此值 → 疑似问题电芯（排名归一化，1=最差）"""

    cell_rank_mean_lag_limit: float = 0.49
    """回溯事件：电芯平均排名 < 此值 → 回溯期间排名正常（排名未持续异常）"""

    n_drop: int = 8
    """问题支路中排名改善（朝0方向变化）的电芯数下限"""

    n_raise: int = 10
    """对侧支路中排名恶化（朝1方向变化）的电芯数下限"""

    # ─ 电池包结构参数（70kWh，固定，不建议修改）────────────────
    n_cells_total: int = 192        # 总电芯数
    n_cells_per_group: int = 24     # 每组电芯数（对应1个 block：2支路串联）
    n_cells_per_branch: int = 12    # 每支路电芯数

    # ─ 数据预处理参数（已在聚合阶段应用，此处仅供文档记录）────
    soc_low: float = 60.0           # SOC采样窗口下限 %
    soc_high: float = 80.0          # SOC采样窗口上限 %
    soc_gap: float = 18.0           # 当前事件最小SOC跨度 %
    back_event_soc_gap: float = 19.0  # 回溯事件最小SOC跨度 %
    scg_start_soc: float = 30.0     # 慢充事件起始SOC上限 %
    back_date_limit: int = 30       # 回溯天数
    block_msg_counter_limit: int = 3  # block消息数下限
    current_limit: float = 200.0    # 快/慢充分界电流（A，绝对值）

    def to_dict(self) -> Dict:
        return {
            "cell_rank_mean_limit":     self.cell_rank_mean_limit,
            "cell_rank_mean_lag_limit": self.cell_rank_mean_lag_limit,
            "n_drop":                   self.n_drop,
            "n_raise":                  self.n_raise,
        }


# ════════════════════════════════════════════════════════════
# 2. 输入 / 输出数据结构
# ════════════════════════════════════════════════════════════

@dataclass
class CaseInput:
    """单个案例（一对「当前事件 + 回溯事件」的聚合排名数据）"""
    volt_rank_mean:     List[float]         # 当前事件 192 个电芯平均排名 [0,1]，1=最差
    volt_rank_mean_lag: List[float]         # 回溯事件 192 个电芯平均排名
    case_id:            str  = "case_0"
    battery_id:         str  = ""
    battery_type:       str  = ""
    device_id:          str  = ""
    process_id:         str  = ""
    process_id_lag:     str  = ""
    charge_mode_tag:    int  = 0            # 2=慢充, 3=快充
    volt_diff_mean:     float = float("nan")
    volt_diff_mean_lag: float = float("nan")
    start_time:         str  = ""
    start_time_lag:     str  = ""
    label:              int  = -1           # 1=应告警, 0=正常, -1=未知
    metadata:           Dict = field(default_factory=dict)


@dataclass
class CandidateCell:
    """满足排名突降条件但支路检查未通过的候选电芯（供漏报分析）"""
    cell_sn:          int    # 电芯编号（1-192）
    group_num:        int    # 所在组（0-7）
    branch_num:       int    # 所在支路（1 或 2）
    rank_mean:        float  # 当前事件排名
    rank_mean_lag:    float  # 回溯事件排名
    rank_change:      float  # current - lag（正=恶化，负=改善）
    dec_num:          int    # 同支路改善电芯数
    rise_num:         int    # 对侧支路恶化电芯数
    dec_shortfall:    int    # 距 n_drop 的差距（0=已达标）
    rise_shortfall:   int    # 距 n_raise 的差距


@dataclass
class AlarmResult:
    """单案例完整输出结果"""
    case_id:              str
    alarm_triggered:      bool
    err_sn:               Optional[int]    # 问题电芯编号（1-192），None=未触发
    triggered_group:      Optional[int]   # 所在组编号（0-7）
    triggered_branch:     Optional[int]   # 所在支路（1 或 2）
    dec_num:              int             # 同支路改善电芯数
    rise_num:             int             # 对侧支路恶化电芯数

    # 问题电芯关键量测（仅 alarm_triggered=True 时有效）
    err_rank_mean:        float           # 问题电芯当前排名
    err_rank_mean_lag:    float           # 问题电芯回溯排名
    err_rank_change:      float           # 排名变化量（current - lag）

    # 组内 24 芯排名快照（供可视化和报告）
    sort_info:            List[float]     # 当前事件
    sort_lag_info:        List[float]     # 回溯事件

    # 裕度信息（供 Skill 分析）
    margins:              Dict[str, float]

    # 未触发告警的候选电芯（供漏报归因）
    candidate_cells:      List[CandidateCell]

    # 全局排名统计
    n_cells_rank_ge_99:   int             # 排名 ≥ 0.99 的电芯数
    n_cells_rank_ge_95:   int             # 排名 ≥ 0.95 的电芯数
    n_cells_rank_ge_90:   int             # 排名 ≥ 0.90 的电芯数
    max_rank_mean:        float           # 全局最大排名值
    max_rank_mean_sn:     int             # 最大排名值对应电芯编号

    def summary(self) -> str:
        """打印友好的分析摘要"""
        status = "🚨 告警触发" if self.alarm_triggered else "✅ 正常（未触发）"
        lines = [
            f"{'='*60}",
            f"案例 ID  : {self.case_id}",
            f"判定结果 : {status}",
            f"{'─'*60}",
        ]

        if self.alarm_triggered:
            lines += [
                f"问题电芯 : #{self.err_sn}  "
                f"（第{self.triggered_group}组，支路{self.triggered_branch}）",
                f"  当前排名   : {self.err_rank_mean:.4f}  "
                f"（阈值 ≥ {AlphaQConfig().cell_rank_mean_limit}）",
                f"  回溯排名   : {self.err_rank_mean_lag:.4f}  "
                f"（阈值 < {AlphaQConfig().cell_rank_mean_lag_limit}）",
                f"  排名变化   : {self.err_rank_change:+.4f}",
                f"支路检查 :",
                f"  同支路改善 : {self.dec_num}  / 阈值 {AlphaQConfig().n_drop}  ✅",
                f"  对侧支路恶化: {self.rise_num} / 阈值 {AlphaQConfig().n_raise} ✅",
            ]
        else:
            lines.append("─── 未触发原因分析 ───")
            # 全局排名分布
            lines += [
                f"全局排名分布 :",
                f"  排名 ≥ 0.99 的电芯数 : {self.n_cells_rank_ge_99}",
                f"  排名 ≥ 0.95 的电芯数 : {self.n_cells_rank_ge_95}",
                f"  排名 ≥ 0.90 的电芯数 : {self.n_cells_rank_ge_90}",
                f"  全局最大排名值       : {self.max_rank_mean:.4f}  "
                f"（电芯#{self.max_rank_mean_sn}）",
            ]
            if self.candidate_cells:
                lines.append("候选电芯（满足排名突降但支路检查未通过）:")
                for c in self.candidate_cells[:5]:  # 最多显示5个
                    lines.append(
                        f"  #{c.cell_sn:3d}  "
                        f"rank={c.rank_mean:.4f}(↑{c.rank_change:+.4f})  "
                        f"dec={c.dec_num}/{AlphaQConfig().n_drop}"
                        f"({'✅' if c.dec_shortfall==0 else f'差{c.dec_shortfall}'})  "
                        f"rise={c.rise_num}/{AlphaQConfig().n_raise}"
                        f"({'✅' if c.rise_shortfall==0 else f'差{c.rise_shortfall}'})"
                    )
            else:
                lines.append("  无电芯同时满足排名突降条件（当前/回溯阈值均未达标）")

        if self.margins:
            lines.append(f"{'─'*60}")
            lines.append("裕度（正=超限，负=距阈值距离）：")
            for k, v in self.margins.items():
                lines.append(f"  {k:<45s}: {v:+.4f}")

        lines.append(f"{'='*60}")
        return "\n".join(lines)

    def to_flat_dict(self) -> Dict:
        """展平为 DataFrame 友好格式"""
        d = {
            "case_id":            self.case_id,
            "alarm_triggered":    self.alarm_triggered,
            "err_sn":             self.err_sn,
            "triggered_group":    self.triggered_group,
            "triggered_branch":   self.triggered_branch,
            "dec_num":            self.dec_num,
            "rise_num":           self.rise_num,
            "err_rank_mean":      self.err_rank_mean,
            "err_rank_mean_lag":  self.err_rank_mean_lag,
            "err_rank_change":    self.err_rank_change,
            "n_cells_rank_ge_99": self.n_cells_rank_ge_99,
            "n_cells_rank_ge_95": self.n_cells_rank_ge_95,
            "n_cells_rank_ge_90": self.n_cells_rank_ge_90,
            "max_rank_mean":      self.max_rank_mean,
            "max_rank_mean_sn":   self.max_rank_mean_sn,
            "n_candidates":       len(self.candidate_cells),
        }
        d.update({f"margin_{k}": v for k, v in self.margins.items()})
        return d


# ════════════════════════════════════════════════════════════
# 3. 核心算法
# ════════════════════════════════════════════════════════════

class AlphaQDetector:
    """
    AlphaQ 离线压差离群检测器
    复现 电池压差离群预警_统计模型_AlphaQ V1.0 全流程逻辑

    电池包结构说明（70kWh）：
      · 共 192 个电芯，按 8 组（block）× 24 芯组织
      · 每组 24 芯分为 2 支路（支路1：芯1-12，支路2：芯13-24）
      · 两支路并联后 8 组串联

    排名归一化说明：
      · volt_rank_mean ∈ [0, 1]，1=排名最差（电压最低），0=排名最好
      · 问题电芯的自放电导致其电压相对下降 → 排名趋近1
    """

    def __init__(self, config: Optional[AlphaQConfig] = None):
        self.cfg = config or AlphaQConfig()

    def _get_group_and_branch(self, cell_sn: int):
        """
        根据电芯编号（1-192）返回 (组号, 支路号, 组内位置)
          组号 c_num：0-7
          支路号：1（组内位置1-12）或 2（组内位置13-24）
          组内位置 pos：1-24
        """
        cfg = self.cfg
        pos_in_group = cell_sn % cfg.n_cells_per_group  # 0 or 1..23
        if pos_in_group == 0:
            c_num = cell_sn // cfg.n_cells_per_group - 1
            pos   = cfg.n_cells_per_group
        else:
            c_num = cell_sn // cfg.n_cells_per_group
            pos   = pos_in_group
        branch = 1 if pos <= cfg.n_cells_per_branch else 2
        return c_num, branch, pos

    def _get_group_changes(
        self,
        ranks:     List[float],
        ranks_lag: List[float],
        c_num:     int,
    ):
        """
        计算指定组内 24 个电芯的排名变化（current - lag）
        返回 (c1_changes, c2_changes) 各 12 个值
          正值 = 排名恶化（值增大，朝1方向）
          负值 = 排名改善（值减小，朝0方向）
        """
        cfg = self.cfg
        base = c_num * cfg.n_cells_per_group  # 0-indexed 组内首个电芯在数组中的下标
        # 支路1：cells c_num*24+1 ~ c_num*24+12，数组下标 base+0 ~ base+11
        c1 = [ranks[base + j] - ranks_lag[base + j]
              for j in range(cfg.n_cells_per_branch)]
        # 支路2：cells c_num*24+13 ~ c_num*24+24，数组下标 base+12 ~ base+23
        c2 = [ranks[base + cfg.n_cells_per_branch + j] - ranks_lag[base + cfg.n_cells_per_branch + j]
              for j in range(cfg.n_cells_per_branch)]
        return c1, c2

    def _compute_branch_check(
        self,
        c1_changes: List[float],
        c2_changes: List[float],
        problem_branch: int,
    ):
        """
        支路检查：
          问题支路中排名改善（变化值<0）的电芯数 ≥ n_drop
          对侧支路中排名恶化（变化值>0）的电芯数 ≥ n_raise

        物理解释：
          问题电芯自放电 → 该支路在充电时获得更多电流补偿 → 支路内其他电芯排名改善
          对侧支路充电不足 → 排名相对恶化
        """
        cfg = self.cfg
        if problem_branch == 1:
            dec_num  = sum(1 for v in c1_changes if v < 0)  # 支路1改善
            rise_num = sum(1 for v in c2_changes if v > 0)  # 支路2恶化
        else:
            dec_num  = sum(1 for v in c2_changes if v < 0)  # 支路2改善
            rise_num = sum(1 for v in c1_changes if v > 0)  # 支路1恶化

        passed = (dec_num >= cfg.n_drop) and (rise_num >= cfg.n_raise)
        return passed, dec_num, rise_num

    def detect(self, case: CaseInput) -> AlarmResult:
        """
        运行完整 AlphaQ 检测流程，返回 AlarmResult

        流程：
          Step1 → 数据完整性校验
          Step2 → 逐电芯扫描，找到排名突降电芯
          Step3 → 支路结构检查（n_drop + n_raise）
        """
        cfg = self.cfg
        ranks     = case.volt_rank_mean
        ranks_lag = case.volt_rank_mean_lag

        # ─ 数据完整性校验 ──────────────────────────────────────
        if len(ranks) != cfg.n_cells_total or len(ranks_lag) != cfg.n_cells_total:
            raise ValueError(
                f"输入数据长度错误：volt_rank_mean={len(ranks)}, "
                f"volt_rank_mean_lag={len(ranks_lag)}，"
                f"期望均为 {cfg.n_cells_total}"
            )

        # ─ 全局排名统计（用于漏报归因）────────────────────────
        ranks_arr = np.array(ranks)
        n_ge_99 = int((ranks_arr >= 0.99).sum())
        n_ge_95 = int((ranks_arr >= 0.95).sum())
        n_ge_90 = int((ranks_arr >= 0.90).sum())
        max_rank     = float(ranks_arr.max())
        max_rank_sn  = int(ranks_arr.argmax()) + 1  # 转为1-indexed

        # ─ 逐电芯扫描 ──────────────────────────────────────────
        alarm_triggered   = False
        err_sn            = None
        triggered_group   = None
        triggered_branch  = None
        dec_num_result    = 0
        rise_num_result   = 0
        err_rank          = float("nan")
        err_rank_lag      = float("nan")
        err_rank_change   = float("nan")
        sort_info         = []
        sort_lag_info     = []
        candidate_cells:  List[CandidateCell] = []
        margins:          Dict[str, float] = {}

        for i in range(1, cfg.n_cells_total + 1):  # 1-indexed: 1..192
            rank_i     = ranks[i - 1]
            rank_i_lag = ranks_lag[i - 1]

            # Step2：排名突降条件
            rank_cond_current = rank_i     >= cfg.cell_rank_mean_limit      # 当前排名极差
            rank_cond_lag     = rank_i_lag <  cfg.cell_rank_mean_lag_limit  # 回溯排名曾正常

            if not (rank_cond_current and rank_cond_lag):
                continue  # 不满足排名突降条件，跳过

            # 确定所在组和支路
            c_num, branch, pos = self._get_group_and_branch(i)

            # 获取组内排名变化
            c1_changes, c2_changes = self._get_group_changes(ranks, ranks_lag, c_num)

            # Step3：支路结构检查
            passed, dec_num, rise_num = self._compute_branch_check(
                c1_changes, c2_changes, branch
            )

            if passed:
                # 触发告警，记录首个满足条件的电芯
                alarm_triggered  = True
                err_sn           = i
                triggered_group  = c_num
                triggered_branch = branch
                dec_num_result   = dec_num
                rise_num_result  = rise_num
                err_rank         = rank_i
                err_rank_lag     = rank_i_lag
                err_rank_change  = rank_i - rank_i_lag

                # 记录所在组的24芯排名快照
                base = c_num * cfg.n_cells_per_group
                sort_info     = [ranks[base + j]     for j in range(cfg.n_cells_per_group)]
                sort_lag_info = [ranks_lag[base + j] for j in range(cfg.n_cells_per_group)]
                break  # 与生产代码一致，仅报告首个触发电芯

            else:
                # 支路检查未通过 → 记录为候选电芯，供漏报分析
                candidate_cells.append(CandidateCell(
                    cell_sn       = i,
                    group_num     = c_num,
                    branch_num    = branch,
                    rank_mean     = rank_i,
                    rank_mean_lag = rank_i_lag,
                    rank_change   = rank_i - rank_i_lag,
                    dec_num       = dec_num,
                    rise_num      = rise_num,
                    dec_shortfall = max(0, cfg.n_drop  - dec_num),
                    rise_shortfall= max(0, cfg.n_raise - rise_num),
                ))

        # ─ 计算裕度（用于 Skill 统计分析）──────────────────────
        if alarm_triggered and err_sn is not None:
            margins[f"ERR_SN#{err_sn}_rank_mean(≥{cfg.cell_rank_mean_limit})"] = \
                err_rank - cfg.cell_rank_mean_limit
            margins[f"ERR_SN#{err_sn}_rank_mean_lag(<{cfg.cell_rank_mean_lag_limit})"] = \
                cfg.cell_rank_mean_lag_limit - err_rank_lag
            margins[f"branch_dec_num(≥{cfg.n_drop})"] = \
                float(dec_num_result - cfg.n_drop)
            margins[f"branch_rise_num(≥{cfg.n_raise})"] = \
                float(rise_num_result - cfg.n_raise)
        else:
            # 漏报场景：记录最高排名电芯的裕度，便于分析差距
            margins[f"max_rank_cell#{max_rank_sn}(≥{cfg.cell_rank_mean_limit})"] = \
                max_rank - cfg.cell_rank_mean_limit
            if candidate_cells:
                best = candidate_cells[0]
                margins[f"best_candidate_sn#{best.cell_sn}_dec_shortfall"] = \
                    float(-best.dec_shortfall)
                margins[f"best_candidate_sn#{best.cell_sn}_rise_shortfall"] = \
                    float(-best.rise_shortfall)

        return AlarmResult(
            case_id           = case.case_id,
            alarm_triggered   = alarm_triggered,
            err_sn            = err_sn,
            triggered_group   = triggered_group,
            triggered_branch  = triggered_branch,
            dec_num           = dec_num_result,
            rise_num          = rise_num_result,
            err_rank_mean     = err_rank,
            err_rank_mean_lag = err_rank_lag,
            err_rank_change   = err_rank_change,
            sort_info         = sort_info,
            sort_lag_info     = sort_lag_info,
            margins           = margins,
            candidate_cells   = candidate_cells,
            n_cells_rank_ge_99 = n_ge_99,
            n_cells_rank_ge_95 = n_ge_95,
            n_cells_rank_ge_90 = n_ge_90,
            max_rank_mean      = max_rank,
            max_rank_mean_sn   = max_rank_sn,
        )


# ════════════════════════════════════════════════════════════
# 4. 文件解析工具
# ════════════════════════════════════════════════════════════

def _parse_rank_list(value: Any, n: int = 192) -> List[float]:
    """
    从单元格解析 192 维排名列表
    支持：JSON 字符串 "[0.1, 0.2, ...]"、Python repr、逗号分隔字符串
    """
    if isinstance(value, (list, np.ndarray)):
        return [float(x) for x in value]
    s = str(value).strip()
    try:
        parsed = json.loads(s)
        return [float(x) for x in parsed]
    except (json.JSONDecodeError, ValueError):
        pass
    s = s.strip("[]()").replace(";", ",")
    try:
        return [float(x.strip()) for x in s.split(",") if x.strip()]
    except ValueError:
        return []


def _load_dataframe(filepath: str) -> pd.DataFrame:
    """根据扩展名加载 CSV 或 Excel"""
    ext = Path(filepath).suffix.lower()
    if ext in (".xls", ".xlsx"):
        return pd.read_excel(filepath, dtype=str)
    return pd.read_csv(filepath, dtype=str)


def _df_to_cases(df: pd.DataFrame) -> List[CaseInput]:
    """将 DataFrame 逐行转换为 CaseInput 列表"""
    cols_lower = {c.lower(): c for c in df.columns}

    # ─ 检测数据格式（展开列 or JSON列）─────────────────────────
    has_num_cols = any(c.lower().startswith("num_") and
                       not c.lower().endswith("_lag")
                       for c in df.columns)
    has_json_col = "volt_rank_mean" in cols_lower

    cases = []
    for idx, row in df.iterrows():
        # 当前事件排名
        if has_num_cols:
            cur_ranks = []
            for i in range(1, 193):
                col = cols_lower.get(f"num_{i}", cols_lower.get(f"num_{i}_mean"))
                if col and pd.notna(row[col]):
                    cur_ranks.append(float(row[col]))
        elif has_json_col:
            cur_ranks = _parse_rank_list(row[cols_lower["volt_rank_mean"]])
        else:
            raise ValueError("找不到排名列：需要 num_1..192 或 volt_rank_mean 列")

        # 回溯事件排名
        has_lag_num_cols = any(c.lower().startswith("num_") and
                               c.lower().endswith("_lag")
                               for c in df.columns)
        if has_lag_num_cols:
            lag_ranks = []
            for i in range(1, 193):
                col = cols_lower.get(f"num_{i}_lag", cols_lower.get(f"num_{i}_mean_lag"))
                if col and pd.notna(row[col]):
                    lag_ranks.append(float(row[col]))
        elif "volt_rank_mean_lag" in cols_lower:
            lag_ranks = _parse_rank_list(row[cols_lower["volt_rank_mean_lag"]])
        else:
            raise ValueError("找不到回溯排名列：需要 num_1_lag..192_lag 或 volt_rank_mean_lag 列")

        # case_id
        cid = str(row[cols_lower["case_id"]]) if "case_id" in cols_lower else f"case_{idx}"

        # label
        label = -1
        for flag_col in ("label", "is_alarm", "is_miss"):
            if flag_col in cols_lower:
                val = str(row[cols_lower[flag_col]]).strip().lower()
                label = 1 if val in ("1", "true", "yes", "漏报", "告警") else 0
                break

        # 元数据
        meta_fields = ["battery_id", "battery_type", "device_id",
                       "process_id", "process_id_lag", "volt_diff_mean",
                       "volt_diff_mean_lag", "start_time", "start_time_lag",
                       "charge_mode_tag"]
        kwargs = {}
        for f in meta_fields:
            if f in cols_lower:
                val = row[cols_lower[f]]
                if pd.notna(val):
                    kwargs[f] = val

        cases.append(CaseInput(
            volt_rank_mean     = cur_ranks,
            volt_rank_mean_lag = lag_ranks,
            case_id            = cid,
            label              = label,
            **kwargs,
        ))
    return cases


# ════════════════════════════════════════════════════════════
# 5. 公开接口
# ════════════════════════════════════════════════════════════

def run_single(
    data:    Union[Dict, CaseInput],
    config:  Optional[AlphaQConfig] = None,
    verbose: bool = True,
) -> AlarmResult:
    """
    单案例运行

    Parameters
    ----------
    data    : dict 或 CaseInput
    config  : 自定义参数（不传则使用 AlphaQ V1.0 默认值）
    verbose : 是否打印摘要
    """
    if isinstance(data, dict):
        data = CaseInput(
            volt_rank_mean     = data["volt_rank_mean"],
            volt_rank_mean_lag = data["volt_rank_mean_lag"],
            case_id            = data.get("case_id", "case_0"),
            battery_id         = data.get("battery_id", ""),
            battery_type       = data.get("battery_type", ""),
            device_id          = data.get("device_id", ""),
            process_id         = data.get("process_id", ""),
            process_id_lag     = data.get("process_id_lag", ""),
            charge_mode_tag    = int(data.get("charge_mode_tag", 0)),
            volt_diff_mean     = float(data.get("volt_diff_mean", float("nan"))),
            volt_diff_mean_lag = float(data.get("volt_diff_mean_lag", float("nan"))),
            start_time         = data.get("start_time", ""),
            start_time_lag     = data.get("start_time_lag", ""),
            label              = int(data.get("label", -1)),
        )
    result = AlphaQDetector(config).detect(data)
    if verbose:
        print(result.summary())
    return result


def run_batch(
    source:  Union[str, pd.DataFrame, List[CaseInput]],
    config:  Optional[AlphaQConfig] = None,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    批量运行

    Parameters
    ----------
    source  : 文件路径（CSV/Excel）、已加载的 DataFrame 或 CaseInput 列表
    config  : 自定义参数
    verbose : 是否打印每个案例的摘要

    Returns
    -------
    pd.DataFrame  包含所有案例的扁平化结果
    """
    if isinstance(source, str):
        df = _load_dataframe(source)
        cases = _df_to_cases(df)
    elif isinstance(source, pd.DataFrame):
        cases = _df_to_cases(source)
    else:
        cases = source

    detector = AlphaQDetector(config)
    rows = []
    for case in cases:
        result = detector.detect(case)
        row = result.to_flat_dict()
        row["label"] = case.label
        for f in ("battery_id", "battery_type", "device_id",
                  "process_id", "process_id_lag",
                  "volt_diff_mean", "volt_diff_mean_lag",
                  "start_time", "start_time_lag", "charge_mode_tag"):
            val = getattr(case, f, None)
            if val is not None and val != "":
                row[f] = val
        if verbose:
            print(result.summary())
        rows.append(row)

    return pd.DataFrame(rows)