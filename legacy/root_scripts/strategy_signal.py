from dataclasses import dataclass
from strategy_signal_type import StrategySignalType


@dataclass
class StrategySignal:
    signal_type: StrategySignalType
    confidence: float
    rationale: str
    supporting_alerts: list[str]
