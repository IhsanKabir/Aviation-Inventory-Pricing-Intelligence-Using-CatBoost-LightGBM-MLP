from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from models.strategy_signal import StrategySignal


class SignalAggregator:
    def __init__(self, session: Session):
        self.session = session

    def aggregate(
        self,
        airline: str,
        flight_key: str,
        window_hours: int = 24
    ) -> dict:

        since = datetime.utcnow() - timedelta(hours=window_hours)

        signals = (
            self.session.query(StrategySignal)
            .filter(
                StrategySignal.airline == airline,
                StrategySignal.flight_key == flight_key,
                StrategySignal.detected_at >= since
            )
            .all()
        )

        if not signals:
            return self._empty_snapshot()

        return {
            "airline": airline,
            "flight_key": flight_key,
            "window_hours": window_hours,
            "pricing": self._pricing_metrics(signals),
            "capacity": self._capacity_metrics(signals),
            "demand": self._demand_metrics(signals),
        }

    def _pricing_metrics(self, signals):
        price_moves = [
            s for s in signals if s.signal_category == "PRICE_ACTION"
        ]

        net_delta = sum(s.severity for s in price_moves)

        return {
            "event_count": len(price_moves),
            "net_price_delta": net_delta,
            "trend": "UP" if net_delta > 0 else "DOWN" if net_delta < 0 else "FLAT",
        }

    def _capacity_metrics(self, signals):
        cap_moves = [
            s for s in signals if s.signal_category == "CAPACITY_ACTION"
        ]

        tightening = sum(
            s.severity for s in cap_moves
            if s.signal_type == "INVENTORY_TIGHTENING"
        )

        return {
            "event_count": len(cap_moves),
            "tightening_score": tightening,
            "trend": "TIGHTENING" if tightening > 0 else "STABLE",
        }

    def _demand_metrics(self, signals):
        confidence = sum(s.confidence for s in signals) / max(len(signals), 1)

        return {
            "confidence": round(confidence, 2),
            "signal": "SURGE" if confidence > 0.7 else "NEUTRAL",
        }

    def _empty_snapshot(self):
        return {
            "pricing": {},
            "capacity": {},
            "demand": {},
        }
