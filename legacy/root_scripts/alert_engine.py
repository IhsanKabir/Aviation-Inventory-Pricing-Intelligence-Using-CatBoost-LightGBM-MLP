from alert_severity import AlertSeverity
from alert_type import AlertType
from market_regime import MarketRegime


class AlertEngine:

    def evaluate(self, snapshot: dict, regime: MarketRegime) -> list[dict]:
        alerts = []

        pricing = snapshot.get("pricing", {})
        capacity = snapshot.get("capacity", {})
        demand = snapshot.get("demand", {})

        price_delta = pricing.get("net_price_delta", 0)
        tightening_score = capacity.get("tightening_score", 0)
        confidence = demand.get("confidence", 0)

        # 🔴 DEMAND SURGE
        if regime == MarketRegime.DEMAND_SURGE:
            alerts.append(self._alert(
                AlertType.DEMAND_SURGE,
                AlertSeverity.CRITICAL,
                "Strong demand surge detected with price increase and seat tightening."
            ))

        # 🟡 YIELD PROTECTION
        if regime == MarketRegime.YIELD_PROTECTION:
            alerts.append(self._alert(
                AlertType.YIELD_PROTECTION,
                AlertSeverity.HIGH,
                "Yield protection behavior detected (inventory tightening without price change)."
            ))

        # 🔵 PRICE DROP
        if price_delta < 0 and confidence < 0.4:
            alerts.append(self._alert(
                AlertType.PRICE_DROP,
                AlertSeverity.MEDIUM,
                "Price drop detected under weak demand conditions."
            ))

        # 🟠 MARKET TESTING
        if regime == MarketRegime.MARKET_TESTING:
            alerts.append(self._alert(
                AlertType.MARKET_TESTING,
                AlertSeverity.LOW,
                "Airline appears to be testing price elasticity."
            ))

        return alerts

    def _alert(self, alert_type, severity, message):
        return {
            "type": alert_type.value,
            "severity": severity.value,
            "message": message
        }
