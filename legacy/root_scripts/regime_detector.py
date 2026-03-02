from market_regime import MarketRegime


class RegimeDetector:
    def detect(self, snapshot: dict) -> MarketRegime:

        pricing = snapshot.get("pricing", {})
        capacity = snapshot.get("capacity", {})
        demand = snapshot.get("demand", {})

        price_trend = pricing.get("trend")
        price_delta = pricing.get("net_price_delta", 0)

        seat_trend = capacity.get("trend")
        tightening_score = capacity.get("tightening_score", 0)

        confidence = demand.get("confidence", 0)

        # 🔴 Demand Surge
        if (
            price_trend == "UP"
            and tightening_score > 0
            and confidence > 0.7
        ):
            return MarketRegime.DEMAND_SURGE

        # 🔵 Demand Softening
        if (
            price_trend == "DOWN"
            and tightening_score <= 0
            and confidence < 0.4
        ):
            return MarketRegime.DEMAND_SOFT

        # 🟡 Yield Protection
        if (
            price_trend == "FLAT"
            and tightening_score > 0
        ):
            return MarketRegime.YIELD_PROTECTION

        # 🟠 Market Testing
        if abs(price_delta) > 0 and tightening_score == 0:
            return MarketRegime.MARKET_TESTING

        # 🟢 Stable
        if price_trend == "FLAT" and tightening_score == 0:
            return MarketRegime.STABLE

        return MarketRegime.UNKNOWN
