from __future__ import annotations


class NarrativeGenerator:
    def district_statement(self, district_id: str, candidate: str, margin: float, uncertainty: float) -> str:
        return f"{candidate} is trending to win {district_id} by {abs(margin):.1f} points with an uncertainty of +/-{uncertainty:.1f}."

    def district_narrative(self, forecast: dict) -> str:
        attrs = forecast.get("factor_attribution", {})
        top = sorted(attrs.items(), key=lambda item: abs(float(item[1])), reverse=True)[:2]
        parts = []
        for name, value in top:
            direction = "helps Democrats" if float(value) > 0 else "helps Republicans"
            parts.append(f"{name.replace('_', ' ').title()} {direction} by about {abs(float(value)):.1f} points.")
        if forecast.get("kalshi_gap_flag"):
            parts.append(f"Kalshi and TAPESTRY disagree by {abs(forecast.get('kalshi_gap') or 0):.1%}.")
        return " ".join(parts) or "The race is being driven by structural district fundamentals."

    def morning_brief(self, senate_shift: float = 0.0, largest_move: str = "AZ-06") -> str:
        party = "Democrats" if senate_shift >= 0 else "Republicans"
        return f"Since yesterday: Senate control shifted {abs(senate_shift):.1f} points toward {party}, driven primarily by national environment. The largest single-district move was {largest_move}."
