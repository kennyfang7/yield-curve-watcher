from typing import List, Dict
from ..types import Signal

class CompositeSignal:
    name = "composite_default"
    def __init__(self, thresholds: Dict[str, float] | None = None):
        self.thresh = thresholds or {"slope_change_bps_30d": 25, "jump_10Y_last5d_bps": 20, "hy_oas_stress_bps": 500, "baa10_stress_bps": 250}
    def evaluate(self, economy: str, f: Dict[str, float]) -> List[Signal]:
        sigs: List[Signal] = []
        if f.get("inversion_10Y_2Y", False):
            sigs.append(Signal(level="warning", code="inv_10y2y", message=f"[{economy}] 10Y–2Y inverted ({f.get('slope_10Y_2Y_bps'):.1f} bps)"))
        elif "slope_10Y_2Y_bps" in f:
            sigs.append(Signal(level="info", code="slope_10y2y", message=f"[{economy}] 10Y–2Y slope = {f['slope_10Y_2Y_bps']:.1f} bps"))
        if f.get("inversion_10Y_3M", False):
            sigs.append(Signal(level="warning", code="inv_10y3m", message=f"[{economy}] 10Y–3M inverted"))
        if f.get("jump_10Y_last5d_bps", 0) >= self.thresh["jump_10Y_last5d_bps"]:
            sigs.append(Signal(level="watch", code="jump_10y", message=f"[{economy}] Large 10Y move last 5d: {f['jump_10Y_last5d_bps']:.1f} bps"))
        if f.get("hy_oas_bps", 0) >= self.thresh["hy_oas_stress_bps"]:
            sigs.append(Signal(level="watch", code="hy_stress", message=f"[{economy}] HY OAS elevated: {f['hy_oas_bps']:.0f} bps"))
        if f.get("baa_minus_10y_bps", 0) >= self.thresh["baa10_stress_bps"]:
            sigs.append(Signal(level="watch", code="baa10_stress", message=f"[{economy}] Baa–10Y wide: {f['baa_minus_10y_bps']:.0f} bps"))
        if f.get("ma200_cross_10Y") == "bullish_cross":
            sigs.append(Signal(level="watch", code="ma_bull", message=f"[{economy}] 10Y crossed ABOVE 200d MA"))
        elif f.get("ma200_cross_10Y") == "bearish_cross":
            sigs.append(Signal(level="watch", code="ma_bear", message=f"[{economy}] 10Y crossed BELOW 200d MA"))
        return sigs
