from lib.services.static_scores.constants import COBB_DOUGLAS_WEIGHTS


class CobbDouglasAggregator:
    def combine(self, docs: float, repr_: float, social: float, legal: float) -> float:
        w = COBB_DOUGLAS_WEIGHTS
        score = (
            docs ** w["docs"]
            * repr_ ** w["repr"]
            * social ** w["social"]
            * legal ** w["legal"]
        )
        return round(score, 4)
