from __future__ import annotations

import math
from typing import Dict

# Master switch for the entire Elo system.
# Set to True to enable Elo calculations and Elo API payloads.
ELO_SYSTEM_ENABLED = False

# Faceit-tyylinen lähtötaso kaikille uusille pelaajille.
DEFAULT_INITIAL_ELO = 1000.0

# Backward compatibility: palvelukoodi käyttää vielä nimeä INITIAL_ELO.
INITIAL_ELO = DEFAULT_INITIAL_ELO

# Kun kausi+divisioona on täysin tyhjä (ei yhtään olemassa olevaa Eloa),
# uuden pelaajan base Elo bootstrappaa divisioonan suhteellisen sijainnin mukaan.
# Kauden ylin divisioona saa aina top_initial_elo-arvon ja alin
# bottom_initial_elo-arvon. Väliin jäävät lineaarisesti.
INITIAL_ELO_BOOTSTRAP: Dict[str, float] = {
    "top_initial_elo": 3000.0,
    "bottom_initial_elo": 500.0,
    "min_initial_elo": 500.0,
    "max_initial_elo": 3000.0,
}

# Kuinka aggressiivisesti ottelu liikuttaa Eloa peruspainolla.
BASE_K_FACTOR = 30.0

# Rajat yhden ottelun Elo-muutokselle ennen tallennusta.
MAX_ELO_DELTA = 65.0
MIN_ELO_DELTA = -55.0

# Ottelun lopputuloksen vaikutus kokonaisimpactiin.
OUTCOME_WEIGHTS: Dict[str, float] = {
    "win_bonus": 0.22,
    "loss_penalty": -0.15,
    "draw_bonus": 0.0,
}

# Pelaajasuorituksen painot. Nämä pidetään yhdessä paikassa, jotta
# säätäminen ei vaadi Elo-palvelun logiikan muokkaamista.
STAT_WEIGHTS: Dict[str, float] = {
    "kd": 0.28,
    "kr": 0.22,
    "adr": 0.20,
    "mvps_per_map": 0.08,
    "entry_success_rate": 0.07,
    "clutch_success_rate": 0.07,
    "utility_per_round": 0.04,
    "flash_success_rate": 0.04,
}

# Normalisointitasot muuttavat raakaluvut 0..1-alueelle ennen painotusta.
STAT_BASELINES: Dict[str, float] = {
    "kd": 1.0,
    "kr": 0.70,
    "adr": 80.0,
    "mvps_per_map": 0.8,
    "entry_success_rate": 0.55,
    "clutch_success_rate": 0.30,
    "utility_per_round": 8.0,
    "flash_success_rate": 0.50,
}

# Divisioonakerroin lasketaan dynaamisesti joka kaudelle datasta.
# Kerroin perustuu kauden sisäisiin suorituseroihin (stat_score), jolloin
# sama division_num voi eri kausissa saada eri painon.
DYNAMIC_DIVISION_ELO: Dict[str, float] = {
    "fallback_multiplier": 1.0,
    "sensitivity": 0.14,
    "rank_reference_division": 10.0,
    "rank_step": 0.03,
    "rank_blend": 0.55,
    "min_multiplier": 0.72,
    "max_multiplier": 1.45,
    "min_samples_per_division": 20.0,
    "shrink_to_mean_samples": 50.0,
}

# Elo liikkuu alussa paljon ja tasoittuu kokemuksen myötä.
DYNAMIC_K_FACTOR: Dict[str, float] = {
    "start_multiplier": 2.6,
    "stabilize_after_maps": 10.0,
    "post_stabilize_multiplier": 1.0,
    "min_multiplier": 0.58,
    "decay_rate": 0.06,
}


def clamp_elo_delta(value: float) -> float:
    return max(MIN_ELO_DELTA, min(MAX_ELO_DELTA, value))


def clamp_initial_elo(value: float) -> float:
    return max(
        float(INITIAL_ELO_BOOTSTRAP["min_initial_elo"]),
        min(float(INITIAL_ELO_BOOTSTRAP["max_initial_elo"]), value),
    )


def clamp_division_multiplier(value: float) -> float:
    return max(
        DYNAMIC_DIVISION_ELO["min_multiplier"],
        min(DYNAMIC_DIVISION_ELO["max_multiplier"], value),
    )


def k_factor_for_maps(maps_played: int) -> float:
    played = max(0, int(maps_played))
    start_mult = float(DYNAMIC_K_FACTOR["start_multiplier"])
    stabilize_after_maps = max(1.0, float(DYNAMIC_K_FACTOR.get("stabilize_after_maps", 10.0)))
    post_stabilize_mult = float(DYNAMIC_K_FACTOR.get("post_stabilize_multiplier", 1.0))
    min_mult = float(DYNAMIC_K_FACTOR["min_multiplier"])
    decay_rate = float(DYNAMIC_K_FACTOR["decay_rate"])

    if played < stabilize_after_maps:
        progress = played / stabilize_after_maps
        dynamic_mult = start_mult + ((post_stabilize_mult - start_mult) * progress)
    else:
        maps_after_stabilize = played - stabilize_after_maps
        dynamic_mult = min_mult + ((post_stabilize_mult - min_mult) * math.exp(-decay_rate * maps_after_stabilize))

    return BASE_K_FACTOR * dynamic_mult


def k_factor_for_matches(matches_played: int) -> float:
    # Backward compatibility alias for older call sites.
    return k_factor_for_maps(matches_played)


def bootstrap_initial_elo_for_division(
    division_num: int,
    *,
    season_min_division: int = 0,
    season_max_division: int | None = None,
) -> float:
    division = max(0, int(division_num))
    top_initial_elo = float(INITIAL_ELO_BOOTSTRAP["top_initial_elo"])
    bottom_initial_elo = float(INITIAL_ELO_BOOTSTRAP["bottom_initial_elo"])

    min_division = int(season_min_division)
    max_division = int(season_max_division) if season_max_division is not None else division
    if max_division < min_division:
        min_division, max_division = max_division, min_division

    # Yksidivarisessa kaudessa ainoa divisioona käsitellään ylimpänä.
    if max_division <= min_division:
        return clamp_initial_elo(top_initial_elo)

    normalized = (float(division) - float(min_division)) / (float(max_division) - float(min_division))
    normalized = max(0.0, min(1.0, normalized))
    raw = top_initial_elo + (normalized * (bottom_initial_elo - top_initial_elo))
    return clamp_initial_elo(raw)
