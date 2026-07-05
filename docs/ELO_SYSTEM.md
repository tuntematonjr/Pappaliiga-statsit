# Elo System

## Goals

The first Elo release tracks only individual player Elo.

- Elo is continuous across seasons and shown as current overall Elo.
- Separate season/championship Elo views are intentionally disabled.
- Division strength is included immediately and calculated dynamically per season.
- A new player does not automatically start from a flat `1000` anymore:
	- if the same season + division already has Elo-rated players, the new player starts from that pool's average Elo
	- if the season + division is still empty, the new player starts from a rank-based bootstrap anchored around `1000`
- Elo is continuous across seasons and shown as current overall Elo.
- Separate season/championship Elo views are intentionally disabled.
- Division strength is included immediately and calculated dynamically per season.
- All tuning values live in one editable config file: [elo_config.py](d:/Github/Pappaliiga-statsit/elo_config.py).

## Editable Inputs

The following values are intended to be edited by hand without touching Elo service code.

### Base Elo settings

- `DEFAULT_INITIAL_ELO`: the global anchor value used by bootstrap logic.
- `BASE_K_FACTOR`: base movement size per processed match.
- `MIN_ELO_DELTA` / `MAX_ELO_DELTA`: hard clamps for one-match Elo change.

### New-player bootstrap settings

When a player has no prior Elo history:

- if there are already players with Elo in the same season + division, the player starts from that average Elo
- otherwise the player starts from a rank-based bootstrap derived from division number

Relevant config:

- `INITIAL_ELO_BOOTSTRAP.rank_reference_division`: division used as the neutral reference point
- `INITIAL_ELO_BOOTSTRAP.rank_step_points`: Elo offset applied per division step
- `INITIAL_ELO_BOOTSTRAP.min_initial_elo`: lower clamp for bootstrap Elo
- `INITIAL_ELO_BOOTSTRAP.max_initial_elo`: upper clamp for bootstrap Elo

### Outcome weights

- `win_bonus`: added to the match impact when the player's team wins.
- `loss_penalty`: added when the player's team loses.
- `draw_bonus`: reserved for draw-like results if needed later.

### Stat weights

Each stat is first normalized against a baseline and then multiplied by its weight.

- `kd`: kill/death efficiency.
- `kr`: kills per round.
- `adr`: average damage per round.
- `mvps_per_map`: MVP contribution.
- `entry_success_rate`: first duel conversion quality.
- `clutch_success_rate`: clutch conversion quality.
- `utility_per_round`: utility damage efficiency.
- `flash_success_rate`: flash conversion quality.

### Dynamic division multiplier

Division strength is modeled dynamically for each season from observed player-performance gaps.

- Each season gets its own division multipliers.
- Multipliers are based on division-level relative `stat_score` versus season mean.
- Multipliers are not purely data-driven: they also blend in a rank-based prior from division number.
- `sensitivity` controls how strongly performance gaps shift multipliers.
- `rank_reference_division` defines the neutral point for rank prior.
- `rank_step` controls how much each division step changes the prior multiplier.
- `rank_blend` controls how much the final multiplier leans on rank prior versus observed data.
- `min_samples_per_division` and `shrink_to_mean_samples` prevent overfitting on tiny samples.
- `min_multiplier` and `max_multiplier` clamp extremes.
- `fallback_multiplier` is used when data is sparse or missing.

### Dynamic K by maps played

K is no longer driven by match count. It is driven by maps played.

- The first 10 maps move Elo much more aggressively so strong and weak players separate quickly.
- After that, Elo movement stabilizes and gradually decays toward a lower long-run floor.

Relevant config:

- `DYNAMIC_K_FACTOR.start_multiplier`: K multiplier at the very beginning
- `DYNAMIC_K_FACTOR.stabilize_after_maps`: number of early maps before the stabilized phase starts
- `DYNAMIC_K_FACTOR.post_stabilize_multiplier`: K multiplier at the start of the stabilized phase
- `DYNAMIC_K_FACTOR.min_multiplier`: long-run lower bound for K
- `DYNAMIC_K_FACTOR.decay_rate`: decay speed after stabilization starts

Implementation summary:

- Maps `0..stabilize_after_maps`: interpolate from `start_multiplier` toward `post_stabilize_multiplier`
- After that: exponential decay from `post_stabilize_multiplier` toward `min_multiplier`

## Tuning Rules

- Change one value at a time.
- Re-run a limited backfill after each meaningful tuning change.
- Keep clamp values conservative so one match never creates unrealistic jumps.
- Keep bootstrap values conservative so division information does not dominate both the starting Elo and the match delta too strongly.
- If lower divisions still rate too highly, increase `sensitivity` or lower `min_multiplier`.
- If top divisions separate too aggressively, reduce `sensitivity` or lower `max_multiplier`.
- If new players settle too slowly, raise `start_multiplier` or `rank_step_points` carefully.
- If new players overshoot too easily, reduce `start_multiplier`, tighten `MIN_ELO_DELTA` / `MAX_ELO_DELTA`, or lower `rank_step_points`.

## Validation Checklist

- A new player with no prior Elo starts from same-season same-division average Elo when that pool exists.
- A new player in an empty season + division starts from rank-based bootstrap, not a flat hardcoded `1000`.
- Division multipliers should differ across seasons when player level gaps differ.
- Division multipliers should reflect both observed stat gaps and the configured rank prior blend.
- The first 10 maps should move Elo clearly more than later maps.
- Config-only changes must affect the Elo output without service-code changes.
- Each config field should remain documented in this file when adjusted.