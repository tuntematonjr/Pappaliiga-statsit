-- schema.sql
-- Pappaliiga (CS2) stats — championship-centric schema
-- Preserves all fields from the older schema (maps, map_votes, per-map team/player stats)
-- and adds season/division_num/slug/game/is_playoffs for stable HTML + joins.

PRAGMA foreign_keys = ON;

------------------------------------------------------------
-- Core reference tables
------------------------------------------------------------

-- One row per Faceit championship (the real unit we fetch by).
CREATE TABLE IF NOT EXISTS championships (
  championship_id TEXT PRIMARY KEY,                 -- Faceit UUID (source of truth)
  season          INTEGER NOT NULL,                 -- e.g., 11
  division_num    INTEGER NOT NULL,                 -- number parsed from name (1..25)
  name            TEXT NOT NULL,                    -- "1 Divisioona S11", etc.
  game            TEXT NOT NULL DEFAULT 'cs2',      -- 'cs2'
  is_playoffs     INTEGER NOT NULL DEFAULT 0,       -- 0/1
  slug            TEXT NOT NULL,                    -- unique page/file slug, e.g. 'div1-s11' or 'div1-s11-po'
  CONSTRAINT uq_champ_slug UNIQUE (slug),
  CONSTRAINT uq_champ_season_div UNIQUE (season, division_num, is_playoffs)
);

CREATE INDEX IF NOT EXISTS ix_champ_season ON championships(season);
CREATE INDEX IF NOT EXISTS ix_champ_game   ON championships(game);

-- Reference entities
CREATE TABLE IF NOT EXISTS teams (
  team_id    TEXT PRIMARY KEY,
  name       TEXT,
  avatar     TEXT,
  updated_at INTEGER
);

CREATE TABLE IF NOT EXISTS players (
  player_id   TEXT PRIMARY KEY,
  nickname    TEXT NOT NULL,
  updated_at  INTEGER DEFAULT (strftime('%s','now'))
);

------------------------------------------------------------
-- Matches & maps
------------------------------------------------------------

-- Single row per match within a championship.
CREATE TABLE IF NOT EXISTS matches (
  match_id        TEXT PRIMARY KEY,
  championship_id TEXT NOT NULL REFERENCES championships(championship_id) ON DELETE CASCADE,
  competition_name TEXT,
  best_of          INTEGER,
  game             TEXT,

  -- Aiemmat (pidä):
  configured_at    INTEGER,
  started_at       INTEGER,
  finished_at      INTEGER,

  -- Uudet header-kentät upcoming/live-seurantaan:
  scheduled_at     INTEGER,            -- Faceitin ilmoittama suunniteltu aloitus (epoch)
  status           TEXT,               -- 'upcoming' | 'live' | 'played' | 'canceled' tms.
  last_seen_at     INTEGER,            -- päivittyy jokaisessa synkassa

  team1_id         TEXT REFERENCES teams(team_id),
  team1_name       TEXT,
  team2_id         TEXT REFERENCES teams(team_id),
  team2_name       TEXT,
  winner_team_id   TEXT
);

CREATE INDEX IF NOT EXISTS ix_matches_scheduled    ON matches(scheduled_at);
CREATE INDEX IF NOT EXISTS ix_matches_status       ON matches(status);

CREATE INDEX IF NOT EXISTS ix_matches_started ON matches(started_at);

-- One row per played map within a match (for BO2/BO3).
CREATE TABLE IF NOT EXISTS maps (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  match_id      TEXT NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
  round_index   INTEGER NOT NULL,                  -- 1,2,3... = map order in match
  map_name      TEXT,
  score_team1   INTEGER,
  score_team2   INTEGER,
  winner_team_id TEXT,
  UNIQUE(match_id, round_index)
);

-- Map veto votes (Faceit Democracy API), per match
CREATE TABLE IF NOT EXISTS map_votes (
  id                  INTEGER PRIMARY KEY AUTOINCREMENT,
  match_id            TEXT NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
  map_name            TEXT,
  status              TEXT,        -- 'drop' or 'pick'
  selected_by_faction TEXT,        -- 'faction1' or 'faction2'
  round_num           INTEGER,
  -- resolved team (nullable if mapping unknown)
  selected_by_team_id TEXT
);

-- Player stats per map (per match_id + round_index).
CREATE TABLE IF NOT EXISTS player_stats (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  match_id         TEXT NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
  round_index      INTEGER NOT NULL,               -- map index in the BO series
  player_id        TEXT REFERENCES players(player_id) ON DELETE SET NULL,
  nickname         TEXT,
  team_id          TEXT REFERENCES teams(team_id) ON DELETE SET NULL,
  team_name        TEXT,
  kills            INTEGER,
  deaths           INTEGER,
  assists          INTEGER,
  kd               REAL,
  kr               REAL,
  adr              REAL,
  hs_pct           REAL,
  mvps             INTEGER,
  sniper_kills     INTEGER,
  utility_damage   INTEGER,
  enemies_flashed  INTEGER,
  flash_count      INTEGER,
  flash_successes  INTEGER,
  mk_2k            INTEGER, 
  mk_3k            INTEGER,
  mk_4k            INTEGER,
  mk_5k            INTEGER,
  clutch_kills     INTEGER DEFAULT 0,
  cl_1v1_attempts  INTEGER DEFAULT 0,
  cl_1v1_wins      INTEGER DEFAULT 0,
  cl_1v2_attempts  INTEGER DEFAULT 0,
  cl_1v2_wins      INTEGER DEFAULT 0,
  entry_count      INTEGER,
  entry_wins       INTEGER,
  pistol_kills     INTEGER,
  damage           INTEGER,

  UNIQUE(match_id, round_index, player_id, nickname)
);

CREATE INDEX IF NOT EXISTS ix_playerstats_team ON player_stats(team_id);
CREATE INDEX IF NOT EXISTS idx_matches_champ ON matches(championship_id);
CREATE INDEX IF NOT EXISTS idx_ps_match_round ON player_stats(match_id, round_index, player_id);
CREATE INDEX IF NOT EXISTS idx_maps_match_round ON maps(match_id, round_index);
CREATE INDEX IF NOT EXISTS idx_votes_match ON map_votes(match_id);
CREATE INDEX IF NOT EXISTS ix_playerstats_match_team ON player_stats(match_id, team_id);
CREATE INDEX IF NOT EXISTS idx_matches_champ_team1 ON matches(championship_id, team1_id);
CREATE INDEX IF NOT EXISTS idx_matches_champ_team2 ON matches(championship_id, team2_id);
CREATE INDEX IF NOT EXISTS ix_matches_finished  ON matches(finished_at);
CREATE INDEX IF NOT EXISTS ix_matches_last_seen ON matches(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_maps_name ON maps(map_name);

------------------------------------------------------------
-- Maps catalog (from FACEIT voting.map.entities)
------------------------------------------------------------
CREATE TABLE IF NOT EXISTS maps_catalog (
  map_id       TEXT PRIMARY KEY,              -- canonical id, e.g. 'de_ancient'
  pretty_name  TEXT,                          -- e.g. 'Ancient'
  image_sm     TEXT,                          -- small image URL from FACEIT
  image_lg     TEXT,                          -- large image URL from FACEIT
  game         TEXT NOT NULL DEFAULT 'cs2',
  first_seen_at INTEGER DEFAULT (strftime('%s','now')),
  last_seen_at  INTEGER DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS ix_maps_catalog_pretty ON maps_catalog(pretty_name);

-- Which seasons include a map in the pool (simple presence flag)
CREATE TABLE IF NOT EXISTS map_pool_seasons (
  season  INTEGER NOT NULL,
  map_id  TEXT NOT NULL REFERENCES maps_catalog(map_id) ON DELETE CASCADE,
  PRIMARY KEY (season, map_id)
);
