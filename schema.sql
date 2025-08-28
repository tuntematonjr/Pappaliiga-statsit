-- SQLite schema for CS2 Faceit league reports.
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS divisions (
  division_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  slug TEXT NOT NULL,
  championship_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS matches (
  match_id TEXT PRIMARY KEY,
  division_id INTEGER NOT NULL,
  competition_id TEXT,
  competition_name TEXT,
  best_of INTEGER,
  game TEXT,
  faceit_url TEXT,
  configured_at INTEGER,
  started_at INTEGER,
  finished_at INTEGER,
  team1_id TEXT,
  team1_name TEXT,
  team2_id TEXT,
  team2_name TEXT,
  winner_team_id TEXT,
  FOREIGN KEY (division_id) REFERENCES divisions(division_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_matches_division ON matches(division_id);

CREATE TABLE IF NOT EXISTS maps (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  match_id TEXT NOT NULL,
  round_index INTEGER NOT NULL,
  map_name TEXT,
  score_team1 INTEGER,
  score_team2 INTEGER,
  winner_team_id TEXT,
  UNIQUE(match_id, round_index),
  FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_maps_match ON maps(match_id);

CREATE TABLE IF NOT EXISTS map_votes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  match_id TEXT NOT NULL,
  map_name TEXT,
  status TEXT,              -- 'drop' or 'pick'
  selected_by_faction TEXT, -- 'faction1' or 'faction2'
  round_num INTEGER,
  -- resolved team (nullable if mapping unknown)
  selected_by_team_id TEXT,
  FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_votes_match ON map_votes(match_id);

CREATE TABLE IF NOT EXISTS team_stats (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  match_id TEXT NOT NULL,
  round_index INTEGER NOT NULL,
  team_id TEXT NOT NULL,
  team_name TEXT,
  kills INTEGER,
  deaths INTEGER,
  assists INTEGER,
  kd REAL,
  kr REAL,
  adr REAL,
  hs_pct REAL,
  mvps INTEGER,
  sniper_kills INTEGER,
  utility_damage INTEGER,
  flash_assists INTEGER,
  entry_count INTEGER,
  entry_wins INTEGER,
  mk_2k INTEGER,
  mk_3k INTEGER,
  mk_4k INTEGER,
  mk_5k INTEGER,
  UNIQUE(match_id, round_index, team_id),
  FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_teamstats_team ON team_stats(team_id);

CREATE TABLE IF NOT EXISTS player_stats (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  match_id TEXT NOT NULL,
  round_index INTEGER NOT NULL,
  player_id TEXT,
  nickname TEXT,
  team_id TEXT,
  team_name TEXT,
  kills INTEGER,
  deaths INTEGER,
  assists INTEGER,
  kd REAL,
  kr REAL,
  adr REAL,
  hs_pct REAL,
  mvps INTEGER,
  sniper_kills INTEGER,
  utility_damage INTEGER,
  flash_assists INTEGER,
  mk_3k INTEGER,
  mk_4k INTEGER,
  mk_5k INTEGER,
  clutch_kills            INTEGER DEFAULT 0,
  cl_1v1_attempts         INTEGER DEFAULT 0,
  cl_1v1_wins             INTEGER DEFAULT 0,
  cl_1v2_attempts         INTEGER DEFAULT 0,
  cl_1v2_wins             INTEGER DEFAULT 0,
  UNIQUE(match_id, round_index, player_id, nickname),
  FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_playerstats_team ON player_stats(team_id);
