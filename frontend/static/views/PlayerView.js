const PLAYER_KPI_SCHEMA = [
    { key: 'kd', label: 'K/D', decimals: 2, max: 2.2 },
    { key: 'kr', label: 'K/R', decimals: 2, max: 1.2 },
    { key: 'adr', label: 'ADR', decimals: 1, max: 130 },
    { key: 'hs_pct', label: 'HS%', decimals: 1, max: 100, percent: true },
    { key: 'entry_pct', label: 'Entry %', decimals: 1, max: 100, percent: true },
    { key: 'clutch_pct', label: 'Clutch %', decimals: 1, max: 100, percent: true },
    { key: 'flash_success_pct', label: 'Flash Suc %', decimals: 1, max: 100, percent: true },
    { key: 'utility_success_pct', label: 'Utility Suc %', decimals: 1, max: 100, percent: true }
];

const SUM_FIELDS = [
    'maps_played', 'rounds_played', 'kills', 'deaths', 'assists', 'mvps', 'headshots', 'damage',
    'sniper_kills', 'pistol_kills', 'knife_kills', 'zeus_kills', 'first_kills',
    'enemies_flashed', 'flash_count', 'flash_successes', 'utility_damage', 'utility_count',
    'utility_successes', 'utility_enemies', 'mk_2k', 'mk_3k', 'mk_4k', 'mk_5k', 'clutch_kills',
    'cl_1v1_attempts', 'cl_1v1_wins', 'cl_1v2_attempts', 'cl_1v2_wins', 'entry_count', 'entry_wins'
];

const TOTALS_SECTION_SCHEMA = [
    {
        key: 'core',
        title: 'Core',
        rows: [
            { label: 'Maps', key: 'maps_played', fmt: 'int' },
            { label: 'Rounds', key: 'rounds_played', fmt: 'int' },
            { label: 'Kills', key: 'kills', fmt: 'int' },
            { label: 'Deaths', key: 'deaths', fmt: 'int' },
            { label: 'Assists', key: 'assists', fmt: 'int' },
            { label: 'MVPs', key: 'mvps', fmt: 'int' },
            { label: 'Headshots', key: 'headshots', fmt: 'int' },
            { label: 'Damage', key: 'damage', fmt: 'int' }
        ]
    },
    {
        key: 'weapons',
        title: 'Weapons',
        rows: [
            { label: 'Sniper Kills', key: 'sniper_kills', fmt: 'int' },
            { label: 'Pistol Kills', key: 'pistol_kills', fmt: 'int' },
            { label: 'Knife Kills', key: 'knife_kills', fmt: 'int' },
            { label: 'Zeus Kills', key: 'zeus_kills', fmt: 'int' },
            { label: 'First Kills', key: 'first_kills', fmt: 'int' }
        ]
    },
    {
        key: 'utility',
        title: 'Utility',
        rows: [
            { label: 'Enemies Flashed', key: 'enemies_flashed', fmt: 'int' },
            { label: 'Flash Count', key: 'flash_count', fmt: 'int' },
            { label: 'Flash Successes', key: 'flash_successes', fmt: 'int' },
            { label: 'Utility Damage', key: 'utility_damage', fmt: 'int' },
            { label: 'Utility Count', key: 'utility_count', fmt: 'int' },
            { label: 'Utility Successes', key: 'utility_successes', fmt: 'int' },
            { label: 'Utility Enemies', key: 'utility_enemies', fmt: 'int' }
        ]
    },
    {
        key: 'impact',
        title: 'Impact',
        rows: [
            { label: '2K', key: 'mk_2k', fmt: 'int' },
            { label: '3K', key: 'mk_3k', fmt: 'int' },
            { label: '4K', key: 'mk_4k', fmt: 'int' },
            { label: '5K', key: 'mk_5k', fmt: 'int' },
            { label: 'Clutch Kills', key: 'clutch_kills', fmt: 'int' },
            { label: '1v1', winKey: 'cl_1v1_wins', attemptsKey: 'cl_1v1_attempts', fmt: 'pair' },
            { label: '1v2', winKey: 'cl_1v2_wins', attemptsKey: 'cl_1v2_attempts', fmt: 'pair' },
            { label: 'Entry', winKey: 'entry_wins', attemptsKey: 'entry_count', fmt: 'pair' }
        ]
    },
    {
        key: 'ratios',
        title: 'Ratios',
        rows: [
            { label: 'K/D', key: 'kd', fmt: 'float', decimals: 2 },
            { label: 'K/R', key: 'kr', fmt: 'float', decimals: 2 },
            { label: 'ADR', key: 'adr', fmt: 'float', decimals: 1 },
            { label: 'HS%', key: 'hs_pct', fmt: 'pct', decimals: 1 },
            { label: 'Entry %', key: 'entry_pct', fmt: 'pct', decimals: 1 },
            { label: 'Clutch %', key: 'clutch_pct', fmt: 'pct', decimals: 1 },
            { label: 'Flash %', key: 'flash_pct', fmt: 'pct', decimals: 1 },
            { label: 'Util / R', key: 'utility_per_round', fmt: 'float', decimals: 2 }
        ]
    }
];

const SUMMARY_CARD_SCHEMA = [
    { key: 'maps_played', label: 'Maps', fmt: 'int' },
    { key: 'rounds_played', label: 'Rounds', fmt: 'int' },
    { key: 'kills', label: 'Kills', fmt: 'int' },
    { key: 'deaths', label: 'Deaths', fmt: 'int' },
    { key: 'assists', label: 'Assists', fmt: 'int' },
    { key: 'kd', label: 'K/D', fmt: 'float', decimals: 2 },
    { key: 'adr', label: 'ADR', fmt: 'float', decimals: 1 },
    { key: 'hs_pct', label: 'HS%', fmt: 'pct', decimals: 1 }
];

const TREND_METRIC_OPTIONS = [
    { key: 'kills', label: 'Kills', decimals: 0, color: '#60a5fa' },
    { key: 'deaths', label: 'Deaths', decimals: 0, color: '#f87171' },
    { key: 'assists', label: 'Assists', decimals: 0, color: '#34d399' },
    { key: 'mvps', label: 'MVPs', decimals: 0, color: '#fbbf24' },
    { key: 'headshots', label: 'Headshots', decimals: 0, color: '#a78bfa' },
    { key: 'damage', label: 'Damage', decimals: 0, color: '#f59e0b' },
    { key: 'sniper_kills', label: 'Sniper Kills', decimals: 0, color: '#22d3ee' },
    { key: 'pistol_kills', label: 'Pistol Kills', decimals: 0, color: '#38bdf8' },
    { key: 'knife_kills', label: 'Knife Kills', decimals: 0, color: '#fb7185' },
    { key: 'zeus_kills', label: 'Zeus Kills', decimals: 0, color: '#f472b6' },
    { key: 'first_kills', label: 'First Kills', decimals: 0, color: '#4ade80' },
    { key: 'enemies_flashed', label: 'Enemies Flashed', decimals: 0, color: '#2dd4bf' },
    { key: 'flash_count', label: 'Flash Count', decimals: 0, color: '#06b6d4' },
    { key: 'flash_successes', label: 'Flash Successes', decimals: 0, color: '#10b981' },
    { key: 'utility_damage', label: 'Utility Damage', decimals: 0, color: '#f59e0b' },
    { key: 'utility_count', label: 'Utility Count', decimals: 0, color: '#0ea5e9' },
    { key: 'utility_successes', label: 'Utility Success', decimals: 0, color: '#14b8a6' },
    { key: 'utility_enemies', label: 'Utility Enemies', decimals: 0, color: '#22c55e' },
    { key: 'mk_2k', label: '2K', decimals: 0, color: '#60a5fa' },
    { key: 'mk_3k', label: '3K', decimals: 0, color: '#34d399' },
    { key: 'mk_4k', label: '4K', decimals: 0, color: '#fbbf24' },
    { key: 'mk_5k', label: '5K', decimals: 0, color: '#f97316' },
    { key: 'clutch_kills', label: 'Clutch Kills', decimals: 0, color: '#a78bfa' },
    { key: 'cl_1v1_attempts', label: '1v1 Attempts', decimals: 0, color: '#22d3ee' },
    { key: 'cl_1v1_wins', label: '1v1 Wins', decimals: 0, color: '#2dd4bf' },
    { key: 'cl_1v2_attempts', label: '1v2 Attempts', decimals: 0, color: '#38bdf8' },
    { key: 'cl_1v2_wins', label: '1v2 Wins', decimals: 0, color: '#10b981' },
    { key: 'entry_count', label: 'Entry Count', decimals: 0, color: '#f59e0b' },
    { key: 'entry_wins', label: 'Entry Wins', decimals: 0, color: '#4ade80' },
    { key: 'kd', label: 'K/D', decimals: 2, color: '#60a5fa' },
    { key: 'kr', label: 'K/R', decimals: 2, color: '#34d399' },
    { key: 'adr', label: 'ADR', decimals: 1, color: '#fbbf24' },
    { key: 'hs_pct', label: 'HS%', decimals: 1, percent: true, color: '#a78bfa' },
    { key: 'entry_pct', label: 'Entry %', decimals: 1, percent: true, color: '#22d3ee' },
    { key: 'clutch_pct', label: 'Clutch %', decimals: 1, percent: true, color: '#2dd4bf' },
    { key: 'flash_pct', label: 'Flash %', decimals: 1, percent: true, color: '#10b981' },
    { key: 'utility_per_round', label: 'Util / Round', decimals: 2, color: '#0ea5e9' }
];

const TREND_METRIC_GROUPS = [
    { key: 'kills_deaths', title: 'Fragging', members: ['kills', 'deaths', 'assists'] },
    { key: 'entry', title: 'Entry', members: ['clutch_kills', 'first_kills', 'headshots', 'mvps', 'adr'] },
    { key: 'ratios', title: 'Ratios', members: ['kd', 'kr'] },
    { key: 'damage_deaths', title: 'Damage', members: ['damage', 'utility_damage'] },
    { key: 'utility', title: 'Utility Impact', members: ['utility_count', 'utility_successes', 'utility_enemies'] },
    { key: 'multikills', title: 'Multikills', members: ['mk_2k', 'mk_3k', 'mk_4k', 'mk_5k'] },
    { key: 'clutch_1v1', title: '1v1 Duel', members: ['cl_1v1_attempts', 'cl_1v1_wins'] },
    { key: 'clutch_1v2', title: '1v2 Duel', members: ['cl_1v2_attempts', 'cl_1v2_wins'] },
    { key: 'entry_duel', title: 'Entry Duel', members: ['entry_count', 'entry_wins'] },
    { key: 'flash_duel', title: 'Flash Impact', members: ['enemies_flashed', 'flash_count', 'flash_successes'] },
    { key: 'percentage_stats', title: 'Percentages', members: ['entry_pct', 'clutch_pct', 'hs_pct', 'flash_pct'] },
    { key: 'gun_game', title: 'Weapon Kills', members: ['sniper_kills', 'pistol_kills', 'knife_kills', 'zeus_kills'] }
];

function toNumber(value, fallback = 0) {
    if (value === null || value === undefined) return fallback;
    const numeric = Number(value);
    if (Number.isFinite(numeric)) return numeric;
    const parsed = Number(String(value).replace(',', '.'));
    return Number.isFinite(parsed) ? parsed : fallback;
}

function toCamelKey(snakeKey) {
    return String(snakeKey).replace(/_([a-zA-Z0-9])/g, (_, token) => {
        if (/[0-9]/.test(token)) return token;
        return token.toUpperCase();
    });
}

function pickField(row, snakeKey, fallback = null) {
    if (!row || typeof row !== 'object') return fallback;
    const camelKey = toCamelKey(snakeKey);
    if (row[snakeKey] !== undefined && row[snakeKey] !== null) return row[snakeKey];
    if (row[camelKey] !== undefined && row[camelKey] !== null) return row[camelKey];
    return fallback;
}

function normalizeSeasonRow(row) {
    if (!row || typeof row !== 'object') return {};
    const normalized = {
        championship_id: String(pickField(row, 'championship_id', pickField(row, 'id', '')) || ''),
        season: toNumber(pickField(row, 'season')),
        division_num: toNumber(pickField(row, 'division_num')),
        team_name: pickField(row, 'team_name', null),
        team_id: pickField(row, 'team_id', null),
        team_avatar: pickField(row, 'team_avatar', null),
        is_playoffs: Boolean(pickField(row, 'is_playoffs', false))
    };
    SUM_FIELDS.forEach(field => {
        normalized[field] = toNumber(pickField(row, field));
    });
    normalized.kd = toNumber(pickField(row, 'kd'));
    normalized.kr = toNumber(pickField(row, 'kr'));
    normalized.adr = toNumber(pickField(row, 'adr'));
    normalized.hs_pct = toNumber(pickField(row, 'hs_pct'));
    return normalized;
}

function safeDivide(num, den) {
    if (!den) return 0;
    return num / den;
}

function formatValue(value, options = {}) {
    const numeric = toNumber(value);
    const decimals = options.decimals ?? 1;
    if (options.percent) return `${numeric.toFixed(decimals)} %`;
    return numeric.toFixed(decimals);
}

function formatInt(value) {
    return toNumber(value).toLocaleString('fi-FI');
}

function formatFloat(value, decimals = 1) {
    return toNumber(value).toFixed(decimals);
}

function formatPercent(value, decimals = 1) {
    return `${toNumber(value).toFixed(decimals)} %`;
}

function aggregateSeasons(seasons) {
    const rows = Array.isArray(seasons) ? seasons : [];
    const totals = { season_count: rows.length };
    SUM_FIELDS.forEach(key => {
        totals[key] = 0;
    });
    rows.forEach(row => {
        SUM_FIELDS.forEach(key => {
            totals[key] += toNumber(row?.[key]);
        });
    });

    totals.kd = safeDivide(totals.kills, totals.deaths);
    totals.kr = safeDivide(totals.kills, totals.rounds_played);
    totals.adr = safeDivide(totals.damage, totals.rounds_played);
    totals.hs_pct = safeDivide(totals.headshots, totals.kills) * 100;
    totals.entry_pct = safeDivide(totals.entry_wins, totals.entry_count) * 100;
    totals.flash_pct = safeDivide(totals.flash_successes, totals.flash_count) * 100;
    const clutch_attempts = totals.cl_1v1_attempts + totals.cl_1v2_attempts;
    const clutch_wins = totals.cl_1v1_wins + totals.cl_1v2_wins;
    totals.clutch_pct = safeDivide(clutch_wins, clutch_attempts) * 100;
    totals.utility_per_round = safeDivide(totals.utility_damage, totals.rounds_played);
    totals.entry_per_round = safeDivide(totals.entry_count, totals.rounds_played);
    totals.first_kills_per_round = safeDivide(totals.first_kills, totals.rounds_played);
    return totals;
}

function buildKpis(stats) {
    if (!stats) return [];
    const computed = { ...stats };
    // Derive KPI fields when API row contains only base counters.
    if (!Number.isFinite(Number(computed.kd))) {
        computed.kd = safeDivide(toNumber(computed.kills), Math.max(1, toNumber(computed.deaths)));
    }
    if (!Number.isFinite(Number(computed.kr))) {
        computed.kr = safeDivide(toNumber(computed.kills), Math.max(1, toNumber(computed.rounds_played)));
    }
    if (!Number.isFinite(Number(computed.adr))) {
        computed.adr = safeDivide(toNumber(computed.damage), Math.max(1, toNumber(computed.rounds_played)));
    }
    if (!Number.isFinite(Number(computed.hs_pct))) {
        computed.hs_pct = safeDivide(toNumber(computed.headshots), Math.max(1, toNumber(computed.kills))) * 100;
    }
    if (!Number.isFinite(Number(computed.entry_pct))) {
        computed.entry_pct = safeDivide(toNumber(computed.entry_wins), Math.max(1, toNumber(computed.entry_count))) * 100;
    }
    if (!Number.isFinite(Number(computed.clutch_pct))) {
        const attempts = toNumber(computed.cl_1v1_attempts) + toNumber(computed.cl_1v2_attempts);
        const wins = toNumber(computed.cl_1v1_wins) + toNumber(computed.cl_1v2_wins);
        computed.clutch_pct = safeDivide(wins, Math.max(1, attempts)) * 100;
    }
    if (!Number.isFinite(Number(computed.utility_per_round))) {
        computed.utility_per_round = safeDivide(
            toNumber(computed.utility_damage),
            Math.max(1, toNumber(computed.rounds_played))
        );
    }
    if (!Number.isFinite(Number(computed.flash_success_pct))) {
        computed.flash_success_pct = safeDivide(
            toNumber(computed.flash_successes),
            Math.max(1, toNumber(computed.flash_count))
        ) * 100;
    }
    if (!Number.isFinite(Number(computed.utility_success_pct))) {
        computed.utility_success_pct = safeDivide(
            toNumber(computed.utility_successes),
            Math.max(1, toNumber(computed.utility_count))
        ) * 100;
    }

    return PLAYER_KPI_SCHEMA.map(def => ({
        key: def.key,
        label: def.label,
        value: toNumber(computed[def.key]),
        display: formatValue(computed[def.key], def),
        max: def.max
    }));
}

function buildRadarMetrics(kpis) {
    return kpis.map(kpi => ({
        key: kpi.key,
        label: kpi.label,
        value: kpi.value,
        max: kpi.max,
        decimals: PLAYER_KPI_SCHEMA.find(def => def.key === kpi.key)?.decimals ?? 1,
        percent: Boolean(PLAYER_KPI_SCHEMA.find(def => def.key === kpi.key)?.percent)
    }));
}

function buildCompareMetrics(baseKpis, compareKpis) {
    const compareMap = new Map(compareKpis.map(item => [item.key, item]));
    return baseKpis.map(item => ({
        key: item.key,
        label: item.label,
        base: item.value,
        compare: compareMap.get(item.key)?.value ?? null,
        decimals: PLAYER_KPI_SCHEMA.find(def => def.key === item.key)?.decimals ?? 1,
        percent: Boolean(PLAYER_KPI_SCHEMA.find(def => def.key === item.key)?.percent),
        format: value => {
            const def = PLAYER_KPI_SCHEMA.find(entry => entry.key === item.key) || {};
            return formatValue(value, { decimals: def.decimals, percent: def.percent });
        }
    }));
}

function createSegment() {
    return { data: null, loading: false, error: null };
}

function buildSeasonOption(season) {
    const championshipId = String(season.championship_id || season.championshipId || season.id || '');
    const seasonNum = toNumber(season.season);
    const divisionNum = toNumber(season.division_num);
    const isPlayoffs = Boolean(
        season.is_playoffs || season.is_playoff || (season.phase && String(season.phase).toLowerCase().includes('playoff'))
    );
    return {
        value: championshipId,
        season: seasonNum,
        division: divisionNum,
        team: season.team_name || season.team || null,
        label: `S${seasonNum} · D${divisionNum} · ${isPlayoffs ? 'Playoffs' : 'Runkosarja'}`,
        isPlayoffs
    };
}

function buildTotalsSections(totals) {
    if (!totals) return [];
    const formatRowValue = row => {
        if (row.fmt === 'pair') {
            return `${formatInt(totals[row.winKey])} / ${formatInt(totals[row.attemptsKey])}`;
        }
        if (row.fmt === 'pct') {
            return formatPercent(totals[row.key], row.decimals ?? 1);
        }
        if (row.fmt === 'float') {
            return formatFloat(totals[row.key], row.decimals ?? 1);
        }
        return formatInt(totals[row.key]);
    };
    return TOTALS_SECTION_SCHEMA.map(section => ({
        key: section.key,
        title: section.title,
        rows: section.rows.map(row => ({
            label: row.label,
            value: formatRowValue(row)
        }))
    }));
}

function buildSummaryCards(totals) {
    if (!totals) return [];
    return SUMMARY_CARD_SCHEMA.map(row => {
        let value = formatInt(totals[row.key]);
        if (row.fmt === 'float') {
            value = formatFloat(totals[row.key], row.decimals ?? 1);
        }
        if (row.fmt === 'pct') {
            value = formatPercent(totals[row.key], row.decimals ?? 1);
        }
        return { key: row.key, label: row.label, value };
    });
}

function trendMetricValue(row, metricKey) {
    if (!row) return 0;
    if (metricKey === 'entry_pct') return safeDivide(toNumber(row.entry_wins), Math.max(1, toNumber(row.entry_count))) * 100;
    if (metricKey === 'clutch_pct') {
        const attempts = toNumber(row.cl_1v1_attempts) + toNumber(row.cl_1v2_attempts);
        const wins = toNumber(row.cl_1v1_wins) + toNumber(row.cl_1v2_wins);
        return safeDivide(wins, Math.max(1, attempts)) * 100;
    }
    if (metricKey === 'flash_pct') return safeDivide(toNumber(row.flash_successes), Math.max(1, toNumber(row.flash_count))) * 100;
    if (metricKey === 'utility_per_round') return safeDivide(toNumber(row.utility_damage), Math.max(1, toNumber(row.rounds_played)));
    return toNumber(row[metricKey]);
}

function metricByKey(metricKey) {
    return TREND_METRIC_OPTIONS.find(metric => metric.key === metricKey) || null;
}

function normalizeProgressionRow(row) {
    if (!row || typeof row !== 'object') return null;
    const normalized = {
        snapshot_ts: toNumber(pickField(row, 'snapshot_ts')),
        snapshot_time: pickField(row, 'snapshot_time', null),
        match_played_at: pickField(row, 'match_played_at', null),
        round_index: toNumber(pickField(row, 'round_index', 0)),
        match_id: pickField(row, 'match_id', null),
        match_team1_id: pickField(row, 'match_team1_id', null),
        match_team2_id: pickField(row, 'match_team2_id', null),
        team_id: pickField(row, 'team_id', null),
        team_name: pickField(row, 'team_name', null),
        opponent_team_id: pickField(row, 'opponent_team_id', null),
        opponent_team_name: pickField(row, 'opponent_team_name', null),
        matchup: pickField(row, 'matchup', null),
        result: pickField(row, 'result', null),
        match_is_playoffs: pickField(row, 'match_is_playoffs', null),
        map_names_csv: pickField(row, 'map_names_csv', null),
        map_scores_csv: pickField(row, 'map_scores_csv', null),
        trend_label: pickField(row, 'trend_label', null),
        season: toNumber(pickField(row, 'season')),
        division_num: toNumber(pickField(row, 'division_num')),
        is_playoffs: Boolean(pickField(row, 'is_playoffs', false))
    };
    SUM_FIELDS.forEach(field => {
        normalized[field] = toNumber(pickField(row, field));
    });
    normalized.kd = toNumber(pickField(row, 'kd'));
    normalized.kr = toNumber(pickField(row, 'kr'));
    normalized.adr = toNumber(pickField(row, 'adr'));
    normalized.hs_pct = toNumber(pickField(row, 'hs_pct'));
    return normalized;
}

function withDerivedMetrics(row) {
    const base = { ...row };
    base.kd = safeDivide(toNumber(base.kills), Math.max(1, toNumber(base.deaths)));
    base.kr = safeDivide(toNumber(base.kills), Math.max(1, toNumber(base.rounds_played)));
    base.adr = safeDivide(toNumber(base.damage), Math.max(1, toNumber(base.rounds_played)));
    base.hs_pct = safeDivide(toNumber(base.headshots), Math.max(1, toNumber(base.kills))) * 100;
    base.entry_pct = safeDivide(toNumber(base.entry_wins), Math.max(1, toNumber(base.entry_count))) * 100;
    const clutchAttempts = toNumber(base.cl_1v1_attempts) + toNumber(base.cl_1v2_attempts);
    const clutchWins = toNumber(base.cl_1v1_wins) + toNumber(base.cl_1v2_wins);
    base.clutch_pct = safeDivide(clutchWins, Math.max(1, clutchAttempts)) * 100;
    base.flash_pct = safeDivide(toNumber(base.flash_successes), Math.max(1, toNumber(base.flash_count))) * 100;
    base.utility_per_round = safeDivide(toNumber(base.utility_damage), Math.max(1, toNumber(base.rounds_played)));
    return base;
}

function emptyProgressionTotals() {
    const totals = {};
    SUM_FIELDS.forEach(field => {
        totals[field] = 0;
    });
    return totals;
}

function parseCsvList(value) {
    if (!value) return [];
    return String(value).split('||').map(item => item.trim()).filter(Boolean);
}

function beautifyMapName(raw) {
    if (!raw) return 'Kartta';
    const value = String(raw).trim();
    const lower = value.toLowerCase();
    if (lower === 'forfeit') return 'Forfeit';
    const core = lower.startsWith('de_') ? lower.slice(3) : lower;
    const parts = core.split(/[_-]/).filter(Boolean);
    if (!parts.length) return value;
    return parts.map(part => part.charAt(0).toUpperCase() + part.slice(1)).join(' ');
}

function orientMapScore(rawScore, teamId, team1Id, team2Id) {
    if (!rawScore) return { own: null, opp: null };
    const parts = String(rawScore).split(':');
    if (parts.length < 2) return { own: null, opp: null };
    const s1 = toNumber(parts[0], null);
    const s2 = toNumber(parts[1], null);
    if (!Number.isFinite(s1) || !Number.isFinite(s2)) return { own: null, opp: null };
    if (teamId && team1Id && String(teamId) === String(team1Id)) {
        return { own: s1, opp: s2 };
    }
    if (teamId && team2Id && String(teamId) === String(team2Id)) {
        return { own: s2, opp: s1 };
    }
    return { own: s1, opp: s2 };
}

function buildProgressionTrendPoints(rows = []) {
    const rowTime = row => {
        const primary = row?.match_played_at || row?.snapshot_time;
        const ts = primary ? Date.parse(primary) : NaN;
        return Number.isFinite(ts) ? ts : NaN;
    };
    const snapshots = rows
        .map(normalizeProgressionRow)
        .filter(Boolean)
        .sort((a, b) => {
            const ta = rowTime(a);
            const tb = rowTime(b);
            if (Number.isFinite(ta) && Number.isFinite(tb) && ta !== tb) return ta - tb;
            const tsCmp = toNumber(a.snapshot_ts) - toNumber(b.snapshot_ts);
            if (tsCmp !== 0) return tsCmp;
            const matchCmp = String(a.match_id || '').localeCompare(String(b.match_id || ''));
            if (matchCmp !== 0) return matchCmp;
            return toNumber(a.round_index) - toNumber(b.round_index);
        });
    if (!snapshots.length) return [];

    const cumulative = emptyProgressionTotals();
    const points = [];
    let prev = null;
    let pointIndex = 0;

    snapshots.forEach(snapshot => {
        const contextChanged = !prev
            || toNumber(snapshot.season) !== toNumber(prev.season)
            || toNumber(snapshot.division_num) !== toNumber(prev.division_num)
            || Boolean(snapshot.is_playoffs) !== Boolean(prev.is_playoffs)
            || String(snapshot.team_id || '') !== String(prev.team_id || '')
            || toNumber(snapshot.maps_played) < toNumber(prev.maps_played);
        const baseline = contextChanged ? emptyProgressionTotals() : prev;
        const rawDeltaMaps = Math.max(0, Math.round(toNumber(snapshot.maps_played) - toNumber(baseline.maps_played)));
        // Map-based progression should move exactly with actual cumulative counters.
        // If there is no cumulative change, this is a duplicate/non-progressing snapshot -> skip.
        if (rawDeltaMaps <= 0) {
            prev = snapshot;
            return;
        }

        const delta = {};
        SUM_FIELDS.forEach(field => {
            const rawDelta = toNumber(snapshot[field]) - toNumber(baseline[field]);
            // Cumulative counters should never pull all-time totals down even if upstream snapshots are corrected/rebuilt.
            delta[field] = Math.max(0, rawDelta);
        });
        const mapSlice = {};
        SUM_FIELDS.forEach(field => {
            const inc = delta[field];
            mapSlice[field] = inc;
            cumulative[field] += inc;
        });
        const mapMetrics = withDerivedMetrics(mapSlice);
        const avgMetrics = withDerivedMetrics(cumulative);
        pointIndex += 1;
        const derivedSeasonLabel = Number.isFinite(snapshot.season) && Number.isFinite(snapshot.division_num)
            ? `S${snapshot.season} · D${snapshot.division_num}${snapshot.is_playoffs ? ' P' : ''}`
            : null;
        const seasonLabel = snapshot.trend_label || derivedSeasonLabel;
        const mapNames = parseCsvList(snapshot.map_names_csv);
        const mapScores = parseCsvList(snapshot.map_scores_csv);
        const mapName = mapNames[0] || `Map ${pointIndex}`;
        const rawMapScore = mapScores[0] || null;
        const orientedScore = orientMapScore(rawMapScore, snapshot.team_id, snapshot.match_team1_id, snapshot.match_team2_id);
        const mapScoreLabel = Number.isFinite(orientedScore.own) && Number.isFinite(orientedScore.opp)
            ? `${Math.round(orientedScore.own)}-${Math.round(orientedScore.opp)}`
            : null;
        let mapResult = null;
        if (Number.isFinite(orientedScore.own) && Number.isFinite(orientedScore.opp)) {
            if (orientedScore.own > orientedScore.opp) mapResult = 'win';
            else if (orientedScore.own < orientedScore.opp) mapResult = 'loss';
            else mapResult = 'draw';
        }
        points.push({
            idx: pointIndex - 1,
            label: seasonLabel ? `${seasonLabel} · M${pointIndex}` : `M${pointIndex}`,
            seasonLabel,
            mapNo: pointIndex,
            snapshotTs: snapshot.snapshot_ts,
            matchPlayedAt: snapshot.match_played_at || snapshot.snapshot_time || null,
            matchId: snapshot.match_id || null,
            mapName,
            mapScoreLabel,
            mapResult,
            teamName: snapshot.team_name || null,
            opponentTeamName: snapshot.opponent_team_name || null,
            matchup: snapshot.matchup || null,
            result: snapshot.result || null,
            map: mapMetrics,
            avg: avgMetrics
        });

        prev = snapshot;
    });

    return points;
}

function buildIndexGrid(count, maxTicks = 10) {
    if (count <= 0) return [];
    if (count <= maxTicks) return Array.from({ length: count }, (_, i) => i);
    const step = Math.ceil((count - 1) / (maxTicks - 1));
    const indices = [];
    for (let i = 0; i < count; i += step) {
        indices.push(i);
    }
    if (indices[indices.length - 1] !== count - 1) {
        indices.push(count - 1);
    }
    return indices;
}

window.PlayerView = {
    name: 'PlayerView',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; },
        get StatPanel() { return window.StatPanel; },
        get RadarChart() { return window.RadarChart; },
        get MapsStats() { return window.MapsStats; },
        get PlayerCompareModal() { return window.PlayerCompareModal; }
    },
    data() {
        const playerStore = typeof window.usePlayerStore === 'function' ? window.usePlayerStore() : null;
        return {
            playerStore,
            selectedSeasonId: null,
            divisionAveragesByChampionship: {},
            trendScope: 'all',
            trendMode: 'avg',
            trendMetricKeys: ['kills', 'deaths', 'assists', 'adr', 'kd', 'kr'],
            trendChartWidth: 640,
            trendChartHeight: 140,
            playerTrendHover: {
                key: null,
                index: null,
                x: 0,
                y: 0
            },
            compareVisible: false,
            compareLoading: false,
            compareError: null,
            comparePlayer: null,
            compareMetrics: []
        };
    },
    computed: {
        playerId() {
            return this.$route.params?.playerId || null;
        },
        playerState() {
            if (!this.playerStore || !this.playerId) return null;
            return this.playerStore.getPlayerState(this.playerId) || null;
        },
        profileSegment() {
            return this.playerState?.profile || createSegment();
        },
        seasonsSegment() {
            return this.playerState?.seasons || createSegment();
        },
        mapStatsSegment() {
            if (!this.selectedSeasonId) return createSegment();
            return this.playerState?.maps?.[this.selectedSeasonId] || createSegment();
        },
        selectedProgressionSegment() {
            if (!this.selectedSeasonId) return createSegment();
            return this.playerState?.progression?.[this.selectedSeasonId] || createSegment();
        },
        profile() {
            return this.profileSegment.data || null;
        },
        allSeasonsRaw() {
            const rows = Array.isArray(this.seasonsSegment.data) ? this.seasonsSegment.data : [];
            return rows.map(normalizeSeasonRow);
        },
        allSeasons() {
            return this.allSeasonsRaw.map(buildSeasonOption);
        },
        seasonOptions() {
            return [...this.allSeasons].sort((a, b) => {
                const seasonCmp = toNumber(b.season) - toNumber(a.season);
                if (seasonCmp !== 0) return seasonCmp;
                const divisionCmp = toNumber(b.division) - toNumber(a.division);
                if (divisionCmp !== 0) return divisionCmp;
                return Number(b.isPlayoffs) - Number(a.isPlayoffs);
            });
        },
        currentSeasonOption() {
            if (!this.selectedSeasonId) return null;
            return this.allSeasons.find(option => option.value === this.selectedSeasonId) || null;
        },
        selectedSeasonStats() {
            if (!this.selectedSeasonId) return null;
            return this.allSeasonsRaw.find(item => String(item.championship_id) === String(this.selectedSeasonId)) || null;
        },
        selectedScopeRawSeasons() {
            return this.selectedSeasonStats ? [this.selectedSeasonStats] : [];
        },
        selectedDivisionTotals() {
            return aggregateSeasons(this.selectedScopeRawSeasons);
        },
        allTimeTotals() {
            return aggregateSeasons(this.allSeasonsRaw);
        },
        kpiMetrics() {
            return buildKpis(this.selectedSeasonStats || this.selectedDivisionTotals);
        },
        allTimeKpiMetrics() {
            return buildKpis(this.allTimeTotals);
        },
        trendMetricOptions() {
            return TREND_METRIC_OPTIONS;
        },
        allTrendMetricsSelected() {
            return this.trendMetricKeys.length >= this.trendMetricOptions.length;
        },
        trendScopeOptions() {
            return [
                { key: 'selected', label: 'Valinta' },
                { key: 'all', label: 'All-time' }
            ];
        },
        trendModeOptions() {
            return [
                { key: 'map', label: 'Kartta' },
                { key: 'avg', label: 'Kumulatiivinen' }
            ];
        },
        selectedProgressionRows() {
            const rows = Array.isArray(this.selectedProgressionSegment.data) ? this.selectedProgressionSegment.data : [];
            const current = this.currentSeasonOption;
            const wantedPlayoffs = Boolean(current?.isPlayoffs);
            return rows
                .filter(item => {
                    const flag = pickField(item, 'match_is_playoffs', null);
                    if (flag === null || flag === undefined) return true;
                    return Boolean(flag) === wantedPlayoffs;
                })
                .map(item => ({
                ...item,
                season: current?.season || 0,
                division_num: current?.division || 0,
                is_playoffs: pickField(item, 'match_is_playoffs', Boolean(current?.isPlayoffs)),
                trend_label: `S${current?.season || 0} · D${current?.division || 0}${pickField(item, 'match_is_playoffs', Boolean(current?.isPlayoffs)) ? ' P' : ''}`
                }));
        },
        allProgressionRows() {
            if (!this.playerState?.progression) return [];
            const rows = [];
            const seenRows = new Set();
            Object.entries(this.playerState.progression || {}).forEach(([championshipId, segment]) => {
                const option = this.seasonOptions.find(item => String(item.value) === String(championshipId)) || null;
                const points = Array.isArray(segment.data) ? segment.data : [];
                points.forEach(item => {
                    const row = normalizeProgressionRow(item);
                    if (!row) return;
                    const season = option?.season || toNumber(row.season);
                    const division = option?.division || toNumber(row.division_num);
                    const rowPlayoffs = pickField(item, 'match_is_playoffs', null);
                    const isPlayoffs = rowPlayoffs === null || rowPlayoffs === undefined
                        ? Boolean(option?.isPlayoffs)
                        : Boolean(rowPlayoffs);
                    // Keep PO and runkosarja rows distinct in all-time merge, even when snapshot/match ids overlap.
                    const rowKey = [
                        String(championshipId || ''),
                        String(toNumber(row.snapshot_ts)),
                        String(row.match_id || ''),
                        String(row.team_id || ''),
                        String(season),
                        String(division),
                        isPlayoffs ? '1' : '0'
                    ].join('::');
                    if (seenRows.has(rowKey)) return;
                    seenRows.add(rowKey);
                    rows.push({
                        ...item,
                        season,
                        division_num: division,
                        is_playoffs: isPlayoffs,
                        trend_label: `S${season} · D${division}${isPlayoffs ? ' P' : ''}`
                    });
                });
            });
            return rows.sort((a, b) => {
                const timeA = a?.snapshot_time ? Date.parse(a.snapshot_time) : NaN;
                const timeB = b?.snapshot_time ? Date.parse(b.snapshot_time) : NaN;
                if (Number.isFinite(timeA) && Number.isFinite(timeB) && timeA !== timeB) {
                    return timeA - timeB;
                }
                const tsCmp = toNumber(a.snapshot_ts) - toNumber(b.snapshot_ts);
                if (tsCmp !== 0) return tsCmp;
                const seasonCmp = toNumber(a.season) - toNumber(b.season);
                if (seasonCmp !== 0) return seasonCmp;
                const divisionCmp = toNumber(a.division_num) - toNumber(b.division_num);
                if (divisionCmp !== 0) return divisionCmp;
                return Number(Boolean(a.is_playoffs)) - Number(Boolean(b.is_playoffs));
            });
        },
        trendProgressionRows() {
            if (this.trendScope === 'selected') return this.selectedProgressionRows;
            return this.allProgressionRows;
        },
        trendBasePoints() {
            return buildProgressionTrendPoints(this.trendProgressionRows);
        },
        activeTrendMetrics() {
            const enabled = new Set(this.trendMetricKeys);
            return this.trendMetricOptions.filter(metric => enabled.has(metric.key));
        },
        activeTrendMetricGroups() {
            const enabled = new Set(this.trendMetricKeys);
            const consumed = new Set();
            const groups = [];

            TREND_METRIC_GROUPS.forEach(group => {
                const metrics = group.members
                    .filter(key => enabled.has(key))
                    .map(key => metricByKey(key))
                    .filter(Boolean);
                if (metrics.length > 1) {
                    metrics.forEach(metric => consumed.add(metric.key));
                    groups.push({
                        key: group.key,
                        title: group.title || metrics.map(metric => metric.label).join(' / '),
                        label: metrics.map(metric => metric.label).join(' / '),
                        metrics
                    });
                }
            });

            this.activeTrendMetrics.forEach(metric => {
                if (consumed.has(metric.key)) return;
                groups.push({
                    key: metric.key,
                    title: metric.label,
                    label: metric.label,
                    metrics: [metric]
                });
            });

            return groups;
        },
        trendCharts() {
            const points = this.trendBasePoints;
            if (!points.length) return [];
            const width = this.trendChartWidth || 640;
            const height = this.trendChartHeight || 140;
            const padding = { left: 46, right: 68, top: 16, bottom: 26 };
            const plotWidth = width - padding.left - padding.right;
            const plotHeight = height - padding.top - padding.bottom;
            const xFor = idx => padding.left + (points.length <= 1 ? 0 : (idx / (points.length - 1)) * plotWidth);
            const gridIndices = buildIndexGrid(points.length, 12);
            const labelIndices = buildIndexGrid(points.length, Math.min(4, points.length));
            const divisionLines = [];
            for (let i = 1; i < points.length; i += 1) {
                const prevLabel = String(points[i - 1]?.seasonLabel || '');
                const nextLabel = String(points[i]?.seasonLabel || '');
                if (prevLabel !== nextLabel) {
                    divisionLines.push({
                        index: i,
                        x: xFor(i),
                        label: nextLabel || prevLabel || ''
                    });
                }
            }

            return this.activeTrendMetricGroups.map(group => {
                const seriesValues = group.metrics.map(metric => points.map(p => {
                    const source = this.trendMode === 'avg' ? p.avg : p.map;
                    return trendMetricValue(source, metric.key);
                }));
                const allValues = seriesValues.flat();
                let max = Math.max(...allValues);
                if (!Number.isFinite(max) || max <= 0) max = 1;
                const yMin = 0;
                const yMax = max * 1.12;
                const yFor = value => padding.top + ((yMax - value) / Math.max(1e-6, (yMax - yMin))) * plotHeight;

                const basePoints = points.map((point, idx) => ({
                    id: `${group.key}-base-${idx}`,
                    index: idx,
                    x: xFor(idx),
                    y: yFor(seriesValues[0]?.[idx] || 0),
                    label: point.label,
                    seasonLabel: point.seasonLabel || null,
                    matchPlayedAt: point.matchPlayedAt || null,
                    mapNo: point.mapNo,
                    matchId: point.matchId,
                    mapName: point.mapName,
                    mapScoreLabel: point.mapScoreLabel,
                    mapResult: point.mapResult,
                    matchup: point.matchup || [point.teamName, point.opponentTeamName].filter(Boolean).join(' vs '),
                    result: point.result || null
                }));

                const series = group.metrics.map((metric, seriesIdx) => {
                    const values = seriesValues[seriesIdx] || [];
                    const chartPoints = values.map((value, idx) => ({
                        id: `${metric.key}-${idx}`,
                        index: idx,
                        value,
                        delta: idx > 0 ? (value - values[idx - 1]) : null,
                        x: xFor(idx),
                        y: yFor(value),
                        modeValue: value
                    }));
                    const path = chartPoints
                        .map((p, idx) => `${idx === 0 ? 'M' : 'L'} ${p.x.toFixed(2)} ${p.y.toFixed(2)}`)
                        .join(' ');
                    return {
                        ...metric,
                        points: chartPoints,
                        path,
                        latest: chartPoints[chartPoints.length - 1] || null,
                        labelY: (chartPoints[chartPoints.length - 1]?.y ?? 0) - 6
                    };
                });

                // Keep right-edge labels readable when multiple series end near each other.
                const labeled = series
                    .map((entry, idx) => ({ idx, y: entry.labelY }))
                    .sort((a, b) => a.y - b.y);
                const minGap = 18;
                const minY = padding.top + 10;
                const maxY = height - padding.bottom - 20;
                let cursor = minY;
                labeled.forEach(item => {
                    const clamped = Math.max(minY, Math.min(maxY, item.y));
                    item.y = Math.max(clamped, cursor);
                    cursor = item.y + minGap;
                });
                const overflow = labeled.length ? Math.max(0, labeled[labeled.length - 1].y - maxY) : 0;
                if (overflow > 0) {
                    for (let i = labeled.length - 1; i >= 0; i -= 1) {
                        const target = labeled[i].y - overflow;
                        const nextY = i < labeled.length - 1 ? labeled[i + 1].y - minGap : maxY;
                        labeled[i].y = Math.min(target, nextY);
                        if (labeled[i].y < minY) labeled[i].y = minY;
                    }
                }
                labeled.forEach(item => {
                    if (series[item.idx]) {
                        series[item.idx].labelY = item.y;
                    }
                });

                const ticks = [yMin, (yMin + yMax) / 2, yMax].map(value => ({
                    value,
                    y: yFor(value)
                }));
                const gridLines = gridIndices.map(index => ({ index, x: xFor(index) }));
                const xLabels = labelIndices.map(index => ({ index, x: xFor(index), label: `M${points[index].mapNo}` }));
                const refValue = (yMin + yMax) / 2;
                return {
                    key: group.key,
                    title: group.title || group.label,
                    label: group.label,
                    decimals: group.metrics[0]?.decimals ?? 1,
                    percent: Boolean(group.metrics[0]?.percent),
                    width,
                    height,
                    padding,
                    plotWidth,
                    plotHeight,
                    series,
                    points: basePoints,
                    ticks,
                    gridLines,
                    divisionLines,
                    xLabels,
                    latest: series[0]?.latest || null,
                    refY: yFor(refValue),
                    zeroY: null,
                    lineClass: 'trend-line--adr',
                    pointClass: 'trend-point--adr'
                };
            });
        },
        playerTrendVisibleCharts() {
            const charts = this.trendCharts;
            return charts.map((chart, idx) => ({
                ...chart,
                showXAxis: idx === charts.length - 1
            }));
        },
        radarMetrics() {
            return buildRadarMetrics(this.kpiMetrics);
        },
        selectedDivisionAverages() {
            if (!this.selectedSeasonId) return null;
            return this.divisionAveragesByChampionship?.[String(this.selectedSeasonId)] || null;
        },
        radarComparisons() {
            const averages = this.selectedDivisionAverages;
            if (!averages || typeof averages !== 'object') return [];
            const metrics = this.radarMetrics || [];
            const metricKeys = metrics.map(metric => String(metric.key || ''));
            if (!metricKeys.length) return [];

            return [
                {
                    key: 'division_median',
                    label: 'Division median',
                    color: '#f59e0b',
                    values: metricKeys.reduce((acc, key) => {
                        acc[key] = toNumber(averages[`median_${key}`], 0);
                        return acc;
                    }, {})
                }
            ];
        },
        mapStats() {
            return Array.isArray(this.mapStatsSegment.data) ? this.mapStatsSegment.data : [];
        },
        heroTeam() {
            return this.selectedSeasonStats?.team_name || this.selectedSeasonStats?.team || null;
        },
        heroTeamAvatar() {
            return this.selectedSeasonStats?.team_avatar || null;
        },
        loading() {
            return this.profileSegment.loading || this.seasonsSegment.loading;
        },
        loadError() {
            return this.profileSegment.error || this.seasonsSegment.error;
        },
        divisionTotalsSections() {
            return buildTotalsSections(this.selectedDivisionTotals);
        },
        allTimeTotalsSections() {
            return buildTotalsSections(this.allTimeTotals);
        },
        selectedSummaryCards() {
            return buildSummaryCards(this.selectedDivisionTotals);
        },
        allTimeSummaryCards() {
            return buildSummaryCards(this.allTimeTotals);
        },
        summaryCompareRows() {
            const selected = this.selectedSummaryCards || [];
            const all = this.allTimeSummaryCards || [];
            const allMap = new Map(all.map(item => [item.key, item]));
            return selected.map(item => ({
                key: item.key,
                label: item.label,
                selected: item.value,
                allTime: allMap.get(item.key)?.value ?? '-'
            }));
        },
        totalsCompareSections() {
            const selectedSections = this.divisionTotalsSections || [];
            const allSections = this.allTimeTotalsSections || [];
            const allMap = new Map(allSections.map(section => [section.key, section]));
            return selectedSections.map(section => {
                const allSection = allMap.get(section.key);
                const allRowsMap = new Map((allSection?.rows || []).map(row => [row.label, row.value]));
                return {
                    key: section.key,
                    title: section.title,
                    rows: (section.rows || []).map(row => ({
                        label: row.label,
                        selected: row.value,
                        allTime: allRowsMap.get(row.label) ?? '-'
                    }))
                };
            });
        }
    },
    watch: {
        playerId: {
            immediate: true,
            handler() {
                this.bootstrap();
            }
        },
        seasonOptions(newOptions) {
            if (!Array.isArray(newOptions) || !newOptions.length) {
                this.selectedSeasonId = null;
                return;
            }
            if (!this.selectedSeasonId || !newOptions.some(option => option.value === this.selectedSeasonId)) {
                this.selectedSeasonId = newOptions[0].value;
            }
            if (this.trendScope === 'all') {
                this.loadAllProgressions();
            }
        },
        selectedSeasonId(newVal, oldVal) {
            if (newVal && newVal !== oldVal) {
                this.loadMapStats();
                this.loadSelectedProgression();
                this.loadDivisionAverages();
                this.syncRouteBreadcrumbContext();
                this.comparePlayer = null;
                this.compareMetrics = [];
            }
        },
        profile(newVal, oldVal) {
            const nextName = newVal?.nickname || '';
            const prevName = oldVal?.nickname || '';
            if (nextName && nextName !== prevName) {
                this.syncRouteBreadcrumbContext();
            }
        },
        trendScope(newVal) {
            if (newVal === 'all') {
                this.loadAllProgressions();
            } else {
                this.loadSelectedProgression();
            }
        },
        trendCharts() {
            this.$nextTick(() => {
                this.setupTrendChartObserver();
                this.updateTrendChartWidth();
            });
        }
    },
    methods: {
        async bootstrap() {
            if (!this.playerStore || !this.playerId) return;
            this.compareVisible = false;
            this.compareMetrics = [];
            this.comparePlayer = null;
            try {
                await this.playerStore.fetchBundle(this.playerId, null, { force: true });
                const defaults = this.seasonOptions;
                if (defaults.length && !this.selectedSeasonId) {
                    this.selectedSeasonId = defaults[0].value;
                }
                if (this.selectedSeasonId) {
                    await this.playerStore.fetchBundle(this.playerId, this.selectedSeasonId, { force: true });
                }
                this.syncRouteBreadcrumbContext();
            } catch (error) {
                console.error('Player bootstrap failed', error);
            }
        },
        syncRouteBreadcrumbContext() {
            if (!this.$router || !this.$route || !this.playerId) return;
            const currentSeason = this.currentSeasonOption || null;
            const currentSeasonStats = this.selectedSeasonStats || null;
            const teamName = this.heroTeam || null;
            const playerName = this.profile?.nickname || null;

            const nextQuery = { ...(this.$route.query || {}) };
            if (this.selectedSeasonId) nextQuery.championship = String(this.selectedSeasonId);
            else delete nextQuery.championship;

            if (currentSeason?.season != null) nextQuery.championship_season = String(currentSeason.season);
            else delete nextQuery.championship_season;

            const divisionName = this.resolveBreadcrumbDivisionName(currentSeason);
            if (divisionName) nextQuery.championship_name = divisionName;
            else delete nextQuery.championship_name;

            if (currentSeason?.isPlayoffs) nextQuery.championship_playoffs = '1';
            else delete nextQuery.championship_playoffs;

            if (currentSeasonStats?.team_id != null) nextQuery.team_id = String(currentSeasonStats.team_id);
            else delete nextQuery.team_id;

            if (teamName) nextQuery.team_name = String(teamName);
            else delete nextQuery.team_name;

            if (playerName) nextQuery.player_name = String(playerName);
            else delete nextQuery.player_name;

            const normalizeQuery = obj => Object.keys(obj)
                .sort()
                .map(key => `${key}:${String(obj[key])}`)
                .join('|');
            if (normalizeQuery(nextQuery) === normalizeQuery(this.$route.query || {})) return;

            this.$router.replace({
                name: this.$route.name || 'player',
                params: { ...(this.$route.params || {}), playerId: this.playerId },
                query: nextQuery
            }).catch(() => {});
        },
        resolveBreadcrumbDivisionName(seasonOption) {
            if (!seasonOption) return null;
            const normalizer = typeof window !== 'undefined' ? window.divisionNormalizer : null;
            if (normalizer?.buildDivisionBreadcrumbMeta) {
                return normalizer.buildDivisionBreadcrumbMeta({
                    name: null,
                    divisionNum: seasonOption.division,
                    season: seasonOption.season,
                    isPlayoffs: Boolean(seasonOption.isPlayoffs)
                }).name;
            }
            const divisionNum = toNumber(seasonOption.division, null);
            if (divisionNum === 0) return 'Mestaruussarja';
            if (divisionNum != null) return `${divisionNum} Divisioona`;
            return null;
        },
        async loadMapStats() {
            if (!this.playerStore || !this.playerId || !this.selectedSeasonId) return;
            try {
                await this.playerStore.fetchMapStats(this.playerId, this.selectedSeasonId, { force: true });
            } catch (error) {
                console.error('Player map stats failed', error);
            }
        },
        async loadSelectedProgression() {
            if (!this.playerStore || !this.playerId || !this.currentSeasonOption) return;
            try {
                await this.playerStore.fetchProgression(
                    this.playerId,
                    this.currentSeasonOption.value,
                    this.currentSeasonOption.season,
                    this.currentSeasonOption.division,
                    { force: false }
                );
            } catch (error) {
                console.error('Player progression failed', error);
            }
        },
        async loadDivisionAverages() {
            if (!this.selectedSeasonId || !window.apiClient?.getDivisionAverages) return;
            const championshipId = String(this.selectedSeasonId);
            if (this.divisionAveragesByChampionship?.[championshipId]) return;
            try {
                const data = await window.apiClient.getDivisionAverages(championshipId);
                this.divisionAveragesByChampionship = {
                    ...(this.divisionAveragesByChampionship || {}),
                    [championshipId]: data && typeof data === 'object' ? data : {}
                };
            } catch (error) {
                console.error('Division averages failed', error);
                this.divisionAveragesByChampionship = {
                    ...(this.divisionAveragesByChampionship || {}),
                    [championshipId]: {}
                };
            }
        },
        async loadAllProgressions() {
            if (!this.playerStore || !this.playerId) return;
            if (!Array.isArray(this.seasonOptions) || !this.seasonOptions.length) return;
            const tasks = this.seasonOptions.map(option => this.playerStore.fetchProgression(
                this.playerId,
                option.value,
                option.season,
                option.division,
                { force: false }
            ));
            try {
                await Promise.allSettled(tasks);
            } catch (error) {
                console.error('Player all progression failed', error);
            }
        },
        handleCompareOpen() {
            this.compareVisible = true;
            this.compareError = null;
            this.comparePlayer = null;
            this.compareMetrics = [];
        },
        handleCompareClose() {
            this.compareVisible = false;
        },
        async handleCompareSubmit(candidateId) {
            if (!candidateId || !this.playerStore) return;
            this.compareLoading = true;
            this.compareError = null;
            this.comparePlayer = null;
            this.compareMetrics = [];
            try {
                const bundle = await this.playerStore.fetchBundle(candidateId, this.selectedSeasonId || null, { force: true });
                const profile = bundle?.player || null;
                const seasons = Array.isArray(bundle?.seasons) ? bundle.seasons : [];
                const seasonMatch = (seasons || []).find(
                    item => String(item?.championship_id || item?.championshipId || '') === String(this.selectedSeasonId || '')
                ) || seasons?.[0];
                const compareKpis = buildKpis(seasonMatch ? aggregateSeasons([normalizeSeasonRow(seasonMatch)]) : null);
                this.comparePlayer = profile;
                this.compareMetrics = buildCompareMetrics(this.kpiMetrics, compareKpis);
            } catch (error) {
                console.error('Compare player failed', error);
                this.compareError = error?.message || 'Vertailtavaa pelaajaa ei loytynyt';
            } finally {
                this.compareLoading = false;
            }
        },
        toggleTrendMetric(metricKey) {
            const current = new Set(this.trendMetricKeys);
            if (current.has(metricKey)) {
                current.delete(metricKey);
            } else {
                current.add(metricKey);
            }
            this.trendMetricKeys = Array.from(current);
        },
        toggleAllTrendMetrics() {
            if (this.allTrendMetricsSelected) {
                this.trendMetricKeys = [];
                return;
            }
            this.trendMetricKeys = this.trendMetricOptions.map(metric => metric.key);
        },
        formatTrendMetric(metric, value) {
            if (!metric) return String(value);
            if (metric.percent) return formatPercent(value, metric.decimals ?? 1);
            return formatFloat(value, metric.decimals ?? 1);
        },
        trendLegendValue(chart, series) {
            if (!chart || !series) return '';
            if (this.playerTrendHover.key === chart.key && Number.isInteger(this.playerTrendHover.index)) {
                const hoverPoint = series.points?.[this.playerTrendHover.index];
                if (hoverPoint) return this.formatTrendMetric(series, hoverPoint.value);
            }
            if (series.latest) return this.formatTrendMetric(series, series.latest.value);
            return this.formatTrendMetric(series, 0);
        },
        setTrendScope(scope) {
            if (scope !== 'selected' && scope !== 'all') return;
            this.trendScope = scope;
        },
        setTrendMode(mode) {
            if (mode !== 'map' && mode !== 'avg') return;
            this.trendMode = mode;
        },
        handlePlayerTrendHover(event, chart) {
            if (!chart || !chart.points?.length) return;
            const rect = event.currentTarget.getBoundingClientRect();
            const x = event.clientX - rect.left;
            const scale = rect.width ? chart.width / rect.width : 1;
            const xView = x * scale;
            const ratio = Math.min(1, Math.max(0, (xView - chart.padding.left) / chart.plotWidth));
            const index = Math.round(ratio * (chart.points.length - 1));
            const point = chart.points[index];
            if (!point) return;
            this.playerTrendHover = {
                key: chart.key,
                index,
                x: point.x,
                y: point.y
            };
        },
        clearPlayerTrendHover() {
            this.playerTrendHover = { key: null, index: null, x: 0, y: 0 };
        },
        getPlayerTrendHoverPoint(chart) {
            if (!chart || this.playerTrendHover.key !== chart.key) return null;
            const idx = this.playerTrendHover.index;
            const base = chart.points?.[idx] || null;
            if (!base) return null;
            const seriesValues = (chart.series || []).map(series => {
                const point = series.points?.[idx] || null;
                return {
                    key: series.key,
                    label: series.label,
                    value: point?.value ?? 0,
                    delta: point?.delta ?? null,
                    percent: Boolean(series.percent),
                    decimals: series.decimals ?? 1
                };
            });
            return {
                ...base,
                modeValue: seriesValues[0]?.value ?? 0,
                delta: seriesValues[0]?.delta ?? null,
                seriesValues
            };
        },
        trendTooltipStyle(chart, point) {
            if (!chart || !point) return {};
            const left = (point.x / chart.width) * 100;
            const top = (point.y / chart.height) * 100;
            return {
                left: `${left}%`,
                top: `${top}%`
            };
        },
        playerTrendValueLabel(chart) {
            if (!chart) return '';
            if (this.trendMode === 'avg') return `${chart.label} avg`;
            return chart.label;
        },
        playerMapResultLabel(point) {
            if (!point?.mapResult) return '';
            if (point.mapResult === 'win') return 'Voitto';
            if (point.mapResult === 'loss') return 'Tappio';
            if (point.mapResult === 'draw') return 'Tasapeli';
            return '';
        },
        playerTooltipTitle(point) {
            if (!point) return '';
            if (point.teamName && point.opponentTeamName) {
                return `${point.teamName} vs ${point.opponentTeamName}`;
            }
            return point.matchup || 'Ottelu';
        },
        playerTooltipMapName(point) {
            if (!point) return 'Kartta';
            return beautifyMapName(point.mapName || `Map ${point.mapNo || ''}`);
        },
        formatTrendDelta(metric, value) {
            if (!metric) return formatFloat(value, 2);
            const abs = Math.abs(toNumber(value));
            const signed = value > 0 ? '+' : value < 0 ? '-' : '';
            if (metric.percent) return `${signed}${abs.toFixed(metric.decimals ?? 1)} %`;
            return `${signed}${abs.toFixed(metric.decimals ?? 2)}`;
        },
        formatTrendDateTime(value) {
            if (!value) return '';
            const ts = Date.parse(value);
            if (!Number.isFinite(ts)) return '';
            return new Date(ts).toLocaleString('fi-FI', {
                day: '2-digit',
                month: '2-digit',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit',
            });
        },
        updateTrendChartWidth() {
            const panel = this.$refs.playerTrendPanel;
            if (!panel) return;
            const width = Math.max(420, Math.floor(panel.clientWidth || 0) - 24);
            if (width > 0 && Math.abs(width - this.trendChartWidth) > 1) {
                this.trendChartWidth = width;
            }
        },
        setupTrendChartObserver() {
            if (this._playerTrendResizeObserver || typeof ResizeObserver === 'undefined') return;
            const panel = this.$refs.playerTrendPanel;
            if (!panel) return;
            const update = () => window.requestAnimationFrame(() => this.updateTrendChartWidth());
            this._playerTrendResizeObserver = new ResizeObserver(update);
            this._playerTrendResizeObserver.observe(panel);
            update();
        },
        teardownTrendChartObserver() {
            if (!this._playerTrendResizeObserver) return;
            this._playerTrendResizeObserver.disconnect();
            this._playerTrendResizeObserver = null;
        }
    },
    mounted() {
        this.$nextTick(() => {
            this.setupTrendChartObserver();
        });
    },
    beforeUnmount() {
        this.teardownTrendChartObserver();
    },
    template: `
        <div class="player-view">
            <loading-spinner v-if="loading && !profile" message="Pelaajaa ladataan..."></loading-spinner>
            <error-message v-else-if="loadError && !profile" :message="loadError" @retry="bootstrap"></error-message>
            <template v-else>
                <header class="player-hero glass-card">
                    <div class="player-hero__identity">
                        <div class="player-hero__avatar">
                            <img v-if="profile?.avatar" :src="profile.avatar" :alt="profile.nickname" loading="lazy" />
                            <span v-else>{{ (profile?.nickname || '?').charAt(0).toUpperCase() }}</span>
                        </div>
                        <div class="player-hero__meta">
                            <div class="player-hero__player-label">Pelaaja</div>
                            <div class="player-hero__name-row">
                                <h1 class="title-accent titleUnderlinePage">{{ profile?.nickname || 'Pelaaja' }}</h1>
                            </div>
                            <div class="player-hero__actions">
                                <a v-if="profile?.faceit_url" :href="profile.faceit_url" target="_blank" rel="noopener" class="btn-primary">Faceit</a>
                            </div>
                        </div>
                    </div>
                    <div v-if="heroTeam" class="player-hero__team-side">
                        <div class="player-hero__team-meta">
                            <div class="player-hero__team-label">Joukkue</div>
                            <h2 class="player-hero__team-name title-accent titleUnderlinePage">{{ heroTeam }}</h2>
                        </div>
                        <div class="player-hero__team-logo">
                            <img v-if="heroTeamAvatar" :src="heroTeamAvatar" :alt="heroTeam" loading="lazy" />
                            <span v-else>{{ String(heroTeam || '?').charAt(0).toUpperCase() }}</span>
                        </div>
                    </div>
                </header>

                <section class="player-controls">
                    <div class="player-select-row">
                        <label v-if="seasonOptions.length" class="player-select-group">
                            <span>Kausi ja divisioona</span>
                            <select v-model="selectedSeasonId" class="player-select">
                                <option
                                    v-for="season in seasonOptions"
                                    :key="season.value"
                                    :value="season.value"
                                >
                                    {{ season.label }}
                                </option>
                            </select>
                        </label>
                        <button type="button" class="btn-link player-controls__compare" @click="handleCompareOpen">Vertaa pelaajaa</button>
                    </div>
                    <p v-if="!seasonOptions.length" class="player-empty">Ei kausia saatavilla.</p>
                </section>

                <section class="player-kpis">
                    <article class="glass-card player-kpi-card">
                        <h3 class="title-accent titleUnderlineCard">Valitun filtterin KPI:t</h3>
                        <stat-panel :items="kpiMetrics.map(kpi => ({ key: kpi.key, label: kpi.label, value: kpi.display }))" :columns="4"></stat-panel>
                    </article>
                    <article class="glass-card player-kpi-card player-kpi-card--radar">
                        <h3 class="title-accent titleUnderlineCard">Pelityylin profiili</h3>
                        <radar-chart
                            v-if="radarMetrics.length"
                            :metrics="radarMetrics"
                            :comparisons="radarComparisons"
                        ></radar-chart>
                        <p v-else class="player-empty">Ei riittavia mittareita.</p>
                    </article>
                    <article class="glass-card player-kpi-card">
                        <h3 class="title-accent titleUnderlineCard">All-time KPI:t</h3>
                        <stat-panel :items="allTimeKpiMetrics.map(kpi => ({ key: 'all-' + kpi.key, label: kpi.label, value: kpi.display }))" :columns="4"></stat-panel>
                    </article>
                </section>

                <section class="scout-panel scout-performance-trends" ref="playerTrendPanel">
                    <div class="section-heading section-heading--split">
                        <div class="section-heading__main">
                            <h3 class="section-title titleUnderline">Otteluiden kehitys</h3>
                            <span class="section-sub">Valitse näytettävät metriikat</span>
                        </div>
                        <div class="section-heading-actions">
                            <div class="trend-toggles trend-toggles--mode">
                                <button
                                    v-for="scope in trendScopeOptions"
                                    :key="scope.key"
                                    type="button"
                                    class="trend-toggle"
                                    :class="{ 'trend-toggle--active': trendScope === scope.key }"
                                    @click="setTrendScope(scope.key)"
                                    :aria-pressed="trendScope === scope.key ? 'true' : 'false'"
                                >
                                    {{ scope.label }}
                                </button>
                            </div>
                            <div class="trend-toggles trend-toggles--mode">
                                <button
                                    v-for="mode in trendModeOptions"
                                    :key="'mode-' + mode.key"
                                    type="button"
                                    class="trend-toggle"
                                    :class="{ 'trend-toggle--active': trendMode === mode.key }"
                                    @click="setTrendMode(mode.key)"
                                    :aria-pressed="trendMode === mode.key ? 'true' : 'false'"
                                >
                                    {{ mode.label }}
                                </button>
                            </div>
                        </div>
                    </div>
                    <div class="trend-toggles">
                        <button
                            type="button"
                            class="trend-toggle trend-toggle--all"
                            :class="{ 'trend-toggle--active': allTrendMetricsSelected }"
                            @click="toggleAllTrendMetrics"
                        >
                            {{ allTrendMetricsSelected ? 'Poista kaikki' : 'Valitse kaikki' }}
                        </button>
                        <button
                            v-for="metric in trendMetricOptions"
                            :key="metric.key"
                            type="button"
                            class="trend-toggle"
                            :class="{ 'trend-toggle--active': trendMetricKeys.includes(metric.key) }"
                            @click="toggleTrendMetric(metric.key)"
                        >
                            {{ metric.label }}
                        </button>
                    </div>
                    <div class="performance-trends" v-if="trendCharts.length">
                        <div
                            v-for="chart in playerTrendVisibleCharts"
                            :key="chart.key"
                            class="trend-chart"
                            :class="'trend-chart--' + chart.key"
                        >
                            <div class="trend-chart__header">
                                <div class="trend-chart__title">{{ chart.title || chart.label }}</div>
                                <div class="trend-chart__legend" v-if="chart.series && chart.series.length">
                                    <span
                                        v-for="series in chart.series"
                                        :key="'legend-' + chart.key + '-' + series.key"
                                        class="trend-chart__legend-item"
                                    >
                                        <span class="trend-chart__legend-dot" :style="{ backgroundColor: series.color }"></span>
                                        <span class="trend-chart__legend-label">{{ series.label }} {{ trendLegendValue(chart, series) }}</span>
                                    </span>
                                </div>
                            </div>
                            <div
                                class="trend-chart__plot"
                                @mousemove="handlePlayerTrendHover($event, chart)"
                                @mouseleave="clearPlayerTrendHover"
                            >
                                <svg
                                    class="trend-chart__svg"
                                    :viewBox="'0 0 ' + chart.width + ' ' + chart.height"
                                    width="100%"
                                    :height="chart.height"
                                    preserveAspectRatio="none"
                                    role="img"
                                    :aria-label="chart.label + ' trendi'"
                                >
                                    <g class="trend-grid">
                                        <line
                                            v-for="line in chart.gridLines"
                                            :key="'v-' + chart.key + '-' + line.index"
                                            class="trend-grid__line trend-grid__line--vertical"
                                            :x1="line.x"
                                            :x2="line.x"
                                            :y1="chart.padding.top"
                                            :y2="chart.height - chart.padding.bottom"
                                        />
                                        <line
                                            v-for="line in chart.divisionLines"
                                            :key="'d-' + chart.key + '-' + line.index"
                                            class="trend-grid__line trend-grid__line--division"
                                            :x1="line.x"
                                            :x2="line.x"
                                            :y1="chart.padding.top"
                                            :y2="chart.height - chart.padding.bottom"
                                        />
                                        <line
                                            v-for="tick in chart.ticks"
                                            :key="'h-' + chart.key + '-' + tick.y"
                                            class="trend-grid__line"
                                            :x1="chart.padding.left"
                                            :x2="chart.width - chart.padding.right"
                                            :y1="tick.y"
                                            :y2="tick.y"
                                        />
                                    </g>
                                    <line
                                        class="trend-ref-line"
                                        :x1="chart.padding.left"
                                        :x2="chart.width - chart.padding.right"
                                        :y1="chart.refY"
                                        :y2="chart.refY"
                                    />
                                    <template v-for="series in chart.series" :key="'series-' + chart.key + '-' + series.key">
                                        <path
                                            class="trend-line"
                                            :class="chart.lineClass"
                                            :style="{ stroke: series.color }"
                                            :d="series.path"
                                            fill="none"
                                        />
                                        <circle
                                            v-for="(point, idx) in series.points"
                                            :key="point.id + '-' + idx"
                                            class="trend-point"
                                            :class="chart.pointClass"
                                            :cx="point.x"
                                            :cy="point.y"
                                            r="2"
                                            :style="{ fill: series.color }"
                                        >
                                            <title>{{ point.label }} · {{ series.label }} {{ formatTrendMetric(series, point.value) }}</title>
                                        </circle>
                                        <circle
                                            v-if="series.latest"
                                            class="trend-point trend-point--latest"
                                            :class="chart.pointClass"
                                            :cx="series.latest.x"
                                            :cy="series.latest.y"
                                            r="4"
                                            :style="{ fill: series.color }"
                                        />
                                        <circle
                                            v-if="playerTrendHover.key === chart.key && getPlayerTrendHoverPoint(chart) && series.points && series.points[getPlayerTrendHoverPoint(chart).index]"
                                            class="trend-point trend-point--hover"
                                            :class="chart.pointClass"
                                            :cx="series.points[getPlayerTrendHoverPoint(chart).index].x"
                                            :cy="series.points[getPlayerTrendHoverPoint(chart).index].y"
                                            r="4"
                                            :style="{ fill: series.color }"
                                        />
                                    </template>
                                    <g class="trend-axis trend-axis--y">
                                        <text
                                            v-for="tick in chart.ticks"
                                            :key="'ylab-' + chart.key + '-' + tick.y"
                                            class="trend-axis__label"
                                            :x="chart.padding.left - 6"
                                            :y="tick.y + 4"
                                            text-anchor="end"
                                        >{{ formatTrendMetric(chart, tick.value) }}</text>
                                    </g>
                                    <g v-if="chart.showXAxis" class="trend-axis trend-axis--x">
                                        <line
                                            class="trend-axis__baseline"
                                            :x1="chart.padding.left"
                                            :x2="chart.width - chart.padding.right"
                                            :y1="chart.height - chart.padding.bottom"
                                            :y2="chart.height - chart.padding.bottom"
                                        />
                                        <text
                                            v-for="line in chart.divisionLines"
                                            :key="'xdiv-' + chart.key + '-' + line.index"
                                            class="trend-axis__label trend-axis__label--division"
                                            :x="line.x"
                                            :y="chart.height - 18"
                                            text-anchor="middle"
                                        >{{ line.label }}</text>
                                        <text
                                            v-for="label in chart.xLabels"
                                            :key="'x-' + chart.key + '-' + label.index"
                                            class="trend-axis__label trend-axis__label--x"
                                            :x="label.x"
                                            :y="chart.height - 6"
                                            text-anchor="middle"
                                        >{{ label.label }}</text>
                                    </g>
                                </svg>
                                <div
                                    v-if="playerTrendHover.key === chart.key && getPlayerTrendHoverPoint(chart)"
                                    class="trend-tooltip"
                                    :style="trendTooltipStyle(chart, getPlayerTrendHoverPoint(chart))"
                                >
                                    <div class="trend-tooltip__title">{{ playerTooltipTitle(getPlayerTrendHoverPoint(chart)) }}</div>
                                    <div class="trend-tooltip__meta">
                                        {{ getPlayerTrendHoverPoint(chart).seasonLabel || 'Kausi/Divisioona puuttuu' }}
                                    </div>
                                    <div v-if="formatTrendDateTime(getPlayerTrendHoverPoint(chart).matchPlayedAt)" class="trend-tooltip__meta">
                                        Pelattu: {{ formatTrendDateTime(getPlayerTrendHoverPoint(chart).matchPlayedAt) }}
                                    </div>
                                    <div class="trend-tooltip__meta">
                                        {{ playerTooltipMapName(getPlayerTrendHoverPoint(chart)) }}
                                        <span v-if="getPlayerTrendHoverPoint(chart).mapScoreLabel"> · {{ getPlayerTrendHoverPoint(chart).mapScoreLabel }}</span>
                                        <span
                                            v-if="playerMapResultLabel(getPlayerTrendHoverPoint(chart))"
                                            class="trend-tooltip__result"
                                            :class="'trend-tooltip__result--' + getPlayerTrendHoverPoint(chart).mapResult"
                                        >
                                            · {{ playerMapResultLabel(getPlayerTrendHoverPoint(chart)) }}
                                        </span>
                                    </div>
                                    <div
                                        v-for="seriesValue in (getPlayerTrendHoverPoint(chart).seriesValues || [])"
                                        :key="'sv-' + chart.key + '-' + seriesValue.key"
                                        class="trend-tooltip__value"
                                    >
                                        {{ seriesValue.label }} {{ formatTrendMetric(seriesValue, seriesValue.value) }}
                                        <span
                                            v-if="trendMode === 'avg' && seriesValue.delta != null"
                                            class="trend-tooltip__delta"
                                            :class="seriesValue.delta > 0 ? 'trend-delta--positive' : seriesValue.delta < 0 ? 'trend-delta--negative' : 'trend-delta--neutral'"
                                        >
                                            · Muutos {{ formatTrendDelta(seriesValue, seriesValue.delta) }}
                                        </span>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                    <div v-else class="empty-state-container compact">
                        <div class="empty-state-card">
                            <h3 class="empty-state-title">Ei trendidataa</h3>
                            <p class="empty-state-description">Pelaajalle ei ole riittävästi pisteitä trendin piirtämiseen.</p>
                        </div>
                    </div>
                </section>

                <section class="player-totals-grid">
                    <article class="glass-card player-totals-card player-totals-card--compare">
                        <h3 class="title-accent titleUnderlineCard">Totals-vertailu</h3>
                        <div class="player-summary-compare">
                            <div v-for="row in summaryCompareRows" :key="'summary-compare-' + row.key" class="player-summary-compare__tile">
                                <span class="player-summary-compare__label">{{ row.label }}</span>
                                <div class="player-summary-compare__values">
                                    <span class="player-summary-compare__row">
                                        <em>Valinta</em>
                                        <strong>{{ row.selected }}</strong>
                                    </span>
                                    <span class="player-summary-compare__row">
                                        <em>All-time</em>
                                        <strong>{{ row.allTime }}</strong>
                                    </span>
                                </div>
                            </div>
                        </div>
                        <div class="player-totals-details__content player-totals-details__content--always-open">
                            <div class="player-totals-sections-grid">
                                <div class="player-totals-section-card" v-for="section in totalsCompareSections" :key="'compare-' + section.key">
                                    <h4>{{ section.title }}</h4>
                                    <div class="player-totals-compare-table__head">
                                        <span>Tilasto</span>
                                        <span>Valinta</span>
                                        <span>All-time</span>
                                    </div>
                                    <div class="player-totals-rows">
                                        <div class="player-totals-row player-totals-row--compare" v-for="row in section.rows" :key="'compare-' + section.key + '-' + row.label">
                                            <span>{{ row.label }}</span>
                                            <strong>{{ row.selected }}</strong>
                                            <strong>{{ row.allTime }}</strong>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </article>
                </section>

                <section class="player-maps">
                    <maps-stats
                        v-if="selectedSeasonId"
                        title="Karttakohtainen suoritus"
                        :map-stats="mapStats"
                        :loading="mapStatsSegment.loading"
                        :error="mapStatsSegment.error"
                        :columns="null"
                    ></maps-stats>
                </section>

                <player-compare-modal
                    :visible="compareVisible"
                    :base-player="profile"
                    :compare-player="comparePlayer"
                    :metrics="compareMetrics"
                    :loading="compareLoading"
                    :error="compareError"
                    @close="handleCompareClose"
                    @submit="handleCompareSubmit"
                ></player-compare-modal>
            </template>
        </div>
    `
};
