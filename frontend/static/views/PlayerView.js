const PLAYER_KPI_SCHEMA = [
    { key: 'kd', label: 'K/D', decimals: 2, max: 2.2 },
    { key: 'kr', label: 'K/R', decimals: 2, max: 1.2 },
    { key: 'adr', label: 'ADR', decimals: 1, max: 130 },
    { key: 'hs_pct', label: 'HS%', decimals: 1, max: 100, percent: true },
    { key: 'entry_pct', label: 'Entry %', decimals: 1, max: 100, percent: true },
    { key: 'first_kills', label: 'First Kills', decimals: 0, max: 220 },
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
    { key: 'percentage_stats', title: 'Percentages', members: ['entry_pct', 'hs_pct', 'flash_pct'] },
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

function isLikelyChampionshipUuid(value) {
    if (value === null || value === undefined) return false;
    const normalized = String(value).trim();
    if (!normalized) return false;
    return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(normalized);
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

function buildSideBySideSections(baseTotals, compareTotals) {
    const baseSections = buildTotalsSections(baseTotals);
    const compareSections = buildTotalsSections(compareTotals);
    const compareMap = new Map(compareSections.map(section => [section.key, section]));
    return baseSections.map(section => {
        const compareSection = compareMap.get(section.key);
        const compareRowsMap = new Map((compareSection?.rows || []).map(row => [row.label, row.value]));
        return {
            key: section.key,
            title: section.title,
            rows: (section.rows || []).map(row => ({
                label: row.label,
                base: row.value,
                compare: compareRowsMap.get(row.label) ?? '-'
            }))
        };
    });
}

function mapNameFromStat(entry) {
    const curr = entry?.curr || entry || {};
    return (
        entry?.pretty_name || curr?.pretty_name ||
        entry?.map_name || curr?.map_name ||
        entry?.mapName || curr?.mapName ||
        entry?.name || curr?.name ||
        'Kartta'
    );
}

function normalizeMapStatForCompare(entry) {
    const curr = entry?.curr || entry || {};
    const kills = toNumber(curr.kills);
    const deaths = toNumber(curr.deaths);
    const rounds = toNumber(curr.rounds_played ?? curr.rounds);
    const headshots = toNumber(curr.headshots);
    const flashSuccesses = toNumber(curr.flash_successes);
    const flashCount = toNumber(curr.flash_count);
    const utilitySuccesses = toNumber(curr.utility_successes);
    const utilityCount = toNumber(curr.utility_count);
    const hsPct = kills > 0 ? (headshots / kills) * 100 : 0;
    const mapName = mapNameFromStat(entry);
    return {
        map_name: mapName,
        map_key: canonicalMapKey(mapName),
        raw: entry,
        maps_played: toNumber(curr.maps_played),
        rounds_played: rounds,
        kills,
        deaths,
        assists: toNumber(curr.assists),
        kd: Number.isFinite(Number(curr.kd)) ? toNumber(curr.kd) : safeDivide(kills, Math.max(1, deaths)),
        kr: Number.isFinite(Number(curr.kr)) ? toNumber(curr.kr) : safeDivide(kills, Math.max(1, rounds)),
        adr: toNumber(curr.adr),
        hs_pct: Number.isFinite(Number(curr.hs_pct)) ? toNumber(curr.hs_pct) : hsPct,
        flash_success_pct: safeDivide(flashSuccesses, Math.max(1, flashCount)) * 100,
        utility_success_pct: safeDivide(utilitySuccesses, Math.max(1, utilityCount)) * 100
    };
}

function buildMapCompareRows(baseRows, compareRows) {
    const baseMap = new Map((baseRows || []).map(row => [row.map_key, row]));
    const compareMap = new Map((compareRows || []).map(row => [row.map_key, row]));
    const keys = new Set([...baseMap.keys(), ...compareMap.keys()]);
    const rows = [];
    keys.forEach(key => {
        const base = baseMap.get(key) || null;
        const compare = compareMap.get(key) || null;
        const rawName = base?.map_name || compare?.map_name || 'Kartta';
        rows.push({
            key,
            map_name: beautifyMapName(rawName),
            base,
            compare,
            maps_played_base: toNumber(base?.maps_played),
            maps_played_compare: toNumber(compare?.maps_played),
            kd_base: toNumber(base?.kd),
            kd_compare: toNumber(compare?.kd),
            kr_base: toNumber(base?.kr),
            kr_compare: toNumber(compare?.kr),
            adr_base: toNumber(base?.adr),
            adr_compare: toNumber(compare?.adr),
            hs_pct_base: toNumber(base?.hs_pct),
            hs_pct_compare: toNumber(compare?.hs_pct)
        });
    });
    return rows.sort((a, b) => {
        const mapsCmp = (b.maps_played_base + b.maps_played_compare) - (a.maps_played_base + a.maps_played_compare);
        if (mapsCmp !== 0) return mapsCmp;
        return String(a.map_name || '').localeCompare(String(b.map_name || ''), 'fi');
    });
}

function trendMetricValue(row, metricKey) {
    if (!row) return 0;
    if (metricKey === 'entry_pct') return safeDivide(toNumber(row.entry_wins), Math.max(1, toNumber(row.entry_count))) * 100;
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

function canonicalMapKey(...values) {
    // Normalize map identifier by stripping prefixes and special chars
    // so "de_ancient", "ancient", "ds_ancient", "cs2_ancient" all match
    const combined = values.filter(Boolean).join(' ').trim();
    if (!combined) return '';
    let normalized = combined.toLowerCase();
    // Strip known CS map prefixes
    normalized = normalized.replace(/^(de_|ds_|cs2_|map_)/i, '');
    // Remove special characters and extra spaces
    normalized = normalized.replace(/[^a-z0-9]/g, '');
    return normalized;
}

function beautifyMapName(raw) {
    if (!raw) return 'Kartta';
    const value = String(raw).trim();
    const lower = value.toLowerCase();
    if (lower === 'forfeit') return 'Forfeit';
    // Handle ds_ prefix (danger zone maps)
    let core = lower;
    if (lower.startsWith('de_')) {
        core = lower.slice(3);
    } else if (lower.startsWith('ds_')) {
        core = lower.slice(3);
    } else if (lower.startsWith('cs2_')) {
        core = lower.slice(4);
    } else if (lower.startsWith('map_')) {
        core = lower.slice(4);
    }
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
            inFlightLoads: {
                bootstrap: {},
                mapStats: {},
                progression: {},
                divisionAverages: {},
                allProgressions: {}
            },
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
            compareScope: 'selected',
            comparePlayer: null,
            compareSeasonsRaw: [],
            compareSelectedSeasonRaw: null,
            compareMapStatsRaw: [],
            compareMapImageLookup: {},
            compareMapCatalogLoaded: false,
            compareMapCatalogLoading: false,
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
        compareSelectedTotals() {
            if (!this.compareSelectedSeasonRaw) return null;
            return aggregateSeasons([this.compareSelectedSeasonRaw]);
        },
        compareAllTimeTotals() {
            if (!Array.isArray(this.compareSeasonsRaw) || !this.compareSeasonsRaw.length) return null;
            return aggregateSeasons(this.compareSeasonsRaw);
        },
        compareBaseTotals() {
            return this.compareScope === 'all' ? this.allTimeTotals : this.selectedDivisionTotals;
        },
        compareOpponentTotals() {
            if (this.compareScope === 'all') return this.compareAllTimeTotals;
            return this.compareSelectedTotals;
        },
        compareBaseKpis() {
            return buildKpis(this.compareBaseTotals);
        },
        compareOpponentKpis() {
            return buildKpis(this.compareOpponentTotals);
        },
        comparePanelRows() {
            return buildCompareMetrics(this.compareBaseKpis, this.compareOpponentKpis);
        },
        compareRadarMetrics() {
            return buildRadarMetrics(this.compareBaseKpis);
        },
        compareRadarComparisons() {
            if (!this.comparePlayer || !this.compareOpponentKpis.length) return [];
            const values = this.compareOpponentKpis.reduce((acc, metric) => {
                acc[metric.key] = metric.value;
                return acc;
            }, {});
            return [{
                key: 'compare_player',
                label: this.comparePlayer?.nickname || 'Vertailupelaaja',
                color: '#22d3ee',
                values
            }];
        },
        compareSelectedTotalsSections() {
            if (!this.compareSelectedTotals) return [];
            return buildSideBySideSections(this.selectedDivisionTotals, this.compareSelectedTotals);
        },
        compareAllTimeTotalsSections() {
            if (!this.compareAllTimeTotals) return [];
            return buildSideBySideSections(this.allTimeTotals, this.compareAllTimeTotals);
        },
        compareMapRows() {
            const baseRows = (this.mapStats || []).map(normalizeMapStatForCompare);
            const compareRows = (this.compareMapStatsRaw || []).map(normalizeMapStatForCompare);
            return buildMapCompareRows(baseRows, compareRows).map(row => {
                const sourceEntry = row.base?.raw || row.compare?.raw || null;
                const logo = this.resolveCompareMapImage(sourceEntry);
                return {
                    ...row,
                    logo
                };
            });
        },
        activeCompareTotalsTitle() {
            return this.compareScope === 'all'
                ? 'All-time · kaikki statsit'
                : 'Valittu kausi · kaikki statsit';
        },
        activeCompareTotalsSections() {
            return this.compareScope === 'all'
                ? this.compareAllTimeTotalsSections
                : this.compareSelectedTotalsSections;
        },
        activeCompareTotalsEmptyMessage() {
            if (this.compareScope === 'all') {
                return 'Vertailupelaajalla ei all-time dataa.';
            }
            return 'Vertailupelaajalla ei dataa valitulle kaudelle/divisioonalle.';
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
            const aggregates = averages.aggregates && typeof averages.aggregates === 'object'
                ? averages.aggregates
                : null;
            const medianKeyByMetric = {
                kd: 'median_kd',
                adr: 'median_adr',
                kr: 'median_kr',
                hs_pct: 'median_hs_pct',
                entry_pct: 'median_entry_pct',
                first_kills: 'median_first_kills',
                flash_success_pct: 'median_flash_success_pct',
                utility_success_pct: 'median_utility_success_pct'
            };

            return [
                {
                    key: 'division_median',
                    label: 'Division median',
                    color: '#f59e0b',
                    values: metricKeys.reduce((acc, key) => {
                        const medianKey = medianKeyByMetric[key] || `median_${key}`;
                        const direct = averages[medianKey];
                        const nested = aggregates ? aggregates[medianKey] : null;
                        acc[key] = toNumber(direct ?? nested, 0);
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
        heroTeamRoute() {
            const teamId = this.selectedSeasonStats?.team_id || this.selectedSeasonStats?.teamId || null;
            if (!teamId) return null;
            const championshipId = this.selectedSeasonId || this.selectedSeasonStats?.championship_id || this.selectedSeasonStats?.championshipId || null;
            if (championshipId) {
                return {
                    name: 'team-detail',
                    params: { championshipId: String(championshipId), teamId: String(teamId) }
                };
            }
            return {
                name: 'team',
                params: { teamId: String(teamId) }
            };
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
            const preferred = this.resolvePreferredSeasonId(newOptions);
            if (preferred && String(preferred) !== String(this.selectedSeasonId || '')) {
                this.selectedSeasonId = preferred;
            }
            if (this.trendScope === 'all') {
                this.loadAllProgressions();
            }
        },
        '$route.params.championshipId'(newVal) {
            if (!Array.isArray(this.seasonOptions) || !this.seasonOptions.length) return;
            const requested = newVal == null ? null : String(newVal).trim();
            if (!requested) return;
            if (!this.seasonOptions.some(option => String(option.value) === requested)) return;
            if (String(this.selectedSeasonId || '') === requested) return;
            this.selectedSeasonId = requested;
        },
        selectedSeasonId(newVal, oldVal) {
            if (newVal && newVal !== oldVal) {
                this.loadMapStats();
                this.loadSelectedProgression();
                this.loadDivisionAverages();
                this.syncRouteBreadcrumbContext();
                this.comparePlayer = null;
                this.compareSeasonsRaw = [];
                this.compareSelectedSeasonRaw = null;
                this.compareMapStatsRaw = [];
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
        },
        mapStats: {
            immediate: true,
            deep: true,
            handler(newStats) {
                const rows = Array.isArray(newStats) ? newStats : [];
                this.compareMapImageLookup = this.buildCompareMapImageLookup(rows, this.compareMapImageLookup);
                if (this.shouldFetchCompareMapCatalog(rows)) {
                    this.ensureCompareMapCatalog();
                }
            }
        },
        compareMapStatsRaw: {
            immediate: true,
            deep: true,
            handler(newStats) {
                const rows = Array.isArray(newStats) ? newStats : [];
                this.compareMapImageLookup = this.buildCompareMapImageLookup(rows, this.compareMapImageLookup);
                if (this.shouldFetchCompareMapCatalog(rows)) {
                    this.ensureCompareMapCatalog();
                }
            }
        }
    },
    methods: {
        getDefaultLogo() {
            return window.PAPPALIIGA_DEFAULT_LOGO || 'https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png';
        },
        proxyAvatar(url) {
            const fallback = this.getDefaultLogo();
            const src = url || fallback;
            try {
                if (window.apiClient && typeof window.apiClient.proxyAvatar === 'function') {
                    return window.apiClient.proxyAvatar(src) || fallback;
                }
                return src;
            } catch (error) {
                return src;
            }
        },
        profileAvatarSrc() {
            return this.proxyAvatar(this.profile?.avatar);
        },
        compareAvatarSrc() {
            return this.proxyAvatar(this.comparePlayer?.avatar);
        },
        heroTeamAvatarSrc() {
            return this.proxyAvatar(this.heroTeamAvatar);
        },
        handleAvatarFallback(event) {
            const fallback = this.getDefaultLogo();
            if (!event?.target || !fallback) return;
            if (event.target.src !== fallback) {
                event.target.src = fallback;
            }
        },
        mapKey(name) {
            return window.MapImageUtils ? window.MapImageUtils.mapKey(name) : null;
        },
        buildCompareMapImageLookup(stats, existing = {}) {
            return window.MapImageUtils
                ? window.MapImageUtils.buildMapImageLookup(stats, existing)
                : { ...(existing || {}) };
        },
        shouldFetchCompareMapCatalog(stats) {
            if (this.compareMapCatalogLoaded || this.compareMapCatalogLoading) return false;
            return window.MapImageUtils ? window.MapImageUtils.shouldFetchCatalog(stats) : false;
        },
        async ensureCompareMapCatalog() {
            if (this.compareMapCatalogLoaded || this.compareMapCatalogLoading) return;
            if (!window.apiClient || typeof window.apiClient.getMapsCatalog !== 'function') return;
            this.compareMapCatalogLoading = true;
            try {
                const catalog = await window.apiClient.getMapsCatalog();
                if (Array.isArray(catalog) && catalog.length) {
                    const lookup = { ...(this.compareMapImageLookup || {}) };
                    catalog.forEach(item => {
                        const key = this.mapKey(item?.map_id || item?.pretty_name || item?.map_name || item?.name);
                        const img = item?.image_sm || item?.image_lg || item?.image;
                        if (key && img && !lookup[key]) {
                            lookup[key] = img;
                        }
                    });
                    this.compareMapImageLookup = lookup;
                }
                this.compareMapCatalogLoaded = true;
            } catch (error) {
                console.warn('Compare map catalog fetch failed', error);
                this.compareMapCatalogLoaded = true;
            } finally {
                this.compareMapCatalogLoading = false;
            }
        },
        resolveCompareMapImage(entry) {
            if (!entry || !window.MapImageUtils) return null;
            return window.MapImageUtils.resolveMapImage(entry, {
                mapImageLookup: this.compareMapImageLookup,
                apiClient: window.apiClient
            });
        },
        runInFlightLoad(group, key, taskFactory) {
            if (!group || !key || typeof taskFactory !== 'function') {
                return Promise.resolve(null);
            }
            const bucket = this.inFlightLoads?.[group];
            if (!bucket) {
                return Promise.resolve(taskFactory());
            }
            if (bucket[key]) {
                return bucket[key];
            }

            const task = (async () => {
                try {
                    return await taskFactory();
                } finally {
                    if (this.inFlightLoads?.[group]?.[key] === task) {
                        delete this.inFlightLoads[group][key];
                    }
                }
            })();

            this.inFlightLoads[group][key] = task;
            return task;
        },
        requestedChampionshipId() {
            const raw = this.$route?.params?.championshipId;
            if (raw === null || raw === undefined) return null;
            const value = String(raw).trim();
            if (!value) return null;
            return isLikelyChampionshipUuid(value) ? value : null;
        },
        resolvePreferredSeasonId(options = []) {
            if (!Array.isArray(options) || !options.length) return null;
            const requested = this.requestedChampionshipId();
            if (requested && options.some(option => String(option.value) === requested)) {
                return requested;
            }
            if (this.selectedSeasonId && options.some(option => String(option.value) === String(this.selectedSeasonId))) {
                return this.selectedSeasonId;
            }
            return options[0].value;
        },
        async bootstrap() {
            if (!this.playerStore || !this.playerId) return;
            const requestedChampionshipId = this.requestedChampionshipId() || 'auto';
            const key = `${String(this.playerId)}::${String(requestedChampionshipId)}`;
            return this.runInFlightLoad('bootstrap', key, async () => {
                this.compareVisible = false;
                this.compareMetrics = [];
                this.comparePlayer = null;
                this.compareSeasonsRaw = [];
                this.compareSelectedSeasonRaw = null;
                this.compareMapStatsRaw = [];
                try {
                    await this.playerStore.fetchBundle(this.playerId, null, { force: true });
                    const defaults = this.seasonOptions;
                    const preferred = this.resolvePreferredSeasonId(defaults);
                    if (preferred && String(preferred) !== String(this.selectedSeasonId || '')) {
                        this.selectedSeasonId = preferred;
                    }
                    if (this.selectedSeasonId) {
                        await this.playerStore.fetchBundle(this.playerId, this.selectedSeasonId, { force: true });
                    }
                    this.syncRouteBreadcrumbContext();
                } catch (error) {
                    console.error('Player bootstrap failed', error);
                }
            });
        },
        syncRouteBreadcrumbContext() {
            if (!this.$router || !this.$route || !this.playerId) return;
            if (!this.selectedSeasonId) return;
            const nextName = 'player-detail';
            const nextParams = {
                championshipId: String(this.selectedSeasonId),
                playerId: this.playerId
            };
            const nextQuery = {};

            const normalizeQuery = obj => Object.keys(obj)
                .sort()
                .map(key => `${key}:${String(obj[key])}`)
                .join('|');
            const sameRoute =
                String(this.$route?.name || '') === nextName
                && String(this.$route?.params?.playerId || '') === String(nextParams.playerId)
                && String(this.$route?.params?.championshipId || '') === String(nextParams.championshipId)
                && normalizeQuery(this.$route.query || {}) === normalizeQuery(nextQuery);
            if (sameRoute) return;

            this.$router.replace({
                name: nextName,
                params: nextParams,
                query: nextQuery
            }).catch(() => {});
        },
        async loadMapStats() {
            if (!this.playerStore || !this.playerId || !this.selectedSeasonId) return;
            const key = `${this.playerId}::${this.selectedSeasonId}`;
            return this.runInFlightLoad('mapStats', key, async () => {
                try {
                    await this.playerStore.fetchMapStats(this.playerId, this.selectedSeasonId, { force: true });
                } catch (error) {
                    console.error('Player map stats failed', error);
                }
            });
        },
        async loadSelectedProgression() {
            if (!this.playerStore || !this.playerId || !this.currentSeasonOption) return;
            const key = `${this.playerId}::${this.currentSeasonOption.value}::${this.currentSeasonOption.season}::${this.currentSeasonOption.division}`;
            return this.runInFlightLoad('progression', key, async () => {
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
            });
        },
        async loadDivisionAverages() {
            if (!this.selectedSeasonId || !window.apiClient?.getDivisionAverages) return;
            const championshipId = String(this.selectedSeasonId);
            if (this.divisionAveragesByChampionship?.[championshipId]) return;
            return this.runInFlightLoad('divisionAverages', championshipId, async () => {
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
            });
        },
        async loadAllProgressions() {
            if (!this.playerStore || !this.playerId) return;
            if (!Array.isArray(this.seasonOptions) || !this.seasonOptions.length) return;
            const optionsKey = this.seasonOptions.map(option => String(option.value)).join(',');
            const key = `${this.playerId}::${optionsKey}`;
            return this.runInFlightLoad('allProgressions', key, async () => {
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
            });
        },
        handleCompareOpen() {
            this.compareVisible = true;
            this.compareError = null;
            if (!this.comparePlayer) {
                this.compareMetrics = [];
            }
        },
        handleCompareClose() {
            this.compareVisible = false;
        },
        clearCompare() {
            this.compareVisible = false;
            this.compareError = null;
            this.comparePlayer = null;
            this.compareSeasonsRaw = [];
            this.compareSelectedSeasonRaw = null;
            this.compareMapStatsRaw = [];
            this.compareMetrics = [];
        },
        async handleCompareSubmit(candidateId) {
            if (!candidateId || !this.playerStore) return;
            this.compareLoading = true;
            this.compareError = null;
            this.comparePlayer = null;
            this.compareMetrics = [];
            try {
                const bundle = await this.playerStore.fetchBundle(candidateId, null, { force: true });
                const profile = bundle?.player || null;
                const seasons = Array.isArray(bundle?.seasons) ? bundle.seasons : [];
                const seasonMatch = (seasons || []).find(
                    item => String(item?.championship_id || item?.championshipId || '') === String(this.selectedSeasonId || '')
                );
                const normalizedSeasons = (seasons || []).map(normalizeSeasonRow);
                const selectedSeasonNum = toNumber(this.currentSeasonOption?.season ?? this.selectedSeasonStats?.season, null);
                const selectedPhaseFlag = Boolean(this.currentSeasonOption?.isPlayoffs ?? this.selectedSeasonStats?.is_playoffs ?? false);
                const seasonRowsSameSeason = Number.isFinite(selectedSeasonNum)
                    ? normalizedSeasons.filter(item => toNumber(item?.season, null) === selectedSeasonNum)
                    : [];
                const seasonRowsSamePhase = seasonRowsSameSeason.filter(item => Boolean(item?.is_playoffs) === selectedPhaseFlag);
                const exactSeasonRow = seasonMatch ? normalizeSeasonRow(seasonMatch) : null;
                const selectedScopeRows = exactSeasonRow
                    ? [exactSeasonRow]
                    : (seasonRowsSamePhase.length ? seasonRowsSamePhase : seasonRowsSameSeason);
                const selectedScope = this.compareScope === 'selected';
                const baseStats = selectedScope
                    ? (this.selectedSeasonStats ? aggregateSeasons([normalizeSeasonRow(this.selectedSeasonStats)]) : null)
                    : this.allTimeTotals;
                const compareStats = selectedScope
                    ? (selectedScopeRows.length ? aggregateSeasons(selectedScopeRows) : null)
                    : aggregateSeasons(normalizedSeasons);
                if (selectedScope && !compareStats) {
                    throw new Error('Pelaajalla ei dataa valitulle kaudelle');
                }
                const baseKpis = buildKpis(baseStats);
                const compareKpis = buildKpis(compareStats);
                this.comparePlayer = profile;
                this.compareSeasonsRaw = normalizedSeasons;
                this.compareSelectedSeasonRaw = selectedScopeRows.length ? aggregateSeasons(selectedScopeRows) : null;

                let compareMapStats = Array.isArray(bundle?.map_stats) ? bundle.map_stats : [];
                const fallbackChampionshipId = (
                    selectedScopeRows[0]?.championship_id
                    || selectedScopeRows[0]?.championshipId
                    || null
                );
                if (selectedScope && !seasonMatch && fallbackChampionshipId) {
                    try {
                        const seasonBundle = await this.playerStore.fetchBundle(candidateId, String(fallbackChampionshipId), { force: true });
                        compareMapStats = Array.isArray(seasonBundle?.map_stats) ? seasonBundle.map_stats : compareMapStats;
                    } catch (_) {
                        // Keep existing map stats if season-specific fetch fails.
                    }
                }
                this.compareMapStatsRaw = compareMapStats;
                this.compareMetrics = buildCompareMetrics(baseKpis, compareKpis);
                this.compareMapImageLookup = this.buildCompareMapImageLookup(
                    [...(this.mapStats || []), ...(this.compareMapStatsRaw || [])],
                    this.compareMapImageLookup
                );
                if (this.shouldFetchCompareMapCatalog([...(this.mapStats || []), ...(this.compareMapStatsRaw || [])])) {
                    this.ensureCompareMapCatalog();
                }
                this.compareVisible = false;
            } catch (error) {
                console.error('Compare player failed', error);
                let message = error?.message || 'Vertailtavaa pelaajaa ei loytynyt';
                if (typeof message === 'string' && message.trim().startsWith('{')) {
                    try {
                        const parsed = JSON.parse(message);
                        if (parsed?.detail) {
                            message = String(parsed.detail);
                        }
                    } catch (_) {
                        // Keep original message when parsing fails.
                    }
                }
                this.compareError = message;
            } finally {
                this.compareLoading = false;
            }
        },
        handleCompareScopeChange(scope) {
            if (scope !== 'selected' && scope !== 'all') return;
            if (this.compareScope === scope) return;
            this.compareScope = scope;
            if (this.comparePlayer?.player_id || this.comparePlayer?.playerId) {
                const currentId = this.comparePlayer?.player_id || this.comparePlayer?.playerId;
                this.handleCompareSubmit(String(currentId));
            }
        },
        formatCompareMapValue(value, decimals = 1, percent = false) {
            const numeric = toNumber(value);
            if (percent) return `${numeric.toFixed(decimals)} %`;
            return numeric.toFixed(decimals);
        },
        mapCompareDeltaClass(base, compare) {
            const b = toNumber(base);
            const c = toNumber(compare);
            if (c > b) return 'is-pos';
            if (c < b) return 'is-neg';
            return 'is-neutral';
        },
        mapComparePair(base, compare, decimals = 1, percent = false) {
            const b = this.formatCompareMapValue(base, decimals, percent);
            const c = this.formatCompareMapValue(compare, decimals, percent);
            return `${b} / ${c}`;
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
                            <img :src="profileAvatarSrc()" :alt="profile?.nickname || 'Pelaaja'" loading="lazy" @error="handleAvatarFallback" />
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
                            <h2 class="player-hero__team-name title-accent titleUnderlinePage">
                                <router-link
                                    v-if="heroTeamRoute"
                                    class="player-hero__team-link"
                                    :to="heroTeamRoute"
                                >{{ heroTeam }}</router-link>
                                <span v-else>{{ heroTeam }}</span>
                            </h2>
                        </div>
                        <div class="player-hero__team-logo">
                            <img :src="heroTeamAvatarSrc()" :alt="heroTeam" loading="lazy" @error="handleAvatarFallback" />
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
                        <button type="button" class="btn-primary" @click="handleCompareOpen">
                            {{ comparePlayer ? 'Vaihda vertailupelaaja' : 'Vertaa pelaajaa' }}
                        </button>
                        <button
                            v-if="comparePlayer"
                            type="button"
                            class="btn-secondary"
                            @click="clearCompare"
                        >
                            Tyhjennä vertailu
                        </button>
                    </div>
                    <p v-if="!seasonOptions.length" class="player-empty">Ei kausia saatavilla.</p>
                </section>

                <section v-if="comparePlayer" class="player-compare-workspace glass-card">
                    <header class="player-compare-workspace__header">
                        <div class="player-compare-workspace__identity">
                            <img :src="compareAvatarSrc()" :alt="comparePlayer?.nickname || 'Vertailupelaaja'" loading="lazy" @error="handleAvatarFallback" />
                            <div>
                                <p class="player-compare-workspace__eyebrow">Vertailupelaaja</p>
                                <h3 class="title-accent titleUnderlineCard">{{ comparePlayer?.nickname || 'Vertailupelaaja' }}</h3>
                            </div>
                        </div>
                        <div class="player-compare-workspace__scope" role="group" aria-label="Vertailun aikajakso">
                            <button
                                type="button"
                                class="player-compare-workspace__scope-toggle"
                                :class="{ 'player-compare-workspace__scope-toggle--active': compareScope === 'selected' }"
                                :aria-pressed="compareScope === 'selected' ? 'true' : 'false'"
                                @click="handleCompareScopeChange('selected')"
                            >Valittu kausi</button>
                            <button
                                type="button"
                                class="player-compare-workspace__scope-toggle"
                                :class="{ 'player-compare-workspace__scope-toggle--active': compareScope === 'all' }"
                                :aria-pressed="compareScope === 'all' ? 'true' : 'false'"
                                @click="handleCompareScopeChange('all')"
                            >All-time</button>
                        </div>
                    </header>

                    <div class="player-compare-kpi-grid">
                        <article class="player-compare-kpi-card">
                            <h4 class="title-accent titleUnderlineCard">KPI-vertailu</h4>
                            <div class="player-compare-kpi-table">
                                <div class="player-compare-kpi-row player-compare-kpi-row--head">
                                    <span>Mittari</span>
                                    <span>{{ profile?.nickname || 'Pelaaja' }}</span>
                                    <span>{{ comparePlayer?.nickname || 'Vertailu' }}</span>
                                    <span>Erotus</span>
                                </div>
                                <div v-for="metric in comparePanelRows" :key="'panel-' + metric.key" class="player-compare-kpi-row">
                                    <span>{{ metric.label }}</span>
                                    <strong>{{ metric.format(metric.base) }}</strong>
                                    <strong>{{ metric.compare == null ? '–' : metric.format(metric.compare) }}</strong>
                                    <strong
                                        :class="metric.compare == null ? 'is-neutral' : (metric.compare > metric.base ? 'is-pos' : metric.compare < metric.base ? 'is-neg' : 'is-neutral')"
                                    >
                                        {{ metric.compare == null ? '–' : (metric.compare > metric.base ? '+' : metric.compare < metric.base ? '-' : '±') + metric.format(Math.abs(metric.compare - metric.base)) }}
                                    </strong>
                                </div>
                            </div>
                        </article>
                        <article class="player-compare-kpi-card player-compare-kpi-card--radar">
                            <h4 class="title-accent titleUnderlineCard">Pelityylin profiili</h4>
                            <radar-chart
                                v-if="compareRadarMetrics.length"
                                :metrics="compareRadarMetrics"
                                :comparisons="compareRadarComparisons"
                            ></radar-chart>
                            <p v-else class="player-empty">Ei riittavia mittareita.</p>
                        </article>
                    </div>

                    <div class="player-compare-totals-grid">
                        <article class="player-compare-totals-card">
                            <h4 class="player-compare-totals-title title-accent titleUnderlineCard">{{ activeCompareTotalsTitle }}</h4>
                            <p v-if="!activeCompareTotalsSections.length" class="player-empty">{{ activeCompareTotalsEmptyMessage }}</p>
                            <div v-else class="player-totals-sections-grid">
                                <div class="player-totals-section-card" v-for="section in activeCompareTotalsSections" :key="'cmp-active-' + section.key">
                                    <h4>{{ section.title }}</h4>
                                    <div class="player-totals-compare-table__head">
                                        <span>Tilasto</span>
                                        <span>{{ profile?.nickname || 'Pelaaja' }}</span>
                                        <span>{{ comparePlayer?.nickname || 'Vertailu' }}</span>
                                    </div>
                                    <div class="player-totals-rows">
                                        <div class="player-totals-row player-totals-row--compare" v-for="row in section.rows" :key="'cmp-active-' + section.key + '-' + row.label">
                                            <span>{{ row.label }}</span>
                                            <strong>{{ row.base }}</strong>
                                            <strong>{{ row.compare }}</strong>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </article>

                        <article class="player-compare-totals-card player-compare-maps-card">
                            <h4 class="player-compare-totals-title title-accent titleUnderlineCard">Karttakohtainen vertailu</h4>
                            <p v-if="!compareMapRows.length" class="player-empty">Karttakohtaista vertailudataa ei saatavilla valitulle kaudelle.</p>
                            <div v-else class="player-compare-maps-table">
                                <div class="player-compare-maps-row player-compare-maps-row--head">
                                    <span class="align-left">Kartta</span>
                                    <span class="align-right">Maps</span>
                                    <span class="align-right">K/D</span>
                                    <span class="align-right">ADR</span>
                                    <span class="align-right">K/R</span>
                                    <span class="align-right">HS%</span>
                                </div>
                                <div v-for="row in compareMapRows" :key="'cmp-map-' + row.key" class="player-compare-maps-row player-compare-maps-row--data">
                                    <div class="player-compare-maps-name">
                                        <img v-if="row.logo" :src="row.logo" :alt="row.map_name" loading="lazy" />
                                        <span>{{ row.map_name }}</span>
                                    </div>
                                    <div class="player-compare-maps-stat" :class="mapCompareDeltaClass(row.maps_played_base, row.maps_played_compare)">
                                        {{ mapComparePair(row.maps_played_base, row.maps_played_compare, 0, false) }}
                                    </div>
                                    <div class="player-compare-maps-stat" :class="mapCompareDeltaClass(row.kd_base, row.kd_compare)">
                                        {{ mapComparePair(row.kd_base, row.kd_compare, 2, false) }}
                                    </div>
                                    <div class="player-compare-maps-stat" :class="mapCompareDeltaClass(row.adr_base, row.adr_compare)">
                                        {{ mapComparePair(row.adr_base, row.adr_compare, 1, false) }}
                                    </div>
                                    <div class="player-compare-maps-stat" :class="mapCompareDeltaClass(row.kr_base, row.kr_compare)">
                                        {{ mapComparePair(row.kr_base, row.kr_compare, 2, false) }}
                                    </div>
                                    <div class="player-compare-maps-stat" :class="mapCompareDeltaClass(row.hs_pct_base, row.hs_pct_compare)">
                                        {{ mapComparePair(row.hs_pct_base, row.hs_pct_compare, 1, true) }}
                                    </div>
                                </div>
                            </div>
                        </article>
                    </div>
                </section>

                <section class="player-kpis">
                    <article class="glass-card player-kpi-card">
                        <h3 class="title-accent titleUnderlineCard">Valitun divarin statsi</h3>
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
                        <h3 class="title-accent titleUnderlineCard">Kaikkie kausien statsi</h3>
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
                    :base-player-id="playerId"
                    :season="currentSeasonOption?.season || null"
                    :division="currentSeasonOption?.division || null"
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
