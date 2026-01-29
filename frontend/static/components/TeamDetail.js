// Team detail view that renders stats, maps, matches, players and veto aggregates.
// Every DB-backed field is surfaced as a stat, column, chart point or tooltip.

const PLAYER_COLUMNS = [
    { key: 'nickname', label: 'Pelaaja', sortable: true, colClass: 'col-name' },
    { key: 'mapsPlayed', label: 'Kartat', sortable: true, numeric: true },
    { key: 'roundsPlayed', label: 'R', sortable: true, numeric: true },
    { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2, colClass: 'col-kd' },
    { key: 'kr', label: 'K/R', sortable: true, numeric: true, decimals: 2 },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1, colClass: 'col-adr' },
    { key: 'hsPct', label: 'HS%', sortable: true, numeric: true, decimals: 1 },
    { key: 'kills', label: 'Kills', sortable: true, numeric: true },
    { key: 'deaths', label: 'Deaths', sortable: true, numeric: true },
    { key: 'assists', label: 'A', sortable: true, numeric: true },
    { key: 'damage', label: 'Dmg', sortable: true, numeric: true },
    { key: 'utilityDamage', label: 'U-Dmg', sortable: true, numeric: true },
    { key: 'mvps', label: 'MVP', sortable: true, numeric: true },
    { key: 'sniperKills', label: 'AWP', sortable: true, numeric: true },
    { key: 'pistolKills', label: 'Pistol', sortable: true, numeric: true },
    { key: 'entryLine', label: 'Entry', sortable: true, numeric: true },
    { key: 'clutch1v1Line', label: '1v1', sortable: true, numeric: true },
    { key: 'clutch1v2Line', label: '1v2', sortable: true, numeric: true },
    { key: 'clutchKills', label: 'Clutch K', sortable: true, numeric: true },
    { key: 'mk2k', label: '2K', sortable: true, numeric: true },
    { key: 'mk3k', label: '3K', sortable: true, numeric: true },
    { key: 'mk4k', label: '4K', sortable: true, numeric: true },
    { key: 'mk5k', label: 'Ace', sortable: true, numeric: true },
    { key: 'enemiesFlashed', label: 'Flashed', sortable: true, numeric: true },
    { key: 'flashSuccessLine', label: 'Flash%', sortable: true, numeric: true }
];

const MAP_GROUP_META = {
    map: { label: 'Kartta', className: 'group-map group-divider' },
    rounds: { label: 'Erät', className: 'group-rounds group-divider' },
    combat: { label: 'Taistelu', className: 'group-combat group-divider' },
    kills: { label: 'Tappiot/Assist', className: 'group-kills group-divider' },
    utility: { label: 'Utility', className: 'group-utility group-divider' },
    awards: { label: 'MVP', className: 'group-awards group-divider' },
    flash: { label: 'Flashbangit', className: 'group-flash group-divider' },
    multikill: { label: 'Multi-kills', className: 'group-multikill group-divider' },
    weapons: { label: 'Aseet', className: 'group-weapons group-divider' },
    damage: { label: 'Vahinko', className: 'group-damage group-divider' },
    clutch: { label: 'Clutch', className: 'group-clutch group-divider' }
};

const SCOUT_GROUP_META = {
    map: { label: 'Kartta', className: 'group-map group-divider' },
    usage: { label: 'Pelattu', className: 'group-usage group-divider' },
    results: { label: 'Tulokset', className: 'group-results group-divider' },
    performance: { label: 'Suorituskyky', className: 'group-performance group-divider' },
    veto: { label: 'Bannit', className: 'group-veto group-divider' },
    series: { label: 'Decider/OT', className: 'group-series group-divider' }
};

function buildColumnGroups(columns, groupMeta) {
    if (!Array.isArray(columns)) return [];
    const groups = [];
    columns.forEach(column => {
        const key = column.group || 'misc';
        const meta = groupMeta[key] || { label: key, className: '' };
        const last = groups[groups.length - 1];
        if (!last || last.key !== key) {
            groups.push({ key, label: meta.label || '', className: meta.className || '', colSpan: 1 });
        } else {
            last.colSpan += 1;
        }
    });
    return groups;
}

const MAP_COLUMNS = [
    { key: 'mapName', label: 'Kartta', sortable: true, colClass: 'col-name col-map-name', width: '210px', tooltip: 'Kartta', group: 'map' },
    { key: 'totalRoundsPlayed', label: 'Erät pelattu', sortable: true, numeric: true, decimals: 0, colClass: 'mono-num', tooltip: 'Todellinen pelattujen erien määrä', group: 'rounds' },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num col-adr', tooltip: 'Average Damage per Round', group: 'combat' },
    { key: 'kr', label: 'KR', sortable: true, numeric: true, decimals: 3, colClass: 'mono-num', tooltip: 'Kills per round', group: 'combat' },
    { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2, colClass: 'mono-num col-kd', tooltip: 'Kills / Deaths', group: 'combat' },
    { key: 'hsPct', label: 'HS%', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num', tooltip: 'Headshot %', group: 'combat' },
    { key: 'kills', label: 'Tappoja', sortable: true, numeric: true, colClass: 'mono-num col-kills', tooltip: 'Kills (total)', group: 'kills' },
    { key: 'deaths', label: 'Kuolemia', sortable: true, numeric: true, colClass: 'mono-num col-deaths', tooltip: 'Deaths (total)', group: 'kills' },
    { key: 'assists', label: 'Assist', sortable: true, numeric: true, colClass: 'mono-num', tooltip: 'Assists (total)', group: 'kills' },
    { key: 'udpr', label: 'UDPR', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num', tooltip: 'Utility damage per round', group: 'utility' },
    { key: 'mvps', label: 'MVP', sortable: true, numeric: true, colClass: 'mono-num col-mvps', tooltip: 'MVP:t (total)', group: 'awards' },
    { key: 'enemiesFlashed', label: 'Enemies flashed', sortable: true, numeric: true, colClass: 'mono-num', tooltip: 'Vastustajat väläytettynä (total)', group: 'flash' },
    { key: 'flashSuccessPct', label: 'Flash%', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num', tooltip: 'Flash-succes % (flash_successes / flash_count)', group: 'flash' },
    { key: 'flashCount', label: 'Flashbangit', sortable: true, numeric: true, colClass: 'mono-num', tooltip: 'Heitetyt flashbangit (total)', group: 'flash' },
    { key: 'multi2k', label: '2k', sortable: true, numeric: true, colClass: 'mono-num', tooltip: '2K (total)', group: 'multikill' },
    { key: 'multi3k', label: '3k', sortable: true, numeric: true, colClass: 'mono-num', tooltip: '3K (total)', group: 'multikill' },
    { key: 'multi4k', label: '4k', sortable: true, numeric: true, colClass: 'mono-num', tooltip: '4K (total)', group: 'multikill' },
    { key: 'multi5k', label: 'Ace', sortable: true, numeric: true, colClass: 'mono-num', tooltip: 'Ace (5K, total)', group: 'multikill' },
    { key: 'pistolKills', label: 'Pistooli', sortable: true, numeric: true, colClass: 'mono-num', tooltip: 'Pistoolitapot (total)', group: 'weapons' },
    { key: 'sniperKills', label: 'Sniper', sortable: true, numeric: true, colClass: 'mono-num', tooltip: 'Sniper-tapot (total)', group: 'weapons' },
    { key: 'totalDamage', label: 'Vahinko', sortable: true, numeric: true, colClass: 'mono-num', tooltip: 'Damage (total)', group: 'damage' },
    { key: 'clutchKills', label: 'Clutch', sortable: true, numeric: true, colClass: 'mono-num', tooltip: 'Clutch-kills (total)', group: 'clutch' }
];

const SCOUT_MAP_COLUMNS = [
    { key: 'mapName', label: 'Kartta', sortable: true, colClass: 'col-name col-map-name', width: '200px', group: 'map' },
    { key: 'played', label: 'Pelattu', sortable: true, numeric: true, colClass: 'mono-num col-played', group: 'usage' },
    { key: 'picks', label: 'Omat pickit', sortable: true, numeric: true, colClass: 'mono-num col-picks', group: 'usage' },
    { key: 'oppPicks', label: 'Vastustajan pickit', sortable: true, numeric: true, colClass: 'mono-num col-opp-picks', group: 'usage' },
    { key: 'winrate', label: 'Win %', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num col-winrate', group: 'results' },
    { key: 'pickWinRate', label: 'Win % (oma pick)', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num col-winrate-own', group: 'results' },
    { key: 'oppPickWinRate', label: 'Win % (vastustajan pick)', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num col-winrate-opp', group: 'results' },
    { key: 'rd', label: 'Eräero', sortable: true, numeric: true, colClass: 'mono-num col-rd', group: 'performance' },
    { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2, colClass: 'mono-num col-kd', group: 'performance' },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num col-adr', group: 'performance' },
    { key: 'ban1', label: '1. banni (oma)', sortable: true, numeric: true, colClass: 'mono-num col-ban1', group: 'veto' },
    { key: 'ban2', label: '2. banni (oma)', sortable: true, numeric: true, colClass: 'mono-num col-ban2', group: 'veto' },
    { key: 'oppBan', label: 'Vastustajan banni', sortable: true, numeric: true, colClass: 'mono-num col-opp-ban', group: 'veto' },
    { key: 'totalOwnBan', label: 'Banneja yhteensä', sortable: true, numeric: true, colClass: 'mono-num col-ban-total', group: 'veto' },
    { key: 'decov', label: 'Decider / overflow', sortable: true, numeric: true, colClass: 'mono-num col-decov', group: 'series' }
];

const VETO_COLUMNS = [
    { key: 'mapName', label: 'Kartta', sortable: true },
    { key: 'timesPicked', label: 'Omat pickit', sortable: true, numeric: true },
    { key: 'timesOpponentPicked', label: 'Vast. pickit', sortable: true, numeric: true },
    { key: 'timesBanned', label: 'Bannit', sortable: true, numeric: true },
    { key: 'pickRate', label: 'Pick%', sortable: true, numeric: true, decimals: 1 },
    { key: 'banRate', label: 'Ban%', sortable: true, numeric: true, decimals: 1 },
    { key: 'pickWinRate', label: 'Win pick', sortable: true, numeric: true, decimals: 1 },
    { key: 'deciderWinRate', label: 'Win decider', sortable: true, numeric: true, decimals: 1 }
];

function createSegment() {
    return { data: null, loading: false, error: null, fetchedAt: null };
}

function safeDivide(num, den) {
    if (!den) return 0;
    return num / den;
}

function toNumber(value, fallback = 0) {
    if (value === null || value === undefined) return fallback;
    const numeric = Number(value);
    if (Number.isFinite(numeric)) return numeric;
    const parsed = Number(String(value).replace(',', '.'));
    return Number.isFinite(parsed) ? parsed : fallback;
}

function formatNumber(value, decimals = 0) {
    const numeric = toNumber(value);
    if (!Number.isFinite(numeric)) return '-';
    return decimals > 0 ? numeric.toFixed(decimals) : numeric.toLocaleString('fi-FI');
}

function formatPerRound(value, decimals = 3) {
    const numeric = toNumber(value);
    if (!Number.isFinite(numeric)) return '-';
    if (numeric === 0) return '0';
    return numeric.toFixed(decimals);
}

function formatPercent(value, decimals = 1) {
    const numeric = toNumber(value);
    const scaled = Math.abs(numeric) <= 1 ? numeric * 100 : numeric;
    return `${scaled.toFixed(decimals)}%`;
}

function normalizePercent(value) {
    const numeric = toNumber(value);
    if (!Number.isFinite(numeric)) return 0;
    return Math.abs(numeric) <= 1 ? numeric * 100 : numeric;
}

function formatSignedNumber(value, decimals = 0) {
    const numeric = toNumber(value);
    if (!Number.isFinite(numeric)) return '-';
    const absValue = Math.abs(numeric);
    const rounded = decimals > 0 ? Number(absValue.toFixed(decimals)) : Math.round(absValue);
    const formatted = decimals > 0 ? rounded.toFixed(decimals) : rounded.toLocaleString('fi-FI');
    if (numeric > 0) return `+${formatted}`;
    if (numeric < 0) return `-${formatted}`;
    return decimals > 0 ? (0).toFixed(decimals) : '0';
}

function clampValue(value, min, max) {
    return Math.min(max, Math.max(min, value));
}

function buildIndexGrid(count, maxTicks = 4) {
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

function computeTrendRange(values, refValue, options = {}) {
    const numericValues = values.map(v => toNumber(v)).filter(v => Number.isFinite(v));
    let min = numericValues.length ? Math.min(...numericValues) : 0;
    let max = numericValues.length ? Math.max(...numericValues) : 0;
    if (Number.isFinite(refValue)) {
        min = Math.min(min, refValue);
        max = Math.max(max, refValue);
    }
    if (min === max) {
        const bump = options.bump || 1;
        min -= bump;
        max += bump;
    }
    const span = Math.max(1e-6, max - min);
    const pad = span * (options.padPct ?? 0.12);
    min -= pad;
    max += pad;
    if (options.clampMinZero) {
        min = Math.max(0, min);
    }
    if (Number.isFinite(options.hardMin)) {
        min = Math.max(options.hardMin, min);
    }
    if (Number.isFinite(options.hardMax)) {
        max = Math.min(options.hardMax, max);
    }
    return { min, max };
}

const PERFORMANCE_TREND_METRICS = [
    {
        key: 'adr',
        label: 'ADR',
        decimals: 1,
        lineClass: 'trend-line--adr',
        pointClass: 'trend-point--adr',
        format: value => formatNumber(value, 1)
    },
    {
        key: 'rd',
        label: 'RD+',
        decimals: 0,
        lineClass: 'trend-line--rd',
        pointClass: 'trend-point--rd',
        refKey: 'avgRoundDiff',
        format: value => formatSignedNumber(value, 0)
    },
    {
        key: 'kd',
        label: 'K/D',
        decimals: 2,
        lineClass: 'trend-line--kd',
        pointClass: 'trend-point--kd',
        format: value => formatNumber(value, 2)
    }
];

function beautifyMapName(raw) {
    if (!raw) return 'Kartta';
    const value = String(raw).trim();
    const lower = value.toLowerCase();
    if (lower === 'forfeit') return null; // never display forfeit as a map
    const core = lower.startsWith('de_') ? lower.slice(3) : lower;
    const parts = core.split(/[_-]/).filter(Boolean);
    if (!parts.length) return value;
    return parts.map(p => p.charAt(0).toUpperCase() + p.slice(1)).join(' ');
}

function mapKey(name) {
    return String(name || '').trim().toLowerCase();
}

function formatDate(ts) {
    if (!ts) return '';
    const d = new Date(ts * 1000);
    return d.toLocaleDateString('fi-FI', { year: 'numeric', month: 'short', day: 'numeric' });
}

function normalizeSeasonData(pageData) {
    if (!pageData) return null;
    return pageData.seasonData || null;
}

function normalizeMap(entry) {
    if (!entry) return null;
    const rawName = entry.mapName || 'Kartta';
    const lowerRaw = String(rawName || '').toLowerCase();
    if (lowerRaw === 'forfeit') {
        return null; // never include forfeit entries as maps
    }
    const beautified = beautifyMapName(rawName) || rawName;
    const games = toNumber(entry.games);
    const played = games || toNumber(entry.played);
    const wins = toNumber(entry.wins);
    const losses = toNumber(entry.losses || Math.max(0, played - wins));
    const picks = toNumber(entry.picks);
    const oppPicks = toNumber(entry.oppPicks);
    const ban1 = toNumber(entry.ban1);
    const ban2 = toNumber(entry.ban2);
    const oppBan = toNumber(entry.oppBan);
    const totalOwnBan = toNumber(entry.totalOwnBan || (ban1 + ban2));
    const decov = toNumber(entry.decov);
    const kills = toNumber(entry.kills);
    const deaths = toNumber(entry.deaths);
    const kd = toNumber(entry.kd || (deaths ? kills / deaths : kills));
    const adr = toNumber(entry.adr);
    const damage = toNumber(entry.damage);
    const utilityDamage = toNumber(entry.utilityDamage);
    const mvps = toNumber(entry.mvps);
    const rd = toNumber(entry.rd);
    const winrate = toNumber(entry.winrate);
    const pickRate = toNumber(entry.pickRate);

    const pickWins = toNumber(entry.pickWins);
    const oppPickWins = toNumber(entry.oppPickWins);
    const pickWinRate = picks ? (pickWins / picks) * 100 : null;
    const oppPickWinRate = oppPicks ? (oppPickWins / oppPicks) * 100 : null;

    const identifier = entry.mapId || rawName;

    const assists = toNumber(entry.assists);
    const kr = toNumber(entry.kr);
    const hsPct = toNumber(entry.hsPct);
    const totalDamage = toNumber(entry.damage);
    const enemiesFlashed = toNumber(entry.enemiesFlashed);
    const flashCount = toNumber(entry.flashCount);
    const flashSuccesses = toNumber(entry.flashSuccesses);
    const flashSuccessPct = flashCount ? (flashSuccesses / flashCount) * 100 : 0;
    const multi2k = toNumber(entry.multi2k);
    const multi3k = toNumber(entry.multi3k);
    const multi4k = toNumber(entry.multi4k);
    const multi5k = toNumber(entry.multi5k);
    const pistolKills = toNumber(entry.pistolKills);
    const sniperKills = toNumber(entry.sniperKills);
    const clutchKills = toNumber(entry.clutchKills);
    const totalUtilityDamage = toNumber(entry.utilityDamage);

    const roundsWon = toNumber(entry.roundsWon);
    const roundsLost = toNumber(entry.roundsLost);
    const totalRoundsPlayed = roundsWon + roundsLost;
    const roundsPerMapAvg = totalRoundsPlayed && played ? totalRoundsPlayed / played : 0;
    const udpr = toNumber(entry.udpr);
    const totalMaps = games || played || 0;
    const multi2kPerRound = totalRoundsPlayed ? multi2k / totalRoundsPlayed : 0;
    const multi3kPerRound = totalRoundsPlayed ? multi3k / totalRoundsPlayed : 0;
    const multi4kPerRound = totalRoundsPlayed ? multi4k / totalRoundsPlayed : 0;
    const multi5kPerRound = totalRoundsPlayed ? multi5k / totalRoundsPlayed : 0;
    const pistolKillsPerRound = totalRoundsPlayed ? pistolKills / totalRoundsPlayed : 0;
    const sniperKillsPerRound = totalRoundsPlayed ? sniperKills / totalRoundsPlayed : 0;
    const totalDamagePerRound = totalRoundsPlayed ? totalDamage / totalRoundsPlayed : 0;
    const multi2kPerMap = totalMaps ? multi2k / totalMaps : 0;
    const multi3kPerMap = totalMaps ? multi3k / totalMaps : 0;
    const multi4kPerMap = totalMaps ? multi4k / totalMaps : 0;
    const multi5kPerMap = totalMaps ? multi5k / totalMaps : 0;
    const pistolKillsPerMap = totalMaps ? pistolKills / totalMaps : 0;
    const sniperKillsPerMap = totalMaps ? sniperKills / totalMaps : 0;
    const totalDamagePerMap = totalMaps ? totalDamage / totalMaps : 0;

    return {
        id: identifier,
        mapName: beautified,
        played,
        games,
        wins,
        losses,
        winrate,
        picks,
        oppPicks,
        pickRate,
        pickWinRate,
        oppPickWinRate,
        pickWins,
        oppPickWins,
        ban1,
        ban2,
        oppBan,
        totalOwnBan,
        decov,
        rd,
        kd,
        kr,
        adr,
        hsPct,
        damage,
        utilityDamage,
        totalDamage,
        totalDamagePerRound,
        totalDamagePerMap,
        roundsWon,
        roundsLost,
        mvps,
        kills,
        deaths,
        assists,
        udpr,
        enemiesFlashed,
        flashCount,
        flashSuccesses,
        flashSuccessPct,
        multi2k,
        multi2kPerRound,
        multi2kPerMap,
        multi3k,
        multi3kPerRound,
        multi3kPerMap,
        multi4k,
        multi4kPerRound,
        multi4kPerMap,
        multi5k,
        multi5kPerRound,
        multi5kPerMap,
        pistolKills,
        pistolKillsPerRound,
        pistolKillsPerMap,
        sniperKills,
        sniperKillsPerRound,
        sniperKillsPerMap,
        clutchKills,
        totalRoundsPlayed,
        roundsPerMapAvg
    };
}

function normalizePlayer(player, idx = 0) {
    if (!player) return null;
    const mapsPlayed = toNumber(player.mapsPlayed);
    const roundsPlayed = toNumber(player.roundsPlayed);
    const kills = toNumber(player.kills);
    const deaths = toNumber(player.deaths);
    const assists = toNumber(player.assists);
    const damage = toNumber(player.damage);
    const utilityDamage = toNumber(player.utilityDamage);
    const mvps = toNumber(player.mvps);
    const sniperKills = toNumber(player.sniperKills);
    const pistolKills = toNumber(player.pistolKills);
    const enemiesFlashed = toNumber(player.enemiesFlashed);
    const flashCount = toNumber(player.flashCount);
    const flashSuccesses = toNumber(player.flashSuccesses);
    const entryCount = toNumber(player.entryCount);
    const entryWins = toNumber(player.entryWins);
    const cl1v1Attempts = toNumber(player.cl1v1Attempts);
    const cl1v1Wins = toNumber(player.cl1v1Wins);
    const cl1v2Attempts = toNumber(player.cl1v2Attempts);
    const cl1v2Wins = toNumber(player.cl1v2Wins);
    const mk2k = toNumber(player.mk2k);
    const mk3k = toNumber(player.mk3k);
    const mk4k = toNumber(player.mk4k);
    const mk5k = toNumber(player.mk5k);
    const clutchKills = toNumber(player.clutchKills);
    const kd = toNumber(player.kd || (deaths ? kills / deaths : kills));
    const rating = kd;
    const adr = toNumber(player.adr);
    const kr = toNumber(player.kr);
    const hsPct = toNumber(player.hsPct);
    const entryWinPct = entryCount ? (entryWins / entryCount) * 100 : 0;
    const clutch1v1Pct = cl1v1Attempts ? (cl1v1Wins / cl1v1Attempts) * 100 : 0;
    const clutch1v2Pct = cl1v2Attempts ? (cl1v2Wins / cl1v2Attempts) * 100 : 0;
    const flashSuccessPct = flashCount ? (flashSuccesses / flashCount) * 100 : 0;

    return {
        playerId: player.playerId || `player-${idx}`,
        nickname: player.nickname || 'Pelaaja',
        mapsPlayed,
        roundsPlayed,
        kd,
        rating,
        adr,
        kr,
        hsPct,
        kills,
        deaths,
        assists,
        damage,
        utilityDamage,
        mvps,
        sniperKills,
        pistolKills,
        entryCount,
        entryWins,
        entryLine: entryWinPct,
        cl1v1Attempts,
        cl1v1Wins,
        cl1v1Line: clutch1v1Pct,
        cl1v2Attempts,
        cl1v2Wins,
        cl1v2Line: clutch1v2Pct,
        clutchKills,
        mk2k,
        mk3k,
        mk4k,
        mk5k,
        enemiesFlashed,
        flashCount,
        flashSuccesses,
        flashSuccessLine: flashSuccessPct
    };
}

function normalizeMatch(match, teamId = null) {
    if (!match) return null;
    const matchId = match.matchId;
    const playedFlag = toNumber(match.played);
    const bestOf = toNumber(match.bestOf);
    const matchWinnerId = match.winnerTeamId || null;
    const matchIsForfeit = !!match.isForfeit;
    const rawMaps = Array.isArray(match.maps) ? match.maps : [];
    const left = match.left || {
        team_id: match.team1Id,
        team_name: match.team1Name,
        avatar: match.t1Avatar
    };
    const right = match.right || {
        team_id: match.team2Id,
        team_name: match.team2Name,
        avatar: match.t2Avatar
    };
    const meOnLeft = teamId ? String(left?.team_id) === String(teamId) : true;
    const mySide = meOnLeft ? left : right;
    const oppSide = meOnLeft ? right : left;
    const myName = mySide?.team_name || '';
    const oppName = oppSide?.team_name || '';
    const maps = [];
    let forfeitedMaps = 0;
    rawMaps.forEach((m, idx) => {
        if (m.is_forfeit) {
            const winnerId = m.winner_team_id || null;
            if (winnerId && String(winnerId) !== String(teamId)) {
                forfeitedMaps += 1;
            }
        }
        const rawMapName = m.map || `Map ${idx + 1}`;
        const displayName = beautifyMapName(rawMapName);
        if (!displayName) return;
        let scoreFor = toNumber(m.rf ?? 0);
        let scoreAgainst = toNumber(m.ra ?? 0);
        const mapWinnerId = m.winner_team_id || null;
        if (m.is_forfeit && mapWinnerId && scoreFor === 0 && scoreAgainst === 0) {
            if (String(mapWinnerId) === String(teamId)) {
                scoreFor = 13;
                scoreAgainst = 0;
            } else {
                scoreFor = 0;
                scoreAgainst = 13;
            }
        }
        const leftStats = m.left || {};
        const rightStats = m.right || {};
        const myStats = meOnLeft ? leftStats : rightStats;
        const oppStats = meOnLeft ? rightStats : leftStats;
        maps.push({
            id: `${matchId}-map-${idx}`,
            mapName: displayName,
            roundIndex: toNumber(m.round_index ?? idx),
            scoreFor,
            scoreAgainst,
            pickTeamId: m.pick_team_id || null,
            isForfeit: !!m.is_forfeit,
            winnerTeamId: mapWinnerId,
            adr: toNumber(myStats.adr),
            kd: toNumber(myStats.kd),
            kills: toNumber(myStats.kills),
            deaths: toNumber(myStats.deaths),
            oppAdr: toNumber(oppStats.adr),
            oppKd: toNumber(oppStats.kd),
            oppKills: toNumber(oppStats.kills),
            oppDeaths: toNumber(oppStats.deaths)
        });
    });
    maps.sort((a, b) => (a.roundIndex || 0) - (b.roundIndex || 0));
    const mapWins = maps.filter(m => m.scoreFor > m.scoreAgainst).length;
    const mapLosses = maps.filter(m => m.scoreFor < m.scoreAgainst).length;
    const mapDraws = maps.length - mapWins - mapLosses;
    const roundsFor = maps.reduce((sum, m) => sum + m.scoreFor, 0);
    const roundsAgainst = maps.reduce((sum, m) => sum + m.scoreAgainst, 0);
    const roundDiff = roundsFor - roundsAgainst;
    const played = maps.length || playedFlag;
    const matchRating = maps.length ? safeDivide(maps.reduce((sum, m) => sum + (m.kd || 0), 0), maps.length) : 0;
    const teamScore = mapWins;
    const oppScore = mapLosses;

    return {
        matchId,
        ts: toNumber(match.ts),
        status: match.status || (playedFlag ? 'finished' : 'scheduled'),
        bestOf: bestOf || Math.max(1, maps.length),
        played,
        forfeitedMaps,
        isForfeit: matchIsForfeit,
        winnerTeamId: matchWinnerId,
        teamScore,
        oppScore,
        mapDraws,
        roundsFor,
        roundsAgainst,
        roundDiff,
        matchRating,
        team1Name: match.team1Name || myName,
        team2Name: match.team2Name || oppName,
        opponentName: oppName,
        me: mySide,
        opponent: oppSide,
        faceitUrl: match.faceitUrl || '',
        maps
    };
}

function recomputeMapDerived(entry) {
    const roundsWon = toNumber(entry.roundsWon);
    const roundsLost = toNumber(entry.roundsLost);
    const totalRoundsPlayed = roundsWon + roundsLost;
    const totalMaps = toNumber(entry.games || entry.played);
    const totalRounds = totalRoundsPlayed || 0;
    const maps = totalMaps || 0;
    const totalDamage = toNumber(entry.totalDamage || entry.damage || 0);
    const multi2k = toNumber(entry.multi2k);
    const multi3k = toNumber(entry.multi3k);
    const multi4k = toNumber(entry.multi4k);
    const multi5k = toNumber(entry.multi5k);
    const pistolKills = toNumber(entry.pistolKills);
    const sniperKills = toNumber(entry.sniperKills);

    return {
        ...entry,
        roundsWon,
        roundsLost,
        totalRoundsPlayed: totalRoundsPlayed || entry.totalRoundsPlayed || 0,
        roundsPerMapAvg: maps ? totalRounds / maps : 0,
        multi2kPerRound: totalRounds ? multi2k / totalRounds : 0,
        multi3kPerRound: totalRounds ? multi3k / totalRounds : 0,
        multi4kPerRound: totalRounds ? multi4k / totalRounds : 0,
        multi5kPerRound: totalRounds ? multi5k / totalRounds : 0,
        pistolKillsPerRound: totalRounds ? pistolKills / totalRounds : 0,
        sniperKillsPerRound: totalRounds ? sniperKills / totalRounds : 0,
        totalDamagePerRound: totalRounds ? totalDamage / totalRounds : 0,
        multi2kPerMap: maps ? multi2k / maps : 0,
        multi3kPerMap: maps ? multi3k / maps : 0,
        multi4kPerMap: maps ? multi4k / maps : 0,
        multi5kPerMap: maps ? multi5k / maps : 0,
        pistolKillsPerMap: maps ? pistolKills / maps : 0,
        sniperKillsPerMap: maps ? sniperKills / maps : 0,
        totalDamagePerMap: maps ? totalDamage / maps : 0
    };
}

function normalizeVeto(entry) {
    if (!entry) return null;
    const pretty = beautifyMapName(entry.mapName);
    return {
        mapName: pretty || 'Kartta',
        timesPicked: toNumber(entry.timesPicked),
        timesBanned: toNumber(entry.timesBanned),
        timesOpponentPicked: toNumber(entry.timesOpponentPicked),
        pickRate: toNumber(entry.pickRate),
        banRate: toNumber(entry.banRate),
        pickWinRate: toNumber(entry.pickWinRate)
    };
}

function getMatchResult(match) {
    if (!match) return 'pending';
    if (match.isForfeit && match.winnerTeamId) {
        return String(match.winnerTeamId) === String(match.me?.team_id) ? 'win' : 'loss';
    }
    if (match.teamScore > match.oppScore) return 'win';
    if (match.teamScore < match.oppScore) return 'loss';
    if (match.roundDiff && match.roundDiff > 0) return 'win';
    if (match.roundDiff && match.roundDiff < 0) return 'loss';
    return 'draw';
}

function resolveTabFromQuery(route) {
    const tab = route?.query?.tab;
    const allowed = new Set(['overview', 'matches', 'players', 'veto']);
    return allowed.has(tab) ? tab : 'overview';
}

window.TeamDetail = {
    name: 'TeamDetail',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; },
        get SortableTable() { return window.SortableTable; }
    },
    props: {
        teamId: { type: [String, Number], required: true },
        championshipId: { type: [String, Number], default: null }
    },
    data() {
        const teamStore = typeof window.useTeamStore === 'function' ? window.useTeamStore() : null;
        return {
            teamStore,
            selectedChampionship: this.championshipId ? String(this.championshipId) : null,
            activeTab: resolveTabFromQuery(this.$route),
            mapViewMode: 'summary', // 'summary' or 'full'
            mapSubMetricMode: 'perRound', // 'perRound' or 'perMap'
            matchMetric: 'roundDiff',
            SCOUT_MAP_COLUMNS,
            MAP_COLUMNS,
            PLAYER_COLUMNS,
            VETO_COLUMNS,
            scoutTableKey: 0,
            detailedTableKey: 0,
            performanceTrendHover: {
                key: null,
                index: null,
                x: 0,
                y: 0
            },
            performanceTrendMode: 'map',
            trendChartWidth: 640,
            trendChartHeight: 140,
            matchesTrendHover: {
                index: null,
                x: 0,
                y: 0
            },
            matchesHoverMatchId: null,
            matchesHoverSource: null,
            matchesChartWidth: 640,
            matchesChartHeight: 140
        };
    },
    computed: {
        teamEntry() {
            if (!this.teamStore || !this.teamId) return null;
            return this.teamStore.getTeamState(this.teamId);
        },
        pageSegment() {
            return this.teamEntry?.page || createSegment();
        },
        pageData() {
            return this.pageSegment.data || null;
        },
        teamInfo() {
            return this.pageData?.team || null;
        },
        seasonOptions() {
            const seasons = Array.isArray(this.pageData?.seasons) ? this.pageData.seasons : [];
            const normalized = seasons.map(season => {
                const value = season.championshipId;
                return {
                    value: value ? String(value) : null,
                    label: season.name || `Kausi ${season.season} · Div ${season.divisionNum}`,
                    season: toNumber(season.season),
                    division: season.divisionNum,
                    isPlayoffs: season.isPlayoffs
                };
            }).filter(option => option.value);
            return normalized.sort((a, b) => {
                const seasonDiff = (b.season || 0) - (a.season || 0);
                if (seasonDiff !== 0) return seasonDiff;
                const av = Number(a.value);
                const bv = Number(b.value);
                if (Number.isFinite(av) && Number.isFinite(bv)) return bv - av;
                return 0;
            });
        },
        selectedSeasonOption() {
            if (!this.seasonOptions.length) return null;
            const current = this.currentChampionshipId ? String(this.currentChampionshipId) : null;
            const match = this.seasonOptions.find(opt => String(opt.value) === String(current));
            if (match) return match;
            return this.seasonOptions[0] || null;
        },
        heroPlayoffsFlag() {
            const option = this.selectedSeasonOption;
            return !!(option?.isPlayoffs);
        },
        currentChampionshipId() {
            if (this.pageData?.currentChampionshipId) return String(this.pageData.currentChampionshipId);
            if (this.selectedChampionship) return String(this.selectedChampionship);
            return this.seasonOptions[0]?.value || null;
        },
        seasonData() {
            const data = normalizeSeasonData(this.pageData);
            if (!data) return null;
            if (this.currentChampionshipId && data.championshipId && String(data.championshipId) !== String(this.currentChampionshipId)) {
                return null;
            }
            return data;
        },
        teamStats() {
            return this.seasonData?.teamStats || {};
        },
        // Map stats (from API only, no fallbacks)
        mapStats() {
            const maps = Array.isArray(this.seasonData?.mapStats) ? this.seasonData.mapStats : [];
            return maps
                .map(normalizeMap)
                .filter(Boolean)
                .map(recomputeMapDerived)
                .sort((a, b) => (b.played || 0) - (a.played || 0) || String(a.mapName).localeCompare(String(b.mapName)));
        },
        mapTotals() {
            const totals = {
                played: 0,
                games: 0,
                wins: 0,
                losses: 0,
                picks: 0,
                oppPicks: 0,
                ban1: 0,
                ban2: 0,
                oppBan: 0,
                totalOwnBan: 0,
                decov: 0,
                rd: 0,
                kills: 0,
                deaths: 0,
                damage: 0,
                utilityDamage: 0,
                mvps: 0,
                adrWeighted: 0
            };
            if (!this.mapStats.length) return { ...totals, avgAdr: 0, kd: 0, winrate: 0, pickRate: 0 };
            this.mapStats.forEach(map => {
                const games = map.games || map.played || (map.wins + map.losses) || 0;
                const played = games || map.played || 0;
                totals.played += played;
                totals.games += games;
                totals.wins += map.wins || 0;
                totals.losses += map.losses || 0;
                totals.picks += map.picks || 0;
                totals.oppPicks += map.oppPicks || 0;
                totals.ban1 += map.ban1 || 0;
                totals.ban2 += map.ban2 || 0;
                totals.oppBan += map.oppBan || 0;
                totals.totalOwnBan += map.totalOwnBan || 0;
                totals.decov += map.decov || 0;
                totals.rd += map.rd || 0;
                totals.kills += map.kills || 0;
                totals.deaths += map.deaths || 0;
                totals.damage += map.damage || 0;
                totals.utilityDamage += map.utilityDamage || 0;
                totals.mvps += map.mvps || 0;
                totals.adrWeighted += (map.adr || 0) * (games || 1);
            });
            const games = totals.games || totals.played || 1;
            const kd = totals.deaths ? totals.kills / totals.deaths : totals.kills || 0;
            const avgAdr = totals.adrWeighted / games;
            const winrate = games ? (totals.wins / games) * 100 : 0;
            const totalPicks = totals.picks + totals.oppPicks;
            const pickRate = totalPicks ? (totals.picks / totalPicks) * 100 : 0;
            return { ...totals, kd, avgAdr, winrate, pickRate };
        },
        seasonSnapshotStats() {
            const s = this.teamStats || {};
            const playedMatches = this.matchesList.filter(m => m.played);
            const upcomingMatches = this.matchesList.length - playedMatches.length;
            const matchWins = playedMatches.filter(m => getMatchResult(m) === 'win').length;
            const matchLosses = playedMatches.filter(m => getMatchResult(m) === 'loss').length;
            const matches = playedMatches.length;
            const winRate = matches ? (matchWins / matches) * 100 : normalizePercent(s.winRate ?? this.mapTotals.winrate ?? 0);
            const mapsPlayed = playedMatches.reduce((acc, m) => {
                if (!m.maps?.length && m.isForfeit && m.winnerTeamId) {
                    return acc + Math.max(1, m.bestOf || 2);
                }
                return acc + (m.maps?.length || 0);
            }, 0);
            const mapWins = playedMatches.reduce((acc, m) => {
                if (!m.maps?.length && m.isForfeit && m.winnerTeamId) {
                    return acc + (String(m.winnerTeamId) === String(this.teamId) ? Math.max(1, m.bestOf || 2) : 0);
                }
                return acc + (m.maps || []).filter(mp => {
                    if (mp.isForfeit && mp.winnerTeamId) {
                        return String(mp.winnerTeamId) === String(this.teamId);
                    }
                    return mp.scoreFor > mp.scoreAgainst;
                }).length;
            }, 0);
            const mapLosses = playedMatches.reduce((acc, m) => {
                if (!m.maps?.length && m.isForfeit && m.winnerTeamId) {
                    return acc + (String(m.winnerTeamId) !== String(this.teamId) ? Math.max(1, m.bestOf || 2) : 0);
                }
                return acc + (m.maps || []).filter(mp => {
                    if (mp.isForfeit && mp.winnerTeamId) {
                        return String(mp.winnerTeamId) !== String(this.teamId);
                    }
                    return mp.scoreFor < mp.scoreAgainst;
                }).length;
            }, 0);
            const activePlayers = this.players.filter(p => (p.mapsPlayed || 0) > 0).length;
            const forfeitedMaps = playedMatches.reduce((acc, m) => {
                const forfeitsFromMaps = m.forfeitedMaps || 0;
                const shouldFallback = !m.maps?.length
                    && forfeitsFromMaps === 0
                    && m.isForfeit
                    && m.winnerTeamId
                    && String(m.winnerTeamId) !== String(this.teamId);
                const fallbackCount = shouldFallback ? Math.max(1, m.bestOf || 2) : 0;
                return acc + forfeitsFromMaps + fallbackCount;
            }, 0);
            const roundsWon = playedMatches.reduce((acc, m) => {
                // Handle forfeit matches without map data - each forfeited map counts as 13-0
                if (!m.maps?.length && m.isForfeit && m.winnerTeamId) {
                    const mapsCount = Math.max(1, m.bestOf || 2);
                    return acc + (String(m.winnerTeamId) === String(this.teamId) ? mapsCount * 13 : 0);
                }
                return acc + (m.roundsFor || 0);
            }, 0);
            const roundsLost = playedMatches.reduce((acc, m) => {
                // Handle forfeit matches without map data - each forfeited map counts as 0-13
                if (!m.maps?.length && m.isForfeit && m.winnerTeamId) {
                    const mapsCount = Math.max(1, m.bestOf || 2);
                    return acc + (String(m.winnerTeamId) !== String(this.teamId) ? mapsCount * 13 : 0);
                }
                return acc + (m.roundsAgainst || 0);
            }, 0);
            const roundsDiff = roundsWon - roundsLost;

            // Phase 1: Division averages for trend indicators
            const divAvgs = this.divisionAverages || {};
            const mapWinRate = mapsPlayed ? (mapWins / mapsPlayed) * 100 : 0;

            const hasWinRate = matches > 0;
            const missingTip = 'Ei dataa valitulle kaudelle.';
            return {
                primary: [
                    {
                        key: 'mapWinRate',
                        label: 'Kartta voitto-%',
                        value: mapsPlayed > 0 ? formatPercent(mapWinRate, 1) : '—',
                        sub: '',
                        tone: 'stat-primary',
                        missing: mapsPlayed === 0,
                        tooltip: mapsPlayed > 0
                            ? 'Voitetut kartat / pelatut kartat. Luovutukset mukana.'
                            : missingTip,
                        trendValue: mapsPlayed > 0 ? mapWinRate : null,
                        divAvg: divAvgs.avgMapWinRate || null,
                        trendTooltip: mapsPlayed > 0 ? `Verrattuna divisioonan kauden keskiarvoon · ${formatPercent(divAvgs.avgMapWinRate || 0, 1)}` : ''
                    },
                    {
                        key: 'rounds',
                        label: 'Eräero',
                        value: formatNumber(roundsDiff),
                        sub: `${formatNumber(roundsWon)}–${formatNumber(roundsLost)}`,
                        tone: 'stat-primary',
                        tooltip: 'Voitetut erät − hävityt erät. Luovutukset mukana (13–0/0–13).',
                        trendValue: roundsDiff,
                        divAvg: divAvgs.avgRoundDiff || null,
                        trendTooltip: `Verrattuna divisioonan kauden keskiarvoon · ${formatNumber(divAvgs.avgRoundDiff || 0, 1)}`
                    },
                    {
                        key: 'matches',
                        label: 'Ottelut',
                        value: formatNumber(matches),
                        sub: `${formatNumber(matchWins)}–${formatNumber(matchLosses)}`,
                        tone: 'stat-primary',
                        tooltip: `Pelatut ottelut. Voitot–tappiot: ${formatNumber(matchWins)}–${formatNumber(matchLosses)}.`,
                        trendValue: null,
                        divAvg: null
                    }
                ],
                secondary: [
                    {
                        key: 'activePlayers',
                        label: 'Aktiiviset pelaajat',
                        value: formatNumber(activePlayers),
                        sub: 'Vähintään 1 ottelu',
                        tone: 'stat-secondary',
                        tooltip: 'Pelaaja, joka on pelannut vähintään yhden ottelun tällä kaudella.',
                        trendValue: null,
                        divAvg: null
                    },
                    {
                        key: 'maps',
                        label: 'Kartat (W–L)',
                        value: formatNumber(mapsPlayed),
                        sub: `${formatNumber(mapWins)}–${formatNumber(mapLosses)}`,
                        tone: 'stat-secondary',
                        tooltip: 'Karttakohtainen tulos (voitot–tappiot).',
                        trendValue: null,
                        divAvg: null
                    },
                    {
                        key: 'forfeits',
                        label: 'Luovutetut kartat',
                        value: formatNumber(forfeitedMaps),
                        sub: 'Kausi yhteensä',
                        tone: 'stat-secondary',
                        tooltip: 'Luovutuksena päättyneet kartat, jotka vaikuttavat kokonaislukuihin.',
                        trendValue: null,
                        divAvg: null
                    },
                    {
                        key: 'upcoming',
                        label: 'Tulevat ottelut',
                        value: formatNumber(upcomingMatches),
                        sub: 'Aikataulussa',
                        tone: 'stat-secondary',
                        tooltip: 'Ottelut, joita ei ole vielä pelattu.',
                        trendValue: null,
                        divAvg: null
                    }
                ]
            };
        },
        scoutMapRows() {
            return this.mapStats.map(map => {
                const played = map.games || map.played || 0;
                const wins = map.wins || 0;
                const losses = map.losses || 0;
                const winrate = Number.isFinite(map.winrate) ? map.winrate : (played ? (wins / played) * 100 : 0);
                return {
                    ...map,
                    played,
                    wins,
                    losses,
                    winrate
                };
            });
        },
        scoutHeaderGroups() {
            return buildColumnGroups(this.SCOUT_MAP_COLUMNS, SCOUT_GROUP_META);
        },
        mapHeaderGroups() {
            return buildColumnGroups(this.MAP_COLUMNS, MAP_GROUP_META);
        },
        scoutMapDefaultSort() {
            return { column: 'played', order: 'desc', numeric: true };
        },
        vetoTrendMatches() {
            return this.matchesList || [];
        },
        vetoMatchMeta() {
            const meta = {};
            this.vetoByMatch.forEach(entry => {
                const match = entry.match || {};
                const matchId = entry.matchId || match.matchId;
                if (!matchId) return;
                const format = entry.format || this.detectSeriesFormat(match.bestOf, entry.steps || []);
                const decider = entry.steps?.find(step => step.action === 'decider')?.mapName || null;
                const overflow = entry.steps?.find(step => step.action === 'overflow')?.mapName || null;
                meta[matchId] = {
                    seriesType: format === 'bo3' ? 'BO3' : 'BO2',
                    decider,
                    overflow
                };
            });
            return meta;
        },
        vetoTrendColumns() {
            return this.vetoTrendMatches.map((match, idx) => {
                const dateLabel = formatDate(match.ts);
                const opponent = match.opponentName || match.team2Name || 'Vastustaja';
                const resultKey = getMatchResult(match);
                const resultLabel = resultKey === 'win' ? 'W' : resultKey === 'loss' ? 'L' : resultKey === 'draw' ? 'D' : 'Kesken';
                const scoreLabel = (match.teamScore != null && match.oppScore != null) ? `${match.teamScore}-${match.oppScore}` : '';
                const meta = this.vetoMatchMeta[match.matchId] || {};
                const badgeTitle = meta.decider
                    ? `Ratkaisukartta: ${meta.decider}`
                    : meta.overflow
                        ? `Overflow: ${meta.overflow}`
                        : '';
                const label = `#${idx + 1}`;
                return {
                    id: match.matchId,
                    label,
                    title: [
                        label,
                        dateLabel,
                        opponent ? `vs ${opponent}` : '',
                        scoreLabel ? `Tulos ${scoreLabel} (${resultLabel})` : `Tulos ${resultLabel}`,
                        meta.seriesType ? meta.seriesType : '',
                        badgeTitle
                    ].filter(Boolean).join(' · ')
                };
            });
        },
        vetoTrendMapPool() {
            const pool = new Map();
            const pickLookup = {};
            const playedLookup = {};
            this.mapStats.forEach(row => {
                pickLookup[mapKey(row.mapName)] = normalizePercent(row.pickRate) || 0;
                playedLookup[mapKey(row.mapName)] = toNumber(row.games || row.played || 0);
                if (row.mapName) {
                    pool.set(mapKey(row.mapName), { mapName: row.mapName });
                }
            });
            this.matchesList.forEach(match => {
                (match.maps || []).forEach(map => {
                    const name = beautifyMapName(map.mapName);
                    if (!name) return;
                    const key = mapKey(name);
                    playedLookup[key] = (playedLookup[key] || 0) + 1;
                    pool.set(mapKey(name), { mapName: name });
                });
            });
            this.vetoByMatch.forEach(entry => {
                entry.steps.forEach(step => {
                    if (!step.mapName) return;
                    const key = mapKey(step.mapName);
                    if (!pool.has(key)) pool.set(key, { mapName: step.mapName });
                });
            });
            const rows = Array.from(pool.values())
                .map(row => ({
                    ...row,
                    pickRate: pickLookup[mapKey(row.mapName)] || 0,
                    played: playedLookup[mapKey(row.mapName)] || 0
                }));

            return rows.sort((a, b) => a.mapName.localeCompare(b.mapName));
        },
        vetoTrendRows() {
            if (!this.vetoTrendMatches.length) return [];
            const byMatch = {};
            this.vetoByMatch.forEach(entry => {
                const actions = {};
                let teamBanCount = 0;
                entry.steps.forEach(step => {
                    if (!step.mapName) return;
                    const key = mapKey(step.mapName);
                    const bucket = actions[key] || { pick: null, ban: null };
                    if (step.action === 'pick') {
                        bucket.pick = { actor: step.actor, teamName: step.teamName };
                    }
                    if (step.action === 'ban') {
                        let order = null;
                        if (step.actor === 'team') {
                            teamBanCount += 1;
                            order = teamBanCount;
                        }
                        bucket.ban = { actor: step.actor, teamName: step.teamName, order };
                    }
                    actions[key] = bucket;
                });
                byMatch[entry.matchId] = { entry, actions };
            });

            return this.vetoTrendMapPool.map(map => {
                const key = mapKey(map.mapName);
                const cells = this.vetoTrendMatches.map(match => {
                    const veto = byMatch[match.matchId];
                    const actionPick = veto?.actions?.[key]?.pick || null;
                    const actionBan = veto?.actions?.[key]?.ban || null;
                    const actionInfo = actionPick || actionBan;
                    const opponent = match.opponentName || match.team2Name || 'Vastustaja';
                    const dateLabel = formatDate(match.ts);
                    const resultKey = getMatchResult(match);
                    const resultLabel = resultKey === 'win' ? 'W' : resultKey === 'loss' ? 'L' : resultKey === 'draw' ? 'D' : 'Kesken';
                    const scoreLabel = (match.teamScore != null && match.oppScore != null) ? `${match.teamScore}-${match.oppScore}` : '';
                    const meta = this.vetoMatchMeta[match.matchId] || {};
                    let className = 'veto-heatmap__cell--none';
                    let actionLabel = 'Ei vetoa';
                    let byLabel = '';

                    if (actionPick) {
                        if (actionPick.actor === 'team') {
                            className = 'veto-heatmap__cell--team-pick';
                            actionLabel = 'Oma pick';
                            byLabel = this.teamInfo?.teamName || 'Oma joukkue';
                        } else if (actionPick.actor === 'opponent') {
                            className = 'veto-heatmap__cell--opp-pick';
                            actionLabel = 'Vastustajan pick';
                            byLabel = opponent;
                        }
                    } else if (actionBan) {
                        if (actionBan.actor === 'team') {
                            if (actionBan.order === 1) {
                                className = 'veto-heatmap__cell--team-ban1';
                                actionLabel = 'Oma banni (1.)';
                            } else if (actionBan.order === 2) {
                                className = 'veto-heatmap__cell--team-ban2';
                                actionLabel = 'Oma banni (2.)';
                            } else {
                                className = 'veto-heatmap__cell--team-ban2';
                                actionLabel = 'Oma banni';
                            }
                            byLabel = this.teamInfo?.teamName || 'Oma joukkue';
                        } else if (actionBan.actor === 'opponent') {
                            className = 'veto-heatmap__cell--opp-ban';
                            actionLabel = 'Vastustajan banni';
                            byLabel = opponent;
                        }
                    }

                    const title = [
                        map.mapName,
                        `Ottelu ${match.matchId}`,
                        dateLabel,
                        opponent ? `vs ${opponent}` : '',
                        scoreLabel ? `Tulos ${scoreLabel} (${resultLabel})` : `Tulos ${resultLabel}`,
                        meta.seriesType ? meta.seriesType : '',
                        actionLabel,
                        byLabel ? `Tekijä ${byLabel}` : '',
                        meta.decider ? `Ratkaisukartta: ${meta.decider}` : '',
                        meta.overflow ? `Overflow: ${meta.overflow}` : ''
                    ].filter(Boolean).join(' · ');

                    return { className, title, hasAction: !!actionInfo };
                });

                return {
                    mapName: map.mapName,
                    rowLabel: map.mapName,
                    cells
                };
            });
        },
        vetoLegendEntries() {
            const buildCell = (className, label) => ({ className, label });
            return [
                buildCell('veto-heatmap__cell--team-pick', 'Oma pick'),
                buildCell('veto-heatmap__cell--opp-pick', 'Vastustajan pick'),
                buildCell('veto-heatmap__cell--team-ban1', 'Oma banni (1.)'),
                buildCell('veto-heatmap__cell--team-ban2', 'Oma banni (2.)'),
                buildCell('veto-heatmap__cell--opp-ban', 'Vastustajan banni'),
                buildCell('veto-heatmap__cell--none', 'Ei vetoa')
            ];
        },
        mapDefaultSort() {
            return { column: 'totalRoundsPlayed', order: 'desc', numeric: true };
        },
        mapMaxRoundDiff() {
            return Math.max(...this.scoutMapRows.map(m => Math.abs(m.rd || 0)), 1);
        },
        performanceTrendMetrics() {
            return PERFORMANCE_TREND_METRICS;
        },
        performanceTrendSeries() {
            const sortedMatches = [...this.matchesList]
                .filter(match => match.played)
                .sort((a, b) => {
                    const at = a.ts || 0;
                    const bt = b.ts || 0;
                    if (!at && bt) return 1;
                    if (at && !bt) return -1;
                    return at - bt;
                });

            const points = [];
            sortedMatches.forEach(match => {
                const maps = Array.isArray(match.maps) ? match.maps : [];
                const opponent = match.opponentName || match.team2Name || '';
                if (!maps.length) {
                    if (match.isForfeit && match.winnerTeamId) {
                        const mapCount = Math.max(1, match.bestOf || 2);
                        const teamWon = String(match.winnerTeamId) === String(this.teamId);
                        for (let idx = 0; idx < mapCount; idx += 1) {
                            const scoreFor = teamWon ? 13 : 0;
                            const scoreAgainst = teamWon ? 0 : 13;
                            const rdValue = clampValue(scoreFor - scoreAgainst, -13, 13);
                            points.push({
                                id: `${match.matchId}-ff-${idx}`,
                                ts: match.ts,
                                dateLabel: formatDate(match.ts),
                                opponent,
                                matchLabel: opponent || 'Vastustaja',
                                mapLabel: 'Luovutus',
                                result: teamWon ? 'win' : 'loss',
                                scoreLabel: `${scoreFor}-${scoreAgainst}`,
                                adr: null,
                                rd: rdValue,
                                kd: null,
                                isForfeit: true
                            });
                        }
                    }
                    return;
                }
                maps.forEach((map, idx) => {
                    const scoreFor = toNumber(map.scoreFor);
                    const scoreAgainst = toNumber(map.scoreAgainst);
                    const mapResult = scoreFor > scoreAgainst ? 'win' : scoreFor < scoreAgainst ? 'loss' : 'draw';
                    const rdValue = clampValue(scoreFor - scoreAgainst, -13, 13);
                    points.push({
                        id: map.id || `${match.matchId}-map-${idx}`,
                        ts: match.ts,
                        dateLabel: formatDate(match.ts),
                        opponent,
                        matchLabel: opponent || 'Vastustaja',
                        mapLabel: map.mapName || 'Kartta',
                        result: mapResult,
                        scoreLabel: (Number.isFinite(scoreFor) && Number.isFinite(scoreAgainst)) ? `${scoreFor}-${scoreAgainst}` : '',
                        adr: toNumber(map.adr),
                        rd: rdValue,
                        kd: toNumber(map.kd),
                        isForfeit: !!map.isForfeit
                    });
                });
            });

            return points;
        },
        performanceTrendCharts() {
            const points = this.performanceTrendSeries;
            if (!points.length) return [];
            const layout = {
                width: this.trendChartWidth || 640,
                height: this.trendChartHeight || 140,
                padding: { left: 46, right: 68, top: 16, bottom: 26 }
            };
            const plotWidth = layout.width - layout.padding.left - layout.padding.right;
            const plotHeight = layout.height - layout.padding.top - layout.padding.bottom;
            const divAvgs = this.divisionAverages || {};
            const gridIndices = buildIndexGrid(points.length, 12);
            const labelIndices = buildIndexGrid(points.length, Math.min(4, points.length));

            return this.performanceTrendMetrics.map(metric => {
                const baseValues = [];
                const avgSourceValues = [];
                let lastValidValue = 0;
                points.forEach((point, idx) => {
                    if (metric.key === 'rd') {
                        const value = toNumber(point[metric.key]);
                        baseValues[idx] = value;
                        avgSourceValues[idx] = value;
                        lastValidValue = value;
                        return;
                    }
                    const raw = toNumber(point[metric.key], null);
                    const isForfeit = !!point.isForfeit;
                    if (!isForfeit && Number.isFinite(raw)) {
                        lastValidValue = raw;
                    }
                    baseValues[idx] = (!isForfeit && Number.isFinite(raw)) ? raw : lastValidValue;
                    avgSourceValues[idx] = (!isForfeit && Number.isFinite(raw)) ? raw : null;
                });

                const cumulativeValues = [];
                let runningSum = 0;
                let runningCount = 0;
                baseValues.forEach((value, idx) => {
                    if (metric.key === 'rd') {
                        runningSum += value;
                        cumulativeValues[idx] = runningSum;
                    } else {
                        const src = avgSourceValues[idx];
                        if (src != null) {
                            runningSum += src;
                            runningCount += 1;
                        }
                        cumulativeValues[idx] = runningCount ? (runningSum / runningCount) : value;
                    }
                });
                const values = this.performanceTrendMode === 'cumulative' ? cumulativeValues : baseValues;
                const average = metric.key === 'rd'
                    ? (baseValues.length ? baseValues.reduce((sum, v) => sum + v, 0) / baseValues.length : 0)
                    : (() => {
                        const valid = avgSourceValues.filter(v => v != null);
                        if (!valid.length) return 0;
                        return valid.reduce((sum, v) => sum + v, 0) / valid.length;
                    })();
                const hasRef = metric.refKey && Object.prototype.hasOwnProperty.call(divAvgs, metric.refKey);
                const refCandidate = hasRef ? toNumber(divAvgs[metric.refKey]) : null;
                const refValue = Number.isFinite(refCandidate) ? refCandidate : average;
                const range = computeTrendRange(values, refValue, {
                    clampMinZero: metric.key !== 'rd',
                    bump: metric.key === 'kd' ? 0.2 : metric.key === 'adr' ? 5 : 3,
                    hardMin: metric.key === 'rd' && this.performanceTrendMode === 'map' ? -13 : null,
                    hardMax: metric.key === 'rd' && this.performanceTrendMode === 'map' ? 13 : null
                });
                const valueToY = value =>
                    layout.padding.top + ((range.max - value) / (range.max - range.min)) * plotHeight;
                const valueToX = idx =>
                    layout.padding.left + (points.length === 1 ? 0 : (idx / (points.length - 1)) * plotWidth);
                const chartPoints = points.map((point, idx) => {
                    const value = values[idx];
                    const delta = idx > 0 ? value - values[idx - 1] : null;
                    return {
                        ...point,
                        value,
                        delta,
                        index: idx,
                        x: valueToX(idx),
                        y: valueToY(value)
                    };
                });
                const path = chartPoints
                    .map((p, idx) => `${idx === 0 ? 'M' : 'L'} ${p.x.toFixed(2)} ${p.y.toFixed(2)}`)
                    .join(' ');
                const ticks = [range.min, (range.min + range.max) / 2, range.max].map(value => ({
                    value,
                    y: valueToY(value)
                }));
                const gridLines = gridIndices.map(idx => ({
                    index: idx,
                    x: valueToX(idx)
                }));
                const xLabels = labelIndices.map(idx => ({
                    index: idx,
                    x: valueToX(idx),
                    label: points[idx].dateLabel || `#${idx + 1}`
                }));
                const latest = chartPoints[chartPoints.length - 1] || null;
                const refY = valueToY(refValue);
                const zeroY = metric.key === 'rd' && range.min < 0 && range.max > 0 ? valueToY(0) : null;

                return {
                    ...metric,
                    width: layout.width,
                    height: layout.height,
                    padding: layout.padding,
                    plotWidth,
                    plotHeight,
                    baseValues,
                    cumulativeValues,
                    points: chartPoints,
                    path,
                    ticks,
                    gridLines,
                    xLabels,
                    average,
                    refValue,
                    refY,
                    zeroY,
                    latest
                };
            });
        },
        performanceTrendVisibleCharts() {
            const charts = this.performanceTrendCharts;
            return charts.map((chart, idx) => ({
                ...chart,
                showXAxis: idx === charts.length - 1
            }));
        },
        // Match history uses every field: status/best_of/played/opponent info/avatars/maps scores/picks/forfeit/ADR/KD plus Faceit URL
        matchesList() {
            const matches = Array.isArray(this.seasonData?.matchHistory) ? this.seasonData.matchHistory : [];
            const normalized = matches.map(m => normalizeMatch(m, this.teamId)).filter(Boolean);
            return normalized.sort((a, b) => {
                const at = a.ts ?? 0;
                const bt = b.ts ?? 0;
                if (!at && bt) return 1; // missing dates go to bottom
                if (at && !bt) return -1;
                return bt - at; // newest first for tables
            });
        },
        matchesTrendMetrics() {
            return [
                {
                    key: 'rd',
                    label: 'RD+',
                    decimals: 0,
                    lineClass: 'trend-line--rd',
                    pointClass: 'trend-point--rd',
                    refKey: 'avgRoundDiff',
                    format: value => formatSignedNumber(value, 0)
                },
                {
                    key: 'net',
                    label: 'Win/Loss',
                    decimals: 0,
                    lineClass: 'trend-line--rd',
                    pointClass: 'trend-point--rd',
                    format: value => formatSignedNumber(value, 0)
                }
            ];
        },
        matchesTrendSeries() {
            const points = this.performanceTrendSeries;
            let net = 0;
            return points.map(point => {
                let delta = 0;
                if (point.result === 'win') delta = 1;
                if (point.result === 'loss') delta = -1;
                net += delta;
                return {
                    ...point,
                    netDelta: delta,
                    net
                };
            });
        },
        matchesTrendCharts() {
            const points = this.matchesTrendSeries;
            if (!points.length) return [];
            const metrics = this.matchesTrendMetrics;
            const layout = {
                width: this.matchesChartWidth || 640,
                height: this.trendChartHeight || 140,
                padding: { left: 46, right: 68, top: 16, bottom: 26 }
            };
            const plotWidth = layout.width - layout.padding.left - layout.padding.right;
            const plotHeight = layout.height - layout.padding.top - layout.padding.bottom;
            const divAvgs = this.divisionAverages || {};
            const gridIndices = buildIndexGrid(points.length, 12);
            const labelIndices = buildIndexGrid(points.length, Math.min(4, points.length));

            return metrics.map(metric => {
                const baseValues = points.map(point => {
                    if (metric.key === 'net') return toNumber(point.netDelta);
                    return toNumber(point[metric.key]);
                });
                const cumulativeValues = [];
                let runningSum = 0;
                baseValues.forEach((value, idx) => {
                    runningSum += value;
                    cumulativeValues[idx] = runningSum;
                });
                const values = cumulativeValues;
                const average = baseValues.length
                    ? baseValues.reduce((sum, v) => sum + v, 0) / baseValues.length
                    : 0;
                const hasRef = metric.refKey && Object.prototype.hasOwnProperty.call(divAvgs, metric.refKey);
                const refCandidate = hasRef ? toNumber(divAvgs[metric.refKey]) : null;
                const refValue = metric.key === 'net'
                    ? 0
                    : (Number.isFinite(refCandidate) ? refCandidate : average);
                const range = computeTrendRange(values, refValue, {
                    clampMinZero: false,
                    bump: 3,
                    hardMin: null,
                    hardMax: null
                });
                const valueToY = value =>
                    layout.padding.top + ((range.max - value) / (range.max - range.min)) * plotHeight;
                const valueToX = idx =>
                    layout.padding.left + (points.length === 1 ? 0 : (idx / (points.length - 1)) * plotWidth);
                const chartPoints = points.map((point, idx) => {
                    const value = values[idx];
                    const delta = idx > 0 ? value - values[idx - 1] : null;
                    return {
                        ...point,
                        value,
                        delta,
                        index: idx,
                        x: valueToX(idx),
                        y: valueToY(value)
                    };
                });
                const path = chartPoints
                    .map((p, idx) => `${idx === 0 ? 'M' : 'L'} ${p.x.toFixed(2)} ${p.y.toFixed(2)}`)
                    .join(' ');
                const ticks = [range.min, (range.min + range.max) / 2, range.max].map(value => ({
                    value,
                    y: valueToY(value)
                }));
                const gridLines = gridIndices.map(idx => ({
                    index: idx,
                    x: valueToX(idx)
                }));
                const xLabels = labelIndices.map(idx => ({
                    index: idx,
                    x: valueToX(idx),
                    label: points[idx].dateLabel || `#${idx + 1}`
                }));
                const latest = chartPoints[chartPoints.length - 1] || null;
                const refY = valueToY(refValue);
                const zeroY = range.min < 0 && range.max > 0 ? valueToY(0) : null;

                return {
                    ...metric,
                    width: layout.width,
                    height: layout.height,
                    padding: layout.padding,
                    plotWidth,
                    plotHeight,
                    points: chartPoints,
                    path,
                    ticks,
                    gridLines,
                    xLabels,
                    average,
                    refValue,
                    refY,
                    zeroY,
                    latest
                };
            });
        },
        matchesTrendVisibleCharts() {
            const charts = this.matchesTrendCharts;
            return charts.map((chart, idx) => ({
                ...chart,
                showXAxis: idx === charts.length - 1
            }));
        },
        // Player stats table uses every DB field: maps/rounds/kills/deaths/assists/mvps/sniper_kills/utility_damage/enemies_flashed/flash_count/flash_successes/entry_count/entry_wins/clutch fields/pistol_kills/adr/kr/kd/rating/hs_pct/damage/multi-kills
        players() {
            const players = Array.isArray(this.seasonData?.playerStats) ? this.seasonData.playerStats : [];
            return players.map((p, idx) => normalizePlayer(p, idx)).filter(Boolean);
        },
        playerDefaultSort() {
            return { column: 'adr', order: 'desc', numeric: true };
        },
        playersByRating() {
            return [...this.players].sort((a, b) => (b.rating || 0) - (a.rating || 0));
        },
        playerStackedKda() {
            return this.players.map(p => ({
                label: p.nickname,
                kills: p.kills,
                deaths: p.deaths,
                assists: p.assists
            }));
        },
        playerMaxKdaTotal() {
            return Math.max(...this.playerStackedKda.map(p => (p.kills + p.deaths + p.assists) || 0), 1);
        },
        // Veto aggregates: times_banned, times_picked, times_opponent_picked, pick/ban rate, pick_win_rate (derived)
        vetoAggregatesData() {
            const raw = Array.isArray(this.seasonData?.vetoAggregates) ? this.seasonData.vetoAggregates : [];
            return raw.map(normalizeVeto).filter(Boolean);
        },
        enhancedVetoAggregates() {
            const aggregates = this.vetoAggregatesData;
            const maxPick = Math.max(...aggregates.map(e => e.timesPicked || 0), 0);
            const maxBan = Math.max(...aggregates.map(e => e.timesBanned || 0), 0);
            return aggregates.map(e => ({
                ...e,
                isTopPick: e.timesPicked === maxPick && maxPick > 0,
                isTopBan: e.timesBanned === maxBan && maxBan > 0
            }));
        },
        vetoDefaultSort() {
            return { column: 'timesPicked', order: 'desc', numeric: true };
        },
        // Veto history: match_id/map_name/status/selected_by_team_id/_name/round_num/order -> rendered as BO2/BO3 step timeline
        vetoHistory() {
            const raw = Array.isArray(this.seasonData?.vetoHistory) ? this.seasonData.vetoHistory : [];
            return raw.map(entry => ({
                matchId: entry.matchId,
                mapName: beautifyMapName(entry.mapName) || 'Kartta',
                status: (entry.status || '').toLowerCase(),
                selectedByTeamId: entry.selectedByTeamId,
                selectedByTeamName: entry.selectedByTeamName,
                roundNum: toNumber(entry.roundNum ?? entry.order),
                order: toNumber(entry.order ?? entry.roundNum)
            }));
        },
        vetoByMatch() {
            if (!this.vetoHistory.length) return [];
            const matchMap = {};
            this.matchesList.forEach(m => { matchMap[m.matchId] = m; });
            const grouped = {};
            this.vetoHistory.forEach(step => {
                const bucket = grouped[step.matchId] || [];
                bucket.push(step);
                grouped[step.matchId] = bucket;
            });
            return Object.entries(grouped).map(([matchId, steps]) => {
                const match = matchMap[matchId] || {};
                const format = this.detectSeriesFormat(match.bestOf, steps);
                const sortedSteps = [...steps].sort((a, b) => (a.order || a.roundNum || 0) - (b.order || b.roundNum || 0));
                const decorated = this.decorateVetoSteps(sortedSteps, format, match);
                return { matchId, match, format, steps: decorated };
            });
        },
        vetoSummaryLookup() {
            const lookup = {};
            this.vetoByMatch.forEach(entry => {
                lookup[entry.matchId] = entry.steps.map(s => `${s.step}. ${s.label}: ${s.mapName}`).join(' • ');
            });
            return lookup;
        },
        // Phase 1: Division averages for comparison
        divisionAverages() {
            return this.seasonData?.divisionAverages || {};
        },
        // Phase 1: Player roles
        playerRoles() {
            const roles = this.seasonData?.playerRoles || [];
            const lookup = {};
            roles.forEach(pr => {
                lookup[pr.playerId] = {
                    roles: pr.roles || [],
                    primaryRole: pr.primaryRole || null
                };
            });
            return lookup;
        },
        loading() {
            return this.pageSegment.loading;
        },
        loadError() {
            return this.pageSegment.error;
        }
    },
    watch: {
        teamId: {
            immediate: true,
            handler() {
                this.selectedChampionship = this.championshipId ? String(this.championshipId) : null;
                this.bootstrap();
            }
        },
        '$route.query.tab'(newVal) {
            const nextTab = resolveTabFromQuery({ query: { tab: newVal } });
            if (nextTab !== this.activeTab) {
                this.activeTab = nextTab;
            }
        },
        activeTab(newVal) {
            if (newVal === 'overview') {
                this.$nextTick(() => {
                    this.setupTrendChartObserver();
                });
            }
            if (newVal === 'matches') {
                this.$nextTick(() => {
                    this.setupMatchesChartObserver();
                });
            }
        },
        championshipId(newVal) {
            if (newVal) {
                this.selectedChampionship = String(newVal);
                this.fetchSeason(String(newVal), { force: true });
            }
        },
        mapViewMode() {
            this.$nextTick(() => {
                this.setupMapTableScroll();
            });
        },
        performanceTrendCharts() {
            this.$nextTick(() => {
                this.updateTrendChartWidth();
            });
        },
        matchesTrendCharts() {
            this.$nextTick(() => {
                this.updateMatchesChartWidth();
            });
        }
    },
    mounted() {
        this.$nextTick(() => {
            this.setupMapTableScroll();
            this.setupTrendChartObserver();
            this.setupMatchesChartObserver();
        });
    },
    beforeUnmount() {
        this.teardownMapTableScroll();
        this.teardownTrendChartObserver();
        this.teardownMatchesChartObserver();
    },
    methods: {
        heatTooltip(metricLabel, value, extra = '') {
            const parts = [metricLabel, `Arvo ${value}`];
            if (extra) parts.push(extra);
            parts.push('Skaalattu tämän joukkueen kartoista valitulla kaudella.');
            return parts.join(' · ');
        },
        setupMapTableScroll() {
            this.teardownMapTableScroll();
            const wrapper = this.mapViewMode === 'summary' ? this.$refs.mapSummaryWrapper : this.$refs.mapFullWrapper;
            if (!wrapper) return;
            const update = () => {
                const maxScroll = wrapper.scrollWidth - wrapper.clientWidth;
                wrapper.classList.toggle('scroll-shadow', maxScroll > 5);
                wrapper.classList.toggle('scroll-shadow-left', wrapper.scrollLeft > 6);
                wrapper.classList.toggle('scroll-shadow-right', wrapper.scrollLeft < maxScroll - 6);
            };
            wrapper.addEventListener('scroll', update, { passive: true });
            window.addEventListener('resize', update);
            update();
            this._mapScrollHandler = { wrapper, update };
        },
        teardownMapTableScroll() {
            if (!this._mapScrollHandler) return;
            const { wrapper, update } = this._mapScrollHandler;
            wrapper.removeEventListener('scroll', update);
            window.removeEventListener('resize', update);
            this._mapScrollHandler = null;
        },
        formatSubMetric(perRound, perMap, decimals = 3) {
            if (this.mapSubMetricMode === 'perMap') {
                return formatPerRound(perMap, decimals);
            }
            return formatPerRound(perRound, decimals);
        },
        subMetricValue(perRound, perMap, decimals = 3) {
            return this.formatSubMetric(perRound, perMap, decimals);
        },
        shouldShowSubMetric(perRound, perMap) {
            const value = this.mapSubMetricMode === 'perMap'
                ? toNumber(perMap)
                : toNumber(perRound);
            return value > 0;
        },
        subMetricLabel() {
            return this.mapSubMetricMode === 'perMap' ? 'per-kartta' : 'per-erä';
        },
        setTrendMode(mode) {
            if (mode !== 'map' && mode !== 'cumulative') return;
            this.performanceTrendMode = mode;
        },
        updateTrendChartWidth() {
            const panel = this.$refs.performanceTrendPanel;
            if (!panel) return;
            const styles = window.getComputedStyle(panel);
            const paddingLeft = parseFloat(styles.paddingLeft) || 0;
            const paddingRight = parseFloat(styles.paddingRight) || 0;
            const width = panel.clientWidth - paddingLeft - paddingRight;
            if (width > 0 && Math.abs(width - this.trendChartWidth) > 1) {
                this.trendChartWidth = width;
            }
        },
        updateMatchesChartWidth() {
            const panel = this.$refs.matchesTrendPanel;
            if (!panel) return;
            const styles = window.getComputedStyle(panel);
            const paddingLeft = parseFloat(styles.paddingLeft) || 0;
            const paddingRight = parseFloat(styles.paddingRight) || 0;
            const width = panel.clientWidth - paddingLeft - paddingRight;
            if (width > 0 && Math.abs(width - this.matchesChartWidth) > 1) {
                this.matchesChartWidth = width;
            }
        },
        setupTrendChartObserver() {
            if (this._trendResizeObserver || typeof ResizeObserver === 'undefined') return;
            const panel = this.$refs.performanceTrendPanel;
            if (!panel) return;
            const update = () => window.requestAnimationFrame(() => this.updateTrendChartWidth());
            this._trendResizeObserver = new ResizeObserver(update);
            this._trendResizeObserver.observe(panel);
            update();
        },
        setupMatchesChartObserver() {
            if (this._matchesResizeObserver || typeof ResizeObserver === 'undefined') return;
            const panel = this.$refs.matchesTrendPanel;
            if (!panel) return;
            const update = () => window.requestAnimationFrame(() => this.updateMatchesChartWidth());
            this._matchesResizeObserver = new ResizeObserver(update);
            this._matchesResizeObserver.observe(panel);
            update();
        },
        teardownTrendChartObserver() {
            if (!this._trendResizeObserver) return;
            this._trendResizeObserver.disconnect();
            this._trendResizeObserver = null;
        },
        teardownMatchesChartObserver() {
            if (!this._matchesResizeObserver) return;
            this._matchesResizeObserver.disconnect();
            this._matchesResizeObserver = null;
        },
        handleTrendHover(event, chart) {
            if (!chart || !chart.points?.length) return;
            const rect = event.currentTarget.getBoundingClientRect();
            const x = event.clientX - rect.left;
            const scale = rect.width ? chart.width / rect.width : 1;
            const xView = x * scale;
            const ratio = clampValue((xView - chart.padding.left) / chart.plotWidth, 0, 1);
            const index = Math.round(ratio * (chart.points.length - 1));
            const point = chart.points[index];
            if (!point) return;
            this.performanceTrendHover = {
                key: chart.key,
                index,
                x: point.x,
                y: point.y
            };
        },
        clearTrendHover() {
            this.performanceTrendHover = { key: null, index: null, x: 0, y: 0 };
        },
        handleMatchesTrendHover(event) {
            const chart = this.matchesWinLossChart;
            if (!chart || !chart.basePoints?.length) return;
            const rect = event.currentTarget.getBoundingClientRect();
            const x = event.clientX - rect.left;
            const scale = rect.width ? chart.width / rect.width : 1;
            const xView = x * scale;
            const ratio = clampValue((xView - chart.padding.left) / chart.plotWidth, 0, 1);
            const index = Math.round(ratio * (chart.basePoints.length - 1));
            const refSeries = chart.series?.[0];
            const point = (refSeries?.points?.[index]) || chart.basePoints[index];
            if (!point) return;
            this.matchesTrendHover = { index, x: point.x, y: point.y };
            this.matchesHoverMatchId = point.matchId;
            this.matchesHoverSource = 'chart';
        },
        clearMatchesTrendHover() {
            this.matchesTrendHover = { index: null, x: 0, y: 0 };
            if (this.matchesHoverSource === 'chart') {
                this.matchesHoverMatchId = null;
                this.matchesHoverSource = null;
            }
        },
        setMatchesHover(matchId) {
            this.matchesHoverMatchId = matchId;
            this.matchesHoverSource = 'table';
        },
        clearMatchesHover() {
            if (this.matchesHoverSource === 'table') {
                this.matchesHoverMatchId = null;
                this.matchesHoverSource = null;
            }
        },
        matchTooltipStyle(chart, point) {
            if (!chart || !point) return {};
            const left = (point.x / chart.width) * 100;
            const top = (point.y / chart.height) * 100;
            return {
                left: `${left}%`,
                top: `${top}%`
            };
        },
        matchMetricLabel(metric, value) {
            if (!metric) return formatNumber(value, 1);
            return metric.format ? metric.format(value) : formatNumber(value, metric.decimals || 0);
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
        getTrendHoverPoint(chart) {
            if (!chart || this.performanceTrendHover.key !== chart.key) return null;
            const idx = this.performanceTrendHover.index;
            return chart.points?.[idx] || null;
        },
        formatTrendDelta(metric, value) {
            if (!metric) return formatSignedNumber(value, 2);
            if (metric.key === 'rd') return formatSignedNumber(value, 0);
            if (metric.key === 'net') return formatSignedNumber(value, 0);
            if (metric.key === 'adr') return formatSignedNumber(value, 1);
            return formatSignedNumber(value, 2);
        },
        matchesTrendValueLabel(chart, point) {
            if (!chart || !point) return '';
            if (chart.key === 'rd' || chart.key === 'net') return `${chart.label} tilanne`;
            return chart.label;
        },
        matchesTooltipTrendValue(chart, point) {
            if (!chart || !point) return '-';
            return this.formatTrendValue(chart, point.value);
        },
        trendValueLabel(chart, point) {
            if (!chart || !point) return '';
            if (this.performanceTrendMode === 'cumulative') {
                if (chart.key === 'rd') return `${chart.label} tilanne`;
                return `${chart.label} avg`;
            }
            return chart.label;
        },
        tooltipTrendValue(chart, point) {
            if (!chart || !point) return '-';
            if (chart.key !== 'rd' && point.isForfeit) return '—';
            return this.formatTrendValue(chart, point.value);
        },
        mapResultLabel(point) {
            if (!point) return '';
            if (point.result === 'win') return 'Voitto';
            if (point.result === 'loss') return 'Tappio';
            if (point.result === 'draw') return 'Tasapeli';
            return '';
        },
        formatTrendValue(metric, value) {
            if (!metric) return formatNumber(value, 2);
            return metric.format ? metric.format(value) : formatNumber(value, metric.decimals || 0);
        },
        async bootstrap() {
            if (!this.teamStore || !this.teamId) return;
            try {
                const data = await this.teamStore.fetchTeamPage(this.teamId, this.selectedChampionship);
                if (data?.currentChampionshipId) {
                    this.selectedChampionship = String(data.currentChampionshipId);
                    this.updateRoute(this.selectedChampionship, this.activeTab);
                }
            } catch (err) {
                console.error('TeamDetail bootstrap failed', err);
            }
        },
        async fetchSeason(championshipId, options = {}) {
            if (!this.teamStore || !this.teamId || !championshipId) return;
            try {
                await this.teamStore.fetchTeamPage(this.teamId, championshipId, options);
            } catch (err) {
                console.error('TeamDetail season fetch failed', err);
            }
        },
        selectChampionship(championshipId) {
            if (!championshipId || championshipId === this.currentChampionshipId) return;
            this.selectedChampionship = championshipId;
            this.fetchSeason(championshipId);
            this.updateRoute(championshipId, this.activeTab);
        },
        updateRoute(championshipId, tab) {
            if (!this.$router || !this.$route) return;
            const params = { ...(this.$route.params || {}), teamId: this.teamId };
            const query = { ...(this.$route.query || {}) };
            if (championshipId) {
                query.championship = championshipId;
            } else {
                delete query.championship;
            }
            const normalizedTab = tab || this.activeTab;
            if (normalizedTab && normalizedTab !== 'overview') {
                query.tab = normalizedTab;
            } else {
                delete query.tab;
            }
            this.$router.replace({
                name: this.$route.name || 'team',
                params,
                query
            }).catch(() => {});
        },
        selectTab(tab) {
            this.activeTab = tab;
            this.updateRoute(this.currentChampionshipId, tab);
        },
        resetMapSort() {
            this.scoutTableKey += 1;
            this.detailedTableKey += 1;
        },
        formatWinLoss(wins, losses) {
            return `${formatNumber(wins)}–${formatNumber(losses)}`;
        },
        winHeatStyle(value) {
            const pct = Math.min(100, Math.max(0, normalizePercent(value)));
            const hue = (pct / 100) * 120;
            const color = `hsla(${hue.toFixed(1)}, 60%, 45%, 0.22)`;
            return {
                background: `linear-gradient(90deg, ${color}, transparent)`
            };
        },
        kdHeatStyle(value) {
            const kd = toNumber(value);
            const pct = Math.min(100, Math.max(0, (kd / 2) * 100));
            const hue = (pct / 100) * 120;
            const color = `hsla(${hue.toFixed(1)}, 60%, 45%, 0.22)`;
            return {
                background: `linear-gradient(90deg, ${color}, transparent)`
            };
        },
        adrHeatStyle(value) {
            const adr = toNumber(value);
            const pct = Math.min(100, Math.max(0, (adr / 120) * 100));
            const hue = (pct / 100) * 120;
            const color = `hsla(${hue.toFixed(1)}, 60%, 45%, 0.22)`;
            return {
                background: `linear-gradient(90deg, ${color}, transparent)`
            };
        },
        rdHeatStyle(value) {
            const rd = toNumber(value);
            const maxAbs = this.mapMaxRoundDiff || 1;
            const pct = Math.min(1, Math.max(0, (rd + maxAbs) / (maxAbs * 2)));
            const hue = pct * 120;
            const color = `hsla(${hue.toFixed(1)}, 60%, 45%, 0.22)`;
            return {
                background: `linear-gradient(90deg, ${color}, transparent)`
            };
        },
        detectSeriesFormat(bestOf, steps = []) {
            if (Number(bestOf) === 2) return 'bo2';
            if (Number(bestOf) === 3) return 'bo3';
            const statuses = (steps || []).map(s => (s.status || s.action || '').toLowerCase());
            if (statuses.some(st => st.includes('overflow'))) return 'bo2';
            if (statuses.some(st => st.includes('decider'))) return 'bo3';
            const stepCount = Array.isArray(steps) ? steps.length : 0;
            if (stepCount === 6) return 'bo2';
            return stepCount >= 6 ? 'bo3' : 'bo2';
        },
        decorateVetoSteps(steps, format, match = {}) {
            const expected = format === 'bo3'
                ? ['ban', 'ban', 'pick', 'pick', 'ban', 'ban']
                : ['ban', 'ban', 'ban', 'ban', 'pick', 'pick'];
            const teamName = this.teamInfo?.teamName || 'Oma joukkue';
            const opponentName = match?.opponentName || match?.team2Name || 'Vastustaja';

            const normalized = (steps || []).map((step, idx) => {
                const raw = (step.status || step.action || '').toLowerCase();
                let action = null;
                if (raw.includes('ban') || raw === 'drop' || raw === 'banned') action = 'ban';
                else if (raw.includes('pick')) action = 'pick';
                else if (raw.includes('decider')) action = 'decider';
                else if (raw.includes('overflow')) action = 'overflow';
                if (!action && expected[idx]) action = expected[idx];
                if (!action) action = 'ban';
                const actor = step.selectedByTeamId
                    ? (String(step.selectedByTeamId) === String(this.teamId) ? 'team' : 'opponent')
                    : 'system';
                const mapName = beautifyMapName(step.mapName) || 'Kartta';
                return {
                    ...step,
                    action,
                    actor,
                    step: idx + 1,
                    label: this.actionLabel(action),
                    mapName,
                    teamName: step.selectedByTeamName || (actor === 'team' ? teamName : opponentName)
                };
            });

            const usedNames = new Set(normalized.map(s => s.mapName).filter(Boolean));
            const hasDecider = normalized.some(s => s.action === 'decider');
            const hasOverflow = normalized.some(s => s.action === 'overflow');
            if (format === 'bo3' && !hasDecider) {
                const deciderName = this.resolveDeciderMap(match, usedNames);
                normalized.push({
                    action: 'decider',
                    actor: 'system',
                    step: normalized.length + 1,
                    label: 'Decider',
                    mapName: deciderName,
                    teamName: 'Decider'
                });
                usedNames.add(deciderName);
            }
            if (format === 'bo2' && !hasOverflow) {
                const overflowName = this.resolveOverflowMap(usedNames);
                normalized.push({
                    action: 'overflow',
                    actor: 'system',
                    step: normalized.length + 1,
                    label: 'Overflow',
                    mapName: overflowName,
                    teamName: 'Overflow'
                });
            }
            return normalized.map((step, idx) => ({ ...step, step: idx + 1 }));
        },
        resolveDeciderMap(match, usedNames = new Set()) {
            const maps = Array.isArray(match?.maps) ? match.maps : [];
            const leftover = maps.find(m => !m.pickTeamId && !usedNames.has(m.mapName)) || maps.find(m => !usedNames.has(m.mapName));
            if (leftover?.mapName) return leftover.mapName;
            const fromPool = this.mapStats.find(m => !usedNames.has(m.mapName));
            return fromPool?.mapName || 'Decider';
        },
        resolveOverflowMap(usedNames = new Set()) {
            const pool = new Set();
            this.mapStats.forEach(m => { if (m.mapName) pool.add(m.mapName); });
            this.vetoAggregatesData.forEach(v => { if (v.mapName) pool.add(v.mapName); });
            const candidate = Array.from(pool).find(name => !usedNames.has(name));
            return candidate || 'Overflow';
        },
        actionLabel(action) {
            if (action === 'pick') return 'Pick';
            if (action === 'ban') return 'Ban';
            if (action === 'decider') return 'Decider';
            return 'Overflow';
        },
        formatPercent,
        formatNumber,
        formatDate,
        getMatchResult,
        teamLogo() {
            return this.teamInfo?.avatar || '';
        },
        getPlayerRoleBadges(playerId) {
            const roleData = this.playerRoles[playerId];
            if (!roleData || !roleData.roles || roleData.roles.length === 0) return [];
            return roleData.roles.map(role => ({
                label: role,
                isPrimary: role === roleData.primaryRole
            }));
        },
        getPerformanceBadge(teamValue, divAvg, metricKey) {
            if (!divAvg || teamValue == null) return null;
            // Skip badges for zero-sum metrics where avg is always ~50%
            if (metricKey === 'winrate' || metricKey === 'mapWinRate') return null;
            const diff = teamValue - divAvg;
            if (Math.abs(diff) < 0.5) return null;
            const pct = ((Math.abs(diff) / divAvg) * 100).toFixed(0);
            return {
                type: diff > 0 ? 'positive' : 'negative',
                label: `${diff > 0 ? '+' : ''}${pct}%`
            };
        },
    },
    template: `
        <div class="team-detail">
            <loading-spinner v-if="loading && !teamInfo" message="Joukkuetta ladataan..."></loading-spinner>
            <error-message v-else-if="loadError && !teamInfo" :message="loadError" @retry="bootstrap"></error-message>
            <div v-else>
                <header class="team-hero">
                    <div class="team-hero__logo" v-if="teamLogo()">
                        <img :src="teamLogo()" :alt="teamInfo?.teamName || 'Joukkue'" />
                    </div>
                    <div class="team-hero__content">
                        <h1 class="team-hero__title title-accent titleUnderlinePage">
                            {{ teamInfo?.displayName || teamInfo?.teamName || 'Joukkue' }}
                        </h1>
                        <div class="team-hero__season" v-if="seasonOptions.length || heroPlayoffsFlag || teamInfo?.faceitUrl">
                            <span class="pill pill--accent" v-if="heroPlayoffsFlag">Playoffs</span>
                            <a v-if="teamInfo?.faceitUrl" class="pill pill--link" :href="teamInfo?.faceitUrl" target="_blank" rel="noopener">Faceit</a>
                        </div>
                    </div>
                    <div v-if="seasonOptions.length" class="team-season-selector">
                        <label class="season-select-label" for="season-select">Valitse kausi</label>
                        <select
                            id="season-select"
                            class="season-select"
                            :value="currentChampionshipId"
                            @change="selectChampionship($event.target.value)"
                        >
                            <option v-for="season in seasonOptions" :key="season.value" :value="season.value">
                                {{ season.label }}
                            </option>
                        </select>
                    </div>
                </header>

                <nav class="team-tabs" role="tablist">
                    <button
                        v-for="tab in ['overview', 'matches', 'players', 'veto']"
                        :key="tab"
                        type="button"
                        class="team-tab"
                        :class="{ 'team-tab--active': activeTab === tab }"
                        @click="selectTab(tab)"
                        role="tab"
                        :aria-selected="activeTab === tab"
                        :aria-controls="'team-tab-' + tab"
                    >
                        {{ { overview: 'Yleiskuva', matches: 'Ottelut', players: 'Pelaajat', veto: 'Veto/Nosto' }[tab] }}
                    </button>
                </nav>

                <section v-if="activeTab === 'overview'" class="team-section scout-view" id="team-tab-overview" role="tabpanel">
                    <div class="scout-panel scout-snapshot">
                        <div class="section-heading">
                            <div>
                            <h2 class="section-title titleUnderline">Kauden yleiskuva</h2>
                            <span class="section-sub">Valittu kausi · ydintilastot</span>
                            </div>
                        </div>
                        <div class="scout-snapshot-tier scout-snapshot-tier--primary">
                            <div v-for="stat in seasonSnapshotStats.primary" :key="stat.key" class="scout-snapshot-item scout-snapshot-item--primary">
                                <div class="snapshot-label">
                                    {{ stat.label }}
                                    <span
                                        v-if="getPerformanceBadge(stat.trendValue, stat.divAvg, stat.key)"
                                        class="performance-badge"
                                        :class="'performance-badge--' + getPerformanceBadge(stat.trendValue, stat.divAvg, stat.key).type"
                                        :title="'Ero divisioonan keskiarvoon'"
                                    >{{ getPerformanceBadge(stat.trendValue, stat.divAvg, stat.key).label }}</span>
                                </div>
                                <div class="snapshot-value mono-num" :class="stat.tone" :title="stat.tooltip || ''">{{ stat.value }}</div>
                                <div class="snapshot-sub">{{ stat.sub }}</div>
                            </div>
                        </div>
                        <div class="scout-snapshot-tier scout-snapshot-tier--secondary">
                            <div v-for="stat in seasonSnapshotStats.secondary" :key="stat.key" class="scout-snapshot-item scout-snapshot-item--secondary">
                                <div class="snapshot-label">{{ stat.label }}</div>
                                <div class="snapshot-value mono-num" :class="stat.tone" :title="stat.tooltip || ''">{{ stat.value }}</div>
                                <div class="snapshot-sub">{{ stat.sub }}</div>
                            </div>
                        </div>
                    </div>

                    <div class="scout-panel scout-veto">
                        <div class="section-heading">
                            <div>
                        <h3 class="section-title titleUnderline">Veto-historia</h3>
                        <span class="section-sub">Pick/Ban otteluittain (uusin → vanhin)</span>
                            </div>
                            <div class="section-heading-actions">
                            </div>
                        </div>
                        <div v-if="vetoTrendRows.length" class="veto-heatmap">
                            <div class="veto-heatmap__header">
                                <div class="veto-heatmap__corner">Kartta</div>
                                <div class="veto-heatmap__cols">
                                <div
                                    v-for="(col, idx) in vetoTrendColumns"
                                    :key="col.id"
                                    class="veto-heatmap__col"
                                    :class="{ 'veto-heatmap__col--divider': (idx + 1) % 4 === 0 }"
                                    :title="col.title"
                                >
                                    <span class="veto-heatmap__col-label">{{ col.label }}</span>
                                </div>
                            </div>
                        </div>
                            <div v-for="row in vetoTrendRows" :key="row.mapName" class="veto-heatmap__row">
                                <div class="veto-heatmap__row-label">{{ row.rowLabel }}</div>
                                <div class="veto-heatmap__cells">
                                    <div
                                        v-for="(cell, idx) in row.cells"
                                        :key="row.mapName + '-' + idx"
                                        class="veto-heatmap__cell"
                                        :class="[cell.className, ((idx + 1) % 4 === 0) ? 'veto-heatmap__cell--divider' : '']"
                                        :title="cell.title"
                                    >
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div v-if="vetoTrendRows.length" class="veto-heatmap-legend veto-heatmap-legend--section">
                            <div class="veto-legend-title">Selite</div>
                            <div class="veto-legend-grid">
                                <div v-for="entry in vetoLegendEntries" :key="entry.label" class="veto-legend-item">
                                    <div class="veto-heatmap__cell veto-legend-cell" :class="entry.className"></div>
                                    <span class="veto-legend-label">{{ entry.label }}</span>
                                </div>
                            </div>
                        </div>
                        <div v-else class="empty-state-container compact">
                            <div class="empty-state-card">
                                <h3 class="empty-state-title">Ei vetoa</h3>
                                <p class="empty-state-description">Tälle kaudelle ei ole veto-historiaa.</p>
                            </div>
                        </div>
                    </div>

                    <div class="scout-panel scout-table">
                        <div class="section-heading">
                            <div>
                        <h3 class="section-title titleUnderline">Karttakohtainen suorituskyky</h3>
                        <span class="section-sub">{{ mapViewMode === 'summary' ? 'Yhteenveto: Voitot, pickit, bannit, eräero' : 'Laaja: Karttakohtaiset pelaajatilastot' }}</span>
                        <div v-if="mapViewMode === 'full'" class="section-legend">📊 <strong>Pääarvo</strong> = kokonaisluku · <strong>Sulkeissa</strong> = {{ subMetricLabel() }}</div>
                            </div>
                            <div class="section-heading-actions">
                                <div v-if="mapViewMode === 'full'" class="submetric-toggle">
                                    <button
                                        type="button"
                                        class="btn-submetric"
                                        :class="mapSubMetricMode === 'perRound' ? 'active' : ''"
                                        @click="mapSubMetricMode = 'perRound'"
                                    >Per-erä</button>
                                    <button
                                        type="button"
                                        class="btn-submetric"
                                        :class="mapSubMetricMode === 'perMap' ? 'active' : ''"
                                        @click="mapSubMetricMode = 'perMap'"
                                    >Per-kartta</button>
                                </div>
                                <button 
                                    type="button" 
                                    class="btn-toggle-view" 
                                    @click="mapViewMode = mapViewMode === 'summary' ? 'full' : 'summary'"
                                    :title="mapViewMode === 'summary' ? 'Näytä karttakohtaiset pelaajatilastot' : 'Näytä voitto/pick/ban yhteenveto'"
                                >
                                    <span v-if="mapViewMode === 'summary'">Laaja näkymä</span>
                                    <span v-else>Yhteenveto</span>
                                </button>
                            </div>
                        </div>
                        <div>
                            <button type="button" class="btn-reset-sort" @click="resetMapSort">Nollaa lajittelu</button>
                        </div>
                        <div v-if="mapViewMode === 'summary'" ref="mapSummaryWrapper" class="table-wrapper table-wrapper--scroll">
                            <sortable-table
                                :key="scoutTableKey"
                                :columns="SCOUT_MAP_COLUMNS"
                                :header-groups="scoutHeaderGroups"
                                :data="scoutMapRows"
                                :default-sort="scoutMapDefaultSort"
                                :sticky-header="true"
                                :compact="true"
                                class="map-summary-table"
                            >
                                <template #cell-mapName="{ row }">
                                    <div class="map-name">
                                        <span class="map-name-text">{{ row.mapName }}</span>
                                    </div>
                                </template>
                                <template #cell-played="{ row }">
                                    <div class="scout-cell mono-num" :class="row.played <= 2 ? 'mono-muted' : ''">{{ row.played }}</div>
                                </template>
                                <template #cell-picks="{ row }">
                                    <div class="scout-cell mono-num">{{ row.picks }}</div>
                                </template>
                                <template #cell-oppPicks="{ row }">
                                    <div class="scout-cell mono-num">{{ row.oppPicks }}</div>
                                </template>
                                <template #cell-winrate="{ row }">
                                    <div
                                        v-if="row.played > 0"
                                        class="scout-cell mono-num"
                                        :style="winHeatStyle(row.winrate)"
                                        :title="heatTooltip('Win %', formatPercent(row.winrate || 0, 1), 'W–L ' + formatWinLoss(row.wins || 0, row.losses || 0))"
                                    >
                                        {{ formatPercent(row.winrate || 0, 1) }} ({{ formatWinLoss(row.wins || 0, row.losses || 0) }})
                                    </div>
                                    <span v-else class="cell-muted mono-num" title="Ei dataa valitulle kaudelle.">—</span>
                                </template>
                                <template #cell-pickWinRate="{ row }">
                                    <div
                                        v-if="row.picks > 0"
                                        class="scout-cell mono-num"
                                        :style="winHeatStyle(row.pickWinRate)"
                                        :title="heatTooltip('Win % (oma pick)', formatPercent(row.pickWinRate || 0, 1), 'W–L ' + formatWinLoss(row.pickWins || 0, Math.max(0, (row.picks || 0) - (row.pickWins || 0))))"
                                    >
                                        {{ formatPercent(row.pickWinRate || 0, 1) }} ({{ formatWinLoss(row.pickWins || 0, Math.max(0, (row.picks || 0) - (row.pickWins || 0))) }})
                                    </div>
                                    <span v-else class="cell-muted mono-num" title="Ei dataa valitulle kaudelle.">—</span>
                                </template>
                                <template #cell-oppPickWinRate="{ row }">
                                    <div
                                        v-if="row.oppPicks > 0"
                                        class="scout-cell mono-num"
                                        :style="winHeatStyle(row.oppPickWinRate)"
                                        :title="heatTooltip('Win % (vastustajan pick)', formatPercent(row.oppPickWinRate || 0, 1), 'W–L ' + formatWinLoss(row.oppPickWins || 0, Math.max(0, (row.oppPicks || 0) - (row.oppPickWins || 0))))"
                                    >
                                        {{ formatPercent(row.oppPickWinRate || 0, 1) }} ({{ formatWinLoss(row.oppPickWins || 0, Math.max(0, (row.oppPicks || 0) - (row.oppPickWins || 0))) }})
                                    </div>
                                    <span v-else class="cell-muted mono-num" title="Ei dataa valitulle kaudelle.">—</span>
                                </template>
                                <template #cell-kd="{ row }">
                                    <div
                                        v-if="row.played > 0"
                                        class="scout-cell mono-num"
                                        :style="kdHeatStyle(row.kd)"
                                        :title="heatTooltip('K/D', formatNumber(row.kd, 2))"
                                    >{{ formatNumber(row.kd, 2) }}</div>
                                    <span v-else class="cell-muted mono-num" title="Ei dataa valitulle kaudelle.">—</span>
                                </template>
                                <template #cell-adr="{ row }">
                                    <div
                                        v-if="row.played > 0"
                                        class="scout-cell mono-num"
                                        :style="adrHeatStyle(row.adr)"
                                        :title="heatTooltip('ADR', formatNumber(row.adr, 1))"
                                    >{{ formatNumber(row.adr, 1) }}</div>
                                    <span v-else class="cell-muted mono-num" title="Ei dataa valitulle kaudelle.">—</span>
                                </template>
                                <template #cell-rd="{ row }">
                                    <div
                                        v-if="row.played > 0"
                                        class="scout-cell mono-num"
                                        :style="rdHeatStyle(row.rd)"
                                        :title="heatTooltip('Eräero', formatNumber(row.rd, 0))"
                                    >{{ formatNumber(row.rd, 0) }}</div>
                                    <span v-else class="cell-muted mono-num" title="Ei dataa valitulle kaudelle.">—</span>
                                </template>
                            </sortable-table>
                        </div>

                        <div v-if="mapViewMode === 'full'" ref="mapFullWrapper" class="table-wrapper table-wrapper--scroll">
                            <sortable-table
                                v-if="mapStats.length"
                                :key="detailedTableKey"
                                :columns="MAP_COLUMNS"
                                :header-groups="mapHeaderGroups"
                                :data="mapStats"
                                :default-sort="mapDefaultSort"
                                :sticky-header="true"
                                :compact="true"
                                class="map-full-table"
                            >
                                <template #cell-mapName="{ row }">
                                    <div class="map-name">
                                        <span class="map-name-text">{{ row.mapName }}</span>
                                    </div>
                                </template>
                                <template #cell-totalRoundsPlayed="{ row }">
                                    <div class="scout-cell scout-cell--stacked mono-num">
                                        <div>{{ formatNumber(row.totalRoundsPlayed, 0) }}</div>
                                        <div
                                            v-if="row.roundsWon || row.roundsLost"
                                            class="rounds-breakdown"
                                        >
                                            <span class="rounds-won">+{{ formatNumber(row.roundsWon, 0) }}</span>
                                            <span class="rounds-sep">/</span>
                                            <span class="rounds-lost">-{{ formatNumber(row.roundsLost, 0) }}</span>
                                            <span
                                                class="rounds-diff"
                                                :class="(row.roundsWon - row.roundsLost) >= 0 ? 'rounds-diff--pos' : 'rounds-diff--neg'"
                                            >
                                                ({{ (row.roundsWon - row.roundsLost) >= 0 ? '+' : '' }}{{ formatNumber(row.roundsWon - row.roundsLost, 0) }})
                                            </span>
                                        </div>
                                    </div>
                                </template>
                                <template #cell-roundsPerMapAvg="{ row }">
                                    <div class="scout-cell mono-num">{{ formatNumber(row.roundsPerMapAvg, 1) }}</div>
                                </template>
                                <template #cell-adr="{ row }">
                                    <div
                                        class="scout-cell mono-num"
                                        :style="adrHeatStyle(row.adr)"
                                        :title="heatTooltip('ADR', formatNumber(row.adr, 1))"
                                    >{{ formatNumber(row.adr, 1) }}</div>
                                </template>
                                <template #cell-kr="{ row }">
                                    <div class="scout-cell mono-num">{{ formatNumber(row.kr, 3) }}</div>
                                </template>
                                <template #cell-kd="{ row }">
                                    <div
                                        class="scout-cell mono-num"
                                        :style="kdHeatStyle(row.kd)"
                                        :title="heatTooltip('K/D', formatNumber(row.kd, 2))"
                                    >{{ formatNumber(row.kd, 2) }}</div>
                                </template>
                                <template #cell-hsPct="{ row }">
                                    <div class="scout-cell mono-num">{{ formatNumber(row.hsPct, 1) }}</div>
                                </template>
                                <template #cell-kills="{ row }">
                                    <div class="scout-cell mono-num">{{ formatNumber(row.kills, 0) }}</div>
                                </template>
                                <template #cell-deaths="{ row }">
                                    <div class="scout-cell mono-num">{{ formatNumber(row.deaths, 0) }}</div>
                                </template>
                                <template #cell-assists="{ row }">
                                    <div class="scout-cell mono-num">{{ formatNumber(row.assists, 0) }}</div>
                                </template>
                                <template #cell-udpr="{ row }">
                                    <div class="scout-cell mono-num">{{ formatNumber(row.udpr, 1) }}</div>
                                </template>
                                <template #cell-mvps="{ row }">
                                    <div class="scout-cell mono-num">{{ formatNumber(row.mvps, 0) }}</div>
                                </template>
                                <template #cell-enemiesFlashed="{ row }">
                                    <div class="scout-cell mono-num">{{ formatNumber(row.enemiesFlashed, 0) }}</div>
                                </template>
                                <template #cell-flashSuccessPct="{ row }">
                                    <div class="scout-cell mono-num">{{ formatNumber(row.flashSuccessPct, 1) }}</div>
                                </template>
                                <template #cell-flashCount="{ row }">
                                    <div class="scout-cell mono-num">{{ formatNumber(row.flashCount, 0) }}</div>
                                </template>
                                <template #cell-multi2k="{ row }">
                                    <div class="scout-cell scout-cell--stacked mono-num">
                                        <div>{{ formatNumber(row.multi2k, 0) }}</div>
                                        <div v-if="shouldShowSubMetric(row.multi2kPerRound, row.multi2kPerMap)" class="sub-metric-line">
                                            ({{ subMetricValue(row.multi2kPerRound, row.multi2kPerMap, 3) }})
                                        </div>
                                    </div>
                                </template>
                                <template #cell-multi3k="{ row }">
                                    <div class="scout-cell scout-cell--stacked mono-num">
                                        <div>{{ formatNumber(row.multi3k, 0) }}</div>
                                        <div v-if="shouldShowSubMetric(row.multi3kPerRound, row.multi3kPerMap)" class="sub-metric-line">
                                            ({{ subMetricValue(row.multi3kPerRound, row.multi3kPerMap, 3) }})
                                        </div>
                                    </div>
                                </template>
                                <template #cell-multi4k="{ row }">
                                    <div class="scout-cell scout-cell--stacked mono-num">
                                        <div>{{ formatNumber(row.multi4k, 0) }}</div>
                                        <div v-if="shouldShowSubMetric(row.multi4kPerRound, row.multi4kPerMap)" class="sub-metric-line">
                                            ({{ subMetricValue(row.multi4kPerRound, row.multi4kPerMap, 3) }})
                                        </div>
                                    </div>
                                </template>
                                <template #cell-multi5k="{ row }">
                                    <div class="scout-cell scout-cell--stacked mono-num">
                                        <div>{{ formatNumber(row.multi5k, 0) }}</div>
                                        <div v-if="shouldShowSubMetric(row.multi5kPerRound, row.multi5kPerMap)" class="sub-metric-line">
                                            ({{ subMetricValue(row.multi5kPerRound, row.multi5kPerMap, 3) }})
                                        </div>
                                    </div>
                                </template>
                                <template #cell-pistolKills="{ row }">
                                    <div class="scout-cell scout-cell--stacked mono-num">
                                        <div>{{ formatNumber(row.pistolKills, 0) }}</div>
                                        <div v-if="shouldShowSubMetric(row.pistolKillsPerRound, row.pistolKillsPerMap)" class="sub-metric-line">
                                            ({{ subMetricValue(row.pistolKillsPerRound, row.pistolKillsPerMap, 3) }})
                                        </div>
                                    </div>
                                </template>
                                <template #cell-sniperKills="{ row }">
                                    <div class="scout-cell scout-cell--stacked mono-num">
                                        <div>{{ formatNumber(row.sniperKills, 0) }}</div>
                                        <div v-if="shouldShowSubMetric(row.sniperKillsPerRound, row.sniperKillsPerMap)" class="sub-metric-line">
                                            ({{ subMetricValue(row.sniperKillsPerRound, row.sniperKillsPerMap, 3) }})
                                        </div>
                                    </div>
                                </template>
                                <template #cell-totalDamage="{ row }">
                                    <div class="scout-cell scout-cell--stacked mono-num">
                                        <div>{{ formatNumber(row.totalDamage, 0) }}</div>
                                        <div v-if="shouldShowSubMetric(row.totalDamagePerRound, row.totalDamagePerMap)" class="sub-metric-line">
                                            ({{ subMetricValue(row.totalDamagePerRound, row.totalDamagePerMap, 1) }})
                                        </div>
                                    </div>
                                </template>
                                <template #cell-clutchKills="{ row }">
                                    <div class="scout-cell mono-num">{{ formatNumber(row.clutchKills, 0) }}</div>
                                </template>
                            </sortable-table>
                            <div v-else class="empty-state-container">
                                <div class="empty-state-card">
                                    <h3 class="empty-state-title">Ei karttadataa</h3>
                                    <p class="empty-state-description">Tälle kaudelle ei ole karttakohtaisia tilastoja.</p>
                                </div>
                            </div>
                        </div>
                    </div>

                    <div class="scout-panel scout-performance-trends" ref="performanceTrendPanel">
                        <div class="section-heading section-heading--split">
                            <div class="section-heading__main">
                                <h3 class="section-title titleUnderline">Kauden statsien kehitys</h3>
                                <span class="section-sub">X-akseli: kartat ottelujärjestyksessä · ADR, RD+, K/D</span>
                            </div>
                            <div class="section-heading-actions" v-if="performanceTrendCharts.length">
                                <div class="trend-toggles trend-toggles--mode">
                                    <button
                                        type="button"
                                        class="trend-toggle"
                                        :class="{ 'trend-toggle--active': performanceTrendMode === 'map' }"
                                        @click="setTrendMode('map')"
                                        :aria-pressed="performanceTrendMode === 'map' ? 'true' : 'false'"
                                    >
                                        Kartta
                                    </button>
                                    <button
                                        type="button"
                                        class="trend-toggle"
                                        :class="{ 'trend-toggle--active': performanceTrendMode === 'cumulative' }"
                                        @click="setTrendMode('cumulative')"
                                        :aria-pressed="performanceTrendMode === 'cumulative' ? 'true' : 'false'"
                                    >
                                        Kausi
                                    </button>
                                </div>
                            </div>
                        </div>
                        <div v-if="performanceTrendCharts.length" class="performance-trends">
                            <div
                                v-for="chart in performanceTrendVisibleCharts"
                                :key="chart.key"
                                class="trend-chart"
                                :class="'trend-chart--' + chart.key"
                            >
                                <div
                                    class="trend-chart__plot"
                                    @mousemove="handleTrendHover($event, chart)"
                                    @mouseleave="clearTrendHover"
                                >
                                    <svg
                                        class="trend-chart__svg"
                                        :viewBox="'0 0 ' + chart.width + ' ' + chart.height"
                                        width="100%"
                                        :height="chart.height"
                                        role="img"
                                        :aria-label="chart.label + ' trendi'"
                                    >
                                        <defs v-if="chart.zeroY != null">
                                            <clipPath :id="'m-pos-' + chart.key">
                                                <rect
                                                    :x="chart.padding.left"
                                                    :y="chart.padding.top"
                                                    :width="chart.plotWidth"
                                                    :height="Math.max(0, chart.zeroY - chart.padding.top)"
                                                />
                                            </clipPath>
                                            <clipPath :id="'m-neg-' + chart.key">
                                                <rect
                                                    :x="chart.padding.left"
                                                    :y="chart.zeroY"
                                                    :width="chart.plotWidth"
                                                    :height="Math.max(0, (chart.height - chart.padding.bottom) - chart.zeroY)"
                                                />
                                            </clipPath>
                                        </defs>
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
                                            v-if="chart.zeroY != null"
                                            class="trend-zero-line"
                                            :x1="chart.padding.left"
                                            :x2="chart.width - chart.padding.right"
                                            :y1="chart.zeroY"
                                            :y2="chart.zeroY"
                                        />
                                        <line
                                            class="trend-ref-line"
                                            :x1="chart.padding.left"
                                            :x2="chart.width - chart.padding.right"
                                            :y1="chart.refY"
                                            :y2="chart.refY"
                                        />
                                        <path
                                            v-if="chart.zeroY == null"
                                            class="trend-line"
                                            :class="chart.lineClass"
                                            :d="chart.path"
                                            fill="none"
                                        />
                                        <path
                                            v-else
                                            class="trend-line"
                                            :class="chart.lineClass"
                                            :d="chart.path"
                                            :clip-path="'url(#m-pos-' + chart.key + ')'"
                                            fill="none"
                                        />
                                        <path
                                            v-if="chart.zeroY != null"
                                            class="trend-line trend-line--negative"
                                            :class="chart.lineClass"
                                            :d="chart.path"
                                            :clip-path="'url(#m-neg-' + chart.key + ')'"
                                            fill="none"
                                        />
                                        <g class="trend-points">
                                            <circle
                                                v-for="point in chart.points"
                                                :key="point.id + '-' + point.index"
                                                class="trend-point"
                                                :class="chart.pointClass"
                                                :cx="point.x"
                                                :cy="point.y"
                                                r="2"
                                            >
                                                <title>{{ chart.label }} {{ formatTrendValue(chart, point.value) }} · {{ point.matchLabel }} · {{ point.mapLabel }} · {{ point.dateLabel }}</title>
                                            </circle>
                                        </g>
                                        <circle
                                            v-if="chart.latest"
                                            class="trend-point trend-point--latest"
                                            :class="chart.pointClass"
                                            :cx="chart.latest.x"
                                            :cy="chart.latest.y"
                                            r="4"
                                        />
                                        <circle
                                            v-if="performanceTrendHover.key === chart.key && getTrendHoverPoint(chart)"
                                            class="trend-point trend-point--hover"
                                            :class="chart.pointClass"
                                            :cx="getTrendHoverPoint(chart).x"
                                            :cy="getTrendHoverPoint(chart).y"
                                            r="4"
                                        />
                                        <text
                                            v-if="chart.latest"
                                            class="trend-line-label"
                                            :class="chart.lineClass"
                                            :x="chart.width - 6"
                                            :y="chart.latest.y - 6"
                                            text-anchor="end"
                                        >
                                            <tspan class="trend-line-label__stat" :x="chart.width - 6">{{ chart.label }}</tspan>
                                            <tspan class="trend-line-label__value" :x="chart.width - 6" dy="14">{{ formatTrendValue(chart, chart.latest.value) }}</tspan>
                                        </text>
                                        <g class="trend-axis trend-axis--y">
                                            <text
                                                v-for="tick in chart.ticks"
                                                :key="'ylab-' + chart.key + '-' + tick.y"
                                                class="trend-axis__label"
                                                :x="chart.padding.left - 6"
                                                :y="tick.y + 4"
                                                text-anchor="end"
                                            >{{ formatTrendValue(chart, tick.value) }}</text>
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
                                                v-for="label in chart.xLabels"
                                                :key="'xlab-' + chart.key + '-' + label.index"
                                                class="trend-axis__label trend-axis__label--x"
                                                :x="label.x"
                                                :y="chart.height - 6"
                                                text-anchor="middle"
                                            >{{ label.label }}</text>
                                            <circle
                                                v-for="point in chart.points"
                                                :key="'res-' + chart.key + '-' + point.index"
                                                class="trend-result-marker"
                                                :class="'trend-result-marker--' + point.result"
                                                :cx="point.x"
                                                :cy="chart.height - chart.padding.bottom"
                                                r="2"
                                            ></circle>
                                        </g>
                                    </svg>
                                    <div
                                        v-if="performanceTrendHover.key === chart.key && getTrendHoverPoint(chart)"
                                        class="trend-tooltip"
                                        :style="trendTooltipStyle(chart, getTrendHoverPoint(chart))"
                                    >
                                        <div class="trend-tooltip__title">{{ getTrendHoverPoint(chart).matchLabel }} · {{ getTrendHoverPoint(chart).mapLabel }}</div>
                                        <div class="trend-tooltip__meta">
                                            {{ getTrendHoverPoint(chart).dateLabel }}
                                            <span v-if="getTrendHoverPoint(chart).scoreLabel"> · {{ getTrendHoverPoint(chart).scoreLabel }}</span>
                                            <span
                                                v-if="mapResultLabel(getTrendHoverPoint(chart))"
                                                class="trend-tooltip__result"
                                                :class="'trend-tooltip__result--' + getTrendHoverPoint(chart).result"
                                            >
                                                · {{ mapResultLabel(getTrendHoverPoint(chart)) }}
                                            </span>
                                        </div>
                                        <div class="trend-tooltip__value">{{ trendValueLabel(chart, getTrendHoverPoint(chart)) }} {{ tooltipTrendValue(chart, getTrendHoverPoint(chart)) }}</div>
                                        <div
                                            v-if="performanceTrendMode === 'cumulative' && getTrendHoverPoint(chart).delta != null"
                                            class="trend-tooltip__delta"
                                            :class="getTrendHoverPoint(chart).delta > 0 ? 'trend-delta--positive' : getTrendHoverPoint(chart).delta < 0 ? 'trend-delta--negative' : 'trend-delta--neutral'"
                                        >
                                            Muutos {{ formatTrendDelta(chart, getTrendHoverPoint(chart).delta) }}
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div v-else class="empty-state-container compact">
                            <div class="empty-state-card">
                                <h3 class="empty-state-title">Ei trendidataa</h3>
                                <p class="empty-state-description">Tälle kaudelle ei ole riittävästi otteluita trendin piirtämiseen.</p>
                            </div>
                        </div>
                    </div>

                </section>

                <section v-if="activeTab === 'matches'" class="team-section scout-view" id="team-tab-matches" role="tabpanel">
                    <div class="scout-panel scout-performance-trends matches-trends" ref="matchesTrendPanel">
                        <div class="section-heading section-heading--split">
                            <div class="section-heading__main">
                                <h3 class="section-title titleUnderline">Otteluiden kehitys</h3>
                                <span class="section-sub">X-akseli: kartat ottelujärjestyksessä · RD+, Win/Loss (kausi)</span>
                            </div>
                        </div>
                        <div v-if="matchesTrendCharts.length" class="performance-trends">
                            <div
                                v-for="chart in matchesTrendVisibleCharts"
                                :key="chart.key"
                                class="trend-chart"
                                :class="'trend-chart--' + chart.key"
                            >
                                <div
                                    class="trend-chart__plot"
                                    @mousemove="handleTrendHover($event, chart)"
                                    @mouseleave="clearTrendHover"
                                >
                                    <svg
                                        class="trend-chart__svg"
                                        :viewBox="'0 0 ' + chart.width + ' ' + chart.height"
                                        width="100%"
                                        :height="chart.height"
                                        role="img"
                                        :aria-label="chart.label + ' trendi'"
                                    >
                                        <defs v-if="chart.zeroY != null">
                                            <clipPath :id="'mt-pos-' + chart.key">
                                                <rect
                                                    :x="chart.padding.left"
                                                    :y="chart.padding.top"
                                                    :width="chart.plotWidth"
                                                    :height="Math.max(0, chart.zeroY - chart.padding.top)"
                                                />
                                            </clipPath>
                                            <clipPath :id="'mt-neg-' + chart.key">
                                                <rect
                                                    :x="chart.padding.left"
                                                    :y="chart.zeroY"
                                                    :width="chart.plotWidth"
                                                    :height="Math.max(0, (chart.height - chart.padding.bottom) - chart.zeroY)"
                                                />
                                            </clipPath>
                                        </defs>
                                        <g class="trend-grid">
                                            <line
                                                v-for="line in chart.gridLines"
                                                :key="'m-v-' + chart.key + '-' + line.index"
                                                class="trend-grid__line trend-grid__line--vertical"
                                                :x1="line.x"
                                                :x2="line.x"
                                                :y1="chart.padding.top"
                                                :y2="chart.height - chart.padding.bottom"
                                            />
                                            <line
                                                v-for="tick in chart.ticks"
                                                :key="'m-h-' + chart.key + '-' + tick.y"
                                                class="trend-grid__line"
                                                :x1="chart.padding.left"
                                                :x2="chart.width - chart.padding.right"
                                                :y1="tick.y"
                                                :y2="tick.y"
                                            />
                                        </g>
                                        <line
                                            v-if="chart.zeroY != null"
                                            class="trend-zero-line"
                                            :x1="chart.padding.left"
                                            :x2="chart.width - chart.padding.right"
                                            :y1="chart.zeroY"
                                            :y2="chart.zeroY"
                                        />
                                        <line
                                            class="trend-ref-line"
                                            :x1="chart.padding.left"
                                            :x2="chart.width - chart.padding.right"
                                            :y1="chart.refY"
                                            :y2="chart.refY"
                                        />
                                        <path
                                            v-if="chart.zeroY == null"
                                            class="trend-line"
                                            :class="chart.lineClass"
                                            :d="chart.path"
                                            fill="none"
                                        />
                                        <path
                                            v-else
                                            class="trend-line"
                                            :class="chart.lineClass"
                                            :d="chart.path"
                                            :clip-path="'url(#mt-pos-' + chart.key + ')'"
                                            fill="none"
                                        />
                                        <path
                                            v-if="chart.zeroY != null"
                                            class="trend-line trend-line--negative"
                                            :class="chart.lineClass"
                                            :d="chart.path"
                                            :clip-path="'url(#mt-neg-' + chart.key + ')'"
                                            fill="none"
                                        />
                                        <g class="trend-points">
                                            <circle
                                                v-for="point in chart.points"
                                                :key="point.id + '-' + point.index"
                                                class="trend-point"
                                                :class="chart.pointClass"
                                                :cx="point.x"
                                                :cy="point.y"
                                                r="2"
                                            >
                                                <title>{{ chart.label }} {{ formatTrendValue(chart, point.value) }} · {{ point.matchLabel }} · {{ point.mapLabel }} · {{ point.dateLabel }}</title>
                                            </circle>
                                        </g>
                                        <circle
                                            v-if="chart.latest"
                                            class="trend-point trend-point--latest"
                                            :class="chart.pointClass"
                                            :cx="chart.latest.x"
                                            :cy="chart.latest.y"
                                            r="4"
                                        />
                                        <circle
                                            v-if="performanceTrendHover.key === chart.key && getTrendHoverPoint(chart)"
                                            class="trend-point trend-point--hover"
                                            :class="chart.pointClass"
                                            :cx="getTrendHoverPoint(chart).x"
                                            :cy="getTrendHoverPoint(chart).y"
                                            r="4"
                                        />
                                        <text
                                            v-if="chart.latest"
                                            class="trend-line-label"
                                            :class="chart.lineClass"
                                            :x="chart.width - 6"
                                            :y="chart.latest.y - 6"
                                            text-anchor="end"
                                        >
                                            <tspan class="trend-line-label__stat" :x="chart.width - 6">{{ chart.label }}</tspan>
                                            <tspan class="trend-line-label__value" :x="chart.width - 6" dy="14">{{ formatTrendValue(chart, chart.latest.value) }}</tspan>
                                        </text>
                                        <g class="trend-axis trend-axis--y">
                                            <text
                                                v-for="tick in chart.ticks"
                                                :key="'m-ylab-' + chart.key + '-' + tick.y"
                                                class="trend-axis__label"
                                                :x="chart.padding.left - 6"
                                                :y="tick.y + 4"
                                                text-anchor="end"
                                            >{{ formatTrendValue(chart, tick.value) }}</text>
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
                                                v-for="label in chart.xLabels"
                                                :key="'m-xlab-' + chart.key + '-' + label.index"
                                                class="trend-axis__label trend-axis__label--x"
                                                :x="label.x"
                                                :y="chart.height - 6"
                                                text-anchor="middle"
                                            >{{ label.label }}</text>
                                            <circle
                                                v-for="point in chart.points"
                                                :key="'m-res-' + chart.key + '-' + point.index"
                                                class="trend-result-marker"
                                                :class="'trend-result-marker--' + point.result"
                                                :cx="point.x"
                                                :cy="chart.height - chart.padding.bottom"
                                                r="2"
                                            ></circle>
                                        </g>
                                    </svg>
                                    <div
                                        v-if="performanceTrendHover.key === chart.key && getTrendHoverPoint(chart)"
                                        class="trend-tooltip"
                                        :style="trendTooltipStyle(chart, getTrendHoverPoint(chart))"
                                    >
                                        <div class="trend-tooltip__title">{{ getTrendHoverPoint(chart).matchLabel }} · {{ getTrendHoverPoint(chart).mapLabel }}</div>
                                        <div class="trend-tooltip__meta">
                                            {{ getTrendHoverPoint(chart).dateLabel }}
                                            <span v-if="getTrendHoverPoint(chart).scoreLabel"> · {{ getTrendHoverPoint(chart).scoreLabel }}</span>
                                            <span
                                                v-if="mapResultLabel(getTrendHoverPoint(chart))"
                                                class="trend-tooltip__result"
                                                :class="'trend-tooltip__result--' + getTrendHoverPoint(chart).result"
                                            >
                                                · {{ mapResultLabel(getTrendHoverPoint(chart)) }}
                                            </span>
                                        </div>
                                        <div class="trend-tooltip__value">{{ matchesTrendValueLabel(chart, getTrendHoverPoint(chart)) }} {{ matchesTooltipTrendValue(chart, getTrendHoverPoint(chart)) }}</div>
                                        <div
                                            v-if="getTrendHoverPoint(chart).delta != null"
                                            class="trend-tooltip__delta"
                                            :class="getTrendHoverPoint(chart).delta > 0 ? 'trend-delta--positive' : getTrendHoverPoint(chart).delta < 0 ? 'trend-delta--negative' : 'trend-delta--neutral'"
                                        >
                                            Muutos {{ formatTrendDelta(chart, getTrendHoverPoint(chart).delta) }}
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div v-else class="empty-state-container compact">
                            <div class="empty-state-card">
                                <h3 class="empty-state-title">Ei trendidataa</h3>
                                <p class="empty-state-description">Tälle kaudelle ei ole riittävästi otteluita trendin piirtämiseen.</p>
                            </div>
                        </div>
                    </div>

                    <div class="scout-panel scout-table">
                        <div class="section-heading">
                            <div>
                                <h3 class="section-title titleUnderline">Ottelulista</h3>
                                <span class="section-sub">Ottelukohtaiset tulokset ja kartat</span>
                            </div>
                        </div>
                        <div v-if="matchesList.length" class="table-wrapper">
                            <table class="data-table matches-table">
                                <thead>
                                    <tr>
                                        <th>Pvm</th>
                                        <th>Vastustaja</th>
                                        <th>BO</th>
                                        <th>Score</th>
                                        <th>Eräero</th>
                                        <th>Maps</th>
                                        <th>Linkki</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr
                                        v-for="match in matchesList"
                                        :key="match.matchId"
                                        :class="{ 'match-row--highlight': match.matchId === matchesHoverMatchId }"
                                        @mouseenter="setMatchesHover(match.matchId)"
                                        @mouseleave="clearMatchesHover"
                                    >
                                        <td>{{ formatDate(match.ts) }}</td>
                                        <td :title="vetoSummaryLookup[match.matchId] || ''">{{ match.opponentName || match.team2Name || 'Vastustaja' }}</td>
                                        <td>BO{{ match.bestOf }}</td>
                                        <td>{{ match.teamScore }} - {{ match.oppScore }}</td>
                                        <td :class="match.roundDiff >= 0 ? 'stat-positive' : 'stat-negative'">{{ match.roundDiff }}</td>
                                        <td>
                                            <div class="micro-stack" v-if="match.maps && match.maps.length">
                                                <span v-for="map in match.maps" :key="map.id" class="micro-chip">{{ map.mapName }} {{ map.scoreFor }}-{{ map.scoreAgainst }}</span>
                                            </div>
                                            <span v-else class="cell-muted">Ei karttoja</span>
                                        </td>
                                        <td>
                                            <a v-if="match.faceitUrl" :href="match.faceitUrl" target="_blank" rel="noopener" class="chip chip--link">FACEIT</a>
                                            <span v-else class="cell-muted">-</span>
                                        </td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                        <div v-else class="empty-state-container">
                            <div class="empty-state-card">
                                <h3 class="empty-state-title">Ei otteluita</h3>
                                <p class="empty-state-description">Tälle kaudelle ei ole otteluhistoriaa saatavilla.</p>
                            </div>
                        </div>
                    </div>
                </section>

                <section v-if="activeTab === 'players'" class="team-section" id="team-tab-players" role="tabpanel">
                    <h2 class="section-title titleUnderline">Pelaajat</h2>
                    <div v-if="players.length" class="table-wrapper">
                        <sortable-table
                            :columns="PLAYER_COLUMNS"
                            :data="players"
                            :default-sort="playerDefaultSort"
                            :colorize-columns="['rating','kd','adr','kr']"
                            :sticky-header="true"
                            :compact="true"
                        >
                            <template #cell-nickname="{ row }">
                                <div class="player-cell">
                                    <div class="avatar-placeholder">{{ row.nickname ? row.nickname.slice(0, 2).toUpperCase() : 'PL' }}</div>
                                    <div>
                                        <div class="player-name">
                                            {{ row.nickname }}
                                            <span v-for="badge in getPlayerRoleBadges(row.playerId)" :key="badge.label" class="role-badge" :class="{ 'role-badge--primary': badge.isPrimary }" :title="badge.isPrimary ? 'Ensisijainen rooli' : ''">{{ badge.label }}</span>
                                        </div>
                                        <div class="player-sub">Maps {{ row.mapsPlayed }} · Rnds {{ row.roundsPlayed }}</div>
                                    </div>
                                </div>
                            </template>
                            <template #cell-rating="{ row }">
                                <div class="cell-with-bar">
                                    <span class="stat-strong">{{ formatNumber(row.rating, 2) }}</span>
                                    <div class="inline-bar inline-bar--thin">
                                        <div class="inline-bar__fill inline-bar__fill--accent" :style="{ width: Math.min(100, row.rating / 2 * 100) + '%' }"></div>
                                    </div>
                                </div>
                            </template>
                            <template #cell-kd="{ row }">
                                <span class="stat-strong">{{ formatNumber(row.kd, 2) }}</span>
                            </template>
                            <template #cell-adr="{ row }">
                                <span class="stat-strong">{{ formatNumber(row.adr, 1) }}</span>
                            </template>
                            <template #cell-hsPct="{ row }">
                                <span>{{ formatPercent(row.hsPct || 0, 1) }}</span>
                            </template>
                            <template #cell-entryLine="{ row }">
                                <span>{{ row.entryWins }}/{{ row.entryCount }} ({{ formatPercent(row.entryLine || 0, 1) }})</span>
                            </template>
                            <template #cell-clutch1v1Line="{ row }">
                                <span>{{ row.cl1v1Wins }}/{{ row.cl1v1Attempts }} ({{ formatPercent(row.cl1v1Line || 0, 1) }})</span>
                            </template>
                            <template #cell-clutch1v2Line="{ row }">
                                <span>{{ row.cl1v2Wins }}/{{ row.cl1v2Attempts }} ({{ formatPercent(row.cl1v2Line || 0, 1) }})</span>
                            </template>
                            <template #cell-flashSuccessLine="{ row }">
                                <span>{{ row.flashSuccesses }}/{{ row.flashCount }} ({{ formatPercent(row.flashSuccessLine || 0, 1) }})</span>
                            </template>
                        </sortable-table>
                    </div>
                    <div v-else class="empty-state-container">
                        <div class="empty-state-card">
                            <h3 class="empty-state-title">Ei pelaajatietoja</h3>
                            <p class="empty-state-description">Tälle kaudelle ei ole pelaajatietoja saatavilla.</p>
                        </div>
                    </div>
                    <div class="players-charts" v-if="playersByRating.length">
                        <div class="glass-card">
                            <div class="section-heading">
                                <h3>Pelaajien rating</h3>
                            </div>
                            <div class="bar-chart bar-chart--horizontal">
                                <div v-for="p in playersByRating" :key="p.playerId" class="bar-chart__row">
                                    <span class="bar-chart__label">{{ p.nickname }}</span>
                                    <div class="bar-chart__track">
                                        <div class="bar-chart__track-fill" :style="{ width: Math.min(100, p.rating / 2 * 100) + '%' }"></div>
                                    </div>
                                    <span class="bar-chart__value">{{ formatNumber(p.rating, 2) }}</span>
                                </div>
                            </div>
                        </div>
                        <div class="glass-card" v-if="playerStackedKda.length">
                            <div class="section-heading">
                                <h3>K/D/A per pelaaja</h3>
                            </div>
                            <div class="stacked-bars">
                                <div v-for="row in playerStackedKda" :key="row.label" class="stacked-bars__row">
                                    <span class="stacked-bars__label">{{ row.label }}</span>
                                    <div class="stacked-bars__bar">
                                        <span class="stacked-seg stacked-seg--kills" :style="{ width: ((row.kills / playerMaxKdaTotal) * 100) + '%' }">K {{ row.kills }}</span>
                                        <span class="stacked-seg stacked-seg--deaths" :style="{ width: ((row.deaths / playerMaxKdaTotal) * 100) + '%' }">D {{ row.deaths }}</span>
                                        <span class="stacked-seg stacked-seg--assists" :style="{ width: ((row.assists / playerMaxKdaTotal) * 100) + '%' }">A {{ row.assists }}</span>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </section>

                <section v-if="activeTab === 'veto'" class="team-section" id="team-tab-veto" role="tabpanel">
                    <h2 class="section-title titleUnderline">Ban/Nosto Tilastot</h2>
                    <div v-if="enhancedVetoAggregates.length" class="table-wrapper">
                        <sortable-table
                            :columns="VETO_COLUMNS"
                            :data="enhancedVetoAggregates"
                            :default-sort="vetoDefaultSort"
                            :sticky-header="true"
                            :compact="true"
                        >
                            <template #cell-mapName="{ row }">
                                <span :class="[{ 'stat-strong': row.isTopPick || row.isTopBan }]">{{ row.mapName }}</span>
                                <span v-if="row.isTopPick" class="chip chip--accent">Top pick</span>
                                <span v-if="row.isTopBan" class="chip chip--warn">Top ban</span>
                            </template>
                            <template #cell-timesPicked="{ row }">
                                <div class="cell-with-bar">
                                    <span>{{ row.timesPicked }}</span>
                                    <div class="inline-bar inline-bar--thin">
                                        <div class="inline-bar__fill inline-bar__fill--accent" :style="{ width: ((row.timesPicked + row.timesOpponentPicked + row.timesBanned) ? (row.timesPicked / (row.timesPicked + row.timesOpponentPicked + row.timesBanned) * 100) : 0) + '%' }"></div>
                                    </div>
                                </div>
                            </template>
                            <template #cell-timesOpponentPicked="{ row }">
                                <span>{{ row.timesOpponentPicked }}</span>
                            </template>
                            <template #cell-timesBanned="{ row }">
                                <div class="cell-with-bar">
                                    <span>{{ row.timesBanned }}</span>
                                    <div class="inline-bar inline-bar--thin">
                                        <div class="inline-bar__fill inline-bar__fill--danger" :style="{ width: ((row.timesPicked + row.timesOpponentPicked + row.timesBanned) ? (row.timesBanned / (row.timesPicked + row.timesOpponentPicked + row.timesBanned) * 100) : 0) + '%' }"></div>
                                    </div>
                                </div>
                            </template>
                            <template #cell-pickRate="{ row }">
                                <span>{{ formatPercent(row.pickRate, 1) }}</span>
                            </template>
                            <template #cell-banRate="{ row }">
                                <span>{{ formatPercent(row.banRate, 1) }}</span>
                            </template>
                            <template #cell-pickWinRate="{ row }">
                                <span>{{ formatPercent(row.pickWinRate, 1) }}</span>
                            </template>
                            <template #cell-deciderWinRate="{ row }">
                                <span>{{ formatPercent(row.deciderWinRate, 1) }}</span>
                            </template>
                        </sortable-table>
                    </div>
                    <div v-else class="empty-state-container">
                        <div class="empty-state-card">
                            <h3 class="empty-state-title">Ei ban/nosto historiaa</h3>
                            <p class="empty-state-description">Tälle kaudelle ei ole ban/nosto historiatietoja saatavilla.</p>
                        </div>
                    </div>
                    <div class="glass-card" v-if="enhancedVetoAggregates.length">
                        <div class="section-heading">
                            <h3>Picks vs bans per kartta</h3>
                        </div>
                        <div class="stacked-bars">
                            <div v-for="row in enhancedVetoAggregates" :key="row.mapName" class="stacked-bars__row">
                                <span class="stacked-bars__label">{{ row.mapName }}</span>
                                <div class="stacked-bars__bar">
                                    <span class="stacked-seg stacked-seg--picks" :class="{ 'stacked-seg--highlight': row.isTopPick }" :style="{ width: ((row.timesPicked + row.timesOpponentPicked + row.timesBanned) ? (row.timesPicked / (row.timesPicked + row.timesOpponentPicked + row.timesBanned) * 100) : 0) + '%' }">Pick {{ row.timesPicked }}</span>
                                    <span class="stacked-seg stacked-seg--picks-opp" :style="{ width: ((row.timesPicked + row.timesOpponentPicked + row.timesBanned) ? (row.timesOpponentPicked / (row.timesPicked + row.timesOpponentPicked + row.timesBanned) * 100) : 0) + '%' }">Vast {{ row.timesOpponentPicked }}</span>
                                    <span class="stacked-seg stacked-seg--bans" :class="{ 'stacked-seg--highlight': row.isTopBan }" :style="{ width: ((row.timesPicked + row.timesOpponentPicked + row.timesBanned) ? (row.timesBanned / (row.timesPicked + row.timesOpponentPicked + row.timesBanned) * 100) : 0) + '%' }">Ban {{ row.timesBanned }}</span>
                                </div>
                            </div>
                        </div>
                    </div>
                    <div class="glass-card" v-if="vetoByMatch.length">
                        <div class="section-heading">
                            <h3>BO2/BO3 veto-polku</h3>
                            <span class="section-sub">Jokainen askel, joukkue ja decider/overflow korostettu</span>
                        </div>
                        <div class="veto-timeline" v-for="v in vetoByMatch" :key="v.matchId">
                            <div class="veto-timeline__header">
                                <span class="pill">Match {{ v.matchId }}</span>
                                <span class="pill">Format {{ v.format.toUpperCase() }}</span>
                                <span class="pill" v-if="v.match?.opponentName || v.match?.team2Name">vs {{ v.match?.opponentName || v.match?.team2Name }}</span>
                            </div>
                            <div class="veto-steps">
                                <div v-for="step in v.steps" :key="step.step + step.mapName" class="veto-step" :class="'veto-step--' + step.action">
                                    <div class="veto-step__order">#{{ step.step }}</div>
                                    <div class="veto-step__title">{{ step.label }}</div>
                                    <div class="veto-step__map">{{ step.mapName }}</div>
                                    <div class="veto-step__actor">{{ step.teamName || 'Järjestelmä' }}</div>
                                </div>
                            </div>
                        </div>
                    </div>
                </section>
            </div>
        </div>
    `
};
