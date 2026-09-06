// Team detail view that renders stats, maps, matches, players and veto aggregates.
// Every DB-backed field is surfaced as a stat, column, chart point or tooltip.

const PLAYER_COLUMNS = [
    { key: 'nickname', label: 'Pelaaja', sortable: true, colClass: 'col-name', group: 'identity', tooltip: 'Pelaajan nimi, roolibadget ja kauden kartta/erämäärä.', mobilePinned: true, mobilePriority: 1 },
    { key: 'mapsPlayed', label: 'Kartat', sortable: true, numeric: true, group: 'volume', tooltip: 'Pelattujen karttojen määrä tällä kaudella.', mobilePriority: 2 },
    { key: 'roundsPlayed', label: 'R', sortable: true, numeric: true, group: 'volume', tooltip: 'Pelattujen erien kokonaismäärä.', mobileHidden: true },
    { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2, colClass: 'col-kd', group: 'core', tooltip: 'Tapot / kuolemat. Yli 1.00 tarkoittaa enemmän tappoja kuin kuolemia.', mobilePriority: 3 },
    { key: 'kr', label: 'K/R', sortable: true, numeric: true, decimals: 2, group: 'core', tooltip: 'Kills per round: tapot per pelattu erä.', mobileHidden: true },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1, colClass: 'col-adr', group: 'core', tooltip: 'Average Damage per Round: keskimääräinen vahinko per erä.', mobilePriority: 4 },
    { key: 'hsPct', label: 'HS%', sortable: true, numeric: true, decimals: 1, group: 'core', tooltip: 'Headshot-osuus prosenteissa kaikista tapoista.', mobilePriority: 5 },
    { key: 'kills', label: 'Kills', sortable: true, numeric: true, group: 'combat', tooltip: 'Kaikki tapot yhteensä.', mobileHidden: true },
    { key: 'deaths', label: 'Deaths', sortable: true, numeric: true, group: 'combat', tooltip: 'Kaikki kuolemat yhteensä.', mobileHidden: true },
    { key: 'assists', label: 'A', sortable: true, numeric: true, group: 'combat', tooltip: 'Assistit yhteensä.', mobileHidden: true },
    { key: 'clutchKills', label: 'Clutch K', sortable: true, numeric: true, group: 'combat', tooltip: 'Tapot clutch-tilanteissa.', mobileHidden: true },
    { key: 'entryLine', label: 'Entry', sortable: true, numeric: true, group: 'impact', tooltip: 'Entry-voitot / entry-yritykset sekä onnistumisprosentti.', mobilePriority: 6 },
    { key: 'clutch1v1Line', label: '1v1', sortable: true, numeric: true, group: 'impact', tooltip: 'Voitetut 1v1 clutchit / yritykset.', mobileHidden: true },
    { key: 'clutch1v2Line', label: '1v2', sortable: true, numeric: true, group: 'impact', tooltip: 'Voitetut 1v2 clutchit / yritykset.', mobileHidden: true },
    { key: 'mvps', label: 'MVP', sortable: true, numeric: true, group: 'impact', tooltip: 'MVP-merkintöjen määrä.', mobileHidden: true },
    { key: 'damage', label: 'Dmg', sortable: true, numeric: true, group: 'utility', tooltip: 'Kokonaisvahinko kaikissa kartoissa.', mobileHidden: true },
    { key: 'utilityDamage', label: 'U-Dmg', sortable: true, numeric: true, group: 'utility', tooltip: 'Utility-vahinko yhteensä (kranaatit ym.).', mobileHidden: true },
    { key: 'enemiesFlashed', label: 'Flashed', sortable: true, numeric: true, group: 'utility', tooltip: 'Kuinka monta vastustajaa pelaaja on väläyttänyt.', mobileHidden: true },
    { key: 'flashSuccessLine', label: 'Flash%', sortable: true, numeric: true, group: 'utility', tooltip: 'Flash-successit / heitetyt flashit sekä onnistumisprosentti.', mobileHidden: true },
    { key: 'mk2k', label: '2K', sortable: true, numeric: true, group: 'multis', tooltip: 'Kierrokset, joissa pelaaja sai 2 tappoa.', mobileHidden: true },
    { key: 'mk3k', label: '3K', sortable: true, numeric: true, group: 'multis', tooltip: 'Kierrokset, joissa pelaaja sai 3 tappoa.', mobileHidden: true },
    { key: 'mk4k', label: '4K', sortable: true, numeric: true, group: 'multis', tooltip: 'Kierrokset, joissa pelaaja sai 4 tappoa.', mobileHidden: true },
    { key: 'mk5k', label: 'Ace', sortable: true, numeric: true, group: 'multis', tooltip: 'Ace: kierrokset, joissa pelaaja tappoi koko vastustajajoukkueen (5K).', mobileHidden: true },
    { key: 'sniperKills', label: 'Sniper', sortable: true, numeric: true, group: 'weapons', tooltip: 'Sniper-tappojen määrä.', mobileHidden: true },
    { key: 'pistolKills', label: 'Pistol', sortable: true, numeric: true, group: 'weapons', tooltip: 'Pistoolitappojen määrä.', mobileHidden: true }
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

const PLAYER_GROUP_META = {
    identity: { label: 'Pelaaja', className: 'group-map group-divider' },
    volume: { label: 'Määrä', className: 'group-usage group-divider' },
    core: { label: 'Ydin', className: 'group-performance group-divider' },
    combat: { label: 'Taistelu', className: 'group-combat group-divider' },
    impact: { label: 'Impact', className: 'group-results group-divider' },
    utility: { label: 'Utility', className: 'group-utility group-divider' },
    multis: { label: 'Multi-kills', className: 'group-multikill group-divider' },
    weapons: { label: 'Aseet', className: 'group-weapons group-divider' }
};

const LINEUP_GROUP_META = {
    identity: { label: 'Lineup', className: 'group-map group-divider' },
    volume: { label: 'Määrä', className: 'group-usage group-divider' },
    results: { label: 'Tulokset', className: 'group-results group-divider' },
    core: { label: 'Ydin', className: 'group-performance group-divider' },
    maps: { label: 'Kartat', className: 'group-map group-divider' }
};

const LINEUP_COLUMNS = [
    { key: 'lineupLabel', label: 'Lineup', sortable: true, colClass: 'col-name', group: 'identity', tooltip: 'Uniikki 5 pelaajan kokoonpano valitulla kaudella.', mobilePinned: true, mobilePriority: 1 },
    { key: 'matchesPlayed', label: 'Ott', sortable: true, numeric: true, group: 'volume', tooltip: 'Ottelut, joissa lineup pelasi vähintään yhden kartan.', mobilePriority: 2 },
    { key: 'mapsPlayed', label: 'Kartat', sortable: true, numeric: true, group: 'volume', tooltip: 'Pelattujen karttojen määrä tällä lineupilla.', mobilePriority: 3 },
    { key: 'recordLine', label: 'Tulokset', sortable: true, group: 'results', tooltip: 'Karttasaldo tällä lineupilla.', mobileHidden: true },
    { key: 'winRate', label: 'Win %', sortable: true, numeric: true, decimals: 1, group: 'results', tooltip: 'Karttakohtainen voittoprosentti tällä lineupilla.', mobilePriority: 4 },
    { key: 'roundDiff', label: 'RD', sortable: true, numeric: true, group: 'results', tooltip: 'Yhteenlaskettu eräero tällä lineupilla.', mobilePriority: 5 },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1, colClass: 'col-adr', group: 'core', tooltip: 'Lineupin keskimääräinen ADR.', mobilePriority: 6 },
    { key: 'udpr', label: 'UDPR', sortable: true, numeric: true, decimals: 2, group: 'core', tooltip: 'Lineupin utility damage per round.', mobilePriority: 7 },
    { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2, colClass: 'col-kd', group: 'core', tooltip: 'Lineupin yhteinen kill/death-suhde.', mobileHidden: true },
    { key: 'hsPct', label: 'HS%', sortable: true, numeric: true, decimals: 1, group: 'core', tooltip: 'Headshot-osuus lineupin kaikista tapoista.', mobileHidden: true },
    { key: 'mapBreakdownSummary', label: 'Karttajakauma', sortable: true, group: 'maps', tooltip: 'Millä kartoilla lineup on esiintynyt.', mobileHidden: true }
];

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
    { key: 'totalRoundsPlayed', label: 'Eriä pelattu', sortable: true, numeric: true, decimals: 0, colClass: 'mono-num', tooltip: 'Todellinen pelattujen erien määrä', group: 'rounds' },
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

function createSegment() {
    return { data: null, loading: false, error: null, fetchedAt: null };
}

function toNumber(value, fallback = 0) {
    if (value === null || value === undefined) return fallback;
    const numeric = Number(value);
    if (Number.isFinite(numeric)) return numeric;
    const parsed = Number(String(value).replace(',', '.'));
    return Number.isFinite(parsed) ? parsed : fallback;
}

const MIN_VALID_MATCH_EPOCH_SECONDS = Math.round(Date.UTC(2001, 0, 1) / 1000);

function isLikelyPlaceholderMatchTs(seconds) {
    if (!Number.isFinite(seconds) || seconds <= 0) return true;
    return seconds < MIN_VALID_MATCH_EPOCH_SECONDS;
}

function coerceEpochSeconds(value) {
    if (value === null || value === undefined || value === '') return null;
    const numeric = Number(value);
    if (Number.isFinite(numeric) && numeric > 0) {
        const seconds = Math.abs(numeric) >= 1_000_000_000_000 ? Math.round(numeric / 1000) : Math.round(numeric);
        return isLikelyPlaceholderMatchTs(seconds) ? null : seconds;
    }
    const parsed = Date.parse(String(value));
    if (!Number.isFinite(parsed) || parsed <= 0) return null;
    const seconds = Math.round(parsed / 1000);
    return isLikelyPlaceholderMatchTs(seconds) ? null : seconds;
}

function scheduledMatchTsSeconds(match) {
    const globalUtils = typeof window !== 'undefined' ? window.matchTimeUtils : null;
    if (globalUtils && typeof globalUtils.getScheduledTs === 'function') {
        const ms = globalUtils.getScheduledTs(match);
        if (Number.isFinite(ms) && ms > 0) {
            const seconds = Math.round(ms / 1000);
            if (!isLikelyPlaceholderMatchTs(seconds)) {
                return seconds;
            }
        }
    }
    // Backend already resolves the best-available timestamp into `ts`; only fall back to
    // the individual date fields when it's missing/zero (unset backend timestamp columns
    // are stored as 0, not null, so a plain ?? chain would pick them up incorrectly).
    const candidates = [
        match?.ts,
        match?.scheduled_ts,
        match?.scheduledTs,
        match?.scheduled_at,
        match?.scheduledAt,
        match?.scheduled,
        match?.finished_ts,
        match?.finishedTs,
        match?.finished_at,
        match?.finishedAt,
        match?.start_ts,
        match?.startTs,
        match?.start_at,
        match?.startAt,
        match?.date,
        match?.datetime
    ];
    for (const candidate of candidates) {
        const seconds = coerceEpochSeconds(candidate);
        if (seconds !== null) return seconds;
    }
    return null;
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

function combinationCount(n, k = 5) {
    const total = Math.floor(toNumber(n, 0));
    const choose = Math.floor(toNumber(k, 0));
    if (choose < 0 || total < choose) return 0;
    if (choose === 0 || total === choose) return 1;
    const span = Math.min(choose, total - choose);
    let result = 1;
    for (let i = 1; i <= span; i += 1) {
        result = (result * (total - span + i)) / i;
    }
    return Math.round(result);
}

function clampValue(value, min, max) {
    return Math.min(max, Math.max(min, value));
}

function buildHeatStyle(percent) {
    const clamped = clampValue(toNumber(percent), 0, 100);
    const hue = (clamped / 100) * 120;
    const color = `hsla(${hue.toFixed(1)}, 60%, 45%, 0.22)`;
    return {
        background: `linear-gradient(90deg, ${color}, transparent)`
    };
}

function computeMedian(values = []) {
    const list = values.filter(v => Number.isFinite(v)).sort((a, b) => a - b);
    if (!list.length) return null;
    const mid = Math.floor(list.length / 2);
    if (list.length % 2 === 0) {
        return (list[mid - 1] + list[mid]) / 2;
    }
    return list[mid];
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
    if (core === 'forfeit') return null; // also catch de_forfeit
    const parts = core.split(/[_-]/).filter(Boolean);
    if (!parts.length) return value;
    return parts.map(p => p.charAt(0).toUpperCase() + p.slice(1)).join(' ');
}

function mapKey(name) {
    return String(name || '').trim().toLowerCase();
}

function formatMatchDate(ts) {
    if (!ts) return '';
    const d = new Date(ts * 1000);
    return d.toLocaleDateString('fi-FI', { year: 'numeric', month: 'short', day: 'numeric' });
}

function formatMatchTime(ts) {
    if (!ts) return '';
    const d = new Date(ts * 1000);
    return d.toLocaleTimeString('fi-FI', { hour: '2-digit', minute: '2-digit' });
}

function normalizeSeasonData(pageData) {
    if (!pageData) return null;
    return pageData.seasonData || null;
}

function normalizeMap(entry) {
    if (!entry) return null;
    const rawMapId = entry.mapId || entry.map_id || entry.map || entry.mapName || null;
    const rawName = entry.prettyName
        || entry.pretty_name
        || entry.mapName
        || entry.mapNameRaw
        || rawMapId
        || entry.map_name
        || entry.map
        || 'Kartta';
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

    const identifier = rawMapId || rawName;

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
        mapId: rawMapId || null,
        map_name: beautified || rawName || null,
        mapName: beautified,
        mapNameRaw: rawName,
        image_sm: entry.image_sm || entry.imageSm || null,
        image_lg: entry.image_lg || entry.imageLg || null,
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
    const adr = toNumber(player.adr);
    const kr = toNumber(player.kr);
    const hsPct = toNumber(player.hsPct);
    const entryWinPct = entryCount ? (entryWins / entryCount) * 100 : 0;
    const clutch1v1Pct = cl1v1Attempts ? (cl1v1Wins / cl1v1Attempts) * 100 : 0;
    const clutch1v2Pct = cl1v2Attempts ? (cl1v2Wins / cl1v2Attempts) * 100 : 0;
    const flashSuccessPct = flashCount ? (flashSuccesses / flashCount) * 100 : 0;

    return {
        playerId: player.playerId || player.player_id || `player-${idx}`,
        nickname: player.nickname || 'Pelaaja',
        mapsPlayed,
        roundsPlayed,
        kd,
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

function readStatNumber(source, keys = [], fallback = 0) {
    if (!source || typeof source !== 'object') return fallback;
    for (const key of keys) {
        if (!key) continue;
        const value = source[key];
        if (value !== null && value !== undefined && value !== '') {
            return toNumber(value, fallback);
        }
    }
    return fallback;
}

function pickBestLineupPlayer(players = [], metric = 'kd') {
    const source = Array.isArray(players) ? players.filter(Boolean) : [];
    if (!source.length) return null;
    const sorted = [...source].sort((left, right) => {
        const metricDiff = toNumber(right?.[metric], 0) - toNumber(left?.[metric], 0);
        if (Math.abs(metricDiff) > 1e-9) return metricDiff;
        const roundsDiff = toNumber(right?.roundsPlayed, 0) - toNumber(left?.roundsPlayed, 0);
        if (roundsDiff !== 0) return roundsDiff;
        const killsDiff = toNumber(right?.kills, 0) - toNumber(left?.kills, 0);
        if (killsDiff !== 0) return killsDiff;
        return String(left?.nickname || left?.playerId || '').localeCompare(String(right?.nickname || right?.playerId || ''), 'fi');
    });
    return sorted[0] || null;
}

function buildLineupRows(items = [], matches = [], teamId = null) {
    if (!Array.isArray(items) || !items.length) return [];

    const mapLookup = new Map();
    (Array.isArray(matches) ? matches : []).forEach(match => {
        const matchId = match?.matchId;
        if (!matchId || !Array.isArray(match.maps)) return;
        match.maps.forEach((mapEntry, idx) => {
            const roundIndex = toNumber(mapEntry?.roundIndex ?? idx, idx);
            mapLookup.set(`${matchId}::${roundIndex}`, {
                mapName: mapEntry?.mapName || 'Kartta',
                scoreFor: toNumber(mapEntry?.scoreFor, 0),
                scoreAgainst: toNumber(mapEntry?.scoreAgainst, 0),
                isForfeit: !!mapEntry?.isForfeit
            });
        });
    });

    const lineupMaps = new Map();
    items.forEach((item, idx) => {
        const matchId = item?.matchId || item?.match_id;
        const roundIndex = toNumber(item?.roundIndex ?? item?.round_index, 0);
        const rowTeamId = item?.teamId || item?.team_id || null;
        const isForfeitMap = !!(item?.isForfeitMap ?? item?.is_forfeit_map);
        if (!matchId || isForfeitMap) return;
        if (teamId && rowTeamId && String(rowTeamId) !== String(teamId)) return;

        const playerIdRaw = item?.playerId || item?.player_id;
        if (!playerIdRaw) return;
        const playerId = String(playerIdRaw);
        const bucketKey = `${matchId}::${roundIndex}`;
        if (!lineupMaps.has(bucketKey)) {
            lineupMaps.set(bucketKey, {
                matchId,
                roundIndex,
                mapName: beautifyMapName(item?.mapName || item?.map_name) || 'Kartta',
                players: new Map(),
                rows: []
            });
        }
        const bucket = lineupMaps.get(bucketKey);
        bucket.rows.push(item);
        if (!bucket.players.has(playerId)) {
            bucket.players.set(playerId, {
                playerId,
                nickname: item?.nickname || `Pelaaja ${idx + 1}`
            });
        }
    });

    const lineups = new Map();

    lineupMaps.forEach((bucket, bucketKey) => {
        const players = Array.from(bucket.players.values());
        if (players.length !== 5) return;

        const signaturePlayers = [...players].sort((left, right) => String(left.playerId).localeCompare(String(right.playerId)));
        const signature = signaturePlayers.map(player => player.playerId).join('|');

        if (!lineups.has(signature)) {
            const lineupPlayers = [...players]
                .sort((left, right) => String(left.nickname).localeCompare(String(right.nickname), 'fi'))
                .map(player => ({
                    ...player,
                    kills: 0,
                    deaths: 0,
                    damage: 0,
                    utilityDamage: 0,
                    headshots: 0,
                    roundsPlayed: 0,
                    kd: 0,
                    adr: 0,
                    udpr: 0,
                    hsPct: 0,
                    mvpBadges: []
                }));
            const playersById = {};
            lineupPlayers.forEach(player => {
                playersById[String(player.playerId)] = player;
            });
            lineups.set(signature, {
                signature,
                players: lineupPlayers,
                playersById,
                matchIds: new Set(),
                mapsPlayed: 0,
                wins: 0,
                losses: 0,
                draws: 0,
                roundsFor: 0,
                roundsAgainst: 0,
                kills: 0,
                deaths: 0,
                damage: 0,
                headshots: 0,
                playerRounds: 0,
                mapBreakdown: {}
            });
        }

        const aggregate = lineups.get(signature);
        aggregate.matchIds.add(String(bucket.matchId));
        aggregate.mapsPlayed += 1;

        const mapMeta = mapLookup.get(bucketKey) || {};
        const scoreFor = toNumber(mapMeta.scoreFor, 0);
        const scoreAgainst = toNumber(mapMeta.scoreAgainst, 0);
        const mapName = mapMeta.mapName || bucket.mapName || 'Kartta';

        aggregate.roundsFor += scoreFor;
        aggregate.roundsAgainst += scoreAgainst;

        if (scoreFor > scoreAgainst) aggregate.wins += 1;
        else if (scoreFor < scoreAgainst) aggregate.losses += 1;
        else aggregate.draws += 1;

        if (!aggregate.mapBreakdown[mapName]) {
            aggregate.mapBreakdown[mapName] = { mapName, played: 0, wins: 0, losses: 0, draws: 0 };
        }
        const mapEntry = aggregate.mapBreakdown[mapName];
        mapEntry.played += 1;
        if (scoreFor > scoreAgainst) mapEntry.wins += 1;
        else if (scoreFor < scoreAgainst) mapEntry.losses += 1;
        else mapEntry.draws += 1;

        const mapRounds = Math.max(0, scoreFor + scoreAgainst);
        bucket.rows.forEach(row => {
            const stats = row?.stats || {};
            const kills = readStatNumber(stats, ['kills', 'Kills']);
            const deaths = readStatNumber(stats, ['deaths', 'Deaths']);
            const damage = readStatNumber(stats, ['damage', 'Damage']);
            const utilityDamage = readStatNumber(stats, ['utility_damage', 'Utility Damage']);
            let headshots = readStatNumber(stats, ['headshots', 'Headshots']);
            if (!headshots && kills > 0) {
                const rawHsPct = readStatNumber(stats, ['hs_pct', 'Headshots %', 'headshots_pct']);
                const normalizedHsPct = Math.abs(rawHsPct) <= 1 ? rawHsPct * 100 : rawHsPct;
                headshots = kills * (normalizedHsPct / 100);
            }

            aggregate.kills += kills;
            aggregate.deaths += deaths;
            aggregate.damage += damage;
            aggregate.headshots += headshots;
            aggregate.playerRounds += mapRounds;

            const rowPlayerId = String(row?.playerId || row?.player_id || '');
            const lineupPlayer = aggregate.playersById?.[rowPlayerId];
            if (lineupPlayer) {
                lineupPlayer.kills += kills;
                lineupPlayer.deaths += deaths;
                lineupPlayer.damage += damage;
                lineupPlayer.utilityDamage += utilityDamage;
                lineupPlayer.headshots += headshots;
                lineupPlayer.roundsPlayed += mapRounds;
            }
        });
    });

    return Array.from(lineups.values())
        .map(entry => {
            const matchesPlayed = entry.matchIds.size;
            const mapsPlayed = entry.mapsPlayed;
            const roundDiff = entry.roundsFor - entry.roundsAgainst;
            const winRate = mapsPlayed ? (entry.wins / mapsPlayed) * 100 : 0;
            const kd = entry.deaths ? entry.kills / entry.deaths : entry.kills;
            const adr = entry.playerRounds ? entry.damage / entry.playerRounds : 0;
            const hsPct = entry.kills ? (entry.headshots / entry.kills) * 100 : 0;
            const mapBreakdown = Object.values(entry.mapBreakdown)
                .map(mapEntry => ({
                    ...mapEntry,
                    winRate: mapEntry.played ? (mapEntry.wins / mapEntry.played) * 100 : 0
                }))
                .sort((left, right) => {
                    if (right.played !== left.played) return right.played - left.played;
                    return String(left.mapName).localeCompare(String(right.mapName), 'fi');
                });
            const players = (Array.isArray(entry.players) ? entry.players : []).map(player => {
                const kills = toNumber(player.kills, 0);
                const deaths = toNumber(player.deaths, 0);
                const damage = toNumber(player.damage, 0);
                const utilityDamage = toNumber(player.utilityDamage, 0);
                const headshots = toNumber(player.headshots, 0);
                const roundsPlayed = toNumber(player.roundsPlayed, 0);
                return {
                    ...player,
                    kills,
                    deaths,
                    damage,
                    utilityDamage,
                    headshots,
                    roundsPlayed,
                    kd: deaths ? kills / deaths : kills,
                    adr: roundsPlayed ? damage / roundsPlayed : 0,
                    udpr: roundsPlayed ? utilityDamage / roundsPlayed : 0,
                    hsPct: kills ? (headshots / kills) * 100 : 0,
                    mvpBadges: []
                };
            });
            const kdLeader = pickBestLineupPlayer(players, 'kd');
            const adrLeader = pickBestLineupPlayer(players, 'adr');
            const udprLeader = pickBestLineupPlayer(players, 'udpr');
            const leaderIds = [kdLeader?.playerId, adrLeader?.playerId, udprLeader?.playerId]
                .filter(Boolean)
                .map(value => String(value));
            const allSharedLeader = leaderIds.length && leaderIds.every(value => value === leaderIds[0])
                ? leaderIds[0]
                : null;
            const sharedKdAdrLeader = !allSharedLeader && kdLeader && adrLeader && String(kdLeader.playerId) === String(adrLeader.playerId)
                ? String(kdLeader.playerId)
                : null;
            const playersWithBadges = players.map(player => {
                const badges = [];
                const playerId = String(player.playerId);
                const isKdLeader = kdLeader && playerId === String(kdLeader.playerId);
                const isAdrLeader = adrLeader && playerId === String(adrLeader.playerId);
                const isUdprLeader = udprLeader && playerId === String(udprLeader.playerId);
                if (allSharedLeader && playerId === allSharedLeader) {
                    badges.push({
                        label: 'MVP',
                        tone: 'both',
                        tooltip: `Johti lineupia K/D (${formatNumber(player.kd, 2)}), ADR (${formatNumber(player.adr, 1)}) ja UDPR (${formatNumber(player.udpr, 2)}) perusteella.`
                    });
                } else {
                    if (sharedKdAdrLeader && playerId === sharedKdAdrLeader) {
                        badges.push({
                            label: 'MVP',
                            tone: 'both',
                            tooltip: `Johti lineupia sekä K/D (${formatNumber(player.kd, 2)}) että ADR (${formatNumber(player.adr, 1)}) perusteella.`
                        });
                    } else {
                        if (isKdLeader) {
                            badges.push({
                                label: 'MVP K/D',
                                tone: 'kd',
                                tooltip: `Lineupin paras K/D: ${formatNumber(player.kd, 2)}`
                            });
                        }
                        if (isAdrLeader) {
                            badges.push({
                                label: 'MVP ADR',
                                tone: 'adr',
                                tooltip: `Lineupin paras ADR: ${formatNumber(player.adr, 1)}`
                            });
                        }
                    }
                    if (isUdprLeader) {
                        badges.push({
                            label: 'MVP UDPR',
                            tone: 'udpr',
                            tooltip: `Lineupin paras UDPR: ${formatNumber(player.udpr, 2)}`
                        });
                    }
                }
                return {
                    ...player,
                    mvpBadges: badges
                };
            });
            const formatMapRecord = mapEntry => {
                const record = mapEntry.draws
                    ? `${mapEntry.wins}-${mapEntry.losses}-${mapEntry.draws}`
                    : `${mapEntry.wins}-${mapEntry.losses}`;
                return `${mapEntry.mapName} ${record}`;
            };
            const preview = mapBreakdown.slice(0, 3).map(formatMapRecord);
            const remainder = mapBreakdown.length > 3 ? ` +${mapBreakdown.length - 3}` : '';

            return {
                signature: entry.signature,
                lineupLabel: playersWithBadges.map(player => player.nickname).join(', '),
                players: playersWithBadges,
                mvpKdPlayerId: kdLeader?.playerId || null,
                mvpAdrPlayerId: adrLeader?.playerId || null,
                matchesPlayed,
                mapsPlayed,
                wins: entry.wins,
                losses: entry.losses,
                draws: entry.draws,
                recordLine: entry.draws ? `${entry.wins}-${entry.losses}-${entry.draws}` : `${entry.wins}-${entry.losses}`,
                winRate,
                roundDiff,
                adr,
                udpr: entry.playerRounds ? entry.players.reduce((sum, player) => sum + toNumber(player.utilityDamage, 0), 0) / entry.playerRounds : 0,
                kd,
                hsPct,
                mapBreakdown,
                mapBreakdownSummary: preview.length ? `${preview.join(' · ')}${remainder}` : '-',
                mapBreakdownTitle: mapBreakdown.length
                    ? mapBreakdown.map(mapEntry => `${mapEntry.mapName}: ${mapEntry.wins}-${mapEntry.losses}${mapEntry.draws ? `-${mapEntry.draws}` : ''} (${mapEntry.played} karttaa, ${formatPercent(mapEntry.winRate, 1)})`).join(' • ')
                    : 'Ei karttajakaumaa'
            };
        })
        .sort((left, right) => {
            if (right.mapsPlayed !== left.mapsPlayed) return right.mapsPlayed - left.mapsPlayed;
            if (right.winRate !== left.winRate) return right.winRate - left.winRate;
            if (right.roundDiff !== left.roundDiff) return right.roundDiff - left.roundDiff;
            return String(left.lineupLabel).localeCompare(String(right.lineupLabel), 'fi');
        });
}

function normalizeMatch(match, teamId = null) {
    if (!match) return null;
    const matchId = match.matchId;
    const playedFlag = toNumber(match.played);
    const bestOf = toNumber(match.bestOf);
    const matchWinnerId = match.winnerTeamId || null;
    const matchIsForfeit = !!match.isForfeit;
    const ignoredDueBan = !!match.ignoredDueBan;
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
            demoUrl: m.demo_url || m.demoUrl || m.download_url || m.downloadUrl || '',
            demoUrls: Array.isArray(m.demo_urls || m.demoUrls) ? (m.demo_urls || m.demoUrls) : [],
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
    let roundsFor = maps.reduce((sum, m) => sum + m.scoreFor, 0);
    let roundsAgainst = maps.reduce((sum, m) => sum + m.scoreAgainst, 0);
    const played = maps.length || playedFlag;
    const seriesMaps = Math.max(1, bestOf || 2);
    let teamScore = mapWins;
    let oppScore = mapLosses;
    if (!maps.length && matchIsForfeit && matchWinnerId) {
        const teamWon = String(matchWinnerId) === String(teamId);
        teamScore = teamWon ? seriesMaps : 0;
        oppScore = teamWon ? 0 : seriesMaps;
        roundsFor = teamWon ? (seriesMaps * 13) : 0;
        roundsAgainst = teamWon ? 0 : (seriesMaps * 13);
    }
    const roundDiff = roundsFor - roundsAgainst;
    const ts = scheduledMatchTsSeconds(match);

    return {
        matchId,
        ts: ts || 0,
        status: match.status || (playedFlag ? 'finished' : 'scheduled'),
        bestOf: bestOf || Math.max(1, maps.length),
        played,
        forfeitedMaps,
        isForfeit: matchIsForfeit,
        ignoredDueBan,
        winnerTeamId: matchWinnerId,
        teamScore,
        oppScore,
        mapDraws,
        roundsFor,
        roundsAgainst,
        roundDiff,
        team1Name: match.team1Name || myName,
        team2Name: match.team2Name || oppName,
        opponentName: oppName,
        me: mySide,
        opponent: oppSide,
        faceitUrl: match.faceitUrl || '',
        demoUrl: match.demo_url || match.demoUrl || '',
        demoUrls: Array.isArray(match.demo_urls || match.demoUrls) ? (match.demo_urls || match.demoUrls) : [],
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

function computePerformanceBadge(teamValue, divAvg, metricKey) {
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
}

function resolveTabFromQuery(route) {
    const tab = route?.query?.tab;
    if (tab === 'veto') return 'matches';
    const allowed = new Set(['overview', 'matches', 'players']);
    return allowed.has(tab) ? tab : 'overview';
}

window.TeamDetail = {
    name: 'TeamDetail',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; },
        get SortableTable() { return window.SortableTable; },
        get SharedMapPerformanceTable() { return window.SharedMapPerformanceTable; },
        get PickBanFlow() { return window.PickBanFlow; },
        get MatchExpandedDetails() { return window.MatchExpandedDetails; }
    },
    props: {
        teamId: { type: [String, Number], required: true },
        championshipId: { type: [String, Number], default: null }
    },
    data() {
        const teamStore = typeof window.useTeamStore === 'function' ? window.useTeamStore() : null;
        const upcomingStore = typeof window.useUpcomingStore === 'function' ? window.useUpcomingStore() : null;
        return {
            teamStore,
            upcomingStore,
            selectedChampionship: this.championshipId ? String(this.championshipId) : null,
            activeTab: resolveTabFromQuery(this.$route),
            mapViewMode: 'summary', // 'summary' or 'full'
            mapSubMetricMode: 'perRound', // 'perRound' or 'perMap'
            matchMetric: 'roundDiff',
            SCOUT_MAP_COLUMNS,
            MAP_COLUMNS,
            PLAYER_COLUMNS,
            LINEUP_COLUMNS,
            scoutTableKey: 0,
            detailedTableKey: 0,
            performanceTrendHover: {
                key: null,
                index: null,
                x: 0,
                y: 0
            },
            performanceTrendMode: 'cumulative',
            trendChartWidth: 640,
            trendChartHeight: 140,
            matchesHoverMatchId: null,
            matchesHoverSource: null,
            matchesChartWidth: 640,
            matchesChartHeight: 140,
            expandedMatches: {},
            matchPlayerStatsState: {},
            replay2StatusByMatch: {},
            mapCatalog: [],
            mapCatalogLoading: false,
            mapCatalogLoaded: false,
            playerBaselineMode: 'avg',
            divisionPlayerBaselinesState: {},
            inFlightLoads: {
                bootstrap: {},
                season: {},
                upcoming: {},
                divisionBaselines: {},
                matchPlayerStats: {},
                demoAvailability: {},
                mapCatalog: {}
            }
        };
    },
    computed: {
        teamEntry() {
            if (!this.teamStore || !this.teamId) return null;
            return this.teamStore.getTeamState(this.teamId);
        },
        pageCacheChampionshipId() {
            if (this.selectedChampionship) return String(this.selectedChampionship);
            if (this.championshipId) return String(this.championshipId);
            return null;
        },
        pageSegment() {
            if (!this.teamStore || !this.teamId || typeof this.teamStore.getTeamPageSegment !== 'function') {
                return this.teamEntry?.page || createSegment();
            }
            return this.teamStore.getTeamPageSegment(this.teamId, this.pageCacheChampionshipId)
                || this.teamEntry?.page
                || createSegment();
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
                const matchesPlayed = toNumber(season.matchesPlayed ?? season.matches_played ?? 0);
                const mapsPlayed = toNumber(season.mapsPlayed ?? season.maps_played ?? 0);
                return {
                    value: value ? String(value) : null,
                    label: season.name || `Kausi ${season.season} · Div ${season.divisionNum}`,
                    season: toNumber(season.season),
                    division: season.divisionNum,
                    isPlayoffs: season.isPlayoffs,
                    matchesPlayed,
                    mapsPlayed,
                    hasActivity: matchesPlayed > 0 || mapsPlayed > 0
                };
            }).filter(option => option.value && (!option.isPlayoffs || option.hasActivity));
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
        upcomingParams() {
            return {
                teamId: this.teamId,
                championshipId: this.currentChampionshipId,
                limit: 100,
                offset: 0
            };
        },
        upcomingState() {
            if (!this.upcomingStore || typeof this.upcomingStore.getEntryForParams !== 'function') {
                return { data: [], loading: false, error: null };
            }
            if (!this.teamId || !this.currentChampionshipId) {
                return { data: [], loading: false, error: null };
            }
            return this.upcomingStore.getEntryForParams(this.upcomingParams);
        },
        upcomingScheduleByMatchId() {
            const scheduleMap = {};
            const globalUtils = typeof window !== 'undefined' ? window.matchTimeUtils : null;
            const rows = Array.isArray(this.upcomingState.data) ? this.upcomingState.data : [];
            rows.forEach(match => {
                if (!match || typeof match !== 'object') return;
                const matchId = match.match_id ?? match.matchId ?? null;
                if (!matchId) return;
                let scheduledMs = null;
                if (globalUtils && typeof globalUtils.getScheduledTs === 'function') {
                    scheduledMs = globalUtils.getScheduledTs(match);
                } else {
                    scheduledMs = scheduledMatchTsSeconds(match) * 1000;
                }
                if (!Number.isFinite(scheduledMs) || scheduledMs <= 0) return;
                scheduleMap[String(matchId)] = Math.round(scheduledMs / 1000);
            });
            return scheduleMap;
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
                .map(entry => ({
                    ...entry,
                    mapImage: window.MapImageUtils
                        ? window.MapImageUtils.resolveMapImage(entry, { mapCatalog: this.mapCatalog, apiClient: window.apiClient })
                        : null
                }))
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
            const playedMatches = this.matchesList.filter(m => m.played && !m.ignoredDueBan);
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

            const missingTip = 'Ei dataa valitulle kaudelle.';
            return {
                primary: [
                    {
                        key: 'mapWinRate',
                        label: 'Kartta voitto-%',
                        value: mapsPlayed > 0 ? formatPercent(mapWinRate, 1) : '—',
                        sub: '',
                        tone: 'stat-primary',
                        tooltip: mapsPlayed > 0
                            ? 'Voitetut kartat / pelatut kartat. Tavalliset luovutukset ovat mukana; bänni- ja lopetusottelut eivät.'
                            : missingTip,
                        trendValue: mapsPlayed > 0 ? mapWinRate : null,
                        divAvg: divAvgs.avgMapWinRate || null,
                        trendTooltip: mapsPlayed > 0 ? `Verrattuna divisioonan kauden keskiarvoon · ${formatPercent(divAvgs.avgMapWinRate || 0, 1)}` : '',
                        performanceBadge: computePerformanceBadge(mapsPlayed > 0 ? mapWinRate : null, divAvgs.avgMapWinRate || null, 'mapWinRate')
                    },
                    {
                        key: 'rounds',
                        label: 'Eräero',
                        value: formatNumber(roundsDiff),
                        sub: `${formatNumber(roundsWon)}–${formatNumber(roundsLost)}`,
                        tone: 'stat-primary',
                        tooltip: 'Voitetut erät miinus hävityt erät. Tavallinen luovutus on 13–0 tai 0–13; bänni- ja keskeyttäneet joukkueet eivät sisälly tilastoihin.',
                        trendValue: roundsDiff,
                        divAvg: divAvgs.avgRoundDiff || null,
                        trendTooltip: `Verrattuna divisioonan kauden keskiarvoon · ${formatNumber(divAvgs.avgRoundDiff || 0, 1)}`,
                        performanceBadge: computePerformanceBadge(roundsDiff, divAvgs.avgRoundDiff || null, 'rounds')
                    },
                    {
                        key: 'matches',
                        label: 'Ottelut',
                        value: formatNumber(matches),
                        sub: `${formatNumber(matchWins)}–${formatNumber(matchLosses)}`,
                        tone: 'stat-primary',
                        tooltip: `Pelatut ottelut. Voitot–tappiot: ${formatNumber(matchWins)}–${formatNumber(matchLosses)}.`,
                        trendValue: null,
                        divAvg: null,
                        performanceBadge: null
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
                    winrate,
                    mapImage: map.mapImage || (window.MapImageUtils
                        ? window.MapImageUtils.resolveMapImage(map, { mapCatalog: this.mapCatalog, apiClient: window.apiClient })
                        : null)
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
                const dateLabel = formatMatchDate(match.ts);
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
                if (!row.mapName || row.mapName.toLowerCase() === 'forfeit') return;
                pickLookup[mapKey(row.mapName)] = normalizePercent(row.pickRate) || 0;
                playedLookup[mapKey(row.mapName)] = toNumber(row.games || row.played || 0);
                pool.set(mapKey(row.mapName), { mapName: row.mapName });
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
                    const stepLower = (step.mapName || '').toLowerCase();
                    if (!step.mapName || stepLower === 'forfeit' || stepLower === 'kartta') return;
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
                let opponentBanCount = 0;
                let teamPickCount = 0;
                let opponentPickCount = 0;
                entry.steps.forEach(step => {
                    if (!step.mapName) return;
                    const key = mapKey(step.mapName);
                    const bucket = actions[key] || { pick: null, ban: null };
                    if (step.action === 'pick') {
                        let order = null;
                        if (step.actor === 'team') {
                            teamPickCount += 1;
                            order = teamPickCount;
                        } else if (step.actor === 'opponent') {
                            opponentPickCount += 1;
                            order = opponentPickCount;
                        }
                        bucket.pick = { actor: step.actor, teamName: step.teamName, order };
                    }
                    if (step.action === 'ban') {
                        let order = null;
                        if (step.actor === 'team') {
                            teamBanCount += 1;
                            order = teamBanCount;
                        } else if (step.actor === 'opponent') {
                            opponentBanCount += 1;
                            order = opponentBanCount;
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
                    const dateLabel = formatMatchDate(match.ts);
                    const resultKey = getMatchResult(match);
                    const resultLabel = resultKey === 'win' ? 'W' : resultKey === 'loss' ? 'L' : resultKey === 'draw' ? 'D' : 'Kesken';
                    const scoreLabel = (match.teamScore != null && match.oppScore != null) ? `${match.teamScore}-${match.oppScore}` : '';
                    const meta = this.vetoMatchMeta[match.matchId] || {};

                    // Find map winner if this map was played
                    let mapWinnerLabel = '';
                    const matchMaps = Array.isArray(match.maps) ? match.maps : [];
                    const playedMap = matchMaps.find(m => mapKey(beautifyMapName(m.map || m.mapName)) === key);
                    if (playedMap) {
                        const mapWinnerId = playedMap.winner_team_id || playedMap.winnerTeamId;
                        if (mapWinnerId) {
                            if (String(mapWinnerId) === String(this.teamId)) {
                                mapWinnerLabel = '✓ Voitto';
                            } else {
                                mapWinnerLabel = '✗ Tappio';
                            }
                        }
                    }

                    let className = 'veto-heatmap__cell--none';
                    let actionLabel = 'Ei vetoa';
                    let actionCode = '';
                    let byLabel = '';

                    if (actionPick) {
                        if (actionPick.actor === 'team') {
                            className = 'veto-heatmap__cell--team-pick';
                            actionLabel = 'Oma pick';
                            actionCode = actionPick.order ? `P${actionPick.order}` : 'P';
                            byLabel = this.teamInfo?.teamName || 'Oma joukkue';
                        } else if (actionPick.actor === 'opponent') {
                            className = 'veto-heatmap__cell--opp-pick';
                            actionLabel = 'Vastustajan pick';
                            actionCode = actionPick.order ? `VP${actionPick.order}` : 'VP';
                            byLabel = opponent;
                        }
                    } else if (actionBan) {
                        if (actionBan.actor === 'team') {
                            className = 'veto-heatmap__cell--team-ban';
                            actionLabel = 'Oma banni';
                            actionCode = actionBan.order ? `B${actionBan.order}` : 'B';
                            byLabel = this.teamInfo?.teamName || 'Oma joukkue';
                        } else if (actionBan.actor === 'opponent') {
                            className = 'veto-heatmap__cell--opp-ban';
                            actionLabel = 'Vastustajan banni';
                            actionCode = actionBan.order ? `VB${actionBan.order}` : 'VB';
                            byLabel = opponent;
                        }
                    }

                    const actionText = actionLabel
                        ? `${actionLabel}${actionCode ? ` (${actionCode})` : ''}`
                        : '';
                    const showMapWinner = mapWinnerLabel && (actionPick || meta.decider === map.mapName || meta.overflow === map.mapName);
                    const title = [
                        map.mapName,
                        actionText,
                        showMapWinner ? mapWinnerLabel : '',
                        opponent ? `vs ${opponent}` : '',
                        scoreLabel ? `Tulos ${scoreLabel} (${resultLabel})` : `Tulos ${resultLabel}`,
                        dateLabel,
                        meta.seriesType ? meta.seriesType : '',
                        (meta.decider === map.mapName) ? 'Ratkaisukartta' : '',
                        (meta.overflow === map.mapName) ? 'Overflow' : ''
                    ].filter(Boolean).join('\n');

                    return { className, title, hasAction: !!actionInfo, actionCode };
                });

                return {
                    mapName: map.mapName,
                    rowLabel: map.mapName,
                    cells
                };
            });
        },
        vetoLegendEntries() {
            const buildCell = (className, label, code = '') => ({ className, label, code });
            return [
                buildCell('veto-heatmap__cell--team-pick', 'Oma pick', 'P1/P2'),
                buildCell('veto-heatmap__cell--opp-pick', 'Vastustajan pick', 'VP1/VP2'),
                buildCell('veto-heatmap__cell--decider', 'Ratkaisukartta / Overflow'),
                buildCell('veto-heatmap__cell--team-ban', 'Oma banni', 'B1/B2'),
                buildCell('veto-heatmap__cell--opp-ban', 'Vastustajan banni', 'VB1/VB2'),
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
                                dateLabel: formatMatchDate(match.ts),
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
                        dateLabel: formatMatchDate(match.ts),
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
            const normalized = matches.map(m => {
                const row = normalizeMatch(m, this.teamId);
                if (!row) return null;
                if (!row.played) {
                    const scheduledTs = this.upcomingScheduleByMatchId[String(row.matchId)] || 0;
                    if (scheduledTs > 0) {
                        row.ts = scheduledTs;
                    }
                }
                return row;
            }).filter(Boolean);
            return normalized.sort((a, b) => {
                const at = a.ts ?? 0;
                const bt = b.ts ?? 0;
                if (!at && bt) return 1; // missing dates go to bottom
                if (at && !bt) return -1;
                return at - bt; // oldest first for tables
            });
        },
        rawMatchesById() {
            const matches = Array.isArray(this.seasonData?.matchHistory) ? this.seasonData.matchHistory : [];
            const lookup = {};
            matches.forEach(match => {
                if (match?.matchId) {
                    lookup[match.matchId] = match;
                }
            });
            return lookup;
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
        // Player stats table uses every DB field: maps/rounds/kills/deaths/assists/mvps/sniper_kills/utility_damage/enemies_flashed/flash_count/flash_successes/entry_count/entry_wins/clutch fields/pistol_kills/adr/kr/kd/hs_pct/damage/multi-kills
        players() {
            const players = Array.isArray(this.seasonData?.playerStats) ? this.seasonData.playerStats : [];
            return players.map((p, idx) => normalizePlayer(p, idx)).filter(Boolean);
        },
        lineupRows() {
            return buildLineupRows(this.matchPlayerStatsCurrent.items || [], this.matchesList || [], this.teamId);
        },
        playerHeaderGroups() {
            return buildColumnGroups(PLAYER_COLUMNS, PLAYER_GROUP_META);
        },
        lineupHeaderGroups() {
            return buildColumnGroups(LINEUP_COLUMNS, LINEUP_GROUP_META);
        },
        possibleLineupCount() {
            return combinationCount(this.players.length, 5);
        },
        lineupCoverageLabel() {
            const rosterSize = this.players.length;
            const possible = this.possibleLineupCount;
            if (rosterSize < 5) {
                return `Pelaajia ${rosterSize} · ei 5 pelaajan lineupia`;
            }
            return `${this.lineupRows.length} pelattu / ${formatNumber(possible)} mahdollista`;
        },
        playerDefaultSort() {
            return { column: 'adr', order: 'desc', numeric: true };
        },
        lineupDefaultSort() {
            return { column: 'mapsPlayed', order: 'desc', numeric: true };
        },
        playerSummaryCards() {
            if (!this.players.length) return [];
            const totals = this.players.reduce((acc, row) => {
                acc.maps += toNumber(row.mapsPlayed);
                acc.rounds += toNumber(row.roundsPlayed);
                acc.adrWeighted += toNumber(row.adr) * Math.max(1, toNumber(row.roundsPlayed));
                acc.kdWeighted += toNumber(row.kd) * Math.max(1, toNumber(row.roundsPlayed));
                acc.entryWins += toNumber(row.entryWins);
                acc.entryCount += toNumber(row.entryCount);
                acc.flashSuccesses += toNumber(row.flashSuccesses);
                acc.flashCount += toNumber(row.flashCount);
                return acc;
            }, {
                maps: 0,
                rounds: 0,
                adrWeighted: 0,
                kdWeighted: 0,
                entryWins: 0,
                entryCount: 0,
                flashSuccesses: 0,
                flashCount: 0
            });
            const weightedRounds = Math.max(1, totals.rounds);
            const avgAdr = totals.adrWeighted / weightedRounds;
            const avgKd = totals.kdWeighted / weightedRounds;
            const entryPct = totals.entryCount ? (totals.entryWins / totals.entryCount) * 100 : 0;
            const flashPct = totals.flashCount ? (totals.flashSuccesses / totals.flashCount) * 100 : 0;
            return [
                { key: 'players', label: 'Pelaajia', value: formatNumber(this.players.length), meta: 'Aktiivinen rosteri' },
                { key: 'maps', label: 'Kartat yhteensä', value: formatNumber(totals.maps), meta: `${formatNumber(totals.rounds)} erää` },
                { key: 'adr', label: 'ADR keskiarvo', value: formatNumber(avgAdr, 1), meta: 'Painotettu erillä' },
                { key: 'kd', label: 'K/D keskiarvo', value: formatNumber(avgKd, 2), meta: 'Painotettu erillä' },
                { key: 'entry', label: 'Entry onnistuminen', value: formatPercent(entryPct, 1), meta: `${formatNumber(totals.entryWins)} / ${formatNumber(totals.entryCount)}` },
                { key: 'flash', label: 'Flash onnistuminen', value: formatPercent(flashPct, 1), meta: `${formatNumber(totals.flashSuccesses)} / ${formatNumber(totals.flashCount)}` }
            ];
        },
        playerBaselineState() {
            const key = this.currentChampionshipId ? String(this.currentChampionshipId) : null;
            if (!key) return { loading: false, error: null, avg: {}, median: {} };
            return this.divisionPlayerBaselinesState[key] || { loading: false, error: null, avg: {}, median: {} };
        },
        activePlayerBaselines() {
            const mode = this.playerBaselineMode === 'median' ? 'median' : 'avg';
            return this.playerBaselineState?.[mode] || {};
        },
        playerBaselineHint() {
            if (this.playerBaselineState.loading) return 'Haetaan divisioonan vertailuarvoja...';
            if (this.playerBaselineState.error) return this.playerBaselineState.error;
            const modeLabel = this.playerBaselineMode === 'median' ? 'mediaani' : 'keskiarvo';
            return `Värikoodaus suhteessa divisioonan ${modeLabel}arvoihin.`;
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
        // Veto history: match_id/map_name/status/selected_by_team_id/_name/round_num/order -> rendered as BO2/BO3 step timeline
        vetoHistory() {
            const raw = Array.isArray(this.seasonData?.vetoHistory) ? this.seasonData.vetoHistory : [];
            return raw
                .filter(entry => (entry.mapName || '').trim().toLowerCase() !== 'forfeit')
                .map(entry => ({
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
        vetoByMatchLookup() {
            const lookup = {};
            this.vetoByMatch.forEach(entry => {
                lookup[entry.matchId] = entry;
            });
            return lookup;
        },
        vetoSummaryLookup() {
            const lookup = {};
            this.vetoByMatch.forEach(entry => {
                lookup[entry.matchId] = entry.steps.map(s => `${s.step}. ${s.label}: ${s.mapName}`).join(' • ');
            });
            return lookup;
        },
        matchPlayerStatsCurrent() {
            const champId = this.currentChampionshipId;
            const fallback = { items: [], byMatch: {}, loading: false, error: null };
            if (!champId) return fallback;
            const scopedKey = this.scopedSeasonKey(champId);
            return this.matchPlayerStatsState[scopedKey] || fallback;
        },
        matchPlayerStatsByMatch() {
            return this.matchPlayerStatsCurrent.byMatch || {};
        },
        matchPlayerStatsLoading() {
            return !!this.matchPlayerStatsCurrent.loading;
        },
        isDemoAvailabilityLoading() {
            return Object.values(this.replay2StatusByMatch).some(statusMap =>
                Object.values(statusMap).some(s => s === 'loading')
            );
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
                const playerId = pr.playerId ?? pr.player_id;
                if (!playerId) return;
                lookup[playerId] = {
                    roles: pr.roles || [],
                    primaryRole: pr.primaryRole ?? pr.primary_role ?? null,
                    roleStats: pr.roleStats || pr.role_stats || null
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
        currentChampionshipId: {
            immediate: true,
            handler() {
                this.loadUpcoming();
                this.ensureDivisionPlayerBaselines(this.currentChampionshipId);
                if (this.activeTab === 'matches') {
                    this.ensureMatchesTabData(this.currentChampionshipId);
                }
                if (this.activeTab === 'players') {
                    this.ensureMatchPlayerStats(this.currentChampionshipId);
                }
            }
        },
        mapStats: {
            immediate: true,
            handler(newStats) {
                if (window.MapImageUtils && window.MapImageUtils.shouldFetchCatalog(newStats)) {
                    this.ensureMapCatalog();
                }
            }
        },
        '$route.query.tab'(newVal) {
            const nextTab = resolveTabFromQuery({ query: { tab: newVal } });
            if (nextTab !== this.activeTab) {
                this.activeTab = nextTab;
            }
            if (newVal === 'veto') {
                this.updateRoute(this.currentChampionshipId, 'matches');
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
                    this.ensureMatchesTabData(this.currentChampionshipId);
                });
            }
            if (newVal === 'players') {
                this.$nextTick(() => {
                    this.ensureMatchPlayerStats(this.currentChampionshipId);
                });
            }
        },
        matchesList() {
            if (this.activeTab === 'matches') {
                this.ensureMatchesTabData(this.currentChampionshipId);
            }
        },
        championshipId(newVal) {
            if (newVal) {
                this.selectedChampionship = String(newVal);
                this.fetchSeason(String(newVal), { force: true });
                this.expandedMatches = {};
                if (this.activeTab === 'matches') {
                    this.ensureMatchesTabData(String(newVal));
                }
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
            if (window.MapImageUtils && window.MapImageUtils.shouldFetchCatalog(this.mapStats)) {
                this.ensureMapCatalog();
            }
            if (this.activeTab === 'matches') {
                this.ensureMatchesTabData(this.currentChampionshipId);
            }
            if (this.activeTab === 'players') {
                this.ensureMatchPlayerStats(this.currentChampionshipId);
            }
        });
    },
    beforeUnmount() {
        this.teardownMapTableScroll();
        this.teardownTrendChartObserver();
        this.teardownMatchesChartObserver();
    },
    methods: {
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
        scopedSeasonKey(championshipId) {
            if (!championshipId) return null;
            const teamPart = this.teamId ? String(this.teamId) : 'unknown';
            return `${teamPart}::${String(championshipId)}`;
        },
        hasDemoCandidateMatches() {
            const matches = Array.isArray(this.matchesList) ? this.matchesList : [];
            return matches.some(match => !!match?.played && Array.isArray(match?.maps) && match.maps.length > 0);
        },
        async ensureMatchesTabData(championshipId) {
            if (!championshipId || !this.teamId) return;
            const key = this.scopedSeasonKey(championshipId);
            if (!key) return;
            const loadKey = `${key}::hydrate`;
            return this.runInFlightLoad('season', loadKey, async () => {
                const hasMatches = Array.isArray(this.matchesList) && this.matchesList.length > 0;
                const hasDemoCandidates = this.hasDemoCandidateMatches();
                if ((!hasMatches || !hasDemoCandidates) && this.teamStore) {
                    await this.teamStore.fetchTeamPage(this.teamId, championshipId, { force: true });
                    await this.$nextTick();
                }
                this.ensureMatchPlayerStats(championshipId);
                this.loadReplay2StatusForAllMatches();
                this.ensureMapCatalog();
            });
        },
        formatDate(ts) {
            return formatMatchDate(ts);
        },
        formatTime(ts) {
            return formatMatchTime(ts);
        },
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
        subMetricValue(perRound, perMap, decimals = 3) {
            if (this.mapSubMetricMode === 'perMap') {
                return formatPerRound(perMap, decimals);
            }
            return formatPerRound(perRound, decimals);
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
        setPlayerBaselineMode(mode) {
            if (mode !== 'avg' && mode !== 'median') return;
            this.playerBaselineMode = mode;
        },
        playerBaselineMetricKey(metricKey) {
            if (this.playerBaselineMode === 'median') {
                if (metricKey === 'kd') return 'median_kd';
                if (metricKey === 'adr') return 'median_adr';
                if (metricKey === 'kr') return 'median_kr';
                if (metricKey === 'hsPct') return 'median_hs_pct';
                if (metricKey === 'udpr') return 'median_udpr';
                if (metricKey === 'flashSuccessLine') return 'median_flash_success_pct';
                return null;
            }
            if (metricKey === 'kd') return 'avg_kd';
            if (metricKey === 'adr') return 'avg_adr';
            if (metricKey === 'udpr') return 'avg_udpr';
            if (metricKey === 'kr') return 'avg_kr';
            if (metricKey === 'hsPct') return 'avg_hs_pct';
            if (metricKey === 'flashSuccessLine') return 'avg_flash_success_pct';
            return null;
        },
        playerMetricBaseline(metricKey) {
            const baselineKey = this.playerBaselineMetricKey(metricKey);
            if (!baselineKey) return null;
            const value = toNumber(this.activePlayerBaselines?.[baselineKey], null);
            return Number.isFinite(value) ? value : null;
        },
        playerMetricValue(row, metricKey) {
            if (!row) return null;
            if (metricKey === 'hsPct') return toNumber(row.hsPct, null);
            if (metricKey === 'flashSuccessLine') return toNumber(row.flashSuccessLine, null);
            if (metricKey === 'udpr') {
                if (row.udpr !== undefined && row.udpr !== null) return toNumber(row.udpr, null);
                const utilityDamage = toNumber(row.utilityDamage, null);
                const roundsPlayed = toNumber(row.roundsPlayed, null);
                if (Number.isFinite(utilityDamage) && Number.isFinite(roundsPlayed) && roundsPlayed > 0) {
                    return utilityDamage / roundsPlayed;
                }
            }
            return toNumber(row[metricKey], null);
        },
        playerMetricClass(row, metricKey) {
            const baseline = this.playerMetricBaseline(metricKey);
            const value = this.playerMetricValue(row, metricKey);
            if (!Number.isFinite(baseline) || !Number.isFinite(value)) return '';
            const epsilonByMetric = {
                kd: 0.01,
                adr: 0.2,
                udpr: 0.05,
                kr: 0.005,
                hsPct: 0.15,
                flashSuccessLine: 0.15
            };
            const epsilon = epsilonByMetric[metricKey] ?? 0.01;
            const diff = value - baseline;
            if (Math.abs(diff) <= epsilon) return 'player-metric-tone--neutral';
            return diff > 0 ? 'player-metric-tone--positive' : 'player-metric-tone--negative';
        },
        playerMetricTitle(row, metricKey, label, decimals = 2, asPercent = false) {
            const baseline = this.playerMetricBaseline(metricKey);
            const value = this.playerMetricValue(row, metricKey);
            if (!Number.isFinite(baseline) || !Number.isFinite(value)) return '';
            const modeLabel = this.playerBaselineMode === 'median' ? 'median' : 'avg';
            const metricValue = asPercent ? formatPercent(value, 1) : formatNumber(value, decimals);
            const baselineValue = asPercent ? formatPercent(baseline, 1) : formatNumber(baseline, decimals);
            return `${label}: ${metricValue} · Div ${modeLabel}: ${baselineValue}`;
        },
        lineupPlayerStatLine(player) {
            if (!player) return '';
            const badges = Array.isArray(player?.mvpBadges) && player.mvpBadges.length
                ? ` | ${player.mvpBadges.map(badge => badge.label).join(', ')}`
                : '';
            return `- ${player.nickname} | K/D ${formatNumber(player.kd, 2)} | ADR ${formatNumber(player.adr, 1)} | UDPR ${formatNumber(player.udpr, 2)} | HS% ${formatPercent(player.hsPct || 0, 1)}${badges}`;
        },
        lineupSummaryTooltip(row) {
            if (!row) return '';
            const lines = [
                `Lineup: ${row.lineupLabel || '-'}`,
                `Ottelut ${formatNumber(row.matchesPlayed)} · Kartat ${formatNumber(row.mapsPlayed)} · Win ${formatPercent(row.winRate, 1)} · RD ${formatSignedNumber(row.roundDiff, 0)}`
            ];
            const players = Array.isArray(row.players) ? row.players : [];
            if (players.length) lines.push('');
            players.forEach(player => {
                const line = this.lineupPlayerStatLine(player);
                if (line) lines.push(line);
            });
            return lines.join('\n');
        },
        lineupMetricTooltip(row, metricKey, label, decimals = 2, asPercent = false) {
            if (!row) return '';
            const baseTitle = this.playerMetricTitle(row, metricKey, label, decimals, asPercent);
            const players = Array.isArray(row.players) ? [...row.players] : [];
            if (!players.length) return baseTitle;
            players.sort((left, right) => {
                const rightValue = toNumber(this.playerMetricValue(right, metricKey), 0);
                const leftValue = toNumber(this.playerMetricValue(left, metricKey), 0);
                if (Math.abs(rightValue - leftValue) > 1e-9) return rightValue - leftValue;
                const killDiff = toNumber(right?.kills, 0) - toNumber(left?.kills, 0);
                if (killDiff !== 0) return killDiff;
                return String(left?.nickname || '').localeCompare(String(right?.nickname || ''), 'fi');
            });
            const metricLines = players.map(player => {
                const value = this.playerMetricValue(player, metricKey);
                const formatted = asPercent ? formatPercent(value || 0, 1) : formatNumber(value, decimals);
                return `- ${player.nickname} | ${label} ${formatted}`;
            });
            return [baseTitle, '', ...metricLines].filter(Boolean).join('\n');
        },
        buildDivisionPlayerBaselines(divisionAverages = {}, divisionDetails = {}) {
            const playerTotals = Array.isArray(divisionDetails?.player_totals) ? divisionDetails.player_totals : [];
            const qualified = playerTotals.filter(row => toNumber(row?.maps_played, 0) >= 3);
            const sourceRows = qualified.length ? qualified : playerTotals;
            const collect = key => sourceRows
                .map(row => toNumber(row?.[key], null))
                .filter(value => Number.isFinite(value));
            const mean = values => {
                if (!values.length) return null;
                return values.reduce((sum, value) => sum + value, 0) / values.length;
            };
            const flashSuccessPcts = sourceRows
                .map(row => {
                    const flashCount = toNumber(row?.flash_count, 0);
                    const flashSuccesses = toNumber(row?.flash_successes, 0);
                    if (!flashCount) return null;
                    return (flashSuccesses / flashCount) * 100;
                })
                .filter(value => Number.isFinite(value));
            const udprValues = sourceRows
                .map(row => {
                    const roundsPlayed = toNumber(row?.rounds_played, 0);
                    const utilityDamage = toNumber(row?.utility_damage, 0);
                    if (!roundsPlayed) return null;
                    return utilityDamage / roundsPlayed;
                })
                .filter(value => Number.isFinite(value));

            const avg = {
                avg_kd: toNumber(divisionAverages?.avg_kd, null),
                avg_adr: toNumber(divisionAverages?.avg_adr, null),
                avg_udpr: mean(udprValues),
                avg_kr: toNumber(divisionAverages?.avg_kr, null),
                avg_hs_pct: toNumber(divisionAverages?.avg_hs_pct, null),
                avg_flash_success_pct: mean(flashSuccessPcts)
            };
            if (!Number.isFinite(avg.avg_kd)) avg.avg_kd = mean(collect('kd'));
            if (!Number.isFinite(avg.avg_adr)) avg.avg_adr = mean(collect('adr'));
            if (!Number.isFinite(avg.avg_kr)) avg.avg_kr = mean(collect('kr'));
            if (!Number.isFinite(avg.avg_hs_pct)) avg.avg_hs_pct = mean(collect('hs_pct'));

            const median = {
                median_kd: computeMedian(collect('kd')),
                median_adr: computeMedian(collect('adr')),
                median_udpr: computeMedian(udprValues),
                median_kr: computeMedian(collect('kr')),
                median_hs_pct: computeMedian(collect('hs_pct')),
                median_flash_success_pct: computeMedian(flashSuccessPcts)
            };

            return { avg, median };
        },
        async ensureDivisionPlayerBaselines(championshipId) {
            if (!championshipId || !window.apiClient) return;
            const key = String(championshipId);
            const existing = this.divisionPlayerBaselinesState[key];
            const hasAvg = !!Object.keys(existing?.avg || {}).length;
            const hasMedian = !!Object.keys(existing?.median || {}).length;
            if ((hasAvg || hasMedian) && !existing?.error) return;
            return this.runInFlightLoad('divisionBaselines', key, async () => {
                this.divisionPlayerBaselinesState = {
                    ...this.divisionPlayerBaselinesState,
                    [key]: {
                        loading: true,
                        error: null,
                        avg: existing?.avg || {},
                        median: existing?.median || {}
                    }
                };
                try {
                    const [divisionAverages, divisionDetails] = await Promise.all([
                        window.apiClient.getDivisionAverages(key).catch(() => ({})),
                        window.apiClient.getDivisionById(key).catch(() => ({}))
                    ]);
                    const baselines = this.buildDivisionPlayerBaselines(divisionAverages, divisionDetails);
                    this.divisionPlayerBaselinesState = {
                        ...this.divisionPlayerBaselinesState,
                        [key]: {
                            loading: false,
                            error: null,
                            avg: baselines.avg || {},
                            median: baselines.median || {}
                        }
                    };
                } catch (error) {
                    this.divisionPlayerBaselinesState = {
                        ...this.divisionPlayerBaselinesState,
                        [key]: {
                            loading: false,
                            error: 'Divisioonan vertailuarvojen lataus epäonnistui.',
                            avg: {},
                            median: {}
                        }
                    };
                }
            });
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
        isMatchExpanded(matchId) {
            return !!this.expandedMatches[matchId];
        },
        toggleMatchExpand(matchId) {
            if (!matchId) return;
            const next = !this.expandedMatches[matchId];
            this.expandedMatches = { ...this.expandedMatches, [matchId]: next };
            if (next) {
                this.ensureMatchPlayerStats(this.currentChampionshipId);
                this.ensureMapCatalog();
            }
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
        matchScoreClass(match) {
            const result = getMatchResult(match);
            if (result === 'win') return 'match-score--win';
            if (result === 'loss') return 'match-score--loss';
            return '';
        },
        isForfeitOnlyMatch(match) {
            if (!match) return false;
            const maps = Array.isArray(match.maps) ? match.maps : [];
            return !!match.isForfeit && maps.length === 0;
        },
        forfeitScoreLabel(match) {
            if (!this.isForfeitOnlyMatch(match)) return '';
            const result = getMatchResult(match);
            if (result === 'win') return 'Forfeit win';
            if (result === 'loss') return 'Forfeit loss';
            return 'Forfeit';
        },
        formatTrendValue(metric, value) {
            if (!metric) return formatNumber(value, 2);
            return metric.format ? metric.format(value) : formatNumber(value, metric.decimals || 0);
        },
        async bootstrap() {
            if (!this.teamStore || !this.teamId) return;
            const key = `${this.teamId}::${this.selectedChampionship || 'auto'}`;
            return this.runInFlightLoad('bootstrap', key, async () => {
                try {
                    const data = await this.teamStore.fetchTeamPage(this.teamId, this.selectedChampionship);
                    if (data?.currentChampionshipId) {
                        this.selectedChampionship = String(data.currentChampionshipId);
                        this.updateRoute(this.selectedChampionship, this.activeTab);
                    }
                    this.loadUpcoming();
                    if (this.activeTab === 'matches') {
                        await this.$nextTick();
                        this.ensureMatchesTabData(this.currentChampionshipId);
                    }
                } catch (err) {
                    console.error('TeamDetail bootstrap failed', err);
                }
            });
        },
        async fetchSeason(championshipId, options = {}) {
            if (!this.teamStore || !this.teamId || !championshipId) return;
            const key = `${this.teamId}::${championshipId}::${options.force === true ? 'force' : 'cached'}`;
            return this.runInFlightLoad('season', key, async () => {
                try {
                    await this.teamStore.fetchTeamPage(this.teamId, championshipId, options);
                    if (this.activeTab === 'matches') {
                        await this.$nextTick();
                        this.ensureMatchesTabData(String(championshipId));
                    }
                } catch (err) {
                    console.error('TeamDetail season fetch failed', err);
                }
            });
        },
        async loadUpcoming(options = {}) {
            if (!this.upcomingStore || !this.teamId || !this.currentChampionshipId) return;
            const key = `${this.teamId}::${this.currentChampionshipId}::${options.force === true ? 'force' : 'cached'}`;
            return this.runInFlightLoad('upcoming', key, async () => {
                try {
                    await this.upcomingStore.fetchUpcomingMatches(
                        this.upcomingParams,
                        { force: options.force === true }
                    );
                } catch (error) {
                    console.error('[TeamDetail] upcoming schedule fetch failed', error);
                }
            });
        },
        selectChampionship(championshipId) {
            if (!championshipId || championshipId === this.currentChampionshipId) return;
            this.selectedChampionship = championshipId;
            this.fetchSeason(championshipId);
            this.updateRoute(championshipId, this.activeTab);
            this.expandedMatches = {};
            this.loadUpcoming();
        },
        matchOpponentRoute(match) {
            const championshipId = this.currentChampionshipId;
            const opponentId = match?.opponent?.team_id || null;
            if (!championshipId || !opponentId) return null;
            return {
                name: 'team-detail',
                params: { championshipId, teamId: String(opponentId) }
            };
        },
        replay2Links(match) {
            const matchId = String(match?.matchId || match?.match_id || '');
            if (!matchId) return [];
            const statusMap = this.replay2StatusByMatch[matchId] || {};
            const links = [];
            for (const [mapIdStr, status] of Object.entries(statusMap)) {
                const mapId = Number(mapIdStr);
                if (!Number.isFinite(mapId) || mapId <= 0) continue;
                if (status === 'queued' || status === 'parsing' || status === 'ready') {
                    links.push({ mapId, status, matchId });
                }
            }
            links.sort((a, b) => a.mapId - b.mapId);
            return links;
        },
        replay2PlayerUrl(matchId, mapId) {
            return `https://replay2.pappa.aukko.net/player?faceit_match_id=${encodeURIComponent(matchId)}&map_id=${mapId}`;
        },
        async loadReplay2StatusForMatch(matchId, mapsCount) {
            if (!matchId || mapsCount <= 0) return;
            const loadingMap = {};
            for (let mapId = 1; mapId <= mapsCount; mapId++) {
                loadingMap[mapId] = 'loading';
            }
            this.replay2StatusByMatch = {
                ...this.replay2StatusByMatch,
                [matchId]: { ...(this.replay2StatusByMatch[matchId] || {}), ...loadingMap }
            };
            await Promise.all(
                Array.from({ length: mapsCount }, (_, i) => i + 1).map(async (mapId) => {
                    let status = 'hidden';
                    try {
                        const resp = await fetch(
                            `https://replay2.pappa.aukko.net/replays/${encodeURIComponent(matchId)}/status?map_id=${mapId}`
                        );
                        if (resp.ok) {
                            const data = await resp.json();
                            const state = data?.state;
                            if (state === 'queued') status = 'queued';
                            else if (state === 'parsing') status = 'parsing';
                            else if (state === 'ready') status = 'ready';
                        }
                    } catch (_error) {
                        // network error → hidden
                    }
                    this.replay2StatusByMatch = {
                        ...this.replay2StatusByMatch,
                        [matchId]: { ...(this.replay2StatusByMatch[matchId] || {}), [mapId]: status }
                    };
                })
            );
        },
        async loadReplay2StatusForAllMatches() {
            const matches = Array.isArray(this.matchesList) ? this.matchesList : [];
            const played = matches.filter(m => m && m.played);
            if (!played.length) return;
            const maxConcurrency = 4;
            const queue = [...played];
            const workers = Array.from({ length: Math.min(maxConcurrency, queue.length) }, async () => {
                while (queue.length) {
                    const match = queue.shift();
                    if (!match) continue;
                    const matchId = String(match.matchId || match.match_id || '');
                    if (!matchId) continue;
                    const mapsCount = Math.max(
                        Array.isArray(match.maps) ? match.maps.length : 0,
                        Number(match.bestOf ?? match.best_of ?? 0),
                        2
                    );
                    await this.loadReplay2StatusForMatch(matchId, mapsCount);
                }
            });
            await Promise.allSettled(workers);
        },
        async _legacyEnsureDemoAvailability_unused(championshipId) {
            if (!championshipId || !window.apiClient) return;
            const key = this.scopedSeasonKey(championshipId);
            if (!key) return;
            const championshipKey = String(championshipId);
            const utils = window.MatchLinksUtils;
            const requests = (utils && typeof utils.buildDemoMatchRequests === 'function')
                ? utils.buildDemoMatchRequests(this.matchesList)
                : [];

            const signature = requests.map(item => `${item.matchId}:${item.expectedCount || 0}`).join('|');
            const existing = this.demoAvailabilityState[key];
            const hasFalseInExisting = Array.isArray(requests) && requests.some(item => {
                const matchId = String(item?.matchId || '');
                if (!matchId) return false;
                const map = existing?.byMatch?.[matchId] || {};
                return Object.values(map).some(payload => payload?.exists === false);
            });
            if (existing?.signature === signature && !existing?.loading && !existing?.error && !hasFalseInExisting) {
                return;
            }

            const loadKey = `${key}::${signature || 'empty'}`;
            return this.runInFlightLoad('demoAvailability', loadKey, async () => {
                this.demoAvailabilityState = {
                    ...this.demoAvailabilityState,
                    [key]: {
                        loading: true,
                        error: null,
                        signature,
                        byMatch: existing?.byMatch || {}
                    }
                };

                if (!requests.length) {
                    this.demoAvailabilityState = {
                        ...this.demoAvailabilityState,
                        [key]: {
                            loading: false,
                            error: null,
                            signature,
                            byMatch: {}
                        }
                    };
                    return;
                }

                const byMatch = { ...(existing?.byMatch || {}) };
                if (utils && typeof utils.fetchDemoAvailabilityForMatch === 'function') {
                    const maxConcurrency = 4;
                    for (let idx = 0; idx < requests.length; idx += maxConcurrency) {
                        const chunk = requests.slice(idx, idx + maxConcurrency);
                        await Promise.all(chunk.map(async req => {
                            const matchId = String(req?.matchId || '');
                            if (!matchId) return;
                            const next = await utils.fetchDemoAvailabilityForMatch({
                                apiClient: window.apiClient,
                                championshipId: championshipKey,
                                matchId,
                                mapsCount: Number(req?.expectedCount || 0),
                                existingByIndex: byMatch[matchId] || {},
                                refreshFalse: true,
                                forceRefresh: false,
                                persistCache: false,
                                onBackgroundResult: (delayedMapped) => {
                                    if (!delayedMapped || !Object.keys(delayedMapped).length) return;
                                    this.demoAvailabilityState = {
                                        ...this.demoAvailabilityState,
                                        [key]: {
                                            loading: false,
                                            error: null,
                                            signature,
                                            byMatch: {
                                                ...(this.demoAvailabilityState?.[key]?.byMatch || {}),
                                                [matchId]: delayedMapped
                                            }
                                        }
                                    };
                                }
                            });
                            byMatch[matchId] = next || {};
                        }));
                    }
                }

                this.demoAvailabilityState = {
                    ...this.demoAvailabilityState,
                    [key]: {
                        loading: false,
                        error: null,
                        signature,
                        byMatch
                    }
                };
            });
        },
        updateRoute(championshipId, tab) {
            if (!this.$router || !this.$route) return;
            const params = championshipId
                ? { championshipId: String(championshipId), teamId: this.teamId }
                : { teamId: this.teamId };
            const query = {};
            const normalizedTab = tab || this.activeTab;
            if (normalizedTab && normalizedTab !== 'overview') {
                query.tab = normalizedTab;
            }
            const nextRouteName = championshipId ? 'team-detail' : 'team';

            const normalizeQuery = obj => Object.keys(obj)
                .sort()
                .map(key => `${key}:${String(obj[key])}`)
                .join('|');
            const sameRoute =
                String(this.$route.name || '') === String(nextRouteName)
                && normalizeQuery(this.$route.query || {}) === normalizeQuery(query)
                && String(this.$route.params?.teamId || '') === String(params.teamId || '')
                && String(this.$route.params?.championshipId || '') === String(params.championshipId || '');
            if (sameRoute) return;

            this.$router.replace({
                name: nextRouteName,
                params,
                query
            }).catch(() => {});
        },
        selectTab(tab) {
            this.activeTab = tab;
            this.updateRoute(this.currentChampionshipId, tab);
        },
        async ensureMatchPlayerStats(championshipId) {
            if (!championshipId || !this.teamId || !window.apiClient) return;
            const key = this.scopedSeasonKey(championshipId);
            if (!key) return;
            const existing = this.matchPlayerStatsState[key];
            if (existing?.items?.length && !existing?.loading) return;
            const loadKey = `${key}::players`;
            return this.runInFlightLoad('matchPlayerStats', loadKey, async () => {
                this.matchPlayerStatsState = {
                    ...this.matchPlayerStatsState,
                    [key]: { items: existing?.items || [], byMatch: existing?.byMatch || {}, loading: true, error: null }
                };
                try {
                    const items = await window.apiClient.getTeamMatchPlayerStats(this.teamId, championshipId);
                    const byMatch = {};
                    items.forEach(row => {
                        const matchId = row?.matchId || row?.match_id;
                        if (!matchId) return;
                        if (!byMatch[matchId]) byMatch[matchId] = [];
                        byMatch[matchId].push(row);
                    });
                    this.matchPlayerStatsState = {
                        ...this.matchPlayerStatsState,
                        [key]: { items, byMatch, loading: false, error: null }
                    };
                } catch (error) {
                    this.matchPlayerStatsState = {
                        ...this.matchPlayerStatsState,
                        [key]: { items: existing?.items || [], byMatch: existing?.byMatch || {}, loading: false, error: error?.message || 'Failed to load player stats' }
                    };
                }
            });
        },
        async ensureMapCatalog() {
            if (this.mapCatalogLoaded || this.mapCatalogLoading || !window.apiClient) return;
            return this.runInFlightLoad('mapCatalog', 'global', async () => {
                this.mapCatalogLoading = true;
                try {
                    const catalog = await window.apiClient.getMapsCatalog();
                    this.mapCatalog = Array.isArray(catalog) ? catalog : [];
                    this.mapCatalogLoaded = true;
                } catch (error) {
                    console.warn('[TeamDetail] map catalog fetch failed', error);
                    this.mapCatalogLoaded = true;
                } finally {
                    this.mapCatalogLoading = false;
                }
            });
        },
        resetMapSort() {
            this.scoutTableKey += 1;
            this.detailedTableKey += 1;
        },
        formatWinLoss(wins, losses) {
            return `${formatNumber(wins)}–${formatNumber(losses)}`;
        },
        winHeatStyle(value) {
            return buildHeatStyle(normalizePercent(value));
        },
        kdHeatStyle(value) {
            return buildHeatStyle((toNumber(value) / 2) * 100);
        },
        adrHeatStyle(value) {
            return buildHeatStyle((toNumber(value) / 120) * 100);
        },
        rdHeatStyle(value) {
            const rd = toNumber(value);
            const maxAbs = this.mapMaxRoundDiff || 1;
            const pct = clampValue((rd + maxAbs) / (maxAbs * 2), 0, 1);
            return buildHeatStyle(pct * 100);
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
                const actorTeamId = step.selectedByTeamId
                    || (actor === 'team' ? this.teamId : (actor === 'opponent' ? match?.opponent?.team_id : null));
                return {
                    ...step,
                    action,
                    actor,
                    step: idx + 1,
                    label: this.actionLabel(action),
                    mapName,
                    teamName: step.selectedByTeamName || (actor === 'team' ? teamName : opponentName),
                    teamId: actorTeamId ? String(actorTeamId) : null
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
                    teamName: 'Decider',
                    teamId: null
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
                    teamName: 'Overflow',
                    teamId: null
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
        formatSignedNumber,
        getMatchResult,
        resolveAvatar(src) {
            const fallback = window.PAPPALIIGA_DEFAULT_LOGO || '';
            const url = src || fallback;
            try {
                if (window.apiClient && typeof window.apiClient.proxyAvatar === 'function') {
                    return window.apiClient.proxyAvatar(url) || fallback;
                }
                return url;
            } catch (error) {
                return url;
            }
        },
        teamLogo() {
            return this.resolveAvatar(this.teamInfo?.avatar || '');
        },
        getPlayerRoleBadges(playerId) {
            const roleData = this.playerRoles[playerId];
            if (!roleData || !roleData.roles || roleData.roles.length === 0) return [];
            return roleData.roles.map(role => ({
                label: role,
                isPrimary: role === roleData.primaryRole,
                tooltip: this.getRoleBadgeTooltip(role, roleData)
            }));
        },
        getRoleBadgeTooltip(role, roleData = null) {
            const descriptions = {
                Rifler: 'Perusrooli: tasainen kivääripelaaja.',
                AWPer: 'AWP-painotteinen pelaaja, korkea sniper-osuus.',
                'Entry Fragger': 'Avaa tilanteita ensimmäisiin kaksinkamppailuihin.',
                Support: 'Tukee utilityllä ja assist-arvolla joukkuetta.',
                Clutcher: 'Vahva clutch-pelaaja tiukoissa lopputilanteissa.',
                'Utility Expert': 'Korostuu utility-vahingossa ja väläytysvaikutuksessa.',
                Playmaker: 'Luo aktiivisesti ratkaisuja fragien ja aloitteiden kautta.',
                Closer: 'Viimeistelee kierroksia clutch- ja fragivaikutuksella.',
                Initiator: 'Käynnistää tilanteita, korkea entry-aktiivisuus.',
                Sharpshooter: 'Tarkka tähtääjä, painotus pitkän kantaman osumiin.',
                Anchor: 'Pitää asemat ja pelaa vakaasti matalammalla riskillä.',
                'Utility Core': 'Joukkueen utility-rungon kantava pelaaja.',
                'Team Player': 'Monipuolinen joukkuerooli tuki- ja tempoarvolla.'
            };
            const stats = roleData?.roleStats || {};
            const statChunks = [];
            const awpRate = toNumber(stats.awpRate ?? stats.awp_rate, null);
            const entrySuccess = toNumber(stats.entrySuccess ?? stats.entry_success, null);
            const assistRate = toNumber(stats.assistRate ?? stats.assist_rate, null);
            const clutchSuccess = toNumber(stats.clutchSuccess ?? stats.clutch_success, null);
            if (Number.isFinite(awpRate)) statChunks.push(`AWP ${formatNumber(awpRate, 1)}%`);
            if (Number.isFinite(entrySuccess)) statChunks.push(`Entry ${formatNumber(entrySuccess, 1)}%`);
            if (Number.isFinite(assistRate)) statChunks.push(`Assist ${formatNumber(assistRate, 1)}%`);
            if (Number.isFinite(clutchSuccess)) statChunks.push(`Clutch ${formatNumber(clutchSuccess, 1)}%`);

            const prefix = role === roleData?.primaryRole ? 'Ensisijainen rooli. ' : '';
            const base = descriptions[role] || 'Pelaajan roolibadge.';
            if (!statChunks.length) return `${prefix}${base}`;
            return `${prefix}${base} (${statChunks.join(' · ')})`;
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
                        v-for="tab in ['overview', 'matches', 'players']"
                        :key="tab"
                        type="button"
                        class="team-tab"
                        :class="{ 'team-tab--active': activeTab === tab }"
                        @click="selectTab(tab)"
                        role="tab"
                        :aria-selected="activeTab === tab"
                        :aria-controls="'team-tab-' + tab"
                    >
                        {{ { overview: 'Yleiskuva', matches: 'Ottelut', players: 'Pelaajat' }[tab] }}
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
                                        v-if="stat.performanceBadge"
                                        class="performance-badge"
                                        :class="'performance-badge--' + stat.performanceBadge.type"
                                        :title="'Ero divisioonan keskiarvoon'"
                                    >{{ stat.performanceBadge.label }}</span>
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
                                        <span v-if="cell.actionCode" class="veto-heatmap__code">{{ cell.actionCode }}</span>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div v-if="vetoTrendRows.length" class="veto-heatmap-legend veto-heatmap-legend--section">
                            <div class="veto-legend-title">Selite</div>
                            <div class="veto-legend-grid">
                                <div v-for="entry in vetoLegendEntries" :key="entry.label" class="veto-legend-item">
                                    <div class="veto-heatmap__cell veto-legend-cell" :class="entry.className">
                                        <span v-if="entry.code" class="veto-heatmap__code">{{ entry.code }}</span>
                                    </div>
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

                    <shared-map-performance-table
                        :map-stats="mapStats"
                        :map-catalog="mapCatalog"
                        title="Karttakohtainen suorituskyky"
                        subtitle-summary="Yhteenveto: Voitot, pickit, bannit, eräero"
                        subtitle-full="Laaja: Karttakohtaiset pelaajatilastot"
                        :show-panel-container="true"
                    ></shared-map-performance-table>

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
                                        <th class="match-expand-cell"></th>
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
                                    <template v-for="match in matchesList" :key="match.matchId">
                                        <tr
                                            :class="{ 'match-row--highlight': match.matchId === matchesHoverMatchId, 'match-row--upcoming': !match.played }"
                                            @mouseenter="setMatchesHover(match.matchId)"
                                            @mouseleave="clearMatchesHover"
                                        >
                                            <td class="match-expand-cell">
                                                <button
                                                    type="button"
                                                    class="expand-button"
                                                    :class="{ 'expand-button--open': isMatchExpanded(match.matchId) }"
                                                    :aria-expanded="isMatchExpanded(match.matchId) ? 'true' : 'false'"
                                                    :aria-label="isMatchExpanded(match.matchId) ? 'Collapse match details' : 'Expand match details'"
                                                    @click.stop="toggleMatchExpand(match.matchId)"
                                                >
                                                    <span class="chevron">›</span>
                                                </button>
                                            </td>
                                            <td>
                                                <div class="match-date-cell">
                                                    <span class="match-date-cell__day">{{ formatDate(match.ts) }}</span>
                                                    <span class="match-date-cell__time">{{ formatTime(match.ts) }}</span>
                                                </div>
                                            </td>
                                            <td :title="vetoSummaryLookup[match.matchId] || ''">
                                                <router-link
                                                    v-if="matchOpponentRoute(match)"
                                                    :to="matchOpponentRoute(match)"
                                                    class="team-link"
                                                >{{ match.opponentName || match.team2Name || 'Vastustaja' }}</router-link>
                                                <span v-else>{{ match.opponentName || match.team2Name || 'Vastustaja' }}</span>
                                            </td>
                                            <td>BO{{ match.bestOf }}</td>
                                            <td>
                                                <span
                                                    v-if="match.ignoredDueBan"
                                                    class="match-status-badge match-status-badge--ignored"
                                                    title="Ottelua ei lasketa tilastoihin, koska joukkue on lopettanut tai bännätty kesken kauden."
                                                >Ei tilastoissa</span>
                                                <span
                                                    v-else-if="match.played && isForfeitOnlyMatch(match)"
                                                    :class="matchScoreClass(match)"
                                                >{{ forfeitScoreLabel(match) }}</span>
                                                <span
                                                    v-else-if="match.played"
                                                    :class="matchScoreClass(match)"
                                                >{{ match.teamScore }} - {{ match.oppScore }}</span>
                                                <span v-else class="cell-muted">Tulossa</span>
                                            </td>
                                            <td>
                                                <span
                                                    v-if="match.played && !match.ignoredDueBan"
                                                    :class="match.roundDiff >= 0 ? 'stat-positive' : 'stat-negative'"
                                                >{{ match.roundDiff }}</span>
                                                <span v-else class="cell-muted">-</span>
                                            </td>
                                            <td>
                                                <div class="micro-stack" v-if="match.maps && match.maps.length">
                                                    <span v-for="(map, mapIdx) in match.maps" :key="map.id" class="micro-chip">{{ map.mapName }} {{ map.scoreFor }}-{{ map.scoreAgainst }}</span>
                                                </div>
                                                <span v-else class="cell-muted">-</span>
                                            </td>
                                            <td>
                                                <div class="micro-stack" v-if="match.faceitUrl || replay2Links(match).length">
                                                    <a v-if="match.faceitUrl" :href="match.faceitUrl" target="_blank" rel="noopener" class="chip chip--link">Faceit Lobbys</a>
                                                    <a
                                                        v-for="link in replay2Links(match)"
                                                        :key="'replay2d-' + link.matchId + '-' + link.mapId"
                                                        :href="replay2PlayerUrl(link.matchId, link.mapId)"
                                                        target="_blank"
                                                        rel="noopener"
                                                        :class="['chip', 'chip--link', ['queued', 'parsing'].includes(link.status) ? 'chip--warn' : '']"
                                                        :title="['queued', 'parsing'].includes(link.status) ? 'Demo käsittelyssä, valmistuu pian.' : ''"
                                                    >2D Demo {{ link.mapId }}</a>
                                                </div>
                                                <span v-else-if="isDemoAvailabilityLoading && match.played && match.maps && match.maps.length" class="cell-muted">Tarkistetaan…</span>
                                                <span v-else class="cell-muted">-</span>
                                            </td>
                                        </tr>
                                        <tr v-if="isMatchExpanded(match.matchId)" class="match-expand-row">
                                            <td :colspan="8">
                                                <div class="match-expand-content">
                                                    <match-expanded-details
                                                        :summary="match"
                                                        :details="rawMatchesById[match.matchId] || match"
                                                        :veto-entry="vetoByMatchLookup[match.matchId] || null"
                                                        :player-stats="matchPlayerStatsByMatch[match.matchId] || []"
                                                        :map-catalog="mapCatalog"
                                                        :loading="matchPlayerStatsLoading"
                                                    ></match-expanded-details>
                                                </div>
                                            </td>
                                        </tr>
                                    </template>
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

                <section v-if="activeTab === 'players'" class="team-section team-section--players" id="team-tab-players" role="tabpanel">
                    <h2 class="section-title titleUnderline">Pelaajat</h2>
                    <div v-if="playerSummaryCards.length" class="players-summary-grid">
                        <article v-for="card in playerSummaryCards" :key="card.key" class="players-summary-card">
                            <div class="players-summary-card__label">{{ card.label }}</div>
                            <div class="players-summary-card__value">{{ card.value }}</div>
                            <div class="players-summary-card__meta">{{ card.meta }}</div>
                        </article>
                    </div>
                    <div v-if="players.length" class="table-wrapper">
                        <div class="players-table-toolbar">
                            <span class="players-table-toolbar__hint">Kaikki pelaajametriikat näkyvissä. Klikkaa saraketta lajitellaksesi.</span>
                            <div class="players-baseline-toggle">
                                <span class="players-baseline-toggle__label">Väritys:</span>
                                <button
                                    type="button"
                                    class="players-baseline-toggle__btn"
                                    :class="{ 'players-baseline-toggle__btn--active': playerBaselineMode === 'avg' }"
                                    @click="setPlayerBaselineMode('avg')"
                                >Div avg</button>
                                <button
                                    type="button"
                                    class="players-baseline-toggle__btn"
                                    :class="{ 'players-baseline-toggle__btn--active': playerBaselineMode === 'median' }"
                                    @click="setPlayerBaselineMode('median')"
                                >Div median</button>
                            </div>
                            <div class="players-abbrev-list">
                                <span class="players-abbrev-chip">K/R = Kills per round</span>
                                <span class="players-abbrev-chip">U-Dmg = Utility damage</span>
                                <span class="players-abbrev-chip">Entry = Entry wins / attempts</span>
                                <span class="players-abbrev-chip">Flash% = Flash successes / flashes</span>
                            </div>
                        </div>
                        <div class="players-baseline-status">{{ playerBaselineHint }}</div>
                        <sortable-table
                            class="players-sortable-table"
                            :columns="PLAYER_COLUMNS"
                            :header-groups="playerHeaderGroups"
                            :data="players"
                            :default-sort="playerDefaultSort"
                            :colorize-columns="[]"
                            :mobile-column-limit="5"
                            :sticky-header="true"
                            :compact="false"
                        >
                            <template #cell-nickname="{ row }">
                                <div class="player-cell">
                                    <div class="avatar-placeholder">{{ row.nickname ? row.nickname.slice(0, 2).toUpperCase() : 'PL' }}</div>
                                    <div>
                                        <div class="player-name">
                                            <router-link
                                                v-if="row.playerId"
                                                class="player-link"
                                                :to="{
                                                    name: currentChampionshipId ? 'player-detail' : 'player',
                                                    params: currentChampionshipId
                                                        ? { championshipId: String(currentChampionshipId), playerId: row.playerId }
                                                        : { playerId: row.playerId }
                                                }"
                                            >
                                                {{ row.nickname }}
                                            </router-link>
                                            <span v-else>{{ row.nickname }}</span>
                                            <span
                                                v-for="badge in getPlayerRoleBadges(row.playerId)"
                                                :key="badge.label"
                                                class="role-badge"
                                                :class="{ 'role-badge--primary': badge.isPrimary }"
                                                :title="badge.tooltip || ''"
                                            >{{ badge.label }}</span>
                                        </div>
                                        <div class="player-sub">Maps {{ row.mapsPlayed }} · Rnds {{ row.roundsPlayed }}</div>
                                    </div>
                                </div>
                            </template>
                            <template #cell-kd="{ row }">
                                <span
                                    class="stat-strong"
                                    :class="playerMetricClass(row, 'kd')"
                                    :title="playerMetricTitle(row, 'kd', 'K/D', 2)"
                                >{{ formatNumber(row.kd, 2) }}</span>
                            </template>
                            <template #cell-adr="{ row }">
                                <span
                                    class="stat-strong"
                                    :class="playerMetricClass(row, 'adr')"
                                    :title="playerMetricTitle(row, 'adr', 'ADR', 1)"
                                >{{ formatNumber(row.adr, 1) }}</span>
                            </template>
                            <template #cell-kr="{ row }">
                                <span
                                    class="stat-strong"
                                    :class="playerMetricClass(row, 'kr')"
                                    :title="playerMetricTitle(row, 'kr', 'K/R', 2)"
                                >{{ formatNumber(row.kr, 2) }}</span>
                            </template>
                            <template #cell-hsPct="{ row }">
                                <span
                                    :class="playerMetricClass(row, 'hsPct')"
                                    :title="playerMetricTitle(row, 'hsPct', 'HS%', 1, true)"
                                >{{ formatPercent(row.hsPct || 0, 1) }}</span>
                            </template>
                            <template #cell-entryLine="{ row }">
                                <div class="player-rate" :title="'Entry: voitetut avausduelit / kaikki avausduelit.'">
                                    <span class="player-rate__main">{{ row.entryWins }}/{{ row.entryCount }}</span>
                                    <span class="player-rate__pct">{{ formatPercent(row.entryLine || 0, 1) }}</span>
                                </div>
                            </template>
                            <template #cell-clutch1v1Line="{ row }">
                                <div class="player-rate" :title="'1v1 clutchit: voitetut / yritykset.'">
                                    <span class="player-rate__main">{{ row.cl1v1Wins }}/{{ row.cl1v1Attempts }}</span>
                                    <span class="player-rate__pct">{{ formatPercent(row.cl1v1Line || 0, 1) }}</span>
                                </div>
                            </template>
                            <template #cell-clutch1v2Line="{ row }">
                                <div class="player-rate" :title="'1v2 clutchit: voitetut / yritykset.'">
                                    <span class="player-rate__main">{{ row.cl1v2Wins }}/{{ row.cl1v2Attempts }}</span>
                                    <span class="player-rate__pct">{{ formatPercent(row.cl1v2Line || 0, 1) }}</span>
                                </div>
                            </template>
                            <template #cell-flashSuccessLine="{ row }">
                                <div class="player-rate" :title="'Flash%: onnistuneet flashit / kaikki heitetyt flashit.'">
                                    <span class="player-rate__main">{{ row.flashSuccesses }}/{{ row.flashCount }}</span>
                                    <span
                                        class="player-rate__pct"
                                        :class="playerMetricClass(row, 'flashSuccessLine')"
                                        :title="playerMetricTitle(row, 'flashSuccessLine', 'Flash%', 1, true)"
                                    >{{ formatPercent(row.flashSuccessLine || 0, 1) }}</span>
                                </div>
                            </template>
                        </sortable-table>
                    </div>
                    <div v-if="players.length" class="glass-card lineup-section">
                        <div class="section-heading section-heading--split">
                            <div class="section-heading__main">
                                <h3>Pelanneet lineupit</h3>
                                <span class="section-sub">Pelaaja kohtaiset statsit löytyy tooltipistä</span>
                            </div>
                            <span class="lineup-section__meta" v-if="matchPlayerStatsLoading">Ladataan lineuppeja…</span>
                            <span
                                class="lineup-section__meta"
                                v-else
                                :title="'Rosterissa ' + players.length + ' pelaajaa → enintään ' + formatNumber(possibleLineupCount) + ' uniikkia 5 pelaajan lineupia.'"
                            >{{ lineupCoverageLabel }}</span>
                        </div>
                        <div v-if="lineupRows.length" class="table-wrapper">
                            <sortable-table
                                class="players-sortable-table"
                                :columns="LINEUP_COLUMNS"
                                :header-groups="lineupHeaderGroups"
                                :data="lineupRows"
                                :default-sort="lineupDefaultSort"
                                :colorize-columns="[]"
                                :mobile-column-limit="5"
                                :sticky-header="true"
                                :compact="false"
                            >
                                <template #cell-lineupLabel="{ row }">
                                    <div class="lineup-cell" :title="lineupSummaryTooltip(row)">
                                        <div class="lineup-player-list">
                                            <span
                                                v-for="player in row.players"
                                                :key="row.signature + '-' + player.playerId"
                                                class="lineup-player-pill"
                                                :title="lineupPlayerStatLine(player)"
                                            >
                                                <span class="lineup-player-pill__name">{{ player.nickname }}</span>
                                                <span
                                                    v-for="badge in player.mvpBadges || []"
                                                    :key="player.playerId + '-' + badge.label"
                                                    class="lineup-mvp-badge"
                                                    :class="'lineup-mvp-badge--' + (badge.tone || 'both')"
                                                    :title="badge.tooltip || ''"
                                                >{{ badge.label }}</span>
                                            </span>
                                        </div>
                                        <div class="player-sub">Ottelut {{ row.matchesPlayed }} · Kartat {{ row.mapsPlayed }}</div>
                                    </div>
                                </template>
                                <template #cell-recordLine="{ row }">
                                    <span
                                        class="chip"
                                        :class="row.winRate >= 60 ? 'chip--ok' : row.winRate >= 45 ? 'chip--accent' : 'chip--err'"
                                        :title="lineupSummaryTooltip(row)"
                                    >{{ row.recordLine }}</span>
                                </template>
                                <template #cell-winRate="{ row }">
                                    <span class="stat-strong" :title="lineupSummaryTooltip(row)">{{ formatPercent(row.winRate, 1) }}</span>
                                </template>
                                <template #cell-roundDiff="{ row }">
                                    <span class="stat-strong" :title="lineupSummaryTooltip(row)">{{ formatSignedNumber(row.roundDiff, 0) }}</span>
                                </template>
                                <template #cell-adr="{ row }">
                                    <span
                                        class="stat-strong"
                                        :class="playerMetricClass(row, 'adr')"
                                        :title="lineupMetricTooltip(row, 'adr', 'ADR', 1)"
                                    >{{ formatNumber(row.adr, 1) }}</span>
                                </template>
                                <template #cell-udpr="{ row }">
                                    <span
                                        class="stat-strong"
                                        :class="playerMetricClass(row, 'udpr')"
                                        :title="lineupMetricTooltip(row, 'udpr', 'UDPR', 2)"
                                    >{{ formatNumber(row.udpr, 2) }}</span>
                                </template>
                                <template #cell-kd="{ row }">
                                    <span
                                        class="stat-strong"
                                        :class="playerMetricClass(row, 'kd')"
                                        :title="lineupMetricTooltip(row, 'kd', 'K/D', 2)"
                                    >{{ formatNumber(row.kd, 2) }}</span>
                                </template>
                                <template #cell-hsPct="{ row }">
                                    <span
                                        :class="playerMetricClass(row, 'hsPct')"
                                        :title="lineupMetricTooltip(row, 'hsPct', 'HS%', 1, true)"
                                    >{{ formatPercent(row.hsPct || 0, 1) }}</span>
                                </template>
                                <template #cell-mapBreakdownSummary="{ row }">
                                    <div class="lineup-map-breakdown" :title="(row.mapBreakdownTitle || '') + (row.mapBreakdownTitle ? '\\n' : '') + lineupSummaryTooltip(row)">
                                        <span
                                            v-for="entry in row.mapBreakdown.slice(0, 3)"
                                            :key="row.signature + '-' + entry.mapName"
                                            class="lineup-map-chip"
                                        >
                                            <span>{{ entry.mapName }}</span>
                                            <span class="lineup-map-chip__record">
                                                <span class="lineup-map-chip__win">{{ entry.wins }}W</span>
                                                <span class="lineup-map-chip__loss">{{ entry.losses }}L</span>
                                                <span v-if="entry.draws" class="lineup-map-chip__draw">{{ entry.draws }}D</span>
                                            </span>
                                        </span>
                                        <span v-if="row.mapBreakdown.length > 3" class="lineup-map-chip lineup-map-chip--muted">+{{ row.mapBreakdown.length - 3 }}</span>
                                    </div>
                                </template>
                            </sortable-table>
                        </div>
                        <div v-else class="empty-state-container compact">
                            <div class="empty-state-card">
                                <h3 class="empty-state-title">{{ matchPlayerStatsLoading ? 'Ladataan lineuppeja' : 'Ei lineup-dataa' }}</h3>
                                <p class="empty-state-description">
                                    {{ matchPlayerStatsLoading
                                        ? 'Kerätään pelattuja viisikoita valitulle kaudelle.'
                                        : 'Tälle kaudelle ei löytynyt viiden pelaajan lineuppeja.' }}
                                </p>
                            </div>
                        </div>
                    </div>
                    <div v-else class="empty-state-container">
                        <div class="empty-state-card">
                            <h3 class="empty-state-title">Ei pelaajatietoja</h3>
                            <p class="empty-state-description">Tälle kaudelle ei ole pelaajatietoja saatavilla.</p>
                        </div>
                    </div>
                    <div class="players-charts" v-if="playerStackedKda.length">
                        <div class="glass-card">
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

            </div>
        </div>
    `
};
