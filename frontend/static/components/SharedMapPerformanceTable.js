function smToNumber(value, fallback = 0) {
    if (value === null || value === undefined) return fallback;
    const numeric = Number(value);
    if (Number.isFinite(numeric)) return numeric;
    const parsed = Number(String(value).replace(',', '.'));
    return Number.isFinite(parsed) ? parsed : fallback;
}

function smClamp(value, min, max) {
    return Math.min(max, Math.max(min, value));
}

function smFormatNumber(value, decimals = 0) {
    const numeric = smToNumber(value);
    if (!Number.isFinite(numeric)) return '-';
    return decimals > 0 ? numeric.toFixed(decimals) : numeric.toLocaleString('fi-FI');
}

function smFormatPercent(value, decimals = 1) {
    const numeric = smToNumber(value);
    const scaled = Math.abs(numeric) <= 1 ? numeric * 100 : numeric;
    return `${scaled.toFixed(decimals)}%`;
}

function smNormalizePercent(value) {
    const numeric = smToNumber(value);
    if (!Number.isFinite(numeric)) return 0;
    return Math.abs(numeric) <= 1 ? numeric * 100 : numeric;
}

function smHeatStyle(percent) {
    const clamped = smClamp(smToNumber(percent), 0, 100);
    const hue = (clamped / 100) * 120;
    const color = `hsla(${hue.toFixed(1)}, 60%, 45%, 0.22)`;
    return { background: `linear-gradient(90deg, ${color}, transparent)` };
}

function smBuildColumnGroups(columns, groupMeta) {
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

function smPrettyMapName(raw) {
    if (!raw) return 'Kartta';
    const value = String(raw).trim();
    if (!value) return 'Kartta';
    const lower = value.toLowerCase();
    if (lower.startsWith('de_')) {
        return lower
            .slice(3)
            .split(/[_-]/)
            .filter(Boolean)
            .map(part => part.charAt(0).toUpperCase() + part.slice(1))
            .join(' ');
    }
    return value;
}

const SM_MAP_GROUP_META = {
    map: { label: 'Kartta', className: 'group-map group-divider' },
    rounds: { label: 'Erät', className: 'group-rounds group-divider' },
    combat: { label: 'Taistelu', className: 'group-combat group-divider' },
    kills: { label: 'Tapot/Assist', className: 'group-kills group-divider' },
    utility: { label: 'Utility', className: 'group-utility group-divider' },
    awards: { label: 'MVP', className: 'group-awards group-divider' },
    flash: { label: 'Flash', className: 'group-flash group-divider' },
    multikill: { label: 'Multi-kills', className: 'group-multikill group-divider' },
    weapons: { label: 'Aseet', className: 'group-weapons group-divider' },
    damage: { label: 'Vahinko', className: 'group-damage group-divider' },
    clutch: { label: 'Clutch', className: 'group-clutch group-divider' }
};

const SM_SCOUT_GROUP_META = {
    map: { label: 'Kartta', className: 'group-map group-divider' },
    usage: { label: 'Pelattu', className: 'group-usage group-divider' },
    results: { label: 'Tulokset', className: 'group-results group-divider' },
    performance: { label: 'Suorituskyky', className: 'group-performance group-divider' },
    veto: { label: 'Bannit', className: 'group-veto group-divider' },
    series: { label: 'Decider/OT', className: 'group-series group-divider' }
};

const SM_MAP_COLUMNS = [
    { key: 'mapName', label: 'Kartta', sortable: true, colClass: 'col-name col-map-name', width: '210px', group: 'map' },
    { key: 'totalRoundsPlayed', label: 'Eriä pelattu', sortable: true, numeric: true, colClass: 'mono-num', group: 'rounds' },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num col-adr', group: 'combat' },
    { key: 'kr', label: 'KR', sortable: true, numeric: true, decimals: 3, colClass: 'mono-num', group: 'combat' },
    { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2, colClass: 'mono-num col-kd', group: 'combat' },
    { key: 'hsPct', label: 'HS%', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num', group: 'combat' },
    { key: 'kills', label: 'Tapot', sortable: true, numeric: true, colClass: 'mono-num col-kills', group: 'kills' },
    { key: 'deaths', label: 'Kuolemat', sortable: true, numeric: true, colClass: 'mono-num col-deaths', group: 'kills' },
    { key: 'assists', label: 'Assist', sortable: true, numeric: true, colClass: 'mono-num', group: 'kills' },
    { key: 'udpr', label: 'UDPR', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num', group: 'utility' },
    { key: 'mvps', label: 'MVP', sortable: true, numeric: true, colClass: 'mono-num col-mvps', group: 'awards' },
    { key: 'enemiesFlashed', label: 'Enemies flashed', sortable: true, numeric: true, colClass: 'mono-num', group: 'flash' },
    { key: 'flashSuccessPct', label: 'Flash%', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num', group: 'flash' },
    { key: 'flashCount', label: 'Flashbangit', sortable: true, numeric: true, colClass: 'mono-num', group: 'flash' },
    { key: 'multi2k', label: '2k', sortable: true, numeric: true, colClass: 'mono-num', group: 'multikill' },
    { key: 'multi3k', label: '3k', sortable: true, numeric: true, colClass: 'mono-num', group: 'multikill' },
    { key: 'multi4k', label: '4k', sortable: true, numeric: true, colClass: 'mono-num', group: 'multikill' },
    { key: 'multi5k', label: 'Ace', sortable: true, numeric: true, colClass: 'mono-num', group: 'multikill' },
    { key: 'pistolKills', label: 'Pistooli', sortable: true, numeric: true, colClass: 'mono-num', group: 'weapons' },
    { key: 'sniperKills', label: 'Sniper', sortable: true, numeric: true, colClass: 'mono-num', group: 'weapons' },
    { key: 'totalDamage', label: 'Vahinko', sortable: true, numeric: true, colClass: 'mono-num', group: 'damage' },
    { key: 'clutchKills', label: 'Clutch', sortable: true, numeric: true, colClass: 'mono-num', group: 'clutch' }
];

const SM_SCOUT_MAP_COLUMNS = [
    { key: 'mapName', label: 'Kartta', sortable: true, colClass: 'col-name col-map-name', width: '200px', group: 'map' },
    { key: 'played', label: 'Pelattu', sortable: true, numeric: true, colClass: 'mono-num col-played', group: 'usage' },
    { key: 'picks', label: 'Omat pickit', sortable: true, numeric: true, colClass: 'mono-num col-picks', group: 'usage' },
    { key: 'oppPicks', label: 'Vastustajan pickit', sortable: true, numeric: true, colClass: 'mono-num col-opp-picks', group: 'usage' },
    { key: 'winrate', label: 'Win %', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num col-winrate', group: 'results' },
    { key: 'pickWinRate', label: 'Win % (oma pick)', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num col-winrate-own', group: 'results' },
    { key: 'oppPickWinRate', label: 'Win % (vastustajan pick)', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num col-winrate-opp', group: 'results' },
    { key: 'rd', label: 'Eraero', sortable: true, numeric: true, colClass: 'mono-num col-rd', group: 'performance' },
    { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2, colClass: 'mono-num col-kd', group: 'performance' },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num col-adr', group: 'performance' },
    { key: 'ban1', label: '1. banni (oma)', sortable: true, numeric: true, colClass: 'mono-num col-ban1', group: 'veto' },
    { key: 'ban2', label: '2. banni (oma)', sortable: true, numeric: true, colClass: 'mono-num col-ban2', group: 'veto' },
    { key: 'oppBan', label: 'Vastustajan banni', sortable: true, numeric: true, colClass: 'mono-num col-opp-ban', group: 'veto' },
    { key: 'totalOwnBan', label: 'Banneja yhteensa', sortable: true, numeric: true, colClass: 'mono-num col-ban-total', group: 'veto' },
    { key: 'decov', label: 'Decider / overflow', sortable: true, numeric: true, colClass: 'mono-num col-decov', group: 'series' }
];

const SM_SCOUT_MAP_COLUMNS_DIVISION = [
    { key: 'mapName', label: 'Kartta', sortable: true, colClass: 'col-name col-map-name', width: '200px', group: 'map' },
    { key: 'played', label: 'Pelattu', sortable: true, numeric: true, colClass: 'mono-num col-played', group: 'usage' },
    { key: 'pickMatchup', label: 'WR% pickit (oma vs vast.)', sortable: false, colClass: 'col-pick-matchup', group: 'results' },
    { key: 'totalOwnBan', label: 'Total bannatty', sortable: true, numeric: true, colClass: 'mono-num col-ban-total', group: 'veto' },
    { key: 'ban1', label: '1st ban', sortable: true, numeric: true, colClass: 'mono-num col-ban1', group: 'veto' },
    { key: 'ban2', label: '2nd ban', sortable: true, numeric: true, colClass: 'mono-num col-ban2', group: 'veto' },
    { key: 'decov', label: 'Decider', sortable: true, numeric: true, colClass: 'mono-num col-decov', group: 'series' }
];

window.SharedMapPerformanceTable = {
    name: 'SharedMapPerformanceTable',
    components: {
        get SortableTable() { return window.SortableTable; },
        get SplitBar() { return window.SplitBar; }
    },
    props: {
        mapStats: {
            type: Array,
            default: () => []
        },
        mapCatalog: {
            type: Array,
            default: () => []
        },
        title: {
            type: String,
            default: 'Karttakohtainen suorituskyky'
        },
        subtitleSummary: {
            type: String,
            default: 'Yhteenveto: voitot, pickit, bannit, eraero'
        },
        subtitleFull: {
            type: String,
            default: 'Laaja: karttakohtaiset pelaajatilastot'
        },
        showPanelContainer: {
            type: Boolean,
            default: true
        },
        variant: {
            type: String,
            default: 'team'
        }
    },
    data() {
        return {
            mapViewMode: 'summary',
            mapSubMetricMode: 'perRound',
            scoutTableKey: 0,
            detailedTableKey: 0,
            SCOUT_MAP_COLUMNS: SM_SCOUT_MAP_COLUMNS,
            MAP_COLUMNS: SM_MAP_COLUMNS,
            mapImageLookup: {},
            catalogLoaded: false,
            catalogLoading: false
        };
    },
    computed: {
        normalizedRows() {
            if (!Array.isArray(this.mapStats) || !this.mapStats.length) return [];
            return this.mapStats.map((entry, idx) => this.normalizeMapRow(entry, idx));
        },
        scoutMapRows() {
            return this.normalizedRows.map(row => ({
                ...row,
                mapImage: row.mapImage || this.resolveMapImage(row)
            }));
        },
        scoutHeaderGroups() {
            return smBuildColumnGroups(this.resolvedScoutColumns, SM_SCOUT_GROUP_META);
        },
        mapHeaderGroups() {
            return smBuildColumnGroups(this.resolvedMapColumns, SM_MAP_GROUP_META);
        },
        scoutMapDefaultSort() {
            return { column: 'played', order: 'desc', numeric: true };
        },
        mapDefaultSort() {
            return { column: 'totalRoundsPlayed', order: 'desc', numeric: true };
        },
        resolvedScoutColumns() {
            return this.variant === 'division' ? SM_SCOUT_MAP_COLUMNS_DIVISION : this.SCOUT_MAP_COLUMNS;
        },
        resolvedMapColumns() {
            return this.MAP_COLUMNS;
        },
        mapMaxRoundDiff() {
            const values = this.scoutMapRows.map(row => Math.abs(smToNumber(row.rd, 0)));
            return Math.max(1, ...values);
        }
    },
    watch: {
        mapStats: {
            immediate: true,
            handler(newStats) {
                this.mapImageLookup = this.buildMapImageLookup(newStats, this.mapImageLookup);
                if (this.shouldFetchCatalog(newStats)) {
                    this.ensureMapCatalog();
                }
            }
        },
        mapCatalog: {
            immediate: true,
            handler(newCatalog) {
                if (!Array.isArray(newCatalog) || !newCatalog.length) return;
                const lookup = { ...this.mapImageLookup };
                newCatalog.forEach(item => {
                    const key = this.mapKey(item?.map_id || item?.pretty_name || item?.map_name || item?.name);
                    const img = item?.image_sm || item?.image_lg || item?.image;
                    if (key && img && !lookup[key]) {
                        lookup[key] = img;
                    }
                });
                this.mapImageLookup = lookup;
                this.catalogLoaded = true;
            }
        }
    },
    methods: {
        normalizeMapRow(entry, idx) {
            const raw = entry?.curr || entry || {};
            const mapName = entry?.mapName
                || entry?.map_name
                || raw?.map_name
                || entry?.pretty_name
                || raw?.pretty_name
                || entry?.name
                || raw?.name
                || `Kartta ${idx + 1}`;

            const played = smToNumber(entry?.played ?? entry?.games ?? entry?.maps_played ?? raw?.played ?? raw?.games ?? raw?.maps_played, 0);
            const wins = smToNumber(entry?.wins ?? raw?.wins ?? raw?.maps_won, 0);
            const losses = smToNumber(entry?.losses ?? raw?.losses ?? raw?.maps_lost, 0);
            const picks = smToNumber(entry?.picks ?? raw?.picks, 0);
            const oppPicks = smToNumber(entry?.oppPicks ?? entry?.opp_picks ?? raw?.oppPicks ?? raw?.opp_picks, 0);
            const pickWins = smToNumber(entry?.pickWins ?? entry?.pick_wins ?? raw?.pickWins ?? raw?.pick_wins, 0);
            const oppPickWins = smToNumber(entry?.oppPickWins ?? entry?.opp_pick_wins ?? raw?.oppPickWins ?? raw?.opp_pick_wins, 0);
            const totalOwnBan = smToNumber(entry?.totalOwnBan ?? entry?.total_own_ban ?? raw?.totalOwnBan ?? raw?.total_own_ban ?? entry?.banned ?? raw?.banned, 0);
            const roundsWon = smToNumber(entry?.roundsWon ?? entry?.rounds_won ?? raw?.roundsWon ?? raw?.rounds_won, 0);
            const roundsLost = smToNumber(entry?.roundsLost ?? entry?.rounds_lost ?? raw?.roundsLost ?? raw?.rounds_lost, 0);
            const totalRoundsPlayed = smToNumber(entry?.totalRoundsPlayed ?? entry?.total_rounds_played ?? entry?.rounds_played ?? raw?.totalRoundsPlayed ?? raw?.total_rounds_played ?? raw?.rounds_played, roundsWon + roundsLost);
            const kills = smToNumber(entry?.kills ?? raw?.kills, 0);
            const deaths = smToNumber(entry?.deaths ?? raw?.deaths, 0);
            const assists = smToNumber(entry?.assists ?? raw?.assists, 0);
            const flashCount = smToNumber(entry?.flashCount ?? entry?.flash_count ?? raw?.flashCount ?? raw?.flash_count, 0);
            const flashSuccesses = smToNumber(entry?.flashSuccesses ?? entry?.flash_successes ?? raw?.flashSuccesses ?? raw?.flash_successes, 0);
            const flashSuccessPctRaw = entry?.flashSuccessPct ?? entry?.flash_success_pct ?? raw?.flashSuccessPct ?? raw?.flash_success_pct;
            const flashSuccessPct = flashSuccessPctRaw != null
                ? smNormalizePercent(flashSuccessPctRaw)
                : (flashCount ? (flashSuccesses / flashCount) * 100 : 0);

            const row = {
                id: entry?.id || entry?.mapId || entry?.map_name || raw?.map_name || `map-${idx}`,
                mapName: smPrettyMapName(mapName),
                mapImage: this.resolveMapImage(entry),
                played,
                games: played,
                wins,
                losses,
                picks,
                oppPicks,
                pickWins,
                oppPickWins,
                winrate: played ? (wins / played) * 100 : 0,
                pickWinRate: picks ? (pickWins / picks) * 100 : 0,
                oppPickWinRate: oppPicks ? (oppPickWins / oppPicks) * 100 : 0,
                ban1: smToNumber(entry?.ban1 ?? raw?.ban1, 0),
                ban2: smToNumber(entry?.ban2 ?? raw?.ban2, 0),
                oppBan: smToNumber(entry?.oppBan ?? entry?.opp_ban ?? raw?.oppBan ?? raw?.opp_ban, 0),
                totalOwnBan,
                decov: smToNumber(entry?.decov ?? raw?.decov, 0),
                rd: smToNumber(entry?.rd ?? raw?.rd, roundsWon - roundsLost),
                roundsWon,
                roundsLost,
                totalRoundsPlayed,
                roundsPerMapAvg: played ? totalRoundsPlayed / played : 0,
                adr: smToNumber(entry?.adr ?? raw?.adr, 0),
                kr: smToNumber(entry?.kr ?? raw?.kr, 0),
                kd: smToNumber(entry?.kd ?? raw?.kd, deaths > 0 ? kills / deaths : kills),
                hsPct: smToNumber(entry?.hsPct ?? entry?.hs_pct ?? raw?.hsPct ?? raw?.hs_pct, 0),
                kills,
                deaths,
                assists,
                udpr: smToNumber(entry?.udpr ?? raw?.udpr, 0),
                mvps: smToNumber(entry?.mvps ?? raw?.mvps, 0),
                enemiesFlashed: smToNumber(entry?.enemiesFlashed ?? entry?.enemies_flashed ?? raw?.enemiesFlashed ?? raw?.enemies_flashed, 0),
                flashSuccessPct,
                flashCount,
                multi2k: smToNumber(entry?.multi2k ?? entry?.mk_2k ?? entry?.k2 ?? raw?.multi2k ?? raw?.mk_2k ?? raw?.k2, 0),
                multi3k: smToNumber(entry?.multi3k ?? entry?.mk_3k ?? entry?.k3 ?? raw?.multi3k ?? raw?.mk_3k ?? raw?.k3, 0),
                multi4k: smToNumber(entry?.multi4k ?? entry?.mk_4k ?? entry?.k4 ?? raw?.multi4k ?? raw?.mk_4k ?? raw?.k4, 0),
                multi5k: smToNumber(entry?.multi5k ?? entry?.mk_5k ?? entry?.ace ?? raw?.multi5k ?? raw?.mk_5k ?? raw?.ace, 0),
                pistolKills: smToNumber(entry?.pistolKills ?? entry?.pistol_kills ?? raw?.pistolKills ?? raw?.pistol_kills, 0),
                sniperKills: smToNumber(entry?.sniperKills ?? entry?.sniper_kills ?? raw?.sniperKills ?? raw?.sniper_kills, 0),
                totalDamage: smToNumber(entry?.totalDamage ?? entry?.damage ?? raw?.totalDamage ?? raw?.damage, 0),
                clutchKills: smToNumber(entry?.clutchKills ?? entry?.clutch_kills ?? raw?.clutchKills ?? raw?.clutch_kills, 0)
            };

            return this.recomputeDerived(row);
        },
        recomputeDerived(entry) {
            const rounds = smToNumber(entry.totalRoundsPlayed, 0);
            const maps = smToNumber(entry.games || entry.played, 0);
            const totalDamage = smToNumber(entry.totalDamage, 0);
            const multi2k = smToNumber(entry.multi2k, 0);
            const multi3k = smToNumber(entry.multi3k, 0);
            const multi4k = smToNumber(entry.multi4k, 0);
            const multi5k = smToNumber(entry.multi5k, 0);
            const pistolKills = smToNumber(entry.pistolKills, 0);
            const sniperKills = smToNumber(entry.sniperKills, 0);
            return {
                ...entry,
                multi2kPerRound: rounds ? multi2k / rounds : 0,
                multi3kPerRound: rounds ? multi3k / rounds : 0,
                multi4kPerRound: rounds ? multi4k / rounds : 0,
                multi5kPerRound: rounds ? multi5k / rounds : 0,
                pistolKillsPerRound: rounds ? pistolKills / rounds : 0,
                sniperKillsPerRound: rounds ? sniperKills / rounds : 0,
                totalDamagePerRound: rounds ? totalDamage / rounds : 0,
                multi2kPerMap: maps ? multi2k / maps : 0,
                multi3kPerMap: maps ? multi3k / maps : 0,
                multi4kPerMap: maps ? multi4k / maps : 0,
                multi5kPerMap: maps ? multi5k / maps : 0,
                pistolKillsPerMap: maps ? pistolKills / maps : 0,
                sniperKillsPerMap: maps ? sniperKills / maps : 0,
                totalDamagePerMap: maps ? totalDamage / maps : 0
            };
        },
        formatNumber(value, decimals = 0) {
            return smFormatNumber(value, decimals);
        },
        formatPercent(value, decimals = 1) {
            return smFormatPercent(value, decimals);
        },
        formatWinLoss(wins, losses) {
            return `${this.formatNumber(wins)}-${this.formatNumber(losses)}`;
        },
        formatPickMatchupLeft(row) {
            return `OMA PICK ${this.formatPercent(row.pickWinRate, 1)} | W-L ${this.formatWinLoss(row.pickWins, Math.max(0, row.picks - row.pickWins))}`;
        },
        formatPickMatchupRight(row) {
            return `VAST. PICK ${this.formatPercent(row.oppPickWinRate, 1)} | W-L ${this.formatWinLoss(row.oppPickWins, Math.max(0, row.oppPicks - row.oppPickWins))}`;
        },
        pickMatchupTitle(row) {
            return [
                `Oma pick WR: ${this.formatPercent(row.pickWinRate, 1)} (W-L ${this.formatWinLoss(row.pickWins, Math.max(0, row.picks - row.pickWins))})`,
                `Vastustajan pick WR: ${this.formatPercent(row.oppPickWinRate, 1)} (W-L ${this.formatWinLoss(row.oppPickWins, Math.max(0, row.oppPicks - row.oppPickWins))})`,
                'W-L = voitot-tappiot kyseisen pick-tyypin peleissa.'
            ].join(' | ');
        },
        resetMapSort() {
            this.scoutTableKey += 1;
            this.detailedTableKey += 1;
        },
        winHeatStyle(value) {
            return smHeatStyle(smNormalizePercent(value));
        },
        kdHeatStyle(value) {
            return smHeatStyle((smToNumber(value) / 2) * 100);
        },
        adrHeatStyle(value) {
            return smHeatStyle((smToNumber(value) / 120) * 100);
        },
        rdHeatStyle(value) {
            const rd = smToNumber(value);
            const maxAbs = this.mapMaxRoundDiff || 1;
            const pct = smClamp((rd + maxAbs) / (maxAbs * 2), 0, 1);
            return smHeatStyle(pct * 100);
        },
        shouldShowSubMetric(perRound, perMap) {
            const value = this.mapSubMetricMode === 'perMap' ? smToNumber(perMap) : smToNumber(perRound);
            return value > 0;
        },
        subMetricValue(perRound, perMap, decimals = 3) {
            const value = this.mapSubMetricMode === 'perMap' ? smToNumber(perMap) : smToNumber(perRound);
            if (!Number.isFinite(value)) return '-';
            if (value === 0) return '0';
            return value.toFixed(decimals);
        },
        subMetricLabel() {
            return this.mapSubMetricMode === 'perMap' ? 'per-kartta' : 'per-era';
        },
        mapKey(name) {
            return window.MapImageUtils ? window.MapImageUtils.mapKey(name) : null;
        },
        buildMapImageLookup(stats, existing = {}) {
            return window.MapImageUtils ? window.MapImageUtils.buildMapImageLookup(stats, existing) : { ...(existing || {}) };
        },
        resolveMapImage(entry) {
            return window.MapImageUtils
                ? window.MapImageUtils.resolveMapImage(entry, {
                    mapCatalog: this.mapCatalog,
                    mapImageLookup: this.mapImageLookup,
                    apiClient: window.apiClient
                })
                : null;
        },
        shouldFetchCatalog(stats) {
            if (this.catalogLoaded || this.catalogLoading) return false;
            return window.MapImageUtils ? window.MapImageUtils.shouldFetchCatalog(stats) : false;
        },
        async ensureMapCatalog() {
            if (this.catalogLoaded || this.catalogLoading || !window.apiClient || typeof window.apiClient.getMapsCatalog !== 'function') {
                return;
            }
            this.catalogLoading = true;
            try {
                const catalog = await window.apiClient.getMapsCatalog();
                if (Array.isArray(catalog) && catalog.length) {
                    const lookup = { ...this.mapImageLookup };
                    catalog.forEach(item => {
                        const key = this.mapKey(item?.map_id || item?.pretty_name || item?.map_name || item?.name);
                        const img = item?.image_sm || item?.image_lg || item?.image;
                        if (key && img && !lookup[key]) {
                            lookup[key] = img;
                        }
                    });
                    this.mapImageLookup = lookup;
                }
                this.catalogLoaded = true;
            } catch (_error) {
                this.catalogLoaded = true;
            } finally {
                this.catalogLoading = false;
            }
        }
    },
    template: `
        <section :class="showPanelContainer ? 'scout-panel scout-table shared-map-performance' : 'shared-map-performance'">
            <div class="section-heading">
                <div>
                    <h3 class="section-title titleUnderline">{{ title }}</h3>
                    <span class="section-sub">{{ mapViewMode === 'summary' ? subtitleSummary : subtitleFull }}</span>
                    <div v-if="mapViewMode === 'full'" class="section-legend"><strong>Paaarvo</strong> = kokonaisluku, <strong>sulkeissa</strong> = {{ subMetricLabel() }}</div>
                </div>
                <div class="section-heading-actions">
                    <div v-if="mapViewMode === 'full'" class="submetric-toggle">
                        <button type="button" class="btn-submetric" :class="mapSubMetricMode === 'perRound' ? 'active' : ''" @click="mapSubMetricMode = 'perRound'">Per-era</button>
                        <button type="button" class="btn-submetric" :class="mapSubMetricMode === 'perMap' ? 'active' : ''" @click="mapSubMetricMode = 'perMap'">Per-kartta</button>
                    </div>
                    <button type="button" class="btn-toggle-view" @click="mapViewMode = mapViewMode === 'summary' ? 'full' : 'summary'">
                        <span v-if="mapViewMode === 'summary'">Laaja nakyma</span>
                        <span v-else>Yhteenveto</span>
                    </button>
                </div>
            </div>
            <div>
                <button type="button" class="btn-reset-sort" @click="resetMapSort">Nollaa lajittelu</button>
            </div>

            <div v-if="mapViewMode === 'summary'" class="table-wrapper table-wrapper--scroll">
                <sortable-table
                    :key="scoutTableKey"
                    :columns="resolvedScoutColumns"
                    :header-groups="scoutHeaderGroups"
                    :data="scoutMapRows"
                    :default-sort="scoutMapDefaultSort"
                    :sticky-header="true"
                    :compact="true"
                    class="map-summary-table"
                >
                    <template #cell-mapName="{ row }">
                        <div class="map-name">
                            <img v-if="row.mapImage" :src="row.mapImage" class="map-logo" alt="" />
                            <span class="map-name-text">{{ row.mapName }}</span>
                        </div>
                    </template>
                    <template #cell-winrate="{ row }">
                        <div v-if="row.played > 0" class="scout-cell mono-num" :style="winHeatStyle(row.winrate)">
                            {{ formatPercent(row.winrate, 1) }} ({{ formatWinLoss(row.wins, row.losses) }})
                        </div>
                        <span v-else class="cell-muted mono-num">-</span>
                    </template>
                    <template #cell-pickWinRate="{ row }">
                        <div v-if="row.picks > 0" class="scout-cell mono-num" :style="winHeatStyle(row.pickWinRate)">
                            {{ formatPercent(row.pickWinRate, 1) }} ({{ formatWinLoss(row.pickWins, Math.max(0, row.picks - row.pickWins)) }})
                        </div>
                        <span v-else class="cell-muted mono-num">-</span>
                    </template>
                    <template #cell-oppPickWinRate="{ row }">
                        <div v-if="row.oppPicks > 0" class="scout-cell mono-num" :style="winHeatStyle(row.oppPickWinRate)">
                            {{ formatPercent(row.oppPickWinRate, 1) }} ({{ formatWinLoss(row.oppPickWins, Math.max(0, row.oppPicks - row.oppPickWins)) }})
                        </div>
                        <span v-else class="cell-muted mono-num">-</span>
                    </template>
                    <template #cell-pickMatchup="{ row }">
                        <div v-if="row.picks > 0 || row.oppPicks > 0" class="pick-matchup-cell" :title="pickMatchupTitle(row)">
                            <split-bar
                                :wins="row.pickWins"
                                :losses="row.oppPickWins"
                                height="40px"
                                :left-text="formatPickMatchupLeft(row)"
                                :right-text="formatPickMatchupRight(row)"
                                :show-percent="false"
                            />
                        </div>
                        <span v-else class="cell-muted mono-num">-</span>
                    </template>
                    <template #cell-kd="{ row }">
                        <div v-if="row.played > 0" class="scout-cell mono-num" :style="kdHeatStyle(row.kd)">{{ formatNumber(row.kd, 2) }}</div>
                        <span v-else class="cell-muted mono-num">-</span>
                    </template>
                    <template #cell-adr="{ row }">
                        <div v-if="row.played > 0" class="scout-cell mono-num" :style="adrHeatStyle(row.adr)">{{ formatNumber(row.adr, 1) }}</div>
                        <span v-else class="cell-muted mono-num">-</span>
                    </template>
                    <template #cell-rd="{ row }">
                        <div v-if="row.played > 0" class="scout-cell mono-num" :style="rdHeatStyle(row.rd)">{{ formatNumber(row.rd, 0) }}</div>
                        <span v-else class="cell-muted mono-num">-</span>
                    </template>
                </sortable-table>
            </div>

            <div v-if="mapViewMode === 'full'" class="table-wrapper table-wrapper--scroll">
                <sortable-table
                    v-if="normalizedRows.length"
                    :key="detailedTableKey"
                    :columns="resolvedMapColumns"
                    :header-groups="mapHeaderGroups"
                    :data="normalizedRows"
                    :default-sort="mapDefaultSort"
                    :sticky-header="true"
                    :compact="true"
                    class="map-full-table"
                >
                    <template #cell-mapName="{ row }">
                        <div class="map-name">
                            <img v-if="row.mapImage" :src="row.mapImage" class="map-logo" alt="" />
                            <span class="map-name-text">{{ row.mapName }}</span>
                        </div>
                    </template>
                    <template #cell-totalRoundsPlayed="{ row }">
                        <div class="scout-cell scout-cell--stacked mono-num">
                            <div>{{ formatNumber(row.totalRoundsPlayed, 0) }}</div>
                            <div v-if="row.roundsWon || row.roundsLost" class="rounds-breakdown">
                                <span class="rounds-won">+{{ formatNumber(row.roundsWon, 0) }}</span>
                                <span class="rounds-sep">/</span>
                                <span class="rounds-lost">-{{ formatNumber(row.roundsLost, 0) }}</span>
                                <span class="rounds-diff" :class="(row.roundsWon - row.roundsLost) >= 0 ? 'rounds-diff--pos' : 'rounds-diff--neg'">({{ (row.roundsWon - row.roundsLost) >= 0 ? '+' : '' }}{{ formatNumber(row.roundsWon - row.roundsLost, 0) }})</span>
                            </div>
                        </div>
                    </template>
                    <template #cell-adr="{ row }"><div class="scout-cell mono-num" :style="adrHeatStyle(row.adr)">{{ formatNumber(row.adr, 1) }}</div></template>
                    <template #cell-kd="{ row }"><div class="scout-cell mono-num" :style="kdHeatStyle(row.kd)">{{ formatNumber(row.kd, 2) }}</div></template>
                    <template #cell-multi2k="{ row }"><div class="scout-cell scout-cell--stacked mono-num"><div>{{ formatNumber(row.multi2k, 0) }}</div><div v-if="shouldShowSubMetric(row.multi2kPerRound, row.multi2kPerMap)" class="sub-metric-line">({{ subMetricValue(row.multi2kPerRound, row.multi2kPerMap, 3) }})</div></div></template>
                    <template #cell-multi3k="{ row }"><div class="scout-cell scout-cell--stacked mono-num"><div>{{ formatNumber(row.multi3k, 0) }}</div><div v-if="shouldShowSubMetric(row.multi3kPerRound, row.multi3kPerMap)" class="sub-metric-line">({{ subMetricValue(row.multi3kPerRound, row.multi3kPerMap, 3) }})</div></div></template>
                    <template #cell-multi4k="{ row }"><div class="scout-cell scout-cell--stacked mono-num"><div>{{ formatNumber(row.multi4k, 0) }}</div><div v-if="shouldShowSubMetric(row.multi4kPerRound, row.multi4kPerMap)" class="sub-metric-line">({{ subMetricValue(row.multi4kPerRound, row.multi4kPerMap, 3) }})</div></div></template>
                    <template #cell-multi5k="{ row }"><div class="scout-cell scout-cell--stacked mono-num"><div>{{ formatNumber(row.multi5k, 0) }}</div><div v-if="shouldShowSubMetric(row.multi5kPerRound, row.multi5kPerMap)" class="sub-metric-line">({{ subMetricValue(row.multi5kPerRound, row.multi5kPerMap, 3) }})</div></div></template>
                    <template #cell-pistolKills="{ row }"><div class="scout-cell scout-cell--stacked mono-num"><div>{{ formatNumber(row.pistolKills, 0) }}</div><div v-if="shouldShowSubMetric(row.pistolKillsPerRound, row.pistolKillsPerMap)" class="sub-metric-line">({{ subMetricValue(row.pistolKillsPerRound, row.pistolKillsPerMap, 3) }})</div></div></template>
                    <template #cell-sniperKills="{ row }"><div class="scout-cell scout-cell--stacked mono-num"><div>{{ formatNumber(row.sniperKills, 0) }}</div><div v-if="shouldShowSubMetric(row.sniperKillsPerRound, row.sniperKillsPerMap)" class="sub-metric-line">({{ subMetricValue(row.sniperKillsPerRound, row.sniperKillsPerMap, 3) }})</div></div></template>
                    <template #cell-totalDamage="{ row }"><div class="scout-cell scout-cell--stacked mono-num"><div>{{ formatNumber(row.totalDamage, 0) }}</div><div v-if="shouldShowSubMetric(row.totalDamagePerRound, row.totalDamagePerMap)" class="sub-metric-line">({{ subMetricValue(row.totalDamagePerRound, row.totalDamagePerMap, 1) }})</div></div></template>
                </sortable-table>
                <div v-else class="empty-state-container">
                    <div class="empty-state-card">
                        <h3 class="empty-state-title">Ei karttadataa</h3>
                        <p class="empty-state-description">Talle kaudelle ei ole karttakohtaisia tilastoja.</p>
                    </div>
                </div>
            </div>
        </section>
    `
};
