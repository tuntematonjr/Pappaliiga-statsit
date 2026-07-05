const DIVISION_METRIC_SCHEMA = [
    { id: 'teams', key: ['team_count', 'teams.length', 'aggregates.team_count'], label: 'Joukkueet', digits: 0 },
    { id: 'players', key: ['player_count', 'aggregates.player_count'], label: 'Pelaajat', digits: 0 },
    { id: 'avg_elo', key: ['aggregates.avg_elo', 'avg_elo'], label: 'Avg Elo', digits: 1 },
    { id: 'matches', key: ['aggregates.played_matches', 'aggregates.matches_played', 'played_matches', 'matches_played'], label: 'Ottelut', digits: 0 },
    { id: 'maps', key: ['aggregates.maps_played_total', 'maps_played_total', 'maps_played'], label: 'Karttoja pelattu', digits: 0 },
    { id: 'rounds', key: ['aggregates.rounds_played_total', 'rounds_played_total', 'rounds'], label: 'Erää pelattu', digits: 0 },
    { id: 'adr', key: ['aggregates.median_adr', 'median_adr'], label: 'Median ADR', digits: 1 },
    { id: 'kr', key: ['aggregates.median_kr', 'median_kr'], label: 'Median K/R', digits: 2 },
    { id: 'kd', key: ['aggregates.avg_kd', 'aggregates.median_kd'], label: 'Keski K/D', digits: 2 },
    { id: 'hs', key: ['aggregates.median_hs_pct', 'median_hs_pct'], label: 'Median HS%', percent: true, digits: 1 },
    { id: 'kills', key: ['aggregates.total_kills', 'kills_total', 'kills'], label: 'Total kills', digits: 0 },
    { id: 'deaths', key: ['aggregates.total_deaths', 'deaths_total', 'deaths'], label: 'Total deaths', digits: 0 },
    { id: 'survival', key: ['aggregates.median_survival', 'median_survival'], label: 'Selviytyminen', percent: true, digits: 1 },
    { id: 'flashbangs', key: ['aggregates.total_flashbangs', 'flashbangs_total', 'flash_count'], label: 'Heitetyt flashbangit', digits: 0 },
    { id: 'flash_success', key: ['aggregates.flash_success_rate', 'flash_success_rate'], label: 'Flash onnistumis%', percent: true, digits: 1 }
];

const DIVISION_MAP_COLUMNS = [
    { key: 'map_name', label: 'Kartta', sortable: true, align: 'left', colClass: 'col-name col-map-name', width: '210px' },
    { key: 'maps_played', label: 'Pelattu', sortable: true, numeric: true, align: 'right', width: '88px' },
    { key: 'banned', label: 'Bannit', sortable: true, numeric: true, align: 'right', width: '88px' },
    { key: 'rounds_played', label: 'Rundeja', sortable: true, numeric: true, align: 'right', width: '94px' },
    { key: 'r_per_map', label: 'R/Map', sortable: true, numeric: true, align: 'right', decimals: 0, width: '88px' },
    { key: 'kills', label: 'Killed', sortable: true, numeric: true, align: 'right', width: '88px' },
    { key: 'deaths', label: 'Deaths', sortable: true, numeric: true, align: 'right', width: '88px' },
    { key: 'assists', label: 'Assists', sortable: true, numeric: true, align: 'right', width: '88px' },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, align: 'right', decimals: 1, width: '90px' },
    { key: 'kr', label: 'K/R', sortable: true, numeric: true, align: 'right', decimals: 2, width: '78px' },
    { key: 'udpr', label: 'UDPR', sortable: true, numeric: true, align: 'right', decimals: 2, width: '94px' },
    { key: 'enemy_flash', label: 'Enemy/Flash', sortable: true, numeric: true, align: 'right', decimals: 2, width: '108px' },
    { key: 'k2', label: '2K', sortable: true, numeric: true, align: 'right', width: '68px' },
    { key: 'k3', label: '3K', sortable: true, numeric: true, align: 'right', width: '68px' },
    { key: 'k4', label: '4K', sortable: true, numeric: true, align: 'right', width: '68px' },
    { key: 'ace', label: 'Ace', sortable: true, numeric: true, align: 'right', width: '68px' },
    { key: 'pistol_kills', label: 'Pistol Kills', sortable: true, numeric: true, align: 'right', width: '104px' },
    { key: 'sniper_kills', label: 'Sniper Kills', sortable: true, numeric: true, align: 'right', width: '104px' }
];

const SANKARI_CARD_GROUPS = [
    {
        id: 'attack',
        groupTitle: 'Tappokoneet',
        cards: [
            {
                id: 'nuori-osuja',
                title: '"Nuori" osuja',
                description: 'Näyttää nuorille, että vanha jaksaa vielä painaa.<br>Paras K/D.',
                metricKey: 'kd',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'adr-luvut',
                title: 'ADR-luvut kunnossa',
                description: 'Jokainen luoti osuu... ainakin johonkin. Käsi tärisee, mutta tilasto tukee tätä.<br>Paras ADR.',
                metricKey: 'adr',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'aim-assist',
                title: 'Papalla on aim assist',
                description: 'Käsi muistaa sen spray-patternin vieläkin.<br>Paras Kills/Round.',
                metricKey: 'killsPerRound',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'grim-reapers',
                title: 'Viikatemiehet',
                description: 'Voittamattomat pelaajat.<br>Eniten tappoja.',
                metricKey: 'totalKills',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'eagle',
                title: 'Eläkeläis-dEagle',
                description: 'Kun rahaa ei ole rifleen, mutta luotto omaan käteen löytyy.<br>Eniten pistooli tappoja.',
                metricKey: 'pistolKills',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'bossikielto',
                title: 'Bossikielto peruttu',
                description: 'Zoomaa ja muistelee CSGO-päiviä.<br>Eniten sniper tappoja.',
                metricKey: 'sniperKills',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'puukko-junnkkari',
                title: 'Sain maineen puukko junnkkari',
                description: 'Hiipii selkään kuin LAN-illan viimeinen yllätys.<br>Eniten puukko tappoja.',
                metricKey: 'knifeKills',
                requirePositive: true,
                showWhenEmpty: true,
                placeholderNames: ['Bot Allu', 'Bot Bob', 'Bot Pete', 'Bot Tuntematon'],
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'ukko-ylijumala',
                title: 'Ukko Ylijumala',
                description: 'Salama ei aina iske kahdesti, mutta Zeus kyllä.<br>Eniten zeus tappoja.',
                metricKey: 'zeusKills',
                requirePositive: true,
                showWhenEmpty: true,
                placeholderNames: ['Bot Allu', 'Bot Bob', 'Bot Pete', 'Bot Tuntematon'],
                sortDirection: 'desc',
                maxEntries: 4
            }
        ]
    },
    {
        id: 'results',
        groupTitle: 'Pelin Tekijät',
        cards: [
            {
                id: 'liiga-ruusu',
                title: 'Liiga Ruusu',
                description: 'Vaimo kyselee koska tuut nukkumaan, mut papalla on vielä yks liiga matsi edessä.<br>Eniten kierroksia pelattu.',
                metricKey: 'roundsPlayed',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'dps-dino',
                title: 'DPS-dinosaurus',
                description: 'Numerot on isompia ku syntymävuosi.<br>Suurin total damage.',
                metricKey: 'totalDamage',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'spektaattori',
                title: 'Spektaattori',
                description: 'Näkee enemmän deathcamia kuin peliä.<br>Eniten kuolemia.',
                metricKey: 'deaths',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'tukipappajoukot',
                title: 'Tukipappajoukot',
                description: 'Syöttää frägejä kuin Pappa grillimakkaraa.<br>Eniten assisteja.',
                metricKey: 'assists',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'parhaat-suonenvedot',
                title: 'Parhaat suonenvedot',
                description: 'Alkulämmöt otettu vasta warmupissa vihun basessa, takaa parhaat suonenvedot.<br>Paras HS%.',
                metricKey: 'hsPercent',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'pelin-isahahmo',
                title: 'Pelin isähahmo',
                description: 'Koko divarin roolimalli.<br>Eniten MVP.',
                metricKey: 'mvps',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'clutch-kills',
                title: 'Vanha pää, kova käsi',
                description: 'Pää on kylmä, mutta oma ruumis ei vielä.<br>Eniten Clutch Kills.',
                metricKey: 'clutchKills',
                sortDirection: 'desc',
                maxEntries: 4
            }
        ]
    },
    {
        id: 'support',
        groupTitle: 'Taustapapat',
        cards: [
            {
                id: 'parhaat-nitrot',
                title: 'Parhaat nitrot',
                description: '1v3? Ei ongelmaa, ainakaan jos nitrot ehtii vaikuttaa ajoissa.<br>Paras Clutch WR%.',
                metricKey: 'clutchWinRate',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'taysmaito',
                title: 'Täysmaito',
                description: 'Kerrankin jollain flash osuu muualle kuin omaan tiimiin.<br>Eniten onnistuneita flashbängejä.',
                metricKey: 'flashAssistPercent',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'utility',
                title: 'Kranaatti vyö tyhjäksi',
                description: 'Polttaa enemmän kuin 2000-luvun LANit.<br>Eniten utility damage.',
                metricKey: 'utilityDamage',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'mikapahan',
                title: 'Mikä pahan tappaisi',
                description: 'Ei puske, ei haasta, säästää eläkkeelle. Klassinen pappa-stratti.<br>Paras Survival%.',
                metricKey: 'survivalPercent',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'valot-paalle',
                title: 'Valot päälle, papat!',
                description: 'Heittää flashin ennen kuin rundi edes alkaa.<br>Eniten heitettyjä flashbängejä.',
                metricKey: 'flashbangsThrown',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'flash-dance',
                title: 'Flash Bang Dance',
                description: 'Vihu näkee enemmän välähdyksiä kuin diskossa 90-luvulla.<br>Eniten vihollisia sokaistu.',
                metricKey: 'enemiesFlashed',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'avustava-konkari',
                title: 'Avustava konkari',
                description: 'Elää tiimille, vähiten frägejä, eniten syöttöjä.<br>Assisteja enemmän kuin tappoja.',
                metricKey: 'assistSupport',
                sortDirection: 'desc',
                maxEntries: 4
            }
        ]
    }
];

const SANKARI_METRIC_META = {
    kd: { decimals: 2 },
    adr: { decimals: 1 },
    killsPerRound: { decimals: 2 },
    totalKills: { decimals: 0 },
    pistolKills: { decimals: 0 },
    sniperKills: { decimals: 0 },
    knifeKills: { decimals: 0 },
    zeusKills: { decimals: 0 },
    roundsPlayed: { decimals: 0 },
    totalDamage: { decimals: 0 },
    deaths: { decimals: 0 },
    assists: { decimals: 0 },
    hsPercent: { decimals: 1, percent: true },
    mvps: { decimals: 0 },
    clutchKills: { decimals: 0 },
    clutchWinRate: { decimals: 1, percent: true },
    flashAssistPercent: { decimals: 1, percent: true },
    utilityDamage: { decimals: 0 },
    survivalPercent: { decimals: 1, percent: true },
    flashbangsThrown: { decimals: 0 },
    enemiesFlashed: { decimals: 0 },
    assistSupport: { decimals: 0 }
};

const DIVISION_DEFAULT_TEAM_LOGO = window.PAPPALIIGA_DEFAULT_LOGO;
const DIVISION_UPCOMING_FETCH_MIN_LIMIT = 16;
const DIVISION_UPCOMING_FETCH_MAX_LIMIT = 40;
const DIVISION_PLAYED_PREFETCH_CONCURRENCY = 6;

function pickValue(obj, keys) {
    if (!obj) return undefined;
    const paths = Array.isArray(keys) ? keys : [keys];
    for (const path of paths) {
        if (!path) continue;
        const segments = String(path).split('.');
        let current = obj;
        let found = true;
        for (const segment of segments) {
            if (current && Object.prototype.hasOwnProperty.call(current, segment)) {
                current = current[segment];
            } else {
                found = false;
                break;
            }
        }
        if (found && current !== undefined) {
            return current;
        }
    }
    return undefined;
}

function formatMetric(value, schema) {
    if (value === undefined || value === null) {
        return '–';
    }
    let numeric = Number(value);
    if (!Number.isFinite(numeric)) {
        numeric = Number(String(value).replace(',', '.'));
    }
    if (!Number.isFinite(numeric)) {
        return value;
    }
    if (schema?.percent) {
        if (Math.abs(numeric) <= 1) {
            numeric *= 100;
        }
        const decimals = schema?.digits ?? 1;
        return `${numeric.toFixed(decimals)} %`;
    }
    const decimals = schema?.digits ?? (numeric >= 100 ? 0 : 1);
    return new Intl.NumberFormat('fi-FI', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    }).format(numeric);
}

function formatIntegerMetric(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
        return '0';
    }
    return new Intl.NumberFormat('fi-FI', {
        minimumFractionDigits: 0,
        maximumFractionDigits: 0
    }).format(Math.max(0, Math.trunc(numeric)));
}

function formatPercentMetric(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
        return null;
    }
    const rounded = Math.round(numeric * 10) / 10;
    const hasDecimal = Math.abs(rounded % 1) > 0;
    return new Intl.NumberFormat('fi-FI', {
        minimumFractionDigits: hasDecimal ? 1 : 0,
        maximumFractionDigits: 1
    }).format(rounded);
}

function buildMetricCards(source, schema) {
    if (!source || !schema) {
        return [];
    }
    return schema.map(def => {
        const raw = pickValue(source, def.key);
        return {
            key: def.id,
            label: def.label,
            value: formatMetric(raw, def)
        };
    });
}

function defaultSegment() {
    return {
        loading: false,
        error: null,
        data: null
    };
}

window.DivisionView = {
    name: 'DivisionView',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; },
        get TeamComparisonBoard() { return window.TeamComparisonBoard; },
        get SharedMapPerformanceTable() { return window.SharedMapPerformanceTable; },
        get MapsStats() { return window.MapsStats; },
        get SummaryStatCard() { return window.SummaryStatCard; },
        get SankariCard() { return window.SankariCard; },
        get UpcomingMatchesList() { return window.UpcomingMatchesList; },
        get MatchExpandedDetails() { return window.MatchExpandedDetails; },
        get PlayoffBracket() { return window.PlayoffBracket; },
        get DivisionPlayersTable() { return window.DivisionPlayersTable; }
    },
    data() {
        const divisionStore = typeof window.useDivisionStore === 'function' ? window.useDivisionStore() : null;
        const upcomingStore = typeof window.useUpcomingStore === 'function' ? window.useUpcomingStore() : null;
        return {
            divisionStore,
            upcomingStore,
            mapColumns: DIVISION_MAP_COLUMNS,
            matchViewMode: 'upcoming',
            divisionMatches: [],
            divisionMatchesLoading: false,
            divisionMatchesError: null,
            expandedPlayedMatches: {},
            playedMatchBundles: {},
            playedMatchBundleLoading: {},
            replay2StatusByMatch: {},
            playedRowsPrefetching: false,
            playedRowsPrefetchKey: '',
            mapCatalog: [],
            mapCatalogLoaded: false,
            quickLinks: [
                { id: 'upcoming', label: 'Tulevat ottelut' },
                { id: 'summary', label: 'Tilastot' },
                { id: 'standings', label: 'Joukkuavertailu' },
                { id: 'players', label: 'Pelaajat' },
                { id: 'maps', label: 'Karttatilastot' },
                { id: 'heroes', label: 'Sankarit' }
            ],
            activeTeamChipId: null,
            sankariPlaceholderOrderCache: {},
            divisionLoadToken: 0,
            upcomingLoadToken: 0,
            matchesLoadToken: 0,
            championshipResolveToken: 0,
            resolvedChampionshipId: null,
            resolvedRouteSource: null
        };
    },
    computed: {
        championshipId() {
            return this.$route.query?.championship || this.$route.query?.division_id || this.$route.params?.championshipId || null;
        },
        divisionState() {
            if (!this.championshipId || !this.divisionStore) {
                return {
                    details: defaultSegment(),
                    standings: defaultSegment(),
                    maps: defaultSegment()
                };
            }
            return this.divisionStore.getDivisionState(this.championshipId) || {
                details: defaultSegment(),
                standings: defaultSegment(),
                maps: defaultSegment()
            };
        },
        divisionDetails() {
            return this.divisionState.details.data;
        },
        divisionLoading() {
            return this.divisionState.details.loading;
        },
        divisionError() {
            return this.divisionState.details.error;
        },
        standingsState() {
            return this.divisionState.standings;
        },
        standings() {
            return Array.isArray(this.standingsState.data) ? this.standingsState.data : [];
        },
        standingsLoading() {
            return this.standingsState.loading || this.divisionLoading;
        },
        standingsError() {
            return this.standingsState.error;
        },
        mapsState() {
            return this.divisionState.maps;
        },
        mapStats() {
            return Array.isArray(this.mapsState.data) ? this.mapsState.data : (this.divisionDetails?.map_stats || []);
        },
        mapsLoading() {
            return this.mapsState.loading;
        },
        mapsError() {
            return this.mapsState.error;
        },
        upcomingState() {
            if (!this.upcomingStore || !this.championshipId) {
                return { data: [], loading: false, error: null };
            }
            if (typeof this.upcomingStore.getEntryForParams !== 'function') {
                return { data: [], loading: false, error: null };
            }
            return this.upcomingStore.getEntryForParams({
                championshipId: this.championshipId,
                limit: this.upcomingFetchLimit,
                offset: 0
            });
        },
        upcomingFetchLimit() {
            const detailsCount = Number(this.divisionDetails?.team_count ?? this.divisionDetails?.teams_count ?? NaN);
            const standingsCount = Array.isArray(this.standings) ? this.standings.length : 0;
            const fallbackTeams = Number.isFinite(detailsCount) && detailsCount > 0 ? detailsCount : standingsCount;
            if (!fallbackTeams) return DIVISION_UPCOMING_FETCH_MIN_LIMIT;
            const calculated = Math.ceil(fallbackTeams * 2);
            return Math.min(DIVISION_UPCOMING_FETCH_MAX_LIMIT, Math.max(DIVISION_UPCOMING_FETCH_MIN_LIMIT, calculated));
        },
        upcomingMatches() {
            const items = Array.isArray(this.upcomingState.data) ? this.upcomingState.data : [];
            if (!items.length) return [];

            const earliestByTeam = new Map();
            for (const match of items) {
                const teamIds = this.extractUpcomingTeamIds(match);
                if (!teamIds.length) continue;
                const ts = this.upcomingMatchTimestamp(match);
                for (const teamId of teamIds) {
                    const current = earliestByTeam.get(teamId);
                    if (!current) {
                        earliestByTeam.set(teamId, { match, ts });
                        continue;
                    }
                    if (ts < current.ts) {
                        earliestByTeam.set(teamId, { match, ts });
                    }
                }
            }

            const selected = Array.from(new Set(Array.from(earliestByTeam.values()).map(entry => entry.match)));
            return selected.sort((a, b) => {
                const at = this.upcomingMatchTimestamp(a);
                const bt = this.upcomingMatchTimestamp(b);
                if (at !== bt) return at - bt;
                return String(this.upcomingMatchId(a)).localeCompare(String(this.upcomingMatchId(b)));
            });
        },
        upcomingLoading() {
            return this.upcomingState.loading;
        },
        upcomingError() {
            return this.upcomingState.error;
        },
        playedMatches() {
            const rows = Array.isArray(this.divisionMatches) ? this.divisionMatches : [];
            return rows
                .filter(match => this.isPlayedDivisionMatch(match))
                .sort((a, b) => this.divisionMatchTimestamp(b) - this.divisionMatchTimestamp(a));
        },
        matchSectionLoading() {
            return this.matchViewMode === 'played'
                ? this.divisionMatchesLoading
                : this.upcomingLoading;
        },
        matchSectionError() {
            return this.matchViewMode === 'played'
                ? this.divisionMatchesError
                : this.upcomingError;
        },
        hasMatchSectionData() {
            return this.matchViewMode === 'played'
                ? this.playedMatches.length > 0
                : this.upcomingMatches.length > 0;
        },
        hasAnyMatchData() {
            return this.playedMatches.length > 0 || this.upcomingMatches.length > 0;
        },
        quickLinksVisible() {
            return this.quickLinks.filter(link => {
                if (link.id === 'upcoming') {
                    return this.matchSectionLoading || this.divisionMatchesLoading || this.hasAnyMatchData;
                }
                return true;
            });
        },
        sankariPlayers() {
            const source = this.divisionDetails?.player_totals || this.divisionDetails?.playerTotals || [];
            if (!Array.isArray(source)) {
                return [];
            }
            return source
                .map(entry => this.normalizeSankariPlayer(entry))
                .filter(Boolean);
        },
        sankariGroups() {
            return SANKARI_CARD_GROUPS.map(group => {
                const cards = group.cards
                    .map(card => {
                        const thresholds = this.sankariThreshold(card, this.sankariPlayers);
                        const entries = this.buildSankariEntries(this.sankariPlayers, card, thresholds);
                        const resolvedEntries = this.mergeSankariEntriesWithPlaceholders(card, entries);
                        return {
                            ...card,
                            thresholds,
                            entries: resolvedEntries,
                            tooltip: this.cardThresholdTooltip(card, thresholds)
                        };
                    })
                    .filter(card => card.showWhenEmpty === true || card.entries.length);
                if (!cards.length) return null;
                return {
                    ...group,
                    cards
                };
            }).filter(Boolean);
        },
        hasSankariGroups() {
            return this.sankariGroups.length > 0;
        },
        sankariLoading() {
            return this.divisionLoading;
        },
        divisionTitle() {
            if (!this.divisionDetails) return 'Divisioona';
            return this.divisionDetails.name || `Divisioona ${this.divisionDetails.division_num}`;
        },
        isPlayoff() {
            return !!this.divisionDetails?.is_playoff;
        },
        playoffBracket() {
            return this.divisionDetails?.bracket || null;
        },
        nextUpcomingMatch() {
            return Array.isArray(this.upcomingMatches) && this.upcomingMatches.length
                ? this.upcomingMatches[0]
                : null;
        },
        divisionHeroStats() {
            const aggregates = this.derivedAggregates || {};
            const teamCount = this.toNumber(
                aggregates.team_count ?? this.divisionDetails?.team_count ?? this.standings.length,
                0
            );
            const playerCount = this.toNumber(
                aggregates.player_count ?? this.divisionDetails?.player_count,
                0
            );
            const progress = this.matchProgressMetric;
            const mapsCount = this.toNumber(aggregates.maps_played_total ?? this.mapStats.length, 0);
            const nextMatchTs = this.nextUpcomingMatch ? this.upcomingMatchTimestamp(this.nextUpcomingMatch) : Number.POSITIVE_INFINITY;

            return [
                {
                    key: 'teams',
                    label: 'Joukkueet',
                    value: formatIntegerMetric(teamCount),
                    tone: 'cool'
                },
                {
                    key: 'players',
                    label: 'Pelaajat',
                    value: formatIntegerMetric(playerCount),
                    tone: 'violet'
                },
                {
                    key: 'matches',
                    label: 'Eteneminen',
                    value: progress.percent != null
                        ? `${formatPercentMetric(progress.percent)} %`
                        : formatIntegerMetric(progress.played),
                    meta: progress.total > 0
                        ? `${formatIntegerMetric(progress.played)} / ${formatIntegerMetric(progress.total)} ottelua`
                        : 'Ottelutiedot päivittyvät',
                    tone: 'cyan'
                },
                Number.isFinite(nextMatchTs) && nextMatchTs < Number.POSITIVE_INFINITY
                    ? {
                        key: 'next-match',
                        label: 'Seuraava',
                        value: this.formatHeroTimestamp(nextMatchTs),
                        meta: 'Seuraava vahvistettu ottelu',
                        tone: 'mint'
                    }
                    : {
                        key: 'maps',
                        label: 'Kartat',
                        value: formatIntegerMetric(mapsCount),
                        meta: 'Karttadataa seurannassa',
                        tone: 'amber'
                    }
            ];
        },
        statMetrics() {
            if (!this.divisionDetails) {
                return [];
            }
            const aggregates = this.derivedAggregates;
            const source = {
                ...aggregates,
                aggregates
            };
            return buildMetricCards(source, DIVISION_METRIC_SCHEMA);
        },
        derivedAggregates() {
            const details = this.divisionDetails || {};
            const aggregates = { ...(details.aggregates || {}) };
            const teams = Array.isArray(details.teams) ? details.teams : [];
            const standings = Array.isArray(this.standings) ? this.standings : [];
            const players = Array.isArray(details.player_totals || details.playerTotals) ? (details.player_totals || details.playerTotals) : [];
            const maps = Array.isArray(this.mapStats) ? this.mapStats : [];

            const activeTeamCount = teams.filter(t => !t.status || (t.status.toLowerCase() !== 'banned' && t.status.toLowerCase() !== 'quit')).length;
            aggregates.team_count = aggregates.team_count ?? activeTeamCount ?? details.team_count;
            aggregates.player_count = aggregates.player_count ?? details.player_count ?? players.length;
            if (aggregates.matches_played == null) {
                const playedMatches =
                    aggregates.played_matches
                    ?? aggregates.playedMatches
                    ?? details.played_matches
                    ?? details.matches_played
                    ?? details.playedMatches
                    ?? details.matchesPlayed;
                if (playedMatches != null) {
                    aggregates.matches_played = playedMatches;
                }
            }
            if (aggregates.played_matches == null && aggregates.matches_played != null) {
                aggregates.played_matches = aggregates.matches_played;
            }
            if (aggregates.total_matches == null) {
                const totalMatches =
                    aggregates.total_matches
                    ?? aggregates.totalMatches
                    ?? aggregates.matches_total
                    ?? aggregates.matchesTotal
                    ?? details.total_matches
                    ?? details.matches_total
                    ?? details.totalMatches
                    ?? details.matchesTotal;
                if (totalMatches != null) {
                    aggregates.total_matches = totalMatches;
                }
            }

            if (aggregates.matches_played == null && standings.length) {
                const teamMatchTotal = standings.reduce(
                    (sum, team) => sum + Number(
                        team.matches_played
                        ?? team.matchesPlayed
                        ?? team.matches
                        ?? 0
                    ),
                    0
                );
                if (teamMatchTotal > 0) {
                    aggregates.matches_played = Math.round(teamMatchTotal / 2);
                    aggregates.played_matches = aggregates.matches_played;
                }
            }
            if (aggregates.maps_played_total == null && maps.length) {
                aggregates.maps_played_total = maps.reduce((sum, entry) => sum + Number(entry.maps_played ?? entry.curr?.maps_played ?? 0), 0);
            }
            if (aggregates.rounds_played_total == null && maps.length) {
                aggregates.rounds_played_total = maps.reduce((sum, entry) => sum + Number(entry.rounds_played ?? entry.curr?.rounds_played ?? 0), 0);
            }
            if (aggregates.total_kills == null && players.length) {
                aggregates.total_kills = players.reduce((sum, p) => sum + this.toNumber(p.kills, 0), 0);
            }
            if (aggregates.total_deaths == null && players.length) {
                aggregates.total_deaths = players.reduce((sum, p) => sum + this.toNumber(p.deaths, 0), 0);
            }
            if (aggregates.total_kills == null && maps.length) {
                aggregates.total_kills = maps.reduce((sum, entry) => sum + this.toNumber(entry.kills ?? entry.curr?.kills, 0), 0);
            }
            if (aggregates.total_deaths == null && maps.length) {
                aggregates.total_deaths = maps.reduce((sum, entry) => sum + this.toNumber(entry.deaths ?? entry.curr?.deaths, 0), 0);
            }
            if (aggregates.total_flashbangs == null && players.length) {
                aggregates.total_flashbangs = players.reduce((sum, p) => sum + this.toNumber(p.flash_count, 0), 0);
            }

            const adrValues = players
                .map(p => this.toNumber(p.adr, null))
                .filter(v => v != null && Number.isFinite(v));
            if (aggregates.median_adr == null && adrValues.length) {
                aggregates.median_adr = this.median(adrValues);
            }

            const kdValues = players
                .map(p => this.toNumber(p.kd ?? (p.deaths ? p.kills / p.deaths : p.kills), null))
                .filter(v => v != null && Number.isFinite(v));
            if (aggregates.avg_kd == null && kdValues.length) {
                aggregates.avg_kd = kdValues.reduce((acc, val) => acc + val, 0) / kdValues.length;
            }
            if ((aggregates.avg_kd == null || aggregates.avg_kd === 0) && aggregates.total_kills != null && aggregates.total_deaths != null) {
                const totalKills = this.toNumber(aggregates.total_kills, null);
                const totalDeaths = this.toNumber(aggregates.total_deaths, null);
                if (totalKills != null && totalDeaths != null) {
                    aggregates.avg_kd = totalDeaths ? totalKills / totalDeaths : totalKills;
                }
            }
            const krValues = players
                .map(p => this.toNumber(p.kr ?? (p.rounds ? p.kills / p.rounds : null), null))
                .filter(v => v != null && Number.isFinite(v));
            if (aggregates.median_kr == null && krValues.length) {
                aggregates.median_kr = this.median(krValues);
            }
            const hsValues = players
                .map(p => this.toNumber(p.hs_pct ?? p.hsPercent ?? p.hs, null))
                .filter(v => v != null && Number.isFinite(v));
            if (aggregates.median_hs_pct == null && hsValues.length) {
                aggregates.median_hs_pct = this.median(hsValues);
            }
            if (aggregates.flash_success_rate == null && players.length) {
                const totalFlash = players.reduce((sum, p) => sum + this.toNumber(p.flash_count, 0), 0);
                const success = players.reduce((sum, p) => sum + this.toNumber(p.flash_successes, 0), 0);
                aggregates.flash_success_rate = totalFlash ? success / totalFlash : null;
            }

            const survivalValues = players
                .map(p => {
                    const rounds = this.toNumber(p.rounds_played ?? p.rounds, null);
                    const deaths = this.toNumber(p.deaths, null);
                    if (!rounds || deaths == null) return null;
                    return (rounds - deaths) / rounds;
                })
                .filter(v => v != null && Number.isFinite(v));
            if (aggregates.median_survival == null && survivalValues.length) {
                aggregates.median_survival = this.median(survivalValues);
            }

            return aggregates;
        },
        matchProgressMetric() {
            const aggregates = this.derivedAggregates || {};
            const playedRaw =
                aggregates.played_matches
                ?? aggregates.matches_played
                ?? this.divisionDetails?.played_matches
                ?? this.divisionDetails?.matches_played
                ?? 0;
            const totalRaw =
                aggregates.total_matches
                ?? this.divisionDetails?.total_matches
                ?? this.divisionDetails?.matches_total
                ?? 0;
            const played = Math.max(0, Math.trunc(this.toNumber(playedRaw, 0)));
            const total = Math.max(0, Math.trunc(this.toNumber(totalRaw, 0)));
            const cappedPlayed = total > 0 ? Math.min(played, total) : played;
            const percent = total > 0 ? (cappedPlayed / total) * 100 : null;
            return {
                played: cappedPlayed,
                total,
                percent
            };
        },
        divisionSummaryMetrics() {
            if (!this.statMetrics.length) return [];
            const matchProgress = this.matchProgressMetric;
            return this.statMetrics.map(metric => {
                const base = {
                    ...metric,
                    icon: this.getMetricIcon(metric.key),
                    subtitle: ''
                };
                if (metric.key !== 'matches') {
                    return base;
                }
                const playedLabel = formatIntegerMetric(matchProgress.played);
                const totalLabel = formatIntegerMetric(matchProgress.total);
                const ratioLabel = matchProgress.total > 0 ? `${playedLabel} / ${totalLabel}` : playedLabel;
                const percentLabel = matchProgress.percent != null ? `${formatPercentMetric(matchProgress.percent)} % pelattu` : '';
                return {
                    ...base,
                    value: ratioLabel,
                    subtitle: percentLabel
                };
            });
        },
        teams() {
            return Array.isArray(this.divisionDetails?.teams) ? this.divisionDetails.teams : [];
        },
        teamChipItems() {
            const source = this.standings.length ? this.standings : this.teams;
            if (!Array.isArray(source)) {
                return [];
            }
            return source.map((team, idx) => {
                const wins = Number(team.maps_won ?? team.wins ?? 0);
                const losses = Number(team.maps_lost ?? team.losses ?? 0);
                const matches = Number(
                    team.matches_played
                    ?? team.matches
                    ?? team.series_played
                    ?? team.match_count
                    ?? team.series_count
                    ?? 0
                );
                const roundDiff = Number(team.round_diff ?? team.rounds_diff ?? 0);
                const id = team.team_id || team.id || `team-${idx}`;
                return {
                    id,
                    label: team.name || team.display_name || team.team_name || `Joukkue ${idx + 1}`,
                    rank: team.rank ?? idx + 1,
                    record: wins || losses ? `${wins}-${losses}` : `${matches} ottelua`,
                    roundDiff,
                    logo: this.teamLogo(team)
                };
            });
        },
    },
    watch: {
        championshipId: {
            immediate: true,
            async handler(id) {
                if (!id) return;
                const rawId = String(id);
                const resolveToken = ++this.championshipResolveToken;
                const resolvedId = await this.resolveChampionshipId(rawId);
                if (resolveToken !== this.championshipResolveToken) {
                    return;
                }
                if (!resolvedId) {
                    return;
                }
                const normalizedId = String(resolvedId);
                this.resolvedRouteSource = rawId;
                this.resolvedChampionshipId = normalizedId;
                this.syncCompactRoute(normalizedId);
                this.expandedPlayedMatches = {};
                this.playedMatchBundles = {};
                this.playedMatchBundleLoading = {};
                this.replay2StatusByMatch = {};
                this.playedRowsPrefetching = false;
                this.playedRowsPrefetchKey = '';
                await this.loadDivision(normalizedId);
                this.loadUpcoming(normalizedId);
                this.loadDivisionMatches(normalizedId);
            }
        },
        matchViewMode: {
            immediate: false,
            handler(mode) {
                if (mode === 'played') {
                    this.prefetchPlayedRowData();
                }
            }
        },
        divisionMatches: {
            deep: false,
            handler() {
                if (this.matchViewMode === 'played') {
                    this.prefetchPlayedRowData();
                }
            }
        },
        teamChipItems: {
            immediate: true,
            handler(newItems) {
                this.ensureActiveTeamChip(newItems);
            }
        }
    },
    methods: {
        isLikelyChampionshipUuid(value) {
            if (value === null || value === undefined) return false;
            const normalized = String(value).trim();
            if (!normalized) return false;
            return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(normalized);
        },
        async resolveChampionshipId(rawId) {
            const normalizedRaw = String(rawId || '').trim();
            if (!normalizedRaw) return null;
            if (this.isLikelyChampionshipUuid(normalizedRaw)) {
                return normalizedRaw;
            }
            if (!window.apiClient || typeof window.apiClient.getDivisionById !== 'function') {
                return null;
            }
            try {
                const details = await window.apiClient.getDivisionById(normalizedRaw, { force: true, noCache: true });
                const canonical = details?.championship_id || details?.championshipId || null;
                if (!canonical) return null;
                return String(canonical);
            } catch (_error) {
                return null;
            }
        },
        syncCompactRoute(championshipId) {
            if (!this.$router || !this.$route || !championshipId) return false;
            const targetName = 'division';
            const targetParams = { championshipId: String(championshipId) };
            const targetQuery = {};
            const currentParam = String(this.$route?.params?.championshipId || '');
            const hasQuery = Object.keys(this.$route?.query || {}).length > 0;
            if (currentParam === targetParams.championshipId && !hasQuery && String(this.$route?.name || '') === targetName) {
                return false;
            }
            this.$router.replace({
                name: targetName,
                params: targetParams,
                query: targetQuery
            }).catch(() => {});
            return true;
        },
        async loadDivision(id, options = {}) {
            if (!id || !this.divisionStore) return;
            const requestToken = ++this.divisionLoadToken;
            // Fetch details+matches bundle first so subsequent store actions find data fresh
            if (typeof this.divisionStore.fetchDivisionBundle === 'function') {
                await this.divisionStore.fetchDivisionBundle(id, { force: options.force === true });
            }
            if (requestToken !== this.divisionLoadToken) return;
            const requests = [
                this.divisionStore.fetchDivisionDetails(id, { force: false }),
                this.divisionStore.fetchDivisionStandings(id, { force: false }),
                this.divisionStore.fetchDivisionMaps(id, { force: false })
            ];
            await Promise.allSettled(requests);
            if (requestToken !== this.divisionLoadToken) {
                return;
            }
        },
        async loadUpcoming(id, options = {}) {
            if (!id || !this.upcomingStore) return;
            const requestToken = ++this.upcomingLoadToken;
            try {
                await this.upcomingStore.fetchUpcomingMatches(
                    { championshipId: id, limit: this.upcomingFetchLimit, offset: 0 },
                    { force: options.force === true }
                );
            } catch (error) {
                console.error('[DivisionView] upcoming matches fetch failed', error);
            } finally {
                if (requestToken !== this.upcomingLoadToken) {
                    return;
                }
            }
        },
        async loadDivisionMatches(id, options = {}) {
            if (!id) {
                this.divisionMatches = [];
                this.divisionMatchesError = null;
                this.divisionMatchesLoading = false;
                return;
            }
            const requestToken = ++this.matchesLoadToken;
            this.divisionMatchesLoading = true;
            this.divisionMatchesError = null;
            try {
                // Use rawMatches from bundle if already fresh
                const storeEntry = this.divisionStore?.getDivisionState?.(id);
                const cachedRaw = storeEntry?.rawMatches;
                let rows;
                if (!options.force && cachedRaw && cachedRaw.fetchedAt && (Date.now() - cachedRaw.fetchedAt < 5 * 60 * 1000) && Array.isArray(cachedRaw.data)) {
                    rows = cachedRaw.data;
                } else if (window.apiClient && typeof window.apiClient.getDivisionMatches === 'function') {
                    rows = await window.apiClient.getDivisionMatches(id, { force: options.force === true });
                } else {
                    rows = [];
                }
                const activeChampionshipId = String(this.resolvedChampionshipId || this.championshipId || '');
                if (requestToken !== this.matchesLoadToken || String(id) !== activeChampionshipId) {
                    return;
                }
                this.divisionMatches = Array.isArray(rows) ? rows : [];
                if (this.matchViewMode === 'played') {
                    this.prefetchPlayedRowData();
                }
            } catch (error) {
                const activeChampionshipId = String(this.resolvedChampionshipId || this.championshipId || '');
                if (requestToken !== this.matchesLoadToken || String(id) !== activeChampionshipId) {
                    return;
                }
                this.divisionMatchesError = error?.message || 'Otteluiden lataus epäonnistui';
                this.divisionMatches = [];
            } finally {
                const activeChampionshipId = String(this.resolvedChampionshipId || this.championshipId || '');
                if (requestToken !== this.matchesLoadToken || String(id) !== activeChampionshipId) {
                    return;
                }
                this.divisionMatchesLoading = false;
            }
        },
        refreshAll() {
            if (!this.championshipId) return;
            this.loadDivision(this.championshipId, { force: true });
            this.loadUpcoming(this.championshipId, { force: true });
            this.loadDivisionMatches(this.championshipId, { force: true });
        },
        setMatchViewMode(mode) {
            if (mode !== 'upcoming' && mode !== 'played') return;
            this.matchViewMode = mode;
            if (mode === 'played') {
                this.prefetchPlayedRowData();
            }
        },
        async prefetchPlayedRowData() {
            const rows = Array.isArray(this.playedMatches) ? this.playedMatches : [];
            const key = rows.map(match => String(match?.match_id || match?.matchId || '')).filter(Boolean).join('|');
            if (!rows.length || !key) return;
            if (this.playedRowsPrefetching) return;
            if (this.playedRowsPrefetchKey === key) return;

            this.playedRowsPrefetching = true;
            this.playedRowsPrefetchKey = key;

            const queue = [...rows];
            const workers = Array.from({ length: Math.min(DIVISION_PLAYED_PREFETCH_CONCURRENCY, queue.length) }, async () => {
                while (queue.length) {
                    const next = queue.shift();
                    if (!next) continue;
                    await this.ensureMatchBundle(next);
                }
            });

            try {
                await Promise.allSettled(workers);
            } finally {
                this.playedRowsPrefetching = false;
            }
        },
        isPlayedDivisionMatch(match) {
            if (!match || typeof match !== 'object') return false;
            const score1 = this.toNumber(match.team1_score ?? match.team1Score, 0);
            const score2 = this.toNumber(match.team2_score ?? match.team2Score, 0);
            const finishedAt = this.toNumber(match.finished_at ?? match.finishedAt, 0);
            return !!match.is_forfeit || finishedAt > 0 || (score1 + score2) > 0;
        },
        playedRowRoundDiff(match) {
            const bundle = this.playedMatchBundles[String(match?.match_id || match?.matchId)] || null;
            const maps = Array.isArray(bundle?.details?.maps) ? bundle.details.maps : [];
            if (!maps.length) return null;
            const diff = maps.reduce((sum, m) => {
                const left = this.toNumber(m?.score_team1, 0);
                const right = this.toNumber(m?.score_team2, 0);
                return sum + (left - right);
            }, 0);
            return diff;
        },
        playedRowTeamAvatar(match, side = 'team1') {
            const bundle = this.playedMatchBundles[String(match?.match_id || match?.matchId)] || null;
            const details = bundle?.details?.match || {};
            const suffix = side === 'team2' ? '2' : '1';

            const direct = match?.[`team${suffix}_avatar`] || match?.[`team${suffix}Avatar`]
                || details?.[`team${suffix}_avatar`] || details?.[`team${suffix}Avatar`] || null;
            if (direct) return direct;

            const teamId = match?.[`team${suffix}_id`] || match?.[`team${suffix}Id`]
                || details?.[`team${suffix}_id`] || details?.[`team${suffix}Id`] || null;
            const teamName = match?.[`team${suffix}_name`] || match?.[`team${suffix}Name`]
                || details?.[`team${suffix}_name`] || details?.[`team${suffix}Name`] || null;
            const found = this.findTeam(teamId, teamName);
            return found?.avatar || found?.logo || found?.team_logo || found?.raw?.avatar || found?.raw?.logo || null;
        },
        playedRowRoundDiffLead(match) {
            const diff = this.playedRowRoundDiff(match);
            if (diff === null) return null;

            const bundle = this.playedMatchBundles[String(match?.match_id || match?.matchId)] || null;
            const details = bundle?.details?.match || {};

            const team1Name = match?.team1_name || match?.team1Name || 'Joukkue 1';
            const team2Name = match?.team2_name || match?.team2Name || 'Joukkue 2';
            const team1Id = match?.team1_id || match?.team1Id || details?.team1_id || details?.team1Id || null;
            const team2Id = match?.team2_id || match?.team2Id || details?.team2_id || details?.team2Id || null;
            const team1Avatar = this.playedRowTeamAvatar(match, 'team1');
            const team2Avatar = this.playedRowTeamAvatar(match, 'team2');

            if (diff > 0) {
                return { teamId: team1Id, teamName: team1Name, teamAvatar: team1Avatar, value: Math.abs(diff) };
            }
            if (diff < 0) {
                return { teamId: team2Id, teamName: team2Name, teamAvatar: team2Avatar, value: Math.abs(diff) };
            }
            return { teamId: null, teamName: 'Tasapeli', teamAvatar: null, value: 0 };
        },
        beautifyMapName(raw) {
            if (!raw) return 'Kartta';
            const value = String(raw).trim();
            const lower = value.toLowerCase();
            if (lower === 'forfeit') return 'Forfeit';
            const core = lower.startsWith('de_') ? lower.slice(3) : lower;
            const parts = core.split(/[_-]/).filter(Boolean);
            if (!parts.length) return value;
            return parts.map(part => part.charAt(0).toUpperCase() + part.slice(1)).join(' ');
        },
        playedRowMaps(match) {
            const bundle = this.playedMatchBundles[String(match?.match_id || match?.matchId)] || null;
            const maps = Array.isArray(bundle?.details?.maps) ? bundle.details.maps : [];
            return maps.map((map, idx) => ({
                id: `${match?.match_id || match?.matchId || 'match'}-map-${idx}`,
                mapName: this.beautifyMapName(map?.map_name || map?.map || 'Kartta'),
                score1: this.toNumber(map?.score_team1, 0),
                score2: this.toNumber(map?.score_team2, 0)
            }));
        },
        isPlayedMatchExpanded(matchId) {
            if (!matchId) return false;
            return !!this.expandedPlayedMatches[String(matchId)];
        },
        async togglePlayedMatchExpand(match) {
            const matchId = String(match?.match_id || match?.matchId || '');
            if (!matchId) return;
            const next = !this.expandedPlayedMatches[matchId];
            this.expandedPlayedMatches = { ...this.expandedPlayedMatches, [matchId]: next };
            if (next) {
                await this.ensureMatchBundle(match);
                this.ensureMapCatalog();
            }
        },
        playedMatchSummary(match) {
            const bundle = this.playedMatchBundles[String(match?.match_id || match?.matchId)] || null;
            const details = bundle?.details?.match || {};
            const team1Id = match?.team1_id || match?.team1Id || details?.team1_id || details?.team1Id || null;
            const team2Id = match?.team2_id || match?.team2Id || details?.team2_id || details?.team2Id || null;
            const team1Name = match?.team1_name || match?.team1Name || details?.team1_name || details?.team1Name || 'Joukkue 1';
            const team2Name = match?.team2_name || match?.team2Name || details?.team2_name || details?.team2Name || 'Joukkue 2';
            return {
                matchId: String(match?.match_id || match?.matchId || details?.match_id || details?.matchId || ''),
                ts: this.toNumber(details?.ts, this.toNumber(match?.finished_at || match?.finishedAt, 0)) || 0,
                bestOf: this.toNumber(match?.best_of ?? match?.bestOf ?? details?.best_of ?? details?.bestOf, 0),
                teamScore: this.toNumber(match?.team1_score ?? match?.team1Score, 0),
                oppScore: this.toNumber(match?.team2_score ?? match?.team2Score, 0),
                team1Name,
                team2Name,
                opponentName: team2Name,
                me: { team_id: team1Id, team_name: team1Name },
                opponent: { team_id: team2Id, team_name: team2Name },
                maps: Array.isArray(bundle?.details?.maps) ? bundle.details.maps : [],
                isForfeit: !!match?.is_forfeit
            };
        },
        playedMatchDetails(match) {
            const bundle = this.playedMatchBundles[String(match?.match_id || match?.matchId)] || null;
            const summary = this.playedMatchSummary(match);
            const details = bundle?.details?.match || {};
            return {
                ...details,
                ...summary,
                team1_id: summary.me?.team_id,
                team2_id: summary.opponent?.team_id,
                team1_name: summary.team1Name,
                team2_name: summary.team2Name,
                maps: Array.isArray(bundle?.details?.maps) ? bundle.details.maps : []
            };
        },
        playedMatchVetoEntry(match) {
            const bundle = this.playedMatchBundles[String(match?.match_id || match?.matchId)] || null;
            return bundle?.details?.veto_entry || null;
        },
        playedMatchPlayerStats(match) {
            const bundle = this.playedMatchBundles[String(match?.match_id || match?.matchId)] || null;
            return Array.isArray(bundle?.playerStats) ? bundle.playerStats : [];
        },
        playedMatchBundleBusy(matchId) {
            if (!matchId) return false;
            return !!this.playedMatchBundleLoading[String(matchId)];
        },
        replay2Links(match) {
            const matchId = String(match?.match_id || match?.matchId || '');
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
        isDemoAvailabilityLoading(match) {
            const matchId = String(match?.match_id || match?.matchId || '');
            if (!matchId) return false;
            const statusMap = this.replay2StatusByMatch[matchId] || {};
            return Object.values(statusMap).some(s => s === 'loading');
        },
        async loadReplay2StatusForMatch(matchId, mapsCount) {
            if (!matchId || mapsCount <= 0) return;

            // Mark all map slots as loading
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
                        const apiBase = window.PL_API_URL || window.__API_BASE__ || '/api';
                        const resp = await fetch(
                            `${apiBase}/replay2/replays/${encodeURIComponent(matchId)}/status?map_id=${mapId}`
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
        async ensureMatchBundle(match) {
            const matchId = String(match?.match_id || match?.matchId || '');
            if (!matchId || !window.apiClient || typeof window.apiClient.getMatchBundle !== 'function') return;
            const existingBundle = this.playedMatchBundles[matchId];
            const existingMaps = Array.isArray(existingBundle?.details?.maps) ? existingBundle.details.maps.length : 0;
            if (existingBundle && existingMaps > 0) return;
            if (this.playedMatchBundleLoading[matchId]) return;
            this.playedMatchBundleLoading = { ...this.playedMatchBundleLoading, [matchId]: true };
            try {
                const payload = await window.apiClient.getMatchBundle(matchId);
                this.playedMatchBundles = {
                    ...this.playedMatchBundles,
                    [matchId]: payload || { details: {}, playerStats: [] }
                };
                const mapsCountFromPayload = Array.isArray(payload?.details?.maps) ? payload.details.maps.length : 0;
                const bestOf = this.toNumber(
                    match?.best_of ?? match?.bestOf ?? payload?.details?.match?.best_of ?? payload?.details?.match?.bestOf,
                    0
                );
                const mapsCount = Math.max(mapsCountFromPayload, bestOf, 2);
                this.loadReplay2StatusForMatch(matchId, mapsCount);
            } catch (error) {
                // Keep previous successful bundle if any; do not lock this match to empty payload.
            } finally {
                this.playedMatchBundleLoading = { ...this.playedMatchBundleLoading, [matchId]: false };
            }
        },
        async ensureMapCatalog() {
            if (this.mapCatalogLoaded || !window.apiClient || typeof window.apiClient.getMapsCatalog !== 'function') return;
            try {
                const catalog = await window.apiClient.getMapsCatalog();
                this.mapCatalog = Array.isArray(catalog) ? catalog : [];
            } catch (_error) {
                this.mapCatalog = [];
            } finally {
                this.mapCatalogLoaded = true;
            }
        },
        divisionMatchTimestamp(match) {
            if (!match || typeof match !== 'object') return 0;
            const finished = this.toNumber(match.finished_at ?? match.finishedAt, 0);
            if (finished > 0) return finished * 1000;
            return 0;
        },
        formatDivisionMatchDate(match) {
            const ts = this.divisionMatchTimestamp(match);
            if (!ts) return '—';
            const date = new Date(ts);
            return date.toLocaleDateString('fi-FI', { year: 'numeric', month: 'short', day: 'numeric' });
        },
        formatHeroTimestamp(ts) {
            if (!Number.isFinite(ts) || ts <= 0) {
                return 'Ajankohta avoin';
            }
            try {
                return new Date(ts).toLocaleDateString('fi-FI', {
                    day: 'numeric',
                    month: 'short'
                });
            } catch (_error) {
                return 'Ajankohta avoin';
            }
        },
        divisionMatchFaceitUrl(match) {
            const utils = window.MatchLinksUtils;
            if (!utils || typeof utils.getFaceitRoomUrl !== 'function') return '';
            const matchId = match?.match_id ?? match?.matchId;
            return utils.getFaceitRoomUrl(matchId);
        },
        replay2PlayerUrl(matchId, mapId) {
            return `https://replay2.pappa.aukko.net/player?faceit_match_id=${encodeURIComponent(matchId)}&map_id=${mapId}`;
        },
        divisionTeamRoute(teamId) {
            if (!teamId || !this.championshipId) return null;
            return {
                name: 'team-detail',
                params: {
                    championshipId: String(this.championshipId),
                    teamId: String(teamId)
                }
            };
        },
        scrollToSection(id) {
            const el = document.getElementById(id);
            if (!el) return;
            try {
                const targetHash = `#${id}`;
                if (history && history.pushState) {
                    history.pushState(null, '', targetHash);
                } else {
                    window.location.hash = targetHash;
                }
                el.scrollIntoView({ behavior: 'smooth', block: 'start' });
            } catch (error) {
                window.location.hash = `#${id}`;
                window.scrollTo(0, el.offsetTop || 0);
            }
        },
        resolveAvatar(src) {
            if (!src) return DIVISION_DEFAULT_TEAM_LOGO;
            try {
                if (window.apiClient && typeof window.apiClient.proxyAvatar === 'function') {
                    const resolved = window.apiClient.proxyAvatar(src);
                    return resolved || DIVISION_DEFAULT_TEAM_LOGO;
                }
                return src || DIVISION_DEFAULT_TEAM_LOGO;
            } catch (error) {
                return src || DIVISION_DEFAULT_TEAM_LOGO;
            }
        },
        normalizeUpcomingTeamId(value) {
            if (value === null || value === undefined || value === '') return null;
            return String(value);
        },
        extractUpcomingTeamIds(match) {
            if (!match || typeof match !== 'object') return [];
            const ids = [
                match.team1_id,
                match.team1Id,
                match.team2_id,
                match.team2Id,
                match.team1?.id,
                match.team1?.team_id,
                match.team1?.teamId,
                match.team2?.id,
                match.team2?.team_id,
                match.team2?.teamId
            ]
                .map(value => this.normalizeUpcomingTeamId(value))
                .filter(Boolean);
            return Array.from(new Set(ids));
        },
        upcomingMatchTimestamp(match) {
            if (!match || typeof match !== 'object') return Number.POSITIVE_INFINITY;
            const globalUtils = typeof window !== 'undefined' ? window.matchTimeUtils : null;
            if (globalUtils && typeof globalUtils.getScheduledTs === 'function') {
                const globalTs = globalUtils.getScheduledTs(match);
                if (Number.isFinite(globalTs) && globalTs > 0) return globalTs;
            }
            const raw = match.scheduled_ts ?? match.scheduledTs ?? match.scheduled_at ?? match.scheduledAt ?? match.ts ?? null;
            const numeric = Number(raw);
            if (Number.isFinite(numeric) && numeric > 0) {
                return Math.abs(numeric) < 1_000_000_000_000 ? numeric * 1000 : numeric;
            }
            const parsed = Date.parse(String(raw));
            if (Number.isFinite(parsed) && parsed > 0) return parsed;
            return Number.POSITIVE_INFINITY;
        },
        upcomingMatchId(match) {
            if (!match || typeof match !== 'object') return '';
            return match.match_id ?? match.matchId ?? '';
        },
        toNumber(value, fallback = 0) {
            if (value === null || value === undefined) return fallback;
            const numeric = typeof value === 'number' ? value : Number(String(value).replace(',', '.'));
            if (Number.isFinite(numeric)) return numeric;
            return fallback;
        },
        median(values) {
            if (!Array.isArray(values) || !values.length) return null;
            const sorted = [...values].sort((a, b) => a - b);
            const mid = Math.floor(sorted.length / 2);
            if (sorted.length % 2 === 0) {
                return (sorted[mid - 1] + sorted[mid]) / 2;
            }
            return sorted[mid];
        },
        findTeam(teamId, teamName) {
            if (!Array.isArray(this.teams) || !this.teams.length) return null;
            const normalizedId = teamId != null ? String(teamId) : null;
            if (normalizedId) {
                const byId = this.teams.find(team => String(team.team_id ?? team.id) === normalizedId);
                if (byId) return byId;
            }
            const normalizedName = teamName ? String(teamName).toLowerCase() : null;
            if (normalizedName) {
                return this.teams.find(team => String(team.name || team.display_name || team.team_name || '').toLowerCase() === normalizedName) || null;
            }
            return null;
        },
        playerTeamLogo(row) {
            if (!row) return null;
            const teamId = row.team_id ?? row.teamId ?? row.teamid;
            const teamName = row.team_name ?? row.teamName ?? row.team;
            const team = this.findTeam(teamId, teamName);
            if (!team) return null;
            const src = team.logo || team.avatar || team.team_logo || team.raw?.avatar || team.raw?.logo;
            if (!src) return null;
            return this.resolveAvatar(src);
        },
        normalizeSankariPlayer(row) {
            if (!row) return null;
            const safe = (value, fallback = 0) => this.toNumber(value, fallback);
            const rounds = safe(row.rounds_played ?? row.rounds);
            const kills = safe(row.kills);
            const deaths = safe(row.deaths);
            const teamLogo = this.playerTeamLogo(row) || DIVISION_DEFAULT_TEAM_LOGO;
            return {
                id: row.player_id || row.id,
                playerId: row.player_id || row.id,
                teamId: row.team_id ?? row.teamId ?? row.teamid,
                nickname: row.nickname || row.name,
                teamName: row.team_name || row.teamName || '',
                avatar: teamLogo,
                logo: teamLogo,
                maps: safe(row.maps_played ?? row.maps),
                rounds,
                kills,
                deaths,
                assists: safe(row.assists),
                adr: this.toNumber(row.adr, null),
                kr: this.toNumber(row.kr, null),
                kd: this.toNumber(row.kd, null),
                hsPct: this.toNumber(row.hs_pct ?? row.hsPercent ?? row.hs, null),
                mvps: safe(row.mvps),
                pistolKills: safe(row.pistol_kills ?? row.pistolKills),
                sniperKills: safe(row.sniper_kills ?? row.sniperKills),
                knifeKills: safe(row.knife_kills ?? row.knifeKills),
                zeusKills: safe(row.zeus_kills ?? row.zeusKills),
                utilityDamage: safe(row.utility_damage ?? row.utilityDamage),
                enemiesFlashed: safe(row.enemies_flashed ?? row.enemiesFlashed),
                flashCount: safe(row.flash_count ?? row.flashCount),
                flashSuccesses: safe(row.flash_successes ?? row.flashSuccesses),
                clutchKills: safe(row.clutch_kills ?? row.clutchKills),
                cl1v1Attempts: safe(row.cl_1v1_attempts ?? row.clutch1v1Attempts),
                cl1v1Wins: safe(row.cl_1v1_wins ?? row.clutch1v1Wins),
                cl1v2Attempts: safe(row.cl_1v2_attempts ?? row.clutch1v2Attempts),
                cl1v2Wins: safe(row.cl_1v2_wins ?? row.clutch1v2Wins),
                damage: safe(row.damage)
            };
        },
        sankariThreshold(card, players) {
            const minValue = (value) => {
                const numeric = this.toNumber(value, null);
                return numeric != null && numeric > 0 ? numeric : null;
            };
            const roundsList = Array.isArray(players) ? players.map(p => minValue(p.rounds)).filter(v => v != null) : [];
            const mapsList = Array.isArray(players) ? players.map(p => minValue(p.maps)).filter(v => v != null) : [];
            const medianRounds = this.median(roundsList);
            const medianMaps = this.median(mapsList);
            const baseRounds = medianRounds != null ? Math.floor(medianRounds * 0.25) : 0;
            const baseMaps = medianMaps != null ? Math.floor(medianMaps * 0.25) : 0;
            const derivedRounds = Math.max(3, baseRounds);
            const derivedMaps = Math.max(1, baseMaps);
            const minRounds = minValue(card?.minRounds) ?? (derivedRounds > 0 ? derivedRounds : null);
            const minMaps = minValue(card?.minMaps) ?? (derivedMaps > 0 ? derivedMaps : null);
            return {
                minRounds,
                minMaps
            };
        },
        cardThresholdTooltip(card, thresholds) {
            if (!card) return '';
            const limits = thresholds || this.sankariThreshold(card, this.sankariPlayers);
            if (!limits?.minRounds && !limits?.minMaps) {
                return '';
            }
            const parts = [];
            if (limits.minRounds) parts.push(`Vähintään ${limits.minRounds} kierrosta`);
            if (limits.minMaps) parts.push(`Vähintään ${limits.minMaps} karttaa`);
            return parts.join(' / ');
        },
        buildSankariEntries(players, card, thresholds) {
            if (!Array.isArray(players) || !card || !card.metricKey) return [];
            const direction = (card.sortDirection || 'desc').toLowerCase();
            const limits = thresholds || this.sankariThreshold(card, players);
            const sorted = players
                .map(player => {
                    if (limits?.minRounds && (!player.rounds || player.rounds < limits.minRounds)) return null;
                    if (limits?.minMaps && (!player.maps || player.maps < limits.minMaps)) return null;
                    const value = this.sankariMetricValue(player, card.metricKey);
                    if (value === null || value === undefined || Number.isNaN(value)) {
                        return null;
                    }
                    if (card.requirePositive === true && value <= 0) {
                        return null;
                    }
                    return {
                        id: player.id || player.playerId,
                        playerId: player.playerId || player.id || null,
                        nickname: player.nickname || 'Tuntematon',
                        teamName: player.teamName || '',
                        avatar: player.logo || DIVISION_DEFAULT_TEAM_LOGO,
                        logo: player.logo || DIVISION_DEFAULT_TEAM_LOGO,
                        maps: player.maps,
                        rounds: player.rounds,
                        rawValue: value,
                        displayValue: card.metricKey === 'assistSupport'
                            ? `${player.assists ?? 0} A / ${player.kills ?? 0} K`
                            : this.formatSankariValue(value, card.metricKey)
                    };
                })
                .filter(Boolean)
                .sort((a, b) => direction === 'asc' ? a.rawValue - b.rawValue : b.rawValue - a.rawValue);
            const limit = Number(card.maxEntries) || 4;
            return sorted.slice(0, limit);
        },
        buildSankariPlaceholderEntries(card) {
            if (!card || !Array.isArray(card.placeholderNames) || !card.placeholderNames.length) {
                return [];
            }
            const limit = Number(card.maxEntries) || 4;
            const names = this.getPlaceholderNames(card).slice(0, limit);
            return names.map((name, idx) => ({
                id: `${card.id || 'sankari'}-placeholder-${idx}`,
                nickname: name || 'Bot Tuntematon',
                teamName: 'Pappaliiga Botit',
                avatar: DIVISION_DEFAULT_TEAM_LOGO,
                logo: DIVISION_DEFAULT_TEAM_LOGO,
                maps: '–',
                rounds: '–',
                rawValue: null,
                displayValue: '–'
            }));
        },
        mergeSankariEntriesWithPlaceholders(card, entries) {
            const baseEntries = Array.isArray(entries) ? entries : [];
            const limit = Number(card?.maxEntries) || 4;
            if (!card || !Array.isArray(card.placeholderNames) || !card.placeholderNames.length) {
                return baseEntries.slice(0, limit);
            }
            if (baseEntries.length >= limit) {
                return baseEntries.slice(0, limit);
            }
            const missing = limit - baseEntries.length;
            if (missing <= 0) return baseEntries.slice(0, limit);
            const placeholders = this.buildSankariPlaceholderEntries(card).slice(0, missing);
            return [...baseEntries, ...placeholders].slice(0, limit);
        },
        getPlaceholderNames(card) {
            if (!card || !card.id || !Array.isArray(card.placeholderNames)) {
                return [];
            }
            const key = String(card.id);
            const cached = this.sankariPlaceholderOrderCache?.[key];
            if (Array.isArray(cached) && cached.length) {
                return cached;
            }
            const shuffled = [...card.placeholderNames];
            for (let i = shuffled.length - 1; i > 0; i -= 1) {
                const j = Math.floor(Math.random() * (i + 1));
                [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
            }
            this.sankariPlaceholderOrderCache[key] = shuffled;
            return shuffled;
        },
        sankariMetricValue(player, metricKey) {
            if (!player) return null;
            switch (metricKey) {
                case 'kd':
                    return player.deaths ? player.kills / player.deaths : (player.kills || 0);
                case 'adr':
                    return player.adr;
                case 'killsPerRound':
                    if (player.kr != null && !Number.isNaN(player.kr)) return player.kr;
                    return player.rounds ? player.kills / player.rounds : null;
                case 'totalKills':
                    return player.kills;
                case 'pistolKills':
                    return player.pistolKills;
                case 'sniperKills':
                    return player.sniperKills;
                case 'knifeKills':
                    return player.knifeKills;
                case 'zeusKills':
                    return player.zeusKills;
                case 'roundsPlayed':
                    return player.rounds;
                case 'totalDamage':
                    return player.damage;
                case 'deaths':
                    return player.deaths;
                case 'assists':
                    return player.assists;
                case 'hsPercent':
                    return this.percentValue(player.hsPct);
                case 'mvps':
                    return player.mvps;
                case 'clutchKills':
                    return player.clutchKills;
                case 'clutchWinRate': {
                    const attempts = player.cl1v1Attempts + player.cl1v2Attempts;
                    const wins = player.cl1v1Wins + player.cl1v2Wins;
                    if (!attempts) return null;
                    return this.percentValue(wins / attempts);
                }
                case 'flashAssistPercent':
                    if (!player.flashCount) return null;
                    return this.percentValue(player.flashSuccesses / player.flashCount);
                case 'utilityDamage':
                    return player.utilityDamage;
                case 'survivalPercent':
                    if (!player.rounds) return null;
                    return this.percentValue((player.rounds - player.deaths) / player.rounds);
                case 'flashbangsThrown':
                    return player.flashCount;
                case 'enemiesFlashed':
                    return player.enemiesFlashed;
                case 'assistSupport': {
                    const assists = this.toNumber(player.assists, null);
                    const kills = this.toNumber(player.kills, null);
                    if (assists === null || kills === null) return null;
                    return assists - kills;
                }
                default:
                    return null;
            }
        },
        percentValue(value) {
            const numeric = this.toNumber(value, null);
            if (numeric === null) return null;
            if (numeric > 1.01) {
                return numeric / 100;
            }
            return numeric;
        },
        formatSankariValue(value, metricKey) {
            const meta = SANKARI_METRIC_META[metricKey] || {};
            const isPercent = meta.percent === true;
            const numeric = this.toNumber(value, null);
            if (numeric === null) {
                return '–';
            }
            if (isPercent) {
                const pct = this.percentValue(numeric);
                if (pct === null) return '–';
                const valuePct = pct * 100;
                const decimals = typeof meta.decimals === 'number' ? meta.decimals : 1;
                return `${valuePct.toFixed(decimals)} %`;
            }
            const decimals = typeof meta.decimals === 'number' ? meta.decimals : null;
            if (decimals === 0) {
                return Math.round(numeric);
            }
            if (decimals != null) {
                return numeric.toFixed(decimals);
            }
            if (Math.abs(numeric) >= 100) return Math.round(numeric);
            return numeric.toFixed(2);
        },
        teamLogo(team) {
            if (!team) return DIVISION_DEFAULT_TEAM_LOGO;
            const src = team.logo || team.avatar || team.team_logo || team.raw?.avatar || team.raw?.logo;
            return this.resolveAvatar(src);
        },
        ensureActiveTeamChip(items) {
            if (!Array.isArray(items) || !items.length) {
                this.activeTeamChipId = null;
                return;
            }
            const normalized = this.activeTeamChipId != null ? String(this.activeTeamChipId) : null;
            const hasCurrent = normalized && items.some(item => String(item.id) === normalized);
            if (hasCurrent) {
                return;
            }
            this.activeTeamChipId = String(items[0].id);
        },
        getMetricIcon(key) {
            const icons = {
                teams: '👥',
                players: '👤',
                matches: '⚔️',
                maps: '🗺️',
                rounds: '🎯',
                adr: '💥',
                kr: '📈',
                kd: '⚖️',
                kills: '🗡️',
                deaths: '💀',
                flashbangs: '💡',
                hs: '🎯',
                flash_success: '✨',
                survival: '🛡️'
            };
            return icons[key] || '📊';
        },
    },
    template: `
        <div class="division-view">
            <section class="division-hero glass-card" aria-labelledby="division-title">
                <div class="division-hero__grid">
                    <div class="division-hero__identity">
                        <div class="division-hero__identity-copy">
                            <h1 id="division-title" class="title-accent titleUnderlinePage">{{ divisionTitle }}</h1>
                        </div>
                        <div v-if="divisionHeroStats.length" class="division-hero__stats" role="list">
                            <article
                                v-for="stat in divisionHeroStats"
                                :key="stat.key"
                                class="division-hero__stat"
                                :class="'division-hero__stat--' + stat.tone"
                                role="listitem"
                            >
                                <span class="division-hero__stat-label">{{ stat.label }}</span>
                                <strong class="division-hero__stat-value">{{ stat.value }}</strong>
                                <span v-if="stat.meta" class="division-hero__stat-meta">{{ stat.meta }}</span>
                            </article>
                        </div>
                    </div>
                </div>
                <nav class="division-hero__nav" aria-label="Pikalinkit divisioonalle">
                    <a
                        v-for="link in quickLinksVisible"
                        :key="link.id"
                        class="division-hero__nav-link"
                        :href="'#' + link.id"
                        @click.prevent="scrollToSection(link.id)"
                    >
                        {{ link.label }}
                    </a>
                </nav>
            </section>

            <loading-spinner
                v-if="divisionLoading && !divisionDetails"
                message="Divisioonaa ladataan..."
            ></loading-spinner>

            <error-message
                v-else-if="divisionError && !divisionDetails"
                :message="divisionError"
                @retry="refreshAll"
            ></error-message>

            <template v-else>
                <section id="upcoming" class="division-section" v-if="matchSectionLoading || divisionMatchesLoading || hasAnyMatchData || (isPlayoff && !divisionLoading)">
                    <div class="division-surface glass-card division-section-card">
                        <header class="division-section__heading division-section__heading--matches">
                            <div class="division-section__heading-copy">
                                <h2 class="title-accent titleUnderlineSection">Ottelut</h2>
                                <p class="division-section__lede" v-if="isPlayoff">Playoff-bracket</p>
                                <p class="division-section__lede" v-else>Tulevat kohtaamiset ja pelattujen otteluiden tarkempi ottelupaketti samassa näkymässä.</p>
                            </div>
                            <!-- Toggle only shown for non-playoff divisions -->
                            <div v-if="!isPlayoff" class="trend-toggles trend-toggles--mode">
                                <button
                                    type="button"
                                    class="trend-toggle"
                                    :class="{ 'trend-toggle--active': matchViewMode === 'upcoming' }"
                                    @click="setMatchViewMode('upcoming')"
                                >Tulevat</button>
                                <button
                                    type="button"
                                    class="trend-toggle"
                                    :class="{ 'trend-toggle--active': matchViewMode === 'played' }"
                                    @click="setMatchViewMode('played')"
                                >Pelatut</button>
                            </div>
                        </header>

                        <!-- Playoff bracket view -->
                        <template v-if="isPlayoff">
                            <loading-spinner
                                v-if="divisionLoading && !playoffBracket"
                                message="Brackettia ladataan..."
                            ></loading-spinner>
                            <playoff-bracket
                                v-else
                                :bracket="playoffBracket"
                                :map-catalog="mapCatalog"
                                :is-expanded-fn="isPlayedMatchExpanded"
                                :toggle-expand-fn="togglePlayedMatchExpand"
                                :match-summary-fn="playedMatchSummary"
                                :match-details-fn="playedMatchDetails"
                                :match-veto-fn="playedMatchVetoEntry"
                                :match-player-stats-fn="playedMatchPlayerStats"
                                :match-bundle-busy-fn="playedMatchBundleBusy"
                                :resolve-avatar-fn="resolveAvatar"
                                :team-route-fn="divisionTeamRoute"
                                :faceit-url-fn="divisionMatchFaceitUrl"
                                :replay2-links-fn="replay2Links"
                                :replay2-player-url-fn="replay2PlayerUrl"
                                :demo-availability-loading-fn="isDemoAvailabilityLoading"
                            ></playoff-bracket>
                        </template>

                        <!-- Regular division: upcoming/played toggle -->
                        <template v-else>
                        <upcoming-matches-list
                            v-if="matchViewMode === 'upcoming'"
                            :items="upcomingMatches"
                            :loading="upcomingLoading"
                            :error="upcomingError"
                            title="Tulevat ottelut"
                            :show-header="false"
                            :compact="true"
                            empty-message="Ei tulevia otteluita tälle divisioonalle."
                        ></upcoming-matches-list>

                        <div v-else>
                            <loading-spinner
                                v-if="divisionMatchesLoading"
                                message="Pelattuja otteluita ladataan..."
                            ></loading-spinner>
                            <error-message
                                v-else-if="divisionMatchesError"
                                :message="divisionMatchesError"
                                @retry="loadDivisionMatches(championshipId, { force: true })"
                            ></error-message>
                            <div v-else-if="playedMatches.length" class="played-matches-layout">
                                <div class="table-wrapper played-matches-desktop">
                                    <table class="data-table matches-table">
                                        <thead>
                                            <tr>
                                                <th class="match-expand-cell"></th>
                                                <th>Pvm</th>
                                                <th>Ottelu</th>
                                                <th>BO</th>
                                                <th>Tulos</th>
                                                <th>Eräero</th>
                                                <th>Maps</th>
                                                <th>Linkki</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            <template v-for="match in playedMatches" :key="match.match_id || match.matchId">
                                            <tr>
                                                <td class="match-expand-cell">
                                                    <button
                                                        type="button"
                                                        class="expand-button"
                                                        :class="{ 'expand-button--open': isPlayedMatchExpanded(match.match_id || match.matchId) }"
                                                        @click.stop="togglePlayedMatchExpand(match)"
                                                    >
                                                        <span class="chevron">›</span>
                                                    </button>
                                                </td>
                                                <td>{{ formatDivisionMatchDate(match) }}</td>
                                                <td>
                                                    <span class="eraro-lead-wrap">
                                                        <img
                                                            v-if="playedRowTeamAvatar(match, 'team1')"
                                                            :src="resolveAvatar(playedRowTeamAvatar(match, 'team1'))"
                                                            :alt="match.team1_name || match.team1Name || 'Joukkue 1'"
                                                            class="eraro-lead-logo"
                                                            loading="lazy"
                                                            decoding="async"
                                                        />
                                                        <router-link
                                                            v-if="divisionTeamRoute(match.team1_id || match.team1Id)"
                                                            :to="divisionTeamRoute(match.team1_id || match.team1Id)"
                                                            class="eraro-lead-team"
                                                        >{{ match.team1_name || match.team1Name || 'Joukkue 1' }}</router-link>
                                                        <span v-else class="eraro-lead-team">{{ match.team1_name || match.team1Name || 'Joukkue 1' }}</span>
                                                        <span class="cell-muted">vs</span>
                                                        <img
                                                            v-if="playedRowTeamAvatar(match, 'team2')"
                                                            :src="resolveAvatar(playedRowTeamAvatar(match, 'team2'))"
                                                            :alt="match.team2_name || match.team2Name || 'Joukkue 2'"
                                                            class="eraro-lead-logo"
                                                            loading="lazy"
                                                            decoding="async"
                                                        />
                                                        <router-link
                                                            v-if="divisionTeamRoute(match.team2_id || match.team2Id)"
                                                            :to="divisionTeamRoute(match.team2_id || match.team2Id)"
                                                            class="eraro-lead-team"
                                                        >{{ match.team2_name || match.team2Name || 'Joukkue 2' }}</router-link>
                                                        <span v-else class="eraro-lead-team">{{ match.team2_name || match.team2Name || 'Joukkue 2' }}</span>
                                                    </span>
                                                </td>
                                                <td>BO{{ toNumber(match.best_of ?? match.bestOf, 0) || 2 }}</td>
                                                <td>
                                                    <span>{{ toNumber(match.team1_score ?? match.team1Score, 0) }} - {{ toNumber(match.team2_score ?? match.team2Score, 0) }}</span>
                                                    <span v-if="match.is_forfeit" class="cell-muted"> · FF</span>
                                                </td>
                                                <td>
                                                    <span v-if="playedRowRoundDiffLead(match) != null" class="eraro-lead-wrap">
                                                        <img
                                                            v-if="playedRowRoundDiffLead(match).teamAvatar"
                                                            :src="resolveAvatar(playedRowRoundDiffLead(match).teamAvatar)"
                                                            :alt="playedRowRoundDiffLead(match).teamName"
                                                            class="eraro-lead-logo"
                                                            loading="lazy"
                                                            decoding="async"
                                                        />
                                                        <router-link
                                                            v-if="divisionTeamRoute(playedRowRoundDiffLead(match).teamId)"
                                                            :to="divisionTeamRoute(playedRowRoundDiffLead(match).teamId)"
                                                            class="eraro-lead-team"
                                                        >{{ playedRowRoundDiffLead(match).teamName }}</router-link>
                                                        <span v-else class="eraro-lead-team">{{ playedRowRoundDiffLead(match).teamName }}</span>
                                                        <span>: </span>
                                                        <span class="stat-positive">+{{ playedRowRoundDiffLead(match).value }}</span>
                                                    </span>
                                                    <span v-else class="cell-muted">-</span>
                                                </td>
                                                <td>
                                                    <div class="micro-stack" v-if="playedRowMaps(match).length">
                                                        <span v-for="map in playedRowMaps(match)" :key="map.id" class="micro-chip">{{ map.mapName }} {{ map.score1 }}-{{ map.score2 }}</span>
                                                    </div>
                                                    <span v-else class="cell-muted">-</span>
                                                </td>
                                                <td>
                                                    <div class="micro-stack" v-if="divisionMatchFaceitUrl(match) || replay2Links(match).length">
                                                        <a
                                                            v-if="divisionMatchFaceitUrl(match)"
                                                            :href="divisionMatchFaceitUrl(match)"
                                                            target="_blank"
                                                            rel="noopener"
                                                            class="chip chip--link"
                                                        >Faceit</a>
                                                        <a
                                                            v-for="link in replay2Links(match)"
                                                            :key="'replay2d-' + (match.match_id || match.matchId) + '-' + link.mapId"
                                                            :href="replay2PlayerUrl(link.matchId, link.mapId)"
                                                            target="_blank"
                                                            rel="noopener"
                                                            :class="['chip', 'chip--link', ['queued', 'parsing'].includes(link.status) ? 'chip--warn' : '']"
                                                            :title="['queued', 'parsing'].includes(link.status) ? 'Demo käsittelyssä, valmistuu pian.' : ''"
                                                        >2D Demo {{ link.mapId }}</a>
                                                    </div>
                                                    <span v-else-if="isDemoAvailabilityLoading(match)" class="cell-muted">Tarkistetaan…</span>
                                                    <span v-else class="cell-muted">-</span>
                                                </td>
                                            </tr>
                                            <tr v-if="isPlayedMatchExpanded(match.match_id || match.matchId)" class="match-expand-row">
                                                <td :colspan="8">
                                                    <div class="match-expand-content">
                                                        <match-expanded-details
                                                            :summary="playedMatchSummary(match)"
                                                            :details="playedMatchDetails(match)"
                                                            :veto-entry="playedMatchVetoEntry(match)"
                                                            :player-stats="playedMatchPlayerStats(match)"
                                                            :map-catalog="mapCatalog"
                                                            :loading="playedMatchBundleBusy(match.match_id || match.matchId)"
                                                        ></match-expanded-details>
                                                    </div>
                                                </td>
                                            </tr>
                                            </template>
                                        </tbody>
                                    </table>
                                </div>

                                <div class="played-matches-mobile" role="list">
                                    <article
                                        v-for="match in playedMatches"
                                        :key="'mobile-' + (match.match_id || match.matchId)"
                                        class="played-match-card"
                                        role="listitem"
                                    >
                                        <header class="played-match-card__head">
                                            <div class="played-match-card__date">{{ formatDivisionMatchDate(match) }}</div>
                                            <button
                                                type="button"
                                                class="expand-button"
                                                :class="{ 'expand-button--open': isPlayedMatchExpanded(match.match_id || match.matchId) }"
                                                @click.stop="togglePlayedMatchExpand(match)"
                                            >
                                                <span class="chevron">›</span>
                                            </button>
                                        </header>

                                        <div class="played-match-card__teams">
                                            <div class="played-match-card__team">
                                                <img
                                                    v-if="playedRowTeamAvatar(match, 'team1')"
                                                    :src="resolveAvatar(playedRowTeamAvatar(match, 'team1'))"
                                                    :alt="match.team1_name || match.team1Name || 'Joukkue 1'"
                                                    class="played-match-card__logo"
                                                    loading="lazy"
                                                    decoding="async"
                                                />
                                                <router-link
                                                    v-if="divisionTeamRoute(match.team1_id || match.team1Id)"
                                                    :to="divisionTeamRoute(match.team1_id || match.team1Id)"
                                                    class="played-match-card__team-name"
                                                >{{ match.team1_name || match.team1Name || 'Joukkue 1' }}</router-link>
                                                <span v-else class="played-match-card__team-name">{{ match.team1_name || match.team1Name || 'Joukkue 1' }}</span>
                                            </div>
                                            <span class="played-match-card__vs">vs</span>
                                            <div class="played-match-card__team">
                                                <img
                                                    v-if="playedRowTeamAvatar(match, 'team2')"
                                                    :src="resolveAvatar(playedRowTeamAvatar(match, 'team2'))"
                                                    :alt="match.team2_name || match.team2Name || 'Joukkue 2'"
                                                    class="played-match-card__logo"
                                                    loading="lazy"
                                                    decoding="async"
                                                />
                                                <router-link
                                                    v-if="divisionTeamRoute(match.team2_id || match.team2Id)"
                                                    :to="divisionTeamRoute(match.team2_id || match.team2Id)"
                                                    class="played-match-card__team-name"
                                                >{{ match.team2_name || match.team2Name || 'Joukkue 2' }}</router-link>
                                                <span v-else class="played-match-card__team-name">{{ match.team2_name || match.team2Name || 'Joukkue 2' }}</span>
                                            </div>
                                        </div>

                                        <div class="played-match-card__stats">
                                            <span class="played-match-pill">BO{{ toNumber(match.best_of ?? match.bestOf, 0) || 2 }}</span>
                                            <span class="played-match-pill">
                                                {{ toNumber(match.team1_score ?? match.team1Score, 0) }} - {{ toNumber(match.team2_score ?? match.team2Score, 0) }}
                                                <span v-if="match.is_forfeit" class="cell-muted"> · FF</span>
                                            </span>
                                            <span v-if="playedRowRoundDiffLead(match) != null" class="played-match-pill">
                                                {{ playedRowRoundDiffLead(match).teamName }}: +{{ playedRowRoundDiffLead(match).value }}
                                            </span>
                                        </div>

                                        <div class="played-match-card__maps" v-if="playedRowMaps(match).length">
                                            <span v-for="map in playedRowMaps(match)" :key="map.id" class="micro-chip">{{ map.mapName }} {{ map.score1 }}-{{ map.score2 }}</span>
                                        </div>

                                        <div class="played-match-card__actions">
                                            <a
                                                v-if="divisionMatchFaceitUrl(match)"
                                                :href="divisionMatchFaceitUrl(match)"
                                                target="_blank"
                                                rel="noopener"
                                                class="chip chip--link"
                                            >Faceit</a>
                                        </div>

                                        <div v-if="isPlayedMatchExpanded(match.match_id || match.matchId)" class="match-expand-content played-match-card__expanded">
                                            <match-expanded-details
                                                :summary="playedMatchSummary(match)"
                                                :details="playedMatchDetails(match)"
                                                :veto-entry="playedMatchVetoEntry(match)"
                                                :player-stats="playedMatchPlayerStats(match)"
                                                :map-catalog="mapCatalog"
                                                :loading="playedMatchBundleBusy(match.match_id || match.matchId)"
                                            ></match-expanded-details>
                                        </div>
                                    </article>
                                </div>
                            </div>
                            <p v-else class="division-section__empty">Ei pelattuja otteluita tälle divisioonalle.</p>
                        </div>
                        </template><!-- end v-else (regular division) -->
                    </div>
                </section>

                <section id="summary" class="division-section">
                    <div class="division-surface glass-card division-section-card">
                        <header class="division-section__heading">
                            <h2 class="title-accent titleUnderlineSection">Divisioonan tilastot</h2>
                        </header>
                        <div class="summary-card-grid division-summary-grid" role="list">
                            <summary-stat-card
                                v-for="metric in divisionSummaryMetrics"
                                :key="metric.key"
                                :icon="metric.icon"
                                :label="metric.label"
                                :value="metric.value"
                                :subtitle="metric.subtitle || ''"
                            ></summary-stat-card>
                        </div>
                    </div>
                </section>

                <section id="standings" class="division-section division-section--stacked">
                    <div class="division-team-module">
                        <header class="division-section__heading division-section__heading--standings">
                            <h2 class="title-accent titleUnderlineSection">Joukkuevertailu</h2>
                        </header>
                        <div class="division-team-panels">
                            <team-comparison-board
                                class="division-team-panel division-team-panel--table"
                                :teams="teams"
                                :loading="standingsLoading"
                                :error="standingsError"
                                :title="'Joukkuevertailu'"
                                :subtitle="'Klikkaa joukkueen nimeä avataksesi joukkuesivun.'"
                                :show-header="false"
                                :show-rank="false"
                                :sticky-header="true"
                                :highlight-team-id="activeTeamChipId"
                                :championship-id="championshipId"
                                :championship-name="divisionDetails?.name"
                                :championship-season="divisionDetails?.season"
                                :is-playoff="isPlayoff"
                                :bracket="playoffBracket"
                            ></team-comparison-board>
                        </div>
                    </div>
                </section>

                <section v-if="divisionDetails" id="players" class="division-section">
                    <div class="division-surface glass-card division-section-card">
                        <header class="division-section__heading">
                            <h2 class="title-accent titleUnderlineSection">Pelaajatilastot</h2>
                            <p class="division-section__lede">Valitse joukkueet ja pelaajat sekä haluamasi tilastosarakkeet.</p>
                        </header>
                        <division-players-table
                            :players="divisionDetails.player_totals || []"
                        ></division-players-table>
                    </div>
                </section>

                <section id="maps" class="division-section">
                    <div class="division-surface glass-card division-section-card">
                        <header class="division-section__heading">
                            <h2 class="title-accent titleUnderlineSection">Karttatilastot</h2>
                            <p class="division-section__lede">Karttamäärät, bannit ja karttakohtainen suoritus divisioonan tasolla.</p>
                        </header>
                        <loading-spinner v-if="mapsLoading" message="Karttatilastoja ladataan..."></loading-spinner>
                        <error-message v-else-if="mapsError" :message="mapsError"></error-message>
                        <shared-map-performance-table
                            v-else
                            :map-stats="mapStats"
                            :map-catalog="mapCatalog"
                            title="Karttatilastot"
                            subtitle-summary="Yhteenveto: karttamaarat, bannit ja suorituskyky"
                            subtitle-full="Laaja: Karttakohtaiset pelaajatilastot"
                            :show-panel-container="true"
                            variant="division"
                        ></shared-map-performance-table>
                    </div>
                </section>

                <section id="heroes" class="division-section division-section--heroes">
                    <div class="division-surface glass-card division-section-card division-section-card--heroes">
                        <header class="division-section__heading">
                            <h2 class="title-accent titleUnderlineSection">Divarin Sankarit</h2>
                            <p class="division-section__lede">Kuka dominoi damagea, clutch-hetkiä, utilitya ja puhtaita fragilukuja juuri tässä divisioonassa.</p>
                        </header>
                        <loading-spinner
                            v-if="sankariLoading && !hasSankariGroups"
                            message="Sankareita kootaan..."
                        ></loading-spinner>
                        <p v-else-if="!hasSankariGroups" class="division-section__empty">Sankaritilastoja ei löytynyt tälle divisioonalle.</p>
                        <div v-else class="sankari-groups">
                            <div v-for="group in sankariGroups" :key="group.id" class="sankari-group">
                                <div class="sankari-group__label">
                                    <h3 class="sankari-group__title title-accent titleUnderlineCard">{{ group.groupTitle }}</h3>
                                </div>
                                <div class="sankari-group__cards">
                                    <sankari-card
                                        v-for="card in group.cards"
                                        :key="card.id"
                                        :title="card.title"
                                        :description="card.description"
                                        :tooltip="card.tooltip"
                                        :entries="card.entries"
                                    ></sankari-card>
                                </div>
                            </div>
                        </div>
                    </div>
                </section>
            </template>
        </div>
    `
};
