const DIVISION_METRIC_SCHEMA = [
    { id: 'teams', key: ['team_count', 'teams.length', 'aggregates.team_count'], label: 'Joukkueet', digits: 0 },
    { id: 'players', key: ['player_count', 'aggregates.player_count'], label: 'Pelaajat', digits: 0 },
    { id: 'matches', key: ['aggregates.matches_played', 'aggregates.total_matches', 'matches_played'], label: 'Ottelut', digits: 0 },
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
    { key: 'r_per_map', label: 'R/Map', sortable: true, numeric: true, align: 'right', decimals: 2, width: '88px' },
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
                title: 'Eläkeläis-Eagle',
                description: 'Kun rahaa ei ole mutta rifleen, mutta luotto omaan käteen löytyy.<br>Eniten pistoolikillejä.',
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
                description: 'Pelaa enemmän kuin ehtii nukkua. Klassinen "vielä yksi matsi" -mentaliteetti.<br>Eniten kierroksia pelattu.',
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
                description: 'Syöttää frägejä kuin Pappa grillimakkaraa.<br>Eniten assists.',
                metricKey: 'assists',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'parhaat-suonenvedot',
                title: 'Parhaat suonenvedot',
                description: 'Vaimo kyselee koska tuut nukkumaan, mut papalla on aim päällä.<br>Paras HS%.',
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
                description: 'Kerrankin jollain flash osuu muualle kuin omaan tiimiin.<br>Eniten onnistuneita flashbangheittoja.',
                metricKey: 'flashAssistPercent',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'utility',
                title: 'Kranaatit syö tyhjäksi',
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
                description: 'Heittää flashin ennen kuin rundi edes alkaa.<br>Eniten heitettyjä flashbangeja.',
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
        get MapsStats() { return window.MapsStats; },
        get CopyLink() { return window.CopyLink; },
        get SummaryStatCard() { return window.SummaryStatCard; },
        get SankariCard() { return window.SankariCard; }
    },
    data() {
        const divisionStore = typeof window.useDivisionStore === 'function' ? window.useDivisionStore() : null;
        const seasonsStore = typeof window.useSeasonsStore === 'function' ? window.useSeasonsStore() : null;
        return {
            divisionStore,
            seasonsStore,
            mapColumns: DIVISION_MAP_COLUMNS,
            quickLinks: [
                { id: 'summary', label: 'Tilastot' },
                { id: 'standings', label: 'Joukkuavertailu' },
                { id: 'maps', label: 'Karttatilastot' },
                { id: 'heroes', label: 'Sankarit' }
            ],
            activeTeamChipId: null
        };
    },
    computed: {
        championshipParam() {
            return this.$route.params?.championshipId || null;
        },
        championshipId() {
            return this.$route.query?.championship || this.$route.query?.division_id || this.$route.params?.championshipId || null;
        },
        divisionState() {
            if (!this.championshipId || !this.divisionStore) {
                return {
                    details: defaultSegment(),
                    standings: defaultSegment(),
                    maps: defaultSegment(),
                    highlights: defaultSegment()
                };
            }
            return this.divisionStore.getDivisionState(this.championshipId) || {
                details: defaultSegment(),
                standings: defaultSegment(),
                maps: defaultSegment(),
                highlights: defaultSegment()
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
            if (!this.sankariPlayers.length) return [];
            return SANKARI_CARD_GROUPS.map(group => {
                const cards = group.cards
                    .map(card => {
                        const thresholds = this.sankariThreshold(card, this.sankariPlayers);
                        return {
                            ...card,
                            thresholds,
                            entries: this.buildSankariEntries(this.sankariPlayers, card, thresholds),
                            tooltip: this.cardThresholdTooltip(card, thresholds)
                        };
                    })
                    .filter(card => card.entries.length);
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
        highlightsState() {
            return this.divisionState.highlights;
        },
        highlights() {
            return Array.isArray(this.highlightsState.data) ? this.highlightsState.data : [];
        },
        highlightsLoading() {
            return this.highlightsState.loading;
        },
        highlightsError() {
            return this.highlightsState.error;
        },
        divisionTitle() {
            if (!this.divisionDetails) return 'Divisioona';
            return this.divisionDetails.name || `Divisioona ${this.divisionDetails.division_num}`;
        },
        divisionSeasonLabel() {
            if (this.breadcrumbSeason?.label) {
                return this.breadcrumbSeason.label;
            }
            if (this.divisionDetails?.season) {
                return `Kausi ${this.divisionDetails.season}`;
            }
            return null;
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

            aggregates.team_count = aggregates.team_count ?? teams.length ?? details.team_count;
            aggregates.player_count = aggregates.player_count ?? details.player_count ?? players.length;

            if (aggregates.matches_played == null) {
                const matchFromStandings = standings.reduce((max, team) => Math.max(max, Number(team.matches_played ?? team.matches ?? 0)), 0);
                if (matchFromStandings > 0) {
                    aggregates.matches_played = matchFromStandings;
                    aggregates.total_matches = aggregates.total_matches ?? matchFromStandings;
                }
            }
            if (aggregates.maps_played_total == null && maps.length) {
                aggregates.maps_played_total = maps.reduce((sum, entry) => sum + Number(entry.maps_played ?? entry.curr?.maps_played ?? 0), 0);
            }
            if ((aggregates.matches_played == null || aggregates.matches_played === 0) && aggregates.maps_played_total > 0) {
                aggregates.matches_played = aggregates.maps_played_total;
                aggregates.total_matches = aggregates.total_matches ?? aggregates.maps_played_total;
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
        divisionBadgeLabel() {
            if (!this.divisionDetails) return 'Divisioona';
            if (this.divisionDetails.division_num != null) {
                return `${this.divisionDetails.division_num} Divisioona`;
            }
            return this.divisionDetails.name || this.divisionTitle;
        },
        divisionHeaderStats() {
            if (!this.statMetrics.length) return [];
            const lookup = this.statMetrics.reduce((acc, metric) => {
                acc[metric.key] = metric;
                return acc;
            }, {});
            const priority = ['teams', 'matches', 'maps'];
            return priority.map(key => lookup[key]).filter(Boolean);
        },
        divisionStatusLabel() {
            return this.divisionDetails?.status_fi || this.divisionDetails?.status_label || this.divisionDetails?.status || null;
        },
        divisionStatusTone() {
            const label = String(this.divisionStatusLabel || '').toLowerCase();
            if (!label) return 'idle';
            if (label.includes('loppu') || label.includes('valmis')) return 'finished';
            if (label.includes('playoff')) return 'playoff';
            if (label.includes('käynn') || label.includes('kaynn')) return 'active';
            return 'idle';
        },
        divisionSummaryMetrics() {
            if (!this.statMetrics.length) return [];
            return this.statMetrics.map(metric => ({
                ...metric,
                icon: this.getMetricIcon(metric.key)
            }));
        },
        teams() {
            return Array.isArray(this.divisionDetails?.teams) ? this.divisionDetails.teams : [];
        },
        hasTeams() {
            return this.teams.length > 0;
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
        heroCards() {
            if (!Array.isArray(this.highlights) || !this.highlights.length) {
                return [];
            }
            return this.highlights
                .map((highlight, idx) => {
                    const entries = this.normalizeHighlightEntries(highlight);
                    if (!entries.length) {
                        return null;
                    }
                    return {
                        id: highlight.id || `highlight-${idx}`,
                        title: highlight.title || 'Sankari',
                        metric: highlight.metric || '',
                        entries
                    };
                })
                .filter(Boolean);
        },
        hasHeroCards() {
            return this.heroCards.length > 0;
        },
        breadcrumbSeason() {
            if (!this.divisionDetails || !this.seasonsStore) {
                return null;
            }
            const target = this.seasonsStore.sortedSeasons?.find(season => {
                const seasonNumber = season?.seasonNumber ?? Number(season?.raw?.season);
                return seasonNumber && Number(this.divisionDetails.season) === Number(seasonNumber);
            });
            if (!target) {
                return {
                    label: `Kausi ${this.divisionDetails.season}`,
                    key: this.divisionDetails.season
                };
            }
            return target;
        },
        shareUrl() {
            try {
                const resolved = this.$router?.resolve({
                    name: this.$route?.name,
                    params: this.$route?.params,
                    query: this.$route?.query
                }) || {};
                const href = resolved.href || this.$route?.fullPath || window.location.pathname;
                if (href.startsWith('http')) {
                    return href;
                }
                return `${window.location.origin}${href}`;
            } catch (error) {
                return window.location.href;
            }
        }
    },
    watch: {
        championshipId: {
            immediate: true,
            async handler(id) {
                if (!id) return;
                await this.loadDivision(id);
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
        async loadDivision(id, options = {}) {
            if (!id || !this.divisionStore) return;
            const requests = [
                this.divisionStore.fetchDivisionDetails(id, { force: options.force === true }),
                this.divisionStore.fetchDivisionStandings(id, { force: options.force === true }),
                this.divisionStore.fetchDivisionMaps(id, { force: options.force === true }),
                this.divisionStore.fetchDivisionHighlights(id, { force: options.force === true })
            ];
            await Promise.allSettled(requests);
        },
        refreshAll() {
            if (!this.championshipId) return;
            this.loadDivision(this.championshipId, { force: true });
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
        sectionLinkTarget(link) {
            return `#${link.id}`;
        },
        teamRoute(team) {
            if (!team || !team.team_id) return null;
            if (this.championshipId) {
                const divisionName = this.divisionDetails?.name || this.divisionTitle || null;
                const divisionSeason = this.divisionDetails?.season || null;
                const divisionParam = this.championshipParam || this.championshipId;
                return {
                    name: 'team-detail',
                    params: { championshipId: divisionParam, teamId: team.team_id },
                    query: {
                        championship: this.championshipId,
                        ...(divisionName ? { championship_name: divisionName } : {}),
                        ...(divisionSeason != null ? { championship_season: divisionSeason } : {})
                    }
                };
            }
            return { name: 'team', params: { teamId: team.team_id }, query: {} };
        },
        highlightTeamRoute(highlight) {
            if (!highlight?.team) return null;
            return this.teamRoute(highlight.team);
        },
        highlightAvatar(highlight) {
            if (!highlight?.team) return null;
            return this.teamLogo(highlight.team);
        },
        retryHighlights() {
            if (!this.divisionStore || !this.championshipId) return;
            this.divisionStore.fetchDivisionHighlights(this.championshipId, { force: true }).catch(err => {
                console.error('Highlight refresh failed', err);
            });
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
                    return {
                        id: player.id || player.playerId,
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
        handleTeamChipSelect(teamId) {
            if (teamId == null) return;
            const normalized = String(teamId);
            this.activeTeamChipId = normalized;
            this.$nextTick(() => this.scrollTeamRow(normalized, { instant: true }));
        },
        scrollTeamRow(teamId, options = {}) {
            if (teamId == null) return;
            const normalized = String(teamId);
            this.scrollTeamTable(normalized, options);
        },
        scrollTeamTable(teamId, options = {}) {
            const board = this.$refs.teamBoard;
            if (board && typeof board.scrollToTeam === 'function') {
                board.scrollToTeam(teamId, options);
                return true;
            }
            return false;
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
        normalizeHighlightEntries(highlight) {
            if (!highlight) return [];
            const entries = [];
            if (Array.isArray(highlight.players) && highlight.players.length) {
                entries.push(
                    ...highlight.players.slice(0, 3).map(player => this.buildHeroEntry({
                        id: player.id || player.player_id,
                        name: player.nickname || player.name,
                        team: player.team || player.team_name,
                        avatar: player.avatar,
                        value: player.value ?? player.metric ?? player.stat
                    }, highlight.description))
                );
            } else if (Array.isArray(highlight.entries) && highlight.entries.length) {
                entries.push(
                    ...highlight.entries.slice(0, 3).map(entry => this.buildHeroEntry(entry, highlight.description))
                );
            } else if (highlight.team) {
                const teamSource = highlight.team.logo || highlight.team.avatar || highlight.team.raw?.avatar;
                entries.push(this.buildHeroEntry({
                    id: highlight.team.team_id,
                    name: highlight.team.name,
                    team: highlight.team.name,
                    avatar: teamSource,
                    value: highlight.metric
                }, highlight.description));
            }
            return entries.filter(Boolean);
        },
        buildHeroEntry(entry, fallbackLabel = '') {
            if (!entry) return null;
            const name = entry.name || entry.nickname || fallbackLabel || 'Nimetön';
            const team = entry.team || entry.team_name || fallbackLabel || '';
            const avatarSource = entry.avatar || entry.logo || entry.image || null;
            const value = entry.value ?? entry.metric ?? entry.stat ?? '';
            const id = entry.id || entry.player_id || entry.team_id || `${name}-${team}`;
            return {
                id,
                name,
                team,
                value,
                avatar: this.resolveAvatar(avatarSource)
            };
        }
    },
    template: `
        <div class="division-view">
            <section class="division-hero glass-card" aria-labelledby="division-title">
                <div class="division-hero__grid">
                    <div class="division-hero__identity">
                        <div>
                            <h1 id="division-title" class="title-accent titleUnderlinePage">{{ divisionTitle }}</h1>
                        </div>
                    </div>
                </div>
                <nav class="division-hero__nav" aria-label="Pikalinkit divisioonalle">
                    <a
                        v-for="link in quickLinks"
                        :key="link.id"
                        class="division-hero__nav-link"
                        :href="sectionLinkTarget(link)"
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
                <section id="summary" class="division-section">
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
                        ></summary-stat-card>
                    </div>
                </section>

                <section id="standings" class="division-section division-section--stacked">
                    <header class="division-section__heading division-team-heading">
                        <h2 class="title-accent titleUnderlineSection">Joukkuavertailu</h2>
                        <p class="division-section__lede">Klikkaa joukkueen nimeä avataksesi joukkuesivun.</p>
                    </header>
                    <div class="division-team-module">
                        <div class="division-team-panels">
                            <team-comparison-board
                                ref="teamBoard"
                                class="division-team-panel division-team-panel--table"
                                :teams="teams"
                                :loading="standingsLoading"
                                :error="standingsError"
                                :show-header="false"
                                :show-rank="false"
                                :sticky-header="true"
                                :highlight-team-id="activeTeamChipId"
                            ></team-comparison-board>
                        </div>
                    </div>
                </section>

                <section id="maps" class="division-section">
                    <header class="division-section__heading">
                        <h2 class="title-accent titleUnderlineSection">Karttatilastot</h2>
                    </header>
                    <maps-stats
                        class="division-surface glass-card"
                        title="Karttatilastot"
                        :loading="mapsLoading"
                        :error="mapsError"
                        :map-stats="mapStats"
                        :columns="mapColumns"
                        heading-variant="main"
                        :show-header="false"
                        :sticky-header="true"
                    ></maps-stats>
                </section>

                <section id="heroes" class="division-section division-section--heroes">
                    <header class="division-section__heading">
                        <h2 class="title-accent titleUnderlineSection">Divarin Sankarit</h2>
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
                </section>
            </template>
        </div>
    `
};
