const DIVISION_METRIC_SCHEMA = [
    { id: 'teams', key: ['team_count', 'teams.length', 'aggregates.team_count'], label: 'Joukkueet', digits: 0 },
    { id: 'players', key: ['player_count', 'aggregates.player_count'], label: 'Pelaajat', digits: 0 },
    { id: 'matches', key: ['aggregates.matches_played', 'aggregates.total_matches', 'matches_played'], label: 'Ottelut', digits: 0 },
    { id: 'maps', key: ['aggregates.maps_played_total', 'maps_played_total', 'maps_played'], label: 'Kartat', digits: 0 },
    { id: 'adr', key: ['aggregates.median_adr', 'median_adr'], label: 'Median ADR', digits: 1 },
    { id: 'kd', key: ['aggregates.avg_kd', 'aggregates.median_kd'], label: 'Keski K/D', digits: 2 },
    { id: 'survival', key: ['aggregates.median_survival', 'median_survival'], label: 'Selviytyminen', percent: true, digits: 1 }
];

const DIVISION_MAP_COLUMNS = [
    { key: 'map_name', label: 'Kartta', sortable: true, align: 'left', colClass: 'col-name col-map-name', width: '220px' },
    { key: 'maps_played', label: 'Pelattu', sortable: true, numeric: true, align: 'right', width: '90px' },
    { key: 'banned', label: 'Bannit', sortable: true, numeric: true, align: 'right', width: '90px' },
    { key: 'rounds_played', label: 'Rundeja', sortable: true, numeric: true, align: 'right', width: '90px' },
    { key: 'r_per_map', label: 'R/Map', sortable: true, numeric: true, align: 'right', decimals: 2, width: '90px' },
    { key: 'kills', label: 'Killed', sortable: true, numeric: true, align: 'right', width: '90px' },
    { key: 'deaths', label: 'Deaths', sortable: true, numeric: true, align: 'right', width: '90px' },
    { key: 'assists', label: 'Assists', sortable: true, numeric: true, align: 'right', width: '90px' },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, align: 'right', decimals: 1, width: '90px' },
    { key: 'kr', label: 'K/R', sortable: true, numeric: true, align: 'right', decimals: 2, width: '80px' },
    { key: 'udpr', label: 'UDPR', sortable: true, numeric: true, align: 'right', decimals: 2, width: '90px' },
    { key: 'enemy_flash', label: 'Enemy/Flash', sortable: true, numeric: true, align: 'right', decimals: 2, width: '110px' },
    { key: 'k2', label: '2K', sortable: true, numeric: true, align: 'right', width: '70px' },
    { key: 'k3', label: '3K', sortable: true, numeric: true, align: 'right', width: '70px' },
    { key: 'k4', label: '4K', sortable: true, numeric: true, align: 'right', width: '70px' },
    { key: 'ace', label: 'Ace', sortable: true, numeric: true, align: 'right', width: '70px' },
    { key: 'pistol_kills', label: 'Pistol Kills', sortable: true, numeric: true, align: 'right', width: '110px' },
    { key: 'sniper_kills', label: 'Sniper Kills', sortable: true, numeric: true, align: 'right', width: '110px' }
];

const SANKARI_CARD_GROUPS = [
    {
        id: 'attack',
        groupTitle: 'Tappokoneet',
        cards: [
            {
                id: 'nuori-osuja',
                title: '"Nuori" osuja',
                description: 'Näyttää nuorille, että vanha jaksaa vielä painaa. Paras K/D.',
                metricKey: 'kd',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'adr-luvut',
                title: 'ADR-luvut kunnossa',
                description: 'Jokainen luoti osuu... ainakin johonkin. Käsi tärisee, mutta tilasto tukee tätä. Paras ADR.',
                metricKey: 'adr',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'aim-assist',
                title: 'Papalla on aim assist',
                description: 'Vaimo kyselee koska tuut nukkumaan, mut papalla on aim päällä. Paras Kills/Round.',
                metricKey: 'killsPerRound',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'grim-reapers',
                title: 'Viikatemiehet',
                description: 'Voittamattomat pelaajat. Eniten tappoja.',
                metricKey: 'totalKills',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'eagle',
                title: 'Eläkeläis-Eagle',
                description: 'Kun rahaa ei ole mutta rölliä riittää, niin mieltä lohduttaa omaan käteen lyöty Eagle. Eniten pistoolikillejä.',
                metricKey: 'pistolKills',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'bossikielto',
                title: 'Bossikielto peruttu',
                description: 'Zoomaa ja muistelee CSGO-päiviä. Eniten sniper tappoja.',
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
                description: 'Pelaa enemmän kuin ehtii nukkua. Klassinen "vielä yksi matsi" -mentaliteetti. Eniten kierroksia pelattu.',
                metricKey: 'roundsPlayed',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'dps-dino',
                title: 'DPS-dinosaurus',
                description: 'Vaikka ikää kertyy, ei numerot valehtele. Suurin total damage.',
                metricKey: 'totalDamage',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'spektaattori',
                title: 'Spektaattori',
                description: 'Näkee enemmän deathcamia kuin peliä. Eniten kuolemia.',
                metricKey: 'deaths',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'tukipappajoukot',
                title: 'Tukipappajoukot',
                description: 'Syöttää frägejä kuin Pappa grillimakkaraa. Eniten assists.',
                metricKey: 'assists',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'parhaat-suonenvedot',
                title: 'Parhaat suonenvedot',
                description: 'Käsi muistaa sen spray-patterin vieläkin. Paras HS%.',
                metricKey: 'hsPercent',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'pelin-isahahmo',
                title: 'Pelin isähahmo',
                description: 'MVP, koska joku muukin tarvitsee roolimallin. Eniten MVP.',
                metricKey: 'mvps',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'clutch-kills',
                title: 'Vanha pää, kova käsi',
                description: 'Refleksit ei ole kuolleet vielä. Eniten Clutch Kills.',
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
                description: '1v3? Ei ongelmaa – ainakaan jos nitrot ehtii vaikuttaa. Paras Clutch WR%.',
                metricKey: 'clutchWinRate',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'tilastopappa',
                title: 'Tilastopappa',
                description: 'Kaikki numerot kunnossa, vaikka crosshair ei ole. Paras Rating 1.0.',
                metricKey: 'rating',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'taysmaito',
                title: 'Täysmaito',
                description: 'Kerran flash osuu muualle kuin omaan tiimiin. Eniten onnistuneita flashbangheittoja.',
                metricKey: 'flashAssistPercent',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'utility',
                title: 'Kranaatit syö tyhjäksi',
                description: 'Polttaa enemmän kuin 2000-luvun LANit. Eniten utility damage.',
                metricKey: 'utilityDamage',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'mikapahan',
                title: 'Mikä pahan tappaisi',
                description: 'Ei puske – jää odottamaan, säästää eläkkeelle. Klassinen pappa-stratti. Paras Survival%.',
                metricKey: 'survivalPercent',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'valot-paalle',
                title: 'Valot päälle, papat!',
                description: 'Heittää flashin ennen kuin round edes alkaa. Eniten heitettyjä flashbangeja.',
                metricKey: 'flashbangsThrown',
                sortDirection: 'desc',
                maxEntries: 4
            },
            {
                id: 'flash-dance',
                title: 'Flash Bang Dance',
                description: 'Vihu näkee enemmän välähdyksiä kuin diskossa 90-luvulla. Eniten vihollisia sokaistu.',
                metricKey: 'enemiesFlashed',
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
    rating: { decimals: 2 },
    flashAssistPercent: { decimals: 1, percent: true },
    utilityDamage: { decimals: 0 },
    survivalPercent: { decimals: 1, percent: true },
    flashbangsThrown: { decimals: 0 },
    enemiesFlashed: { decimals: 0 }
};

const DEFAULT_TEAM_LOGO = window.PAPPALIIGA_DEFAULT_LOGO;

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
        get TeamNav() { return window.TeamNav; },
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
                { id: 'standings', label: 'Joukkuavertailu' },
                { id: 'summary', label: 'Tilastot' },
                { id: 'maps', label: 'Kartat' },
                { id: 'heroes', label: 'Sankarit' },
                { id: 'teams', label: 'Joukkueet' }
            ],
            activeTeamChipId: null
        };
    },
    computed: {
        championshipId() {
            return this.$route.params?.championshipId || null;
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
                    .map(card => ({
                        ...card,
                        entries: this.buildSankariEntries(this.sankariPlayers, card)
                    }))
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
        divisionSubtitle() {
            if (!this.divisionDetails) return '';
            const pieces = [];
            if (this.divisionDetails.season) {
                pieces.push(`Season ${this.divisionDetails.season}`);
            }
            if (this.divisionDetails.division_num != null) {
                pieces.push(`${this.divisionDetails.division_num} Divisioona`);
            }
            if (this.divisionDetails.is_playoff) {
                pieces.push('Playoffs');
            }
            return pieces.join(' | ');
        },
        statMetrics() {
            if (!this.divisionDetails) {
                return [];
            }
            const source = {
                ...this.divisionDetails.aggregates,
                team_count: Array.isArray(this.divisionDetails.teams) ? this.divisionDetails.teams.length : this.divisionDetails.team_count,
                player_count: this.divisionDetails.player_count
            };
            return buildMetricCards(source, DIVISION_METRIC_SCHEMA);
        },
        divisionBadgeLabel() {
            if (!this.divisionDetails) return 'Divisioona';
            if (this.divisionDetails.division_num != null) {
                return `${this.divisionDetails.division_num} Divisioona`;
            }
            return this.divisionDetails.name || this.divisionTitle;
        },
        divisionSeasonLabel() {
            return this.divisionSubtitle || this.divisionTitle;
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
                        subtitle: highlight.description || '',
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
                el.scrollIntoView({ behavior: 'smooth', block: 'start' });
            } catch (error) {
                window.scrollTo(0, el.offsetTop || 0);
            }
        },
        sectionLinkTarget(link) {
            return `#${link.id}`;
        },
        teamRoute(team) {
            if (!team || !team.team_id) return null;
            if (this.championshipId) {
                return { name: 'team-detail', params: { championshipId: this.championshipId, teamId: team.team_id } };
            }
            return { name: 'team', params: { teamId: team.team_id } };
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
            if (!src) return DEFAULT_TEAM_LOGO;
            try {
                if (window.apiClient && typeof window.apiClient.proxyAvatar === 'function') {
                    const resolved = window.apiClient.proxyAvatar(src);
                    return resolved || DEFAULT_TEAM_LOGO;
                }
                return src || DEFAULT_TEAM_LOGO;
            } catch (error) {
                return src || DEFAULT_TEAM_LOGO;
            }
        },
        toNumber(value, fallback = 0) {
            if (value === null || value === undefined) return fallback;
            const numeric = typeof value === 'number' ? value : Number(String(value).replace(',', '.'));
            if (Number.isFinite(numeric)) return numeric;
            return fallback;
        },
        normalizeSankariPlayer(row) {
            if (!row) return null;
            const safe = (value, fallback = 0) => this.toNumber(value, fallback);
            const rounds = safe(row.rounds_played ?? row.rounds);
            const kills = safe(row.kills);
            const deaths = safe(row.deaths);
            return {
                id: row.player_id || row.id,
                playerId: row.player_id || row.id,
                nickname: row.nickname || row.name,
                teamName: row.team_name || row.teamName || '',
                avatar: this.resolveAvatar(row.avatar || row.photo),
                maps: safe(row.maps_played ?? row.maps),
                rounds,
                kills,
                deaths,
                assists: safe(row.assists),
                adr: this.toNumber(row.adr, null),
                kr: this.toNumber(row.kr, null),
                kd: this.toNumber(row.kd, null),
                rating: this.toNumber(row.rating, null),
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
        buildSankariEntries(players, card) {
            if (!Array.isArray(players) || !card || !card.metricKey) return [];
            const direction = (card.sortDirection || 'desc').toLowerCase();
            const sorted = players
                .map(player => {
                    const value = this.sankariMetricValue(player, card.metricKey);
                    if (value === null || value === undefined || Number.isNaN(value)) {
                        return null;
                    }
                    return {
                        id: player.id || player.playerId,
                        nickname: player.nickname || 'Tuntematon',
                        teamName: player.teamName || '',
                        avatar: player.avatar,
                        maps: player.maps,
                        rounds: player.rounds,
                        rawValue: value,
                        displayValue: this.formatSankariValue(value, card.metricKey)
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
                case 'rating':
                    return player.rating != null && !Number.isNaN(player.rating) ? player.rating : player.kd;
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
            if (!team) return DEFAULT_TEAM_LOGO;
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
                adr: '💥',
                kd: '⚖️',
                survival: '🛡️',
                rounds: '🔄'
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
                        <div class="division-hero__badge">{{ divisionBadgeLabel }}</div>
                        <div>
                            <p class="section-eyebrow">Divisioona</p>
                            <h1 id="division-title" class="title-accent titleUnderlineMain">{{ divisionTitle }}</h1>
                            <p v-if="divisionSubtitle" class="division-hero__subtitle">{{ divisionSubtitle }}</p>
                        </div>
                    </div>
                    <div class="division-hero__meta">
                        <p v-if="divisionSeasonLabel" class="division-hero__season">{{ divisionSeasonLabel }}</p>
                        <div v-if="divisionHeaderStats.length" class="division-hero__stats">
                            <div v-for="stat in divisionHeaderStats" :key="stat.key" class="division-hero__stat">
                                <span class="division-hero__stat-label">{{ stat.label }}</span>
                                <span class="division-hero__stat-value">{{ stat.value }}</span>
                            </div>
                        </div>
                        <span
                            v-if="divisionStatusLabel"
                            class="division-hero__status"
                            :class="'division-hero__status--' + divisionStatusTone"
                        >
                            {{ divisionStatusLabel }}
                        </span>
                        <copy-link
                            v-if="championshipId"
                            :url="shareUrl"
                            label="Kopioi divisioona linkki"
                            variant="primary"
                        ></copy-link>
                    </div>
                </div>
                <nav class="division-hero__nav" aria-label="Pikalinkit divisioonalle">
                    <button
                        v-for="link in quickLinks"
                        :key="link.id"
                        type="button"
                        class="division-hero__nav-link"
                        @click="scrollToSection(link.id)"
                    >
                        {{ link.label }}
                    </button>
                    <router-link
                        v-if="breadcrumbSeason"
                        class="division-hero__nav-link division-hero__nav-link--subtle"
                        :to="{ name: 'seasons' }"
                    >
                        {{ breadcrumbSeason.label || ('Kausi ' + breadcrumbSeason.seasonNumber) }}
                    </router-link>
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
                <section id="standings" class="division-section division-section--stacked">
                    <header class="division-section__heading division-team-heading">
                        <p class="section-eyebrow">Joukkuavertailu</p>
                        <h2 class="title-accent titleUnderlineMain">Joukkuavertailu</h2>
                        <p class="division-section__lede division-section__lede--balanced">Visualisoitu katsaus joukkueiden sijoituksiin, voittoprosentteihin ja suorituskykyyn.</p>
                    </header>
                    <div class="division-team-module">
                        <div class="division-team-panels">
                            <team-comparison-board
                                ref="teamBoard"
                                class="division-team-panel division-team-panel--table"
                                :teams="teams"
                                :loading="standingsLoading"
                                :error="standingsError"
                                title="Joukkuevertailu"
                                subtitle="Kokonaiskuva joukkueiden suorituskyvystä"
                                :sticky-header="true"
                                :highlight-team-id="activeTeamChipId"
                            ></team-comparison-board>
                        </div>
                    </div>
                    <div v-if="teamChipItems.length" class="division-team-chips" role="tablist" aria-label="Joukkuepikalinkit">
                        <button
                            v-for="team in teamChipItems"
                            :key="team.id"
                            type="button"
                            class="season-pill division-chip"
                            :class="{ 'division-chip--active': String(activeTeamChipId) === String(team.id) }"
                            :aria-pressed="String(activeTeamChipId) === String(team.id)"
                            @click="handleTeamChipSelect(team.id)"
                        >
                            <span class="division-chip__rank">#{{ team.rank }}</span>
                            <img
                                v-if="team.logo"
                                :src="team.logo"
                                :alt="team.label + ' logo'"
                                class="division-chip__logo"
                                loading="lazy"
                            >
                            <span class="division-chip__label">{{ team.label }}</span>
                            <span class="division-chip__meta">{{ team.record }}</span>
                            <span class="division-chip__delta" :class="{ positive: team.roundDiff > 0, negative: team.roundDiff < 0 }">
                                {{ team.roundDiff > 0 ? '+' : '' }}{{ team.roundDiff }}
                            </span>
                        </button>
                    </div>
                </section>

                <section id="summary" class="division-section">
                    <header class="division-section__heading">
                        <p class="section-eyebrow">Divisioonan tilastot</p>
                        <h2 class="title-accent titleUnderlineMain">Divisioonan tilastot</h2>
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

                <section id="maps" class="division-section">
                    <header class="division-section__heading">
                        <p class="section-eyebrow">Karttatilastot</p>
                        <h2 class="title-accent titleUnderlineMain">Karttatilastot</h2>
                    </header>
                    <p class="division-section__lede division-section__lede--compact">Kokonaiskuva divisioonan suosituimmista kartoista ja niiden suorituskyvystä.</p>
                    <maps-stats
                        class="division-surface glass-card"
                        title="Karttatilastot"
                        subtitle=""
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
                        <h2 class="title-accent titleUnderlineMain">Divarin Sankarit</h2>
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
                                    :entries="card.entries"
                                ></sankari-card>
                            </div>
                        </div>
                    </div>
                </section>

                <section id="teams" class="division-section">
                    <header class="division-section__heading">
                        <p class="section-eyebrow">Joukkuelista</p>
                        <h2 class="title-accent titleUnderlineMain">Joukkueet</h2>
                    </header>
                    <div v-if="hasTeams" class="glass-card division-surface division-team-list">
                        <team-nav
                            :teams="teams"
                            :championship-id="championshipId"
                        ></team-nav>
                    </div>
                    <p v-else class="division-section__empty">
                        Joukkueita ei löytynyt.
                    </p>
                </section>
            </template>
        </div>
    `
};
