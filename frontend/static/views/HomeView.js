const GLOBAL_METRIC_SCHEMA = [
    {
        id: 'matches',
        label: 'Matches',
        key: [
            'aggregates.matches_played_total',
            'aggregates.total_matches',
            'matches_played_total',
            'matches_played',
            'matches',
            'total_matches',
            'totalMatches',
            'matchesPlayedTotal'
        ],
        digits: 0
    },
    {
        id: 'divisions',
        label: 'Divisions',
        key: ['aggregates.total_divisions', 'total_divisions', 'totalDivisions'],
        digits: 0
    },
    {
        id: 'teams',
        label: 'Teams',
        key: ['aggregates.total_teams', 'team_count', 'teams', 'total_teams', 'totalTeams'],
        digits: 0
    },
    {
        id: 'players',
        label: 'Players',
        key: ['aggregates.total_players', 'player_count', 'players', 'total_players', 'totalPlayers'],
        digits: 0
    },
    {
        id: 'maps',
        label: 'Maps',
        key: [
            'aggregates.total_maps_played',
            'total_maps_played',
            'maps_played_total',
            'totalMapsPlayed',
            'mapsPlayedTotal'
        ],
        digits: 0
    },
    {
        id: 'kills',
        label: 'Kills',
        key: ['aggregates.total_kills', 'kills_total', 'total_kills', 'totalKills'],
        digits: 0
    },
    {
        id: 'deaths',
        label: 'Deaths',
        key: ['aggregates.total_deaths', 'deaths_total', 'total_deaths', 'totalDeaths'],
        digits: 0
    },
    {
        id: 'rounds',
        label: 'Rounds',
        key: [
            'aggregates.rounds_played_total',
            'rounds_played_total',
            'rounds_played',
            'rounds',
            'total_rounds',
            'totalRounds'
        ],
        digits: 0
    }
];

const SEASON_SUMMARY_SCHEMA = [
    {
        id: 'divisions',
        label: 'Divisions',
        digits: 0,
        getter: (stats, context) => {
            if (stats && Object.prototype.hasOwnProperty.call(stats, '__divisionCount')) {
                return stats.__divisionCount;
            }
            return context?.divisionCount ?? 0;
        }
    },
    {
        id: 'teams',
        label: 'Teams',
        digits: 0,
        key: ['aggregates.total_teams', 'team_count', 'teams', 'teams_count', 'total_teams']
    },
    {
        id: 'players',
        label: 'Players',
        digits: 0,
        key: ['aggregates.total_players', 'player_count', 'players', 'total_players']
    },
    {
        id: 'matches',
        label: 'Matches',
        digits: 0,
        key: ['aggregates.matches_played_total', 'matches_played_total', 'matches_played', 'matches_total', 'matches']
    },
    {
        id: 'rounds',
        label: 'Rounds',
        digits: 0,
        key: ['aggregates.rounds_played_total', 'rounds_played_total', 'rounds_played', 'rounds']
    },
    {
        id: 'kills',
        label: 'Kills',
        digits: 0,
        key: ['aggregates.total_kills', 'kills_total', 'kills', 'total_kills']
    },
    {
        id: 'deaths',
        label: 'Deaths',
        digits: 0,
        key: ['aggregates.total_deaths', 'deaths_total', 'deaths', 'total_deaths']
    }
];

function toNumber(value, fallback = 0) {
    if (value === null || value === undefined) {
        return fallback;
    }
    const direct = Number(value);
    if (Number.isFinite(direct)) {
        return direct;
    }
    const parsed = Number(String(value).replace(',', '.'));
    return Number.isFinite(parsed) ? parsed : fallback;
}

function pickValue(obj, candidates) {
    if (!obj) return undefined;
    const paths = Array.isArray(candidates) ? candidates : [candidates];
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
        return '0';
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
            numeric = numeric * 100;
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

function buildMetricCards(source, schema, context) {
    if (!source || !schema) {
        return [];
    }
    return schema.map(definition => {
        const rawValue =
            typeof definition.getter === 'function'
                ? definition.getter(source, context)
                : pickValue(source, definition.key);
        return {
            key: definition.id,
            label: definition.label,
            value: formatMetric(rawValue, definition)
        };
    });
}

function sortSeasonsDescending(seasons = []) {
    if (!Array.isArray(seasons)) {
        return [];
    }
    return [...seasons].sort((a, b) => {
        const aId = Number.isFinite(a?.id) ? a.id : Number.NEGATIVE_INFINITY;
        const bId = Number.isFinite(b?.id) ? b.id : Number.NEGATIVE_INFINITY;
        if (aId !== bId) {
            return bId - aId;
        }
        const aNum = Number.isFinite(a?.seasonNumber) ? a.seasonNumber : Number.NEGATIVE_INFINITY;
        const bNum = Number.isFinite(b?.seasonNumber) ? b.seasonNumber : Number.NEGATIVE_INFINITY;
        if (aNum !== bNum) {
            return bNum - aNum;
        }
        const aLabel = a?.label || '';
        const bLabel = b?.label || '';
        return aLabel.localeCompare(bLabel, 'fi');
    });
}

function emptySeasonState() {
    return {
        loading: false,
        error: null,
        stats: {},
        progress: {
            overall: { played: 0, total: 0, percent: 0 },
            regular: { played: 0, total: 0, percent: 0 },
            playoffs: { played: 0, total: 0, percent: 0 }
        },
        divisions: []
    };
}

window.HomeView = {
    name: 'HomeView',
    components: {
        get LoadingSpinner() {
            return window.LoadingSpinner;
        },
        get ErrorMessage() {
            return window.ErrorMessage;
        },
        get HeroBanner() {
            return window.HeroBanner;
        },
        get StatPanel() {
            return window.StatPanel;
        },
        get SeasonToggle() {
            return window.SeasonToggle;
        },
        get ProgressBar() {
            return window.ProgressBar;
        },
        get DivisionCardList() {
            return window.DivisionCardList;
        }
    },
    data() {
        const seasonsStore = typeof window.useSeasonsStore === 'function' ? window.useSeasonsStore() : null;
        const homeStore = typeof window.useHomeStore === 'function' ? window.useHomeStore() : null;
        return {
            seasonsStore,
            homeStore,
            divisionFilter: 'all',
            divisionSearch: ''
        };
    },
    computed: {
        heroTitle() {
            return 'AFI × Pappaliiga Stats Hub';
        },
        heroSubtitle() {
            return 'Nopea näkymä Pappaliigan kausien divisiooniin, tuloksiin ja seuraaviin askeliin.';
        },
        // heroEyebrow() {
        //     return 'AFI · Faceit API DATA';
        // },
        partnerCallouts() {
            return [
                {
                    id: 'armafi',
                    // eyebrow: 'Epävirallisen statsisivuston ylläpitäjä',
                    name: 'Armafinland',
                    description: 'Yhteisö on avoin kaikille pelaajille ja ryhmille, jotka haluavat kokeilla taktista pelaamista myös Arma-sarjan peleissä. Pelaamme Arma 3 ja Arma Reforger, sekä järjestämme kansainvälisiä TvT-tehtäviä, joissa painotetaan realismia, joukkuepeliä ja yhteistoimintaa. Pelien ulkopuolella meno on rentoa ja mutkatonta, mutta pelissä otetaan tehtävät tosissaan. ',
                    primaryLabel: 'Liity AFI Discord',
                    primaryHref: 'https://discord.gg/armafinland',
                    secondaryLabel: 'Tutustu sivustoon',
                    secondaryHref: 'https://armafinland.fi',
                    logo: 'https://armafinland.fi/logot/images/armafin-logo-400px.png'
                },
                {
                    id: 'pappaliiga',
                    // eyebrow: 'Liiga',
                    name: 'Pappaliiga',
                    description: 'Pappaliigan tarkoituksena on tarjota varttuneemmalle väelle mahdollisuus kilpapelaamiseen; tosissaan ja `ei niin tosissaan`. ',
                    primaryLabel: 'Liity Pappaliiga Discord',
                    primaryHref: 'https://discord.gg/pappaliiga',
                    secondaryLabel: 'Lue lisää',
                    secondaryHref: 'https://pappaliiga.fi',
                    logo: 'https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png'
                }
            ];
        },
        summaryLoading() {
            return this.homeStore?.summaryLoading ?? false;
        },
        summaryError() {
            return this.homeStore?.summaryError ?? null;
        },
        globalSummaryMetrics() {
            const aggregates = this.homeStore?.lifetimeSummary?.aggregates || {};
            return buildMetricCards(aggregates, GLOBAL_METRIC_SCHEMA);
        },
        globalSummaryEyebrow() {
            return 'All Seasons Overview';
        },

       seasonsLoading() {
            return this.seasonsStore?.loading ?? false;
        },
        seasonsError() {
            return this.seasonsStore?.error ?? null;
        },
        sortedSeasons() {
            const source =
                (Array.isArray(this.seasonsStore?.sortedSeasons) && this.seasonsStore.sortedSeasons.length
                    ? this.seasonsStore.sortedSeasons
                    : this.seasonsStore?.seasons) || [];
            return sortSeasonsDescending(source);
        },
        seasonSelectGroups() {
            if (!this.sortedSeasons.length) {
                return [];
            }
            return [
                {
                    id: 'all',
                    label: 'All Seasons',
                    options: this.sortedSeasons
                }
            ];
        },
        selectedSeasonKey() {
            return this.seasonsStore?.selectedSeasonKey ?? null;
        },
        selectedSeason() {
            if (!this.selectedSeasonKey || !this.seasonsStore) {
                return null;
            }
            return this.seasonsStore.getSeasonByKey(this.selectedSeasonKey);
        },
        seasonState() {
            if (!this.selectedSeasonKey || !this.homeStore) {
                return emptySeasonState();
            }
            const getter = this.homeStore.getSeasonState;
            if (typeof getter === 'function') {
                return getter(this.selectedSeasonKey) || emptySeasonState();
            }
            return this.homeStore.seasonCache?.[this.selectedSeasonKey] || emptySeasonState();
        },
        seasonLoading() {
            return this.seasonState.loading;
        },
        seasonError() {
            return this.seasonState.error;
        },
        seasonSummaryMetrics() {
            const stats = this.seasonState.stats || {};
            const metrics = buildMetricCards(stats, SEASON_SUMMARY_SCHEMA, this);
            const divisionValue = formatMetric(this.divisionCount, { digits: 0 });
            return metrics.map(metric => {
                if (metric.key === 'divisions') {
                    return { ...metric, value: divisionValue };
                }
                return metric;
            });
        },
        seasonDivisions() {
            const list = Array.isArray(this.seasonState.divisions) ? this.seasonState.divisions : [];
            if (typeof window !== 'undefined' && window.console && window.console.info) {
                console.info('[HomeView] seasonDivisions resolved', {
                    count: list.length,
                    hasSelectedSeason: Boolean(this.selectedSeasonKey),
                    loading: this.seasonState.loading,
                    cached: this.seasonState.usingCache
                });
            }
            return list;
        },
        seasonTitle() {
            const season = this.selectedSeason;
            if (!season) {
                return 'Valitse kausi';
            }
            return season.label || `Kausi ${season.seasonNumber ?? ''}`.trim();
        },
        seasonSubtitle() {
            const season = this.selectedSeason;
            if (!season) {
                return '';
            }
            const segments = [];
            if (season.seasonNumber) {
                segments.push(`Kausi ${season.seasonNumber}`);
            }
            if (season.phase) {
                segments.push(season.phase);
            }
            if (season.isCurrent) {
                segments.push('Käynnissä');
            }
            return segments.join(' · ');
        },
        seasonSummaryHeading() {
            const season = this.selectedSeason;
            if (!season) {
                return 'Season Summary';
            }
            if (season.seasonNumber) {
                return `Season ${season.seasonNumber} Summary`;
            }
            return `${season.label || 'Season'} Summary`;
        },
        seasonSummaryMeta() {
            if (!this.selectedSeason) {
                return 'Valitse kausi nähdäksesi kausikohtaiset luvut.';
            }
            const stats = this.seasonState.stats || {};
            const teams = formatMetric(pickValue(stats, ['aggregates.total_teams', 'team_count', 'teams']), { digits: 0 });
            const players = formatMetric(
                pickValue(stats, ['aggregates.total_players', 'player_count', 'players']),
                { digits: 0 }
            );
            return `Teams: ${teams} · Players: ${players}`;
        },
        seasonProgressSummary() {
            const stats = this.seasonState?.stats || {};
            const percentFromStats = toNumber(
                pickValue(stats, ['progress.finished_percent', 'finished_percent']),
                null
            );
            const overall = this.seasonState?.progress?.overall || {};
            const played = toNumber(overall.played, 0);
            const total = toNumber(overall.total, 0);
            const percent = total > 0 ? Math.min(100, Math.round((played / total) * 100)) : 0;
            return {
                played,
                total,
                percent: percentFromStats != null && percentFromStats > 0 ? percentFromStats : percent
            };
        },
        seasonProgressLabel() {
            const { played, total, percent } = this.seasonProgressSummary;
            if (!total) {
                return '';
            }
            return `Matches: ${played}/${total} · ${percent}%`;
        },
        hasSeasonProgress() {
            return this.seasonProgressSummary.total > 0;
        },
        divisionCount() {
            const stats = this.seasonState?.stats || {};
            const summaryCount = toNumber(
                pickValue(stats, [
                    'progress.divisions_total',
                    'aggregates.divisions_total',
                    'divisions_total'
                ]),
                0
            );
            if (summaryCount > 0) {
                return summaryCount;
            }
            return this.seasonDivisions.length;
        },
        divisionProgressPercent() {
            const percent = this.seasonState?.progress?.overall?.percent ?? 0;
            return Number.isFinite(percent) ? percent : 0;
        },
        divisionOfflineMessage() {
            if (this.seasonState.bannerMessage) {
                return this.seasonState.bannerMessage;
            }
            if (!this.seasonState.offline) return '';
            const timestamp = this.seasonState.cacheTimestamp;
            let formatted = 'unknown time';
            if (timestamp) {
                try {
                    formatted = new Date(timestamp).toLocaleString('fi-FI', {
                        hour: '2-digit',
                        minute: '2-digit',
                        day: 'numeric',
                        month: 'numeric',
                        year: 'numeric'
                    });
                } catch (error) {
                    formatted = new Date(timestamp).toISOString();
                }
            }
            return `Offline: showing cached data (${formatted}). Some values may be outdated.`;
        },
        divisionDataBadge() {
            return this.seasonState.dataBadge || '';
        },
        divisionWarningMessage() {
            const warnings = this.seasonState.validationWarnings || [];
            if (!warnings.length) return '';
            return 'Some divisions could not be loaded (validation error).';
        },
        divisionHeaderMeta() {
            if (!this.selectedSeason) return '';
            const percent = this.divisionProgressPercent.toFixed(0);
            return `${this.divisionCount} divisioonaa · ${percent}% pelattu`;
        },
        divisionEmptyMessage() {
            if (!this.selectedSeasonKey) {
                return 'Valitse kausi tarkasteltavaksi.';
            }
            if (this.divisionFilter !== 'all' || this.divisionSearch.trim().length > 0) {
                return 'Ei divisioonia valituilla suodattimilla.';
            }
            return 'Tälle kaudelle ei löytynyt divisioonia.';
        },
        divisionFilterOptions() {
            return [
                { id: 'all', label: 'All' },
                { id: 'active', label: 'Active' },
                { id: 'finished', label: 'Finished' },
                { id: 'waiting', label: 'Waiting' }
            ];
        }
    },
    async mounted() {
        await this.bootstrap();
    },
    watch: {
        '$route.query.season': {
            handler(newValue, oldValue) {
                if (newValue === oldValue) {
                    return;
                }
                if (newValue == null) {
                    if (this.selectedSeasonKey) {
                        this.syncRouteWithSelectedSeason({ replace: true });
                    } else {
                        this.initializeSeasonSelection({ ensureRoute: true });
                    }
                    return;
                }
                this.syncSeasonFromRoute(newValue, {
                    fallbackToNewest: true,
                    scroll: false,
                    replaceRoute: true
                });
                this.syncRouteWithSelectedSeason({ replace: true });
            }
        }
    },
    methods: {
        async bootstrap() {
            const tasks = [];
            if (this.homeStore) {
                tasks.push(
                    this.homeStore
                        .ensureSummary()
                        .catch(error => {
                            console.warn('[HomeView] ensureSummary failed', error);
                        })
                );
            }
            if (this.seasonsStore) {
                tasks.push(
                    this.seasonsStore
                        .fetchSeasons()
                        .then(() => {
                            const season = this.initializeSeasonSelection({ ensureRoute: true });
                            // Load initial season data
                            if (season) {
                                this.loadSeason(season.key, { apiParam: season.apiParam });
                            }
                        })
                        .catch(error => {
                            console.error('[HomeView] fetchSeasons failed', error);
                        })
                );
            }
            await Promise.allSettled(tasks);
            if (!this.selectedSeasonKey && this.sortedSeasons.length) {
                const season = this.initializeSeasonSelection({ ensureRoute: true });
                if (season) {
                    this.loadSeason(season.key, { apiParam: season.apiParam });
                }
            }
        },
        initializeSeasonSelection(options = {}) {
            if (!this.sortedSeasons.length || !this.seasonsStore) {
                return null;
            }
            const routeSeason = this.$route?.query?.season;
            if (routeSeason != null) {
                const resolved = this.syncSeasonFromRoute(routeSeason, {
                    fallbackToNewest: true,
                    scroll: false,
                    replaceRoute: true
                });
                if (resolved && options.ensureRoute) {
                    this.syncRouteWithSelectedSeason({ replace: true });
                }
                return resolved;
            }

            const existing = this.findSeasonRecord(this.selectedSeasonKey);
            if (existing) {
                if (options.ensureRoute) {
                    this.syncRouteWithSelectedSeason({ replace: true });
                }
                return existing;
            }

            const fallback = this.sortedSeasons[0];
            if (fallback) {
                this.seasonsStore.selectSeason(fallback.key);
                if (options.ensureRoute) {
                    this.syncRouteWithSelectedSeason({ replace: true });
                }
                return fallback;
            }
            return null;
        },
        matchSeasonByParam(value) {
            if (value === undefined || value === null) {
                return null;
            }
            const target = String(value);
            const numeric = Number(value);
            return (
                this.sortedSeasons.find(season => {
                    if (!season) return false;
                    if (String(season.key) === target) return true;
                    if (season.apiParam != null && String(season.apiParam) === target) return true;
                    if (Number.isFinite(numeric)) {
                        if (Number.isFinite(season.id) && season.id === numeric) return true;
                        if (Number.isFinite(season.seasonNumber) && season.seasonNumber === numeric) return true;
                    }
                    return false;
                }) || null
            );
        },
        findSeasonRecord(identifier) {
            if (identifier && typeof identifier === 'object') {
                return identifier;
            }
            return this.matchSeasonByParam(identifier);
        },
        syncSeasonFromRoute(param, options = {}) {
            if (!this.sortedSeasons.length || !this.seasonsStore) {
                return null;
            }
            const matched = this.matchSeasonByParam(param);
            let targetSeason = matched;
            if (!targetSeason && options.fallbackToNewest) {
                targetSeason = this.sortedSeasons[0] || null;
            }
            if (targetSeason && targetSeason.key !== this.selectedSeasonKey) {
                this.seasonsStore.selectSeason(targetSeason.key);
                // Load season data when syncing from route
                this.loadSeason(targetSeason.key, { apiParam: targetSeason.apiParam });
                // Only scroll if explicitly requested (not on initial load)
                if (options.scroll) {
                    this.scrollToSeasonSummary();
                }
            }
            if (!matched && targetSeason && options.replaceRoute) {
                this.syncRouteWithSelectedSeason({ replace: true });
            }
            return targetSeason;
        },
        syncRouteWithSelectedSeason(options = {}) {
            if (!this.$router || !this.selectedSeason) {
                return;
            }
            const season = this.selectedSeason;
            const targetId = season.id ?? season.seasonNumber ?? season.key;
            const normalized = targetId != null ? String(targetId) : null;
            const current = this.$route?.query?.season ?? null;
            if (normalized === (current != null ? String(current) : null)) {
                return;
            }
            const nextQuery = { ...(this.$route?.query || {}) };
            if (normalized) {
                nextQuery.season = normalized;
            } else {
                delete nextQuery.season;
            }
            const method = options.replace ? 'replace' : 'push';
            this.$router[method]({
                query: nextQuery,
                hash: this.$route?.hash || undefined
            }).catch(() => {});
        },
        handleSeasonSelect(value) {
            const season = this.findSeasonRecord(value);
            if (!season) {
                return;
            }

            // If same season selected, just scroll to it (user wants to see it again)
            if (season.key === this.selectedSeasonKey) {
                this.scrollToSeasonSummary();
                return;
            }

            console.info('[HomeView] handleSeasonSelect', { selected: season.key });

            // Update store selection (this triggers data load in bootstrap if needed)
            this.seasonsStore?.selectSeason(season.key);

            // Load season data immediately without waiting for watchers
            this.loadSeason(season.key, { apiParam: season?.apiParam });

            // Update route silently (for sharing/refresh) without triggering navigation
            this.syncRouteWithSelectedSeason({ replace: true });

            // Scroll to season summary section
            this.scrollToSeasonSummary();
        },
        async loadSeason(key, options = {}) {
            if (!key || !this.homeStore) {
                return;
            }
            const season = this.seasonsStore?.getSeasonByKey(key);
            const apiParam = options.apiParam ?? season?.apiParam ?? key;
            try {
                const payload = await this.homeStore.fetchSeason(key, {
                    apiParam,
                    force: options.force === true
                });
                if (typeof window !== 'undefined' && window.console) {
                    console.info('[HomeView] Season loaded', {
                        seasonKey: key,
                        divisions: payload?.divisions?.length ?? 0,
                        offline: payload?.offline,
                        cacheTimestamp: payload?.cacheTimestamp
                    });
                }
            } catch (error) {
                console.error('Season fetch failed', error);
            }
        },
        retrySeasons() {
            if (!this.seasonsStore) return;
            this.seasonsStore
                .fetchSeasons({ force: true })
                .then(() => {
                    this.initializeSeasonSelection({ ensureRoute: true });
                })
                .catch(error => {
                    console.error('Season list refresh failed', error);
                });
        },
        retrySummary() {
            if (!this.homeStore) return;
            this.homeStore.fetchLifetimeSummary({ force: true }).catch(error => {
                console.error('Summary refresh failed', error);
            });
        },
        retrySeason() {
            if (!this.selectedSeasonKey) return;
            const season = this.selectedSeason;
            if (season) {
                console.info('[HomeView] retrySeason triggered', {
                    key: this.selectedSeasonKey,
                    apiParam: season?.apiParam
                });
            } else {
                console.info('[HomeView] retrySeason triggered without selectedSeason', {
                    key: this.selectedSeasonKey
                });
            }
            this.loadSeason(this.selectedSeasonKey, {
                apiParam: season?.apiParam,
                force: true
            });
        },
        scrollToSeasonSummary() {
            this.$nextTick(() => {
                const target = this.$refs.seasonControls;
                if (!target) return;

                try {
                    const rect = target.getBoundingClientRect();
                    const scrollY = window.scrollY || window.pageYOffset || document.documentElement.scrollTop || 0;
                    const vh = window.innerHeight || document.documentElement.clientHeight || 0;

                    // Check if target is already well-positioned in viewport (with generous threshold)
                    // Target is visible if it's within top 30% of viewport
                    const visibleThreshold = vh * 0.3;
                    const isWellPositioned = rect.top >= 0 && rect.top <= visibleThreshold;

                    if (isWellPositioned) {
                        console.info('[HomeView] scrollToSeasonSummary: already visible, skipping scroll');
                        return;
                    }

                    // Scroll to position target near top of viewport with some padding
                    const targetY = scrollY + rect.top - 80; // 80px padding from top
                    window.scrollTo({
                        top: Math.max(0, targetY),
                        behavior: 'smooth'
                    });
                } catch (error) {
                    console.warn('[HomeView] scrollToSeasonSummary failed', error);
                }
            });
        },
        setDivisionFilter(filter) {
            console.info('[HomeView] setDivisionFilter', filter);
            this.divisionFilter = filter;
        },
        resetDivisionFilters() {
            console.info('[HomeView] resetDivisionFilters');
            this.divisionFilter = 'all';
            this.divisionSearch = '';
        }
    },
    template: `
        <div class="home-view">
            <hero-banner
                :title="heroTitle"
                :subtitle="heroSubtitle"
                :eyebrow="heroEyebrow"
            >
                <template #actions>
                    <button class="btn-primary" type="button" @click="scrollToSeasonSummary">View Current Season</button>
                    <a class="btn-secondary" href="https://discord.gg/pappaliiga" target="_blank" rel="noopener">Join Discord</a>
                </template>
            </hero-banner>

            <section class="home-partners" aria-label="Kumppanikuvaukset">
                <article
                    v-for="callout in partnerCallouts"
                    :key="callout.id"
                    class="partner-callout"
                >
                    <header class="partner-callout__header">
                        <div
                            class="logo-wrap logo-card partner-callout__logo-wrap"
                            :class="callout.id === 'armafi' ? 'logo-card--armafinland' : 'logo-card--pappaliiga'"
                        >
                            <img
                                class="partner-callout__logo"
                                :src="callout.logo"
                                :alt="callout.name + ' logo'"
                                loading="lazy"
                            >
                        </div>
                        <div class="partner-callout__titles">
                            <span class="partner-callout__eyebrow">{{ callout.eyebrow }}</span>
                            <h2>{{ callout.name }}</h2>
                        </div>
                    </header>
                    <p class="partner-callout__body">{{ callout.description }}</p>
                    <footer class="partner-callout__footer">
                        <a :href="callout.primaryHref" class="btn-primary" target="_blank" rel="noopener">
                            {{ callout.primaryLabel }}
                        </a>
                        <a :href="callout.secondaryHref" class="btn-link" target="_blank" rel="noopener">
                            {{ callout.secondaryLabel }}
                        </a>
                    </footer>
                </article>
            </section>

            <section class="stats-section stats-section--global" aria-labelledby="global-summary-heading">
                <header class="section-heading">
                    <span class="section-eyebrow">{{ globalSummaryEyebrow }}</span>
                    <h2 id="global-summary-heading">Global Totals</h2>
                    <p class="section-subtext">Combined performance across every recorded season.</p>
                </header>
                <loading-spinner
                    v-if="summaryLoading"
                    message="Kokonaisstatistiikkaa ladataan..."
                ></loading-spinner>
                <error-message
                    v-else-if="summaryError"
                    :message="summaryError"
                    @retry="retrySummary"
                ></error-message>
                <stat-panel
                    v-else
                    :items="globalSummaryMetrics"
                    :columns="4"
                ></stat-panel>
            </section>

            <section
                class="season-explorer glass-card"
                ref="seasonControls"
                aria-labelledby="season-explorer-heading"
            >
                <header class="season-explorer__intro section-heading">
                    <div>
                        <span class="section-eyebrow">Season Explorer</span>
                        <h2 id="season-explorer-heading">Season Explorer</h2>
                        <p class="section-subtext">Select a season to refresh the summary and division list in one place.</p>
                    </div>
                </header>

                <div class="season-explorer__section season-explorer__selection">
                    <div class="season-explorer__selector">
                        <season-toggle
                            :seasons="sortedSeasons"
                            :model-value="selectedSeasonKey"
                            :loading="seasonsLoading"
                            :error="seasonsError"
                            :show-heading="false"
                            :flat="true"
                            @update:modelValue="handleSeasonSelect"
                            @retry="retrySeasons"
                        ></season-toggle>
                    </div>

                    <div
                        class="season-explorer__summary"
                        aria-live="polite"
                        aria-atomic="true"
                    >
                        <div
                            v-if="seasonLoading"
                            class="season-skeleton"
                            role="status"
                        >
                            <div class="season-skeleton__header"></div>
                            <div class="season-skeleton__grid">
                                <div v-for="n in 5" :key="'skeleton-block-' + n" class="season-skeleton__card"></div>
                            </div>
                        </div>

                        <error-message
                            v-else-if="seasonError"
                            :message="seasonError"
                            @retry="retrySeason"
                        ></error-message>

                        <template v-else-if="selectedSeasonKey">
                            <div class="season-explorer__summary-header">
                                <div>
                                    <span class="section-eyebrow">{{ seasonSubtitle || 'Active Season' }}</span>
                                    <h3
                                        id="season-summary-heading"
                                        aria-live="polite"
                                        aria-atomic="true"
                                    >
                                        {{ seasonSummaryHeading }}
                                    </h3>
                                </div>
                            </div>
                            <div
                                class="season-explorer__summary-grid"
                                :class="{ 'season-explorer__summary-grid--single': !hasSeasonProgress }"
                            >
                                <div class="season-explorer__metrics" role="list">
                                    <div
                                        v-for="metric in seasonSummaryMetrics"
                                        :key="metric.key"
                                        class="season-explorer__metric"
                                        role="listitem"
                                    >
                                        <span class="season-explorer__metric-label">{{ metric.label }}</span>
                                        <span class="season-explorer__metric-value">{{ metric.value }}</span>
                                    </div>
                                </div>
                                <div
                                    v-if="hasSeasonProgress"
                                    class="season-progress-card season-explorer__progress"
                                >
                                    <div class="season-progress-card__meta">
                                        <span class="season-progress-card__label">{{ seasonProgressLabel }}</span>
                                        <span class="season-progress-card__value">{{ seasonProgressSummary.played }} / {{ seasonProgressSummary.total }}</span>
                                    </div>
                                    <progress-bar
                                        :value="seasonProgressSummary.played"
                                        :max="seasonProgressSummary.total"
                                        color="accent"
                                        height="18px"
                                        :show-percentage="true"
                                    ></progress-bar>
                                    <p class="season-progress-card__hint">Shows how many matches have been played versus scheduled this season.</p>
                                </div>
                            </div>
                        </template>
                        <div
                            v-else
                            class="season-empty-state"
                            role="status"
                            aria-live="polite"
                        >
                            <p>Valitse kausi yläpuolisesta selectorista tai odota, että kausitiedot latautuvat.</p>
                            <button class="btn-primary" type="button" @click="retrySeason">Retry now</button>
                        </div>
                    </div>
                </div>

                <div
                    class="season-explorer__section season-explorer__filters"
                    :class="{ 'season-explorer__filters--disabled': !selectedSeasonKey }"
                >
                    <div class="season-explorer__filters-heading">
                        <div>
                            <span class="season-explorer__label">Divisions</span>
                            <p class="season-explorer__toolbar-meta">{{ divisionHeaderMeta }}</p>
                        </div>
                        <span class="season-explorer__toolbar-hint">Suodata ja selaa divisioonia nopeasti.</span>
                    </div>
                    <div class="season-explorer__filter-grid">
                        <div class="season-explorer__filter-group">
                            <span class="season-explorer__label">Status</span>
                            <div class="season-explorer__chips">
                                <button
                                    v-for="option in divisionFilterOptions"
                                    :key="option.id"
                                    type="button"
                                    class="season-filter-chip"
                                    :class="{ 'season-filter-chip--active': divisionFilter === option.id }"
                                    :aria-pressed="divisionFilter === option.id"
                                    @click="setDivisionFilter(option.id)"
                                >
                                    {{ option.label }}
                                </button>
                            </div>
                        </div>
                        <label class="season-explorer__search">
                            <span class="season-explorer__label">Search</span>
                            <div class="season-explorer__search-field">
                                <input
                                    type="search"
                                    class="season-explorer__input"
                                    placeholder="Search divisions"
                                    :value="divisionSearch"
                                    @input="divisionSearch = $event.target.value"
                                >
                            </div>
                        </label>
                        <button
                            type="button"
                            class="season-filter-reset"
                            :disabled="divisionFilter === 'all' && !divisionSearch"
                            @click="resetDivisionFilters"
                        >
                            Reset
                        </button>
                    </div>
                </div>

                <div class="season-explorer__section season-explorer__divisions">
                    <division-card-list
                        class="season-explorer__divisions-list"
                        :divisions="seasonDivisions"
                        :season-label="seasonTitle"
                        :season-subtitle="seasonSubtitle"
                        :season-options="seasonSelectGroups"
                        :season-loading="seasonsLoading"
                        :selected-season="selectedSeasonKey"
                        :offline-message="divisionOfflineMessage"
                        :data-badge="divisionDataBadge"
                        :warning-message="divisionWarningMessage"
                        :is-loading="seasonLoading"
                        :empty-message="divisionEmptyMessage"
                        :filter-state="divisionFilter"
                        :search-query="divisionSearch"
                        :show-season-picker="false"
                        :show-controls="false"
                        @change-season="handleSeasonSelect"
                        @change-filter="setDivisionFilter"
                        @change-search="divisionSearch = $event"
                        @reset-filters="resetDivisionFilters"
                    ></division-card-list>
                </div>
            </section>
        </div>
    `
};
