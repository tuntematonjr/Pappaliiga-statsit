const GLOBAL_METRIC_SCHEMA = [
    {
        id: 'matches',
        label: 'Matches',
        key: ['aggregates.matches_played_total', 'aggregates.total_matches', 'matches_played_total', 'matches_played', 'matches'],
        digits: 0
    },
    {
        id: 'teams',
        label: 'Teams',
        key: ['aggregates.total_teams', 'team_count', 'teams', 'total_teams'],
        digits: 0
    },
    {
        id: 'players',
        label: 'Players',
        key: ['aggregates.total_players', 'player_count', 'players', 'total_players'],
        digits: 0
    },
    {
        id: 'kills',
        label: 'Kills',
        key: ['aggregates.total_kills', 'kills_total', 'total_kills'],
        digits: 0
    },
    {
        id: 'deaths',
        label: 'Deaths',
        key: ['aggregates.total_deaths', 'deaths_total', 'total_deaths'],
        digits: 0
    },
    {
        id: 'rounds',
        label: 'Rounds',
        key: ['aggregates.rounds_played_total', 'rounds_played_total', 'rounds_played', 'rounds'],
        digits: 0
    },
    {
        id: 'adr',
        label: 'ADR Avg',
        key: ['aggregates.median_adr', 'aggregates.avg_adr', 'median_adr', 'adr_median'],
        digits: 1
    },
    {
        id: 'winrate',
        label: 'Win Rate',
        key: ['aggregates.win_percent', 'win_percent', 'win_pct', 'wins_percent'],
        percent: true,
        digits: 1
    }
];

const SEASON_SUMMARY_SCHEMA = [
    {
        id: 'divisions',
        label: 'Divisions',
        digits: 0,
        getter: (_, context) => context?.divisionCount ?? 0
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
    },
    {
        id: 'adr',
        label: 'ADR Avg',
        digits: 1,
        key: ['aggregates.median_adr', 'aggregates.avg_adr', 'median_adr', 'adr_median']
    },
    {
        id: 'winrate',
        label: 'Win Rate',
        percent: true,
        digits: 1,
        key: ['aggregates.win_percent', 'win_percent', 'win_pct', 'wins_percent']
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
            return this.seasonsStore?.sortedSeasons ?? [];
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
            return buildMetricCards(this.seasonState.stats || {}, SEASON_SUMMARY_SCHEMA, this);
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
            return `Divisions: ${this.divisionCount} · Teams: ${teams} · Players: ${players}`;
        },
        seasonProgressSummary() {
            const overall = this.seasonState?.progress?.overall || {};
            const played = toNumber(overall.played, 0);
            const total = toNumber(overall.total, 0);
            const percent = total > 0 ? Math.min(100, Math.round((played / total) * 100)) : 0;
            return {
                played,
                total,
                percent
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
        }
    },
    async mounted() {
        await this.bootstrap();
    },
    watch: {
        selectedSeasonKey: {
            immediate: true,
            handler(newKey, oldKey) {
                if (typeof window !== 'undefined' && window.console) {
                    console.info('[HomeView] selectedSeasonKey changed', { newKey, oldKey });
                }
                if (!newKey || newKey === oldKey) {
                    return;
                }
                const season = this.selectedSeason;
                this.loadSeason(newKey, { apiParam: season?.apiParam });
            }
        },
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
                        .then(() => this.initializeSeasonSelection({ ensureRoute: true }))
                        .catch(error => {
                            console.error('[HomeView] fetchSeasons failed', error);
                        })
                );
            }
            await Promise.allSettled(tasks);
            if (!this.selectedSeasonKey && this.sortedSeasons.length) {
                this.initializeSeasonSelection({ ensureRoute: true });
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
            if (!season || season.key === this.selectedSeasonKey) {
                if (season) {
                    this.scrollToSeasonSummary();
                }
                return;
            }
            this.seasonsStore?.selectSeason(season.key);
            this.syncRouteWithSelectedSeason({ replace: false });
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
            const target = this.$refs.seasonControls;
            if (!target) return;
            try {
                target.scrollIntoView({ behavior: 'smooth', block: 'start' });
            } catch (error) {
                window.scrollTo(0, target.offsetTop || 0);
            }
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
                    <router-link to="/seasons" class="btn-link">All Seasons</router-link>
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

            <section class="stats-section stats-section--selector" aria-labelledby="season-selector-heading">
                <header class="section-heading">
                    <span class="section-eyebrow">Season Selector</span>
                    <h2 id="season-selector-heading">Choose the season to explore</h2>
                    <p class="section-subtext">Newest season is selected automatically. Tap a pill to refresh the summary and divisions.</p>
                </header>
                <season-toggle
                    :seasons="sortedSeasons"
                    :model-value="selectedSeasonKey"
                    :loading="seasonsLoading"
                    :error="seasonsError"
                    @update:modelValue="handleSeasonSelect"
                    @retry="retrySeasons"
                ></season-toggle>
            </section>

            <section
                class="stats-section stats-section--season"
                ref="seasonControls"
                aria-labelledby="season-summary-heading"
            >
                <header class="section-heading">
                    <div>
                        <span class="section-eyebrow">{{ seasonSubtitle || 'Active Season' }}</span>
                        <h2
                            id="season-summary-heading"
                            aria-live="polite"
                            aria-atomic="true"
                        >
                            {{ seasonSummaryHeading }}
                        </h2>
                        <p class="section-subtext">{{ seasonSummaryMeta }}</p>
                    </div>
                    <router-link to="/seasons" class="btn-link">All Seasons</router-link>
                </header>

                <div
                    v-if="seasonLoading"
                    class="season-skeleton"
                    role="status"
                    aria-live="polite"
                >
                    <div class="season-skeleton__header"></div>
                    <div class="season-skeleton__grid">
                        <div v-for="n in 3" :key="'skeleton-' + n" class="season-skeleton__card"></div>
                    </div>
                </div>

                <error-message
                    v-else-if="seasonError"
                    :message="seasonError"
                    @retry="retrySeason"
                ></error-message>

                <template v-else-if="selectedSeasonKey">
                    <div class="season-summary-grid">
                        <stat-panel
                            :items="seasonSummaryMetrics"
                            :columns="3"
                        ></stat-panel>
                        <div
                            v-if="hasSeasonProgress"
                            class="season-progress-card"
                        >
                            <div class="season-progress-card__meta">
                                <span class="season-progress-card__label">{{ seasonProgressLabel }}</span>
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
            </section>

            <section class="stats-section divisions-section" aria-labelledby="divisions-heading">
                <header class="section-heading divisions-section__header">
                    <div>
                        <span class="section-eyebrow">Season {{ selectedSeason?.seasonNumber || seasonTitle }} Divisions</span>
                        <h2 id="divisions-heading">Season {{ selectedSeason?.seasonNumber || seasonTitle }} Divisions</h2>
                        <p class="section-subtext divisions-section__meta">{{ divisionHeaderMeta }}</p>
                    </div>
                    <span class="divisions-section__hint">Suodata ja selaa divisioonia nopeasti.</span>
                </header>
                <division-card-list
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
                    @change-season="handleSeasonSelect"
                    @change-filter="setDivisionFilter"
                    @change-search="divisionSearch = $event"
                    @reset-filters="resetDivisionFilters"
                ></division-card-list>
            </section>
        </div>
    `
};
