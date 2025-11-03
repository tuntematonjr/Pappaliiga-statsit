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
    { key: 'map_name', label: 'Kartta', sortable: true, align: 'left', colClass: 'col-name col-map-name' },
    { key: 'maps_played', label: 'Pelattu', sortable: true, numeric: true },
    { key: 'banned', label: 'Bannattu', sortable: true, numeric: true },
    { key: 'rounds_played', label: 'Erät', sortable: true, numeric: true },
    { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2 },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1 },
    { key: 'clutches', label: 'Clutchit', sortable: true, numeric: true },
    { key: 'sniper_kills', label: 'AWP tap.', sortable: true, numeric: true },
    { key: 'pistol_kills', label: 'Pistoolitap.', sortable: true, numeric: true }
];

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
        get TeamComparisonChart() { return window.TeamComparisonChart; },
        get StatPanel() { return window.StatPanel; },
        get CopyLink() { return window.CopyLink; },
        get SortableTable() { return window.SortableTable; }
    },
    data() {
        const divisionStore = typeof window.useDivisionStore === 'function' ? window.useDivisionStore() : null;
        const seasonsStore = typeof window.useSeasonsStore === 'function' ? window.useSeasonsStore() : null;
        return {
            divisionStore,
            seasonsStore,
            mapColumns: DIVISION_MAP_COLUMNS,
            quickLinks: [
                { id: 'overview', label: 'Katsaus' },
                { id: 'standings', label: 'Sarjataulukko' },
                { id: 'maps', label: 'Kartat' },
                { id: 'highlights', label: 'Nostot' },
                { id: 'teams', label: 'Joukkueet' }
            ]
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
                pieces.push(`Kausi ${this.divisionDetails.season}`);
            }
            if (this.divisionDetails.division_num != null) {
                pieces.push(`Div ${this.divisionDetails.division_num}`);
            }
            if (this.divisionDetails.is_playoff) {
                pieces.push('Playoffs');
            }
            return pieces.join(' · ');
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
        teams() {
            return Array.isArray(this.divisionDetails?.teams) ? this.divisionDetails.teams : [];
        },
        hasTeams() {
            return this.teams.length > 0;
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
            const src = highlight.team.logo || highlight.team.avatar || highlight.team.raw?.avatar;
            if (!src) return DEFAULT_TEAM_LOGO;
            try {
                const resolved = window.apiClient.proxyAvatar(src);
                return resolved || DEFAULT_TEAM_LOGO;
            } catch (error) {
                return src || DEFAULT_TEAM_LOGO;
            }
        },
        retryHighlights() {
            if (!this.divisionStore || !this.championshipId) return;
            this.divisionStore.fetchDivisionHighlights(this.championshipId, { force: true }).catch(err => {
                console.error('Highlight refresh failed', err);
            });
        }
    },
    template: `
        <div class="division-view">
            <header class="division-header glass-card">
                <div class="division-header__meta">
                    <div>
                        <p class="section-eyebrow">Divisioona</p>
                        <h1 class="division-header__title">{{ divisionTitle }}</h1>
                        <p v-if="divisionSubtitle" class="division-header__subtitle">{{ divisionSubtitle }}</p>
                    </div>
                    <copy-link v-if="championshipId" :url="shareUrl" label="Jaa divisioona"></copy-link>
                </div>
                <nav class="division-header__nav" aria-label="Pikalinkit divisioonalle">
                    <button
                        v-for="link in quickLinks"
                        :key="link.id"
                        type="button"
                        class="division-header__nav-link"
                        @click="scrollToSection(link.id)"
                    >
                        {{ link.label }}
                    </button>
                    <router-link
                        v-if="breadcrumbSeason"
                        class="division-header__nav-link division-header__nav-link--subtle"
                        :to="{ name: 'seasons' }"
                    >
                        {{ breadcrumbSeason.label || ('Kausi ' + breadcrumbSeason.seasonNumber) }}
                    </router-link>
                </nav>
            </header>

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
                <section id="overview" class="division-section">
                    <div class="division-section__header">
                        <h2>Katsaus</h2>
                        <button type="button" class="btn-link" @click="refreshAll">Päivitä data</button>
                    </div>
                    <stat-panel
                        :items="statMetrics"
                        :columns="3"
                    ></stat-panel>
                </section>

                <section id="standings" class="division-section">
                    <div class="division-section__header">
                        <h2>Sarjataulukko</h2>
                    </div>
                    <team-comparison-chart
                        :teams="standings"
                        :limit="12"
                        :title="divisionTitle + ' standings'"
                    ></team-comparison-chart>

                    <team-comparison-board
                        :teams="teams"
                        :loading="standingsLoading"
                        :error="standingsError"
                        title="Joukkuevertailu"
                        subtitle="Kokonaiskuva joukkueiden suorituskyvystä"
                    ></team-comparison-board>
                </section>

                <section id="maps" class="division-section">
                    <div class="division-section__header">
                        <h2>Karttanäkymä</h2>
                    </div>
                    <maps-stats
                        title="Karttatilastot"
                        subtitle="Kokonaiskuva divisioonan suosituimmista kartoista"
                        :loading="mapsLoading"
                        :error="mapsError"
                        :map-stats="mapStats"
                        :columns="mapColumns"
                    ></maps-stats>
                </section>

                <section id="highlights" class="division-section">
                    <div class="division-section__header">
                        <h2>Nostot</h2>
                    </div>

                    <loading-spinner
                        v-if="highlightsLoading && !highlights.length"
                        message="Nostoja kootaan..."
                    ></loading-spinner>
                    <error-message
                        v-else-if="highlightsError && !highlights.length"
                        :message="highlightsError"
                        @retry="retryHighlights"
                    ></error-message>

                    <div v-else class="division-highlights">
                        <article
                            v-for="highlight in highlights"
                            :key="highlight.id"
                            class="division-highlight glass-card"
                        >
                            <p class="division-highlight__eyebrow">{{ highlight.title }}</p>
                            <h3 class="division-highlight__title">{{ highlight.description }}</h3>
                            <p class="division-highlight__metric">{{ highlight.metric }}</p>
                            <p v-if="highlight.tooltip" class="division-highlight__meta">{{ highlight.tooltip }}</p>
                            <router-link
                                v-if="highlightTeamRoute(highlight)"
                                class="division-highlight__link"
                                :to="highlightTeamRoute(highlight)"
                            >
                                <img
                                    v-if="highlightAvatar(highlight)"
                                    :src="highlightAvatar(highlight)"
                                    :alt="highlight.team.name + ' logo'"
                                >
                                <span>{{ highlight.team.name }}</span>
                            </router-link>
                        </article>
                        <p v-if="!highlights.length" class="division-highlights__empty">
                            Ei merkittäviä nostoja tälle divisioonalle.
                        </p>
                    </div>
                </section>

                <section id="teams" class="division-section">
                    <div class="division-section__header">
                        <h2>Joukkueet</h2>
                    </div>
                    <team-nav
                        v-if="hasTeams"
                        :teams="teams"
                        :championship-id="championshipId"
                    ></team-nav>
                    <p v-else class="division-section__empty">
                        Joukkueita ei löytynyt.
                    </p>
                </section>
            </template>
        </div>
    `
};
