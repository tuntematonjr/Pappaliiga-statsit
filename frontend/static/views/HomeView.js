const HOME_METRIC_SCHEMA = [
    { id: 'teams', key: ['aggregates.total_teams', 'team_count', 'teams', 'total_teams'], label: 'Joukkueet', digits: 0 },
    { id: 'players', key: ['aggregates.total_players', 'player_count', 'players', 'total_players'], label: 'Pelaajat', digits: 0 },
    { id: 'maps', key: ['aggregates.maps_played_total', 'maps_played_total', 'maps_played', 'maps'], label: 'Kartat', digits: 0 },
    { id: 'rounds', key: ['aggregates.rounds_played_total', 'rounds_played_total', 'rounds_played', 'rounds'], label: 'Erät', digits: 0 },
    { id: 'adr', key: ['aggregates.median_adr', 'median_adr', 'adr_median'], label: 'Median ADR', digits: 1 },
    { id: 'kills', key: ['aggregates.total_kills', 'kills_total', 'total_kills'], label: 'Killit', digits: 0 },
    { id: 'deaths', key: ['aggregates.total_deaths', 'deaths_total', 'total_deaths'], label: 'Deaths', digits: 0 },
    { id: 'survival', key: ['aggregates.median_survival', 'median_survival', 'survival_percent'], label: 'Selviytyminen', percent: true, digits: 1 }
];

const PROGRESS_LABELS = {
    overall: 'Kokonaiskausi',
    regular: 'Runkosarja',
    playoffs: 'Playoffit'
};

const SEASON_KPI_SCHEMA = [
    {
        id: 'divisions',
        label: 'Divisions',
        tooltip: 'Sisältää kaikki valitun kauden divisioonat.',
        digits: 0,
        getter: vm => vm.divisionCount
    },
    {
        id: 'teams',
        label: 'Teams',
        tooltip: 'Yksilöllisten joukkueiden määrä kaudella.',
        digits: 0,
        key: [
            'aggregates.total_teams',
            'team_count',
            'teams',
            'teams_count',
            'total_teams'
        ]
    },
    {
        id: 'maps',
        label: 'Maps',
        tooltip: 'Kauden aikana pelattujen karttojen määrä.',
        digits: 0,
        key: [
            'aggregates.maps_played_total',
            'maps_played_total',
            'maps_played',
            'map_count',
            'maps'
        ]
    },
    {
        id: 'rounds',
        label: 'Rounds',
        tooltip: 'Yhteensä pelatut erät runkosarjassa ja playoffeissa.',
        digits: 0,
        key: [
            'aggregates.rounds_played_total',
            'rounds_played_total',
            'rounds_played',
            'rounds'
        ]
    },
    {
        id: 'kills',
        label: 'Kills',
        tooltip: 'Kauden kokonaistapot.',
        digits: 0,
        key: [
            'aggregates.total_kills',
            'kills_total',
            'kills',
            'total_kills'
        ]
    },
    {
        id: 'headshots',
        label: 'Headshots',
        tooltip: 'Headshot-osumien kokonaismäärä.',
        digits: 0,
        key: [
            'aggregates.total_headshots',
            'headshots_total',
            'headshots',
            'total_headshots'
        ]
    },
    {
        id: 'winrate',
        label: 'Win%',
        tooltip: 'Kauden keskimääräinen voittoprosentti.',
        percent: true,
        digits: 1,
        key: [
            'aggregates.win_percent',
            'win_percent',
            'win_pct',
            'wins_percent'
        ]
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

function buildMetricCards(source, schema) {
    if (!source || !schema) {
        return [];
    }
    return schema.map(definition => {
        const rawValue = pickValue(source, definition.key);
        return {
            key: definition.id,
            label: definition.label,
            value: formatMetric(rawValue, definition)
        };
    });
}

function buildProgressItems(progress) {
    if (!progress || typeof progress !== 'object') {
        return [];
    }
    return Object.entries(PROGRESS_LABELS)
        .map(([key, label]) => {
            const value = progress[key] || {};
            const played = toNumber(value.played, 0);
            const total = toNumber(value.total, 0);
            const percent = toNumber(
                value.percent != null ? value.percent : (total ? Math.round((played / total) * 100) : 0),
                0
            );
            return {
                key,
                label,
                played,
                total,
                percent
            };
        })
        .filter(entry => entry.total > 0 || entry.played > 0);
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
            progressGlowMeta: {},
            divisionFilter: 'all',
            divisionSort: 'tier',
            divisionSearch: ''
        };
    },
    computed: {
        heroTitle() {
            return 'AFI × Pappaliiga Stats Hub';
        },
        heroSubtitle() {
            return 'Nopea näkymä Pappaliigan kauden 11 divisiooniin, tuloksiin ja seuraaviin askeliin.';
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
        summaryMetrics() {
            const aggregates = this.homeStore?.lifetimeSummary?.aggregates || {};
            return buildMetricCards(aggregates, HOME_METRIC_SCHEMA);
        },

        seasonsLoading() {
            return this.seasonsStore?.loading ?? false;
        },
        seasonsError() {
            return this.seasonsStore?.error ?? null;
        },
        activeSeasons() {
            return this.seasonsStore?.activeSeasons ?? [];
        },
        archivedSeasons() {
            return this.seasonsStore?.archivedSeasons ?? [];
        },
        seasonSelectGroups() {
            return [
                {
                    id: 'active',
                    label: 'Käynnissä olevat kaudet',
                    options: this.activeSeasons
                },
                {
                    id: 'archived',
                    label: 'Arkistoidut kaudet',
                    options: this.archivedSeasons
                }
            ].filter(group => Array.isArray(group.options) && group.options.length > 0);
        },
        selectedSeasonKey: {
            get() {
                return this.seasonsStore?.selectedSeasonKey ?? null;
            },
            set(value) {
                if (this.seasonsStore) {
                    this.seasonsStore.selectSeason(value);
                }
            }
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
        seasonMetrics() {
            return buildMetricCards(this.seasonState.stats || {}, HOME_METRIC_SCHEMA);
        },
        seasonDivisions() {
            return Array.isArray(this.seasonState.divisions) ? this.seasonState.divisions : [];
        },
        progressMetrics() {
            const meta = this.progressGlowMeta;
            return buildProgressItems(this.seasonState.progress).map(item => {
                if (!meta[item.key]) {
                    meta[item.key] = {
                        glowDelay: Number((Math.random() * 2.5).toFixed(2)),
                        glowDuration: Number((5 + Math.random() * 3).toFixed(2))
                    };
                }
                return {
                    ...item,
                    glowDelay: meta[item.key].glowDelay,
                    glowDuration: meta[item.key].glowDuration
                };
            });
        },
        hasProgress() {
            return this.progressMetrics.length > 0;
        },
        seasonKpiChips() {
            const stats = this.seasonState.stats || {};
            return SEASON_KPI_SCHEMA.map(schema => {
                const rawValue =
                    typeof schema.getter === 'function'
                        ? schema.getter(this, stats)
                        : pickValue(stats, schema.key);
                return {
                    id: schema.id,
                    label: schema.label,
                    value: formatMetric(rawValue, schema),
                    tooltip: schema.tooltip
                };
            });
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
        divisionCount() {
            return this.seasonDivisions.length;
        },
        divisionProgressPercent() {
            const percent = this.seasonState?.progress?.overall?.percent ?? 0;
            return Number.isFinite(percent) ? percent : 0;
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
                if (!newKey || newKey === oldKey) {
                    return;
                }
                const season = this.selectedSeason;
                this.loadSeason(newKey, { apiParam: season?.apiParam });
            }
        }
    },
    methods: {
        async bootstrap() {
            const tasks = [];
            if (this.homeStore) {
                tasks.push(this.homeStore.ensureSummary());
            }
            if (this.seasonsStore) {
                tasks.push(
                    this.seasonsStore
                        .fetchSeasons()
                        .then(() => this.seasonsStore.ensureSelectedSeason())
                        .catch(() => {})
                );
            }
            await Promise.allSettled(tasks);
        },
        async loadSeason(key, options = {}) {
            if (!key || !this.homeStore) {
                return;
            }
            const season = this.seasonsStore?.getSeasonByKey(key);
            const apiParam = options.apiParam ?? season?.apiParam ?? key;
            try {
                await this.homeStore.fetchSeason(key, {
                    apiParam,
                    force: options.force === true
                });
            } catch (error) {
                console.error('Season fetch failed', error);
            }
        },
        retrySeasons() {
            if (!this.seasonsStore) return;
            this.seasonsStore
                .fetchSeasons({ force: true })
                .then(() => {
                    this.seasonsStore.ensureSelectedSeason();
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
            this.loadSeason(this.selectedSeasonKey, {
                apiParam: season?.apiParam,
                force: true
            });
        },
        scrollToSeasonControls() {
            const target = this.$refs.seasonControls;
            if (!target) return;
            try {
                target.scrollIntoView({ behavior: 'smooth', block: 'start' });
            } catch (error) {
                window.scrollTo(0, target.offsetTop || 0);
            }
        },
        setDivisionFilter(filter) {
            this.divisionFilter = filter;
        },
        handleDivisionSearch(event) {
            this.divisionSearch = event?.target?.value ?? '';
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
                    <button class="btn-primary" type="button" @click="scrollToSeasonControls">View Current Season</button>
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

            <section class="home-summary">
                <header class="home-summary__header">
                    <h2>Kaikki kaudet yhteensä</h2>
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
                    :items="summaryMetrics"
                    :columns="4"
                ></stat-panel>
            </section>

            <section
                class="season-dashboard"
                v-if="selectedSeasonKey"
            >
                <div class="season-control-bar glass-card" ref="seasonControls">
                    <div class="season-control-bar__season">
                        <label class="season-control-bar__label" for="season-select">Season</label>
                        <select
                            id="season-select"
                            class="season-control-bar__select"
                            v-model="selectedSeasonKey"
                            :disabled="seasonsLoading || !seasonSelectGroups.length"
                        >
                            <template v-if="seasonSelectGroups.length">
                                <optgroup
                                    v-for="group in seasonSelectGroups"
                                    :key="group.id"
                                    :label="group.label"
                                >
                                    <option
                                        v-for="season in group.options"
                                        :key="season.key"
                                        :value="season.key"
                                    >
                                        {{ season.label }}
                                    </option>
                                </optgroup>
                            </template>
                            <option v-else disabled>Ei kausia</option>
                        </select>
                    </div>

                    <div class="season-control-bar__filters" role="group" aria-label="Divisioona tilat">
                        <button
                            v-for="state in ['all', 'active', 'finished', 'waiting']"
                            :key="state"
                            type="button"
                            class="filter-chip"
                            :data-state="state"
                            :class="{ 'filter-chip--active': divisionFilter === state }"
                            :aria-pressed="divisionFilter === state"
                            @click="setDivisionFilter(state)"
                        >
                            {{ state === 'all' ? 'All' : state.charAt(0).toUpperCase() + state.slice(1) }}
                        </button>
                    </div>

                    <label class="season-control-bar__search" aria-label="Hae divisioonaa">
                        <span class="sr-only">Search divisions</span>
                        <input
                            type="search"
                            class="season-control-bar__input"
                            placeholder="Search divisions"
                            :value="divisionSearch"
                            @input="handleDivisionSearch"
                        >
                    </label>

                    <div class="season-control-bar__sort">
                        <label class="season-control-bar__label" for="division-sort">Sort</label>
                        <select
                            id="division-sort"
                            class="season-control-bar__select"
                            v-model="divisionSort"
                        >
                            <option value="tier">By tier</option>
                            <option value="progress">By progress</option>
                            <option value="alphabetical">Alphabetical</option>
                        </select>
                    </div>
                </div>

                <loading-spinner
                    v-if="seasonLoading"
                    message="Kausitietoja ladataan..."
                ></loading-spinner>

                <error-message
                    v-else-if="seasonError"
                    :message="seasonError"
                    @retry="retrySeason"
                ></error-message>

                <template v-else>
                    <section class="season-summary-row glass-card">
                        <div class="season-summary-row__header">
                            <div>
                                <span v-if="seasonSubtitle" class="section-eyebrow">{{ seasonSubtitle }}</span>
                                <h2>{{ seasonTitle }}</h2>
                            </div>
                            <router-link to="/seasons" class="btn-link">All Seasons</router-link>
                        </div>
                        <div class="season-summary-row__chips" role="list">
                            <article
                                v-for="chip in seasonKpiChips"
                                :key="chip.id"
                                class="kpi-chip"
                                role="listitem"
                                tabindex="0"
                                :title="chip.tooltip"
                            >
                                <span class="kpi-chip__label">{{ chip.label }}</span>
                                <span class="kpi-chip__value">{{ chip.value }}</span>
                            </article>
                        </div>
                        <div
                            v-if="hasProgress"
                            class="season-summary-row__progress"
                            role="group"
                            aria-label="Kausiprogessiot"
                        >
                            <div
                                v-for="item in progressMetrics"
                                :key="item.key"
                                class="season-summary-row__progress-item"
                                :aria-label="item.label + ' ' + item.played + ' / ' + item.total"
                            >
                                <div class="season-summary-row__progress-headline">
                                    <span>{{ item.label }}</span>
                                    <span>{{ item.played }} / {{ item.total }}</span>
                                </div>
                                <div
                                    class="season-summary-row__progress-bar"
                                    role="progressbar"
                                    :aria-valuenow="item.percent"
                                    aria-valuemin="0"
                                    aria-valuemax="100"
                                    :title="item.label + ' eteneminen'"
                                    :style="{ '--progress': item.percent }"
                                >
                                    <span>{{ item.percent }}%</span>
                                </div>
                            </div>
                        </div>
                    </section>

                    <section class="divisions-section">
                        <header class="divisions-section__header">
                            <div>
                                <h2>Season {{ selectedSeason?.seasonNumber || seasonTitle }} Divisions</h2>
                                <p class="divisions-section__meta">{{ divisionHeaderMeta }}</p>
                            </div>
                            <span class="divisions-section__hint">Suodata ja selaa divisioonia nopeasti.</span>
                        </header>
                        <division-card-list
                            :divisions="seasonDivisions"
                            :season-label="seasonTitle"
                            :empty-message="divisionEmptyMessage"
                            :filter-state="divisionFilter"
                            :sort-mode="divisionSort"
                            :search-query="divisionSearch"
                        ></division-card-list>
                    </section>
                </template>
            </section>
        </div>
    `
};
