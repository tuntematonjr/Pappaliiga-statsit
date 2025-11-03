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
        get SeasonToggle() {
            return window.SeasonToggle;
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
            localSegment: 'active'
        };
    },
    computed: {
        heroTitle() {
            return 'Pappaliiga Stats Hub';
        },
        heroSubtitle() {
            return 'Uusimmat kausikatsaukset, joukkueiden kehitys ja pelaajatilastot yhdestä näkymästä.';
        },
        heroEyebrow() {
            return 'AFI · Faceit API data';
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
        seasonSegment: {
            get() {
                if (this.seasonsStore) {
                    return this.seasonsStore.selectedSegment || this.localSegment;
                }
                return this.localSegment;
            },
            set(value) {
                const segment = value === 'archived' ? 'archived' : 'active';
                if (this.seasonsStore) {
                    this.seasonsStore.setSegment(segment);
                }
                this.localSegment = segment;
            }
        },
        activeSeasons() {
            return this.seasonsStore?.activeSeasons ?? [];
        },
        archivedSeasons() {
            return this.seasonsStore?.archivedSeasons ?? [];
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
            return buildProgressItems(this.seasonState.progress);
        },
        hasProgress() {
            return this.progressMetrics.length > 0;
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
        handleSeasonChange(seasonKey) {
            if (!seasonKey || seasonKey === this.selectedSeasonKey) {
                return;
            }
            this.selectedSeasonKey = seasonKey;
        },
        handleSegmentChange(segment) {
            this.seasonSegment = segment;
            const collection = segment === 'archived' ? this.archivedSeasons : this.activeSeasons;
            if (!collection || !collection.length) {
                return;
            }
            const exists = collection.some(entry => entry.key === this.selectedSeasonKey);
            if (!exists) {
                this.handleSeasonChange(collection[0].key);
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
        scrollToSeasons() {
            const target = this.$refs.seasonPicker;
            if (!target) return;
            try {
                target.scrollIntoView({ behavior: 'smooth', block: 'start' });
            } catch (error) {
                window.scrollTo(0, target.offsetTop || 0);
            }
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
                    <button class="btn-primary" type="button" @click="scrollToSeasons">Siirry kausiin</button>
                    <router-link to="/seasons" class="btn-link">Kaikki kaudet</router-link>
                </template>
                <template #meta>
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
                        dense
                    ></stat-panel>
                </template>
            </hero-banner>

            <section class="home-section" ref="seasonPicker">
                <header class="section-heading">
                    <div>
                        <h2>Kaudet</h2>
                        <p class="section-subtitle">
                            Valitse käynnissä oleva kausi tai tutustu arkistoon.
                        </p>
                    </div>
                </header>

                <season-toggle
                    :active-seasons="activeSeasons"
                    :archived-seasons="archivedSeasons"
                    :model-value="selectedSeasonKey"
                    :segment="seasonSegment"
                    :loading="seasonsLoading"
                    :error="seasonsError"
                    @update:modelValue="handleSeasonChange"
                    @update:segment="handleSegmentChange"
                    @retry="retrySeasons"
                ></season-toggle>
            </section>

            <section class="home-section season-overview" v-if="selectedSeasonKey">
                <header class="section-heading">
                    <div>
                        <p v-if="seasonSubtitle" class="section-eyebrow">{{ seasonSubtitle }}</p>
                        <h2>{{ seasonTitle }}</h2>
                    </div>
                    <router-link to="/seasons" class="btn-link">Kaikki kaudet</router-link>
                </header>

                <loading-spinner
                    v-if="seasonLoading"
                    message="Kausitietoja ladataan..."
                ></loading-spinner>

                <error-message
                    v-else-if="seasonError"
                    :message="seasonError"
                    @retry="retrySeason"
                ></error-message>

                <div v-else class="season-overview__content">
                    <stat-panel
                        :items="seasonMetrics"
                        :columns="4"
                    ></stat-panel>

                    <div
                        v-if="hasProgress"
                        class="season-progress"
                        role="group"
                        aria-label="Kausien eteneminen"
                    >
                        <div
                            v-for="item in progressMetrics"
                            :key="item.key"
                            class="progress-pill"
                            :aria-label="item.label + ' ' + item.played + '/' + item.total"
                        >
                            <div class="progress-pill__header">
                                <span class="progress-pill__label">{{ item.label }}</span>
                                <span class="progress-pill__value">{{ item.played }} / {{ item.total }}</span>
                            </div>
                            <div class="progress-pill__bar">
                                <span class="progress-pill__fill" :style="{ width: item.percent + '%' }"></span>
                            </div>
                            <span class="progress-pill__percent">{{ item.percent }} %</span>
                        </div>
                    </div>

                    <division-card-list
                        :divisions="seasonDivisions"
                        :season-label="seasonTitle"
                        empty-message="Tälle kaudelle ei löytynyt divisioonia."
                    ></division-card-list>
                </div>
            </section>
        </div>
    `
};

