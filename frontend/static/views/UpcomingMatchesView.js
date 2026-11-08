window.UpcomingMatchesView = {
    name: 'UpcomingMatchesView',
    components: {
        get UpcomingMatchesList() { return window.UpcomingMatchesList; },
        get ErrorMessage() { return window.ErrorMessage; },
        get LoadingSpinner() { return window.LoadingSpinner; }
    },
    data() {
        const seasonsStore = typeof window.useSeasonsStore === 'function' ? window.useSeasonsStore() : null;
        const upcomingStore = typeof window.useUpcomingStore === 'function' ? window.useUpcomingStore() : null;
        return {
            seasonsStore,
            upcomingStore,
            limit: 50,
            groupedViewEnabled: true,
            viewMode: 'upcoming',
            liveMatches: [],
            liveLoading: false,
            liveError: null
        };
    },
    computed: {
        seasonsLoaded() {
            return Boolean(this.seasonsStore && this.seasonsStore.seasons && this.seasonsStore.seasons.length);
        },
        currentSeason() {
            if (!this.seasonsStore) return null;
            const direct = this.seasonsStore.currentSeason;
            if (direct) return direct;
            const active = this.seasonsStore.seasons?.find(season => season?.isActive);
            if (active) return active;
            return this.seasonsStore.newestSeason || this.seasonsStore.sortedSeasons?.[0] || null;
        },
        currentSeasonId() {
            return this.currentSeason?.id ?? this.currentSeason?.seasonNumber ?? null;
        },
        currentSeasonLabel() {
            if (!this.currentSeason) return 'Tulevat ottelut';
            return this.currentSeason.label || `Kausi ${this.currentSeasonId}`;
        },
        upcomingParams() {
            return {
                season: this.currentSeasonId,
                includePlayoffs: true,
                limit: this.limit,
                offset: 0
            };
        },
        upcomingState() {
            if (!this.upcomingStore || !this.upcomingStore.getEntryForParams) {
                return { data: [], loading: false, error: null };
            }
            return this.upcomingStore.getEntryForParams(this.upcomingParams);
        },
        showingUpcoming() {
            return this.viewMode === 'upcoming';
        }
    },
    watch: {
        currentSeasonId: {
            immediate: true,
            handler() {
                this.loadUpcoming();
            }
        }
    },
    mounted() {
        this._loadBundle().catch(err => {
            console.error('[UpcomingMatchesView] bundle fetch failed', err);
        });
    },
    methods: {
        async _loadBundle() {
            if (!window.apiClient || typeof window.apiClient.getSeasonsUpcoming !== 'function') {
                if (this.seasonsStore && !this.seasonsLoaded) {
                    this.seasonsStore.fetchSeasons().catch(() => {});
                }
                return;
            }
            try {
                const bundle = await window.apiClient.getSeasonsUpcoming({
                    includePlayoffs: true,
                    limit: this.limit,
                    offset: 0
                });
                if (this.seasonsStore && typeof this.seasonsStore.seedSeasons === 'function') {
                    this.seasonsStore.seedSeasons(bundle.seasons);
                }
                if (this.upcomingStore && typeof this.upcomingStore.seedUpcoming === 'function') {
                    this.upcomingStore.seedUpcoming({ ...this.upcomingParams }, bundle.upcoming);
                }
            } catch (err) {
                console.warn('[UpcomingMatchesView] bundle fetch failed, falling back', err);
                if (this.seasonsStore && !this.seasonsLoaded) {
                    this.seasonsStore.fetchSeasons().catch(() => {});
                }
            }
        },
        async loadUpcoming() {
            if (!this.upcomingStore || !this.currentSeasonId) return;
            try {
                await this.upcomingStore.fetchUpcomingMatches(this.upcomingParams);
            } catch (error) {
                console.error('[UpcomingMatchesView] upcoming fetch failed', error);
            }
        },
        async selectView(viewMode) {
            this.viewMode = viewMode;
            if (viewMode === 'live' && !this.liveMatches.length && !this.liveLoading) {
                await this.loadLiveMatches();
            }
        },
        async loadLiveMatches() {
            this.liveLoading = true;
            this.liveError = null;
            try {
                if (!window.apiClient || typeof window.apiClient.getLiveMatches !== 'function') {
                    throw new Error('Live-otteluiden rajapintaa ei ole saatavilla.');
                }
                this.liveMatches = await window.apiClient.getLiveMatches();
            } catch (err) {
                console.error('[UpcomingMatchesView] live match fetch failed', err);
                this.liveError = err?.message || 'Live-otteluiden haku epäonnistui.';
            } finally {
                this.liveLoading = false;
            }
        },
        toggleGrouping() {
            this.groupedViewEnabled = !this.groupedViewEnabled;
        }
    },
    template: `
        <div class="upcoming-view">
            <header class="upcoming-view__header">
                <h1 class="title-accent titleUnderlinePage">Tulevat ottelut</h1>
                <p class="upcoming-view__subtitle">{{ showingUpcoming ? currentSeasonLabel : 'Käynnissä olevat ottelut Faceitista' }}</p>
                <div class="upcoming-view__controls">
                    <button
                        type="button"
                        class="btn-secondary upcoming-view__toggle"
                        :class="{ 'is-active': showingUpcoming }"
                        @click="selectView('upcoming')"
                    >Tulevat ottelut</button>
                    <button
                        type="button"
                        class="btn-secondary upcoming-view__toggle"
                        :class="{ 'is-active': !showingUpcoming }"
                        @click="selectView('live')"
                    >Käynnissä nyt</button>
                    <button
                        v-if="showingUpcoming"
                        type="button"
                        class="btn-secondary upcoming-view__toggle"
                        @click="toggleGrouping"
                    >{{ groupedViewEnabled ? 'Nayta vain aikajarjestys' : 'Nayta paiva- ja divisioonaryhmittely' }}</button>
                </div>
            </header>

            <loading-spinner
                v-if="showingUpcoming && !currentSeasonId"
                message="Kautta haetaan..."
            ></loading-spinner>
            <upcoming-matches-list
                v-else-if="showingUpcoming"
                :title="'Tulevat ottelut'"
                :subtitle="currentSeasonLabel"
                :items="upcomingState.data"
                :loading="upcomingState.loading"
                :error="upcomingState.error"
                :show-division="true"
                :group-by-day-division="groupedViewEnabled"
                :show-week-separators="!groupedViewEnabled"
                separator-granularity="day"
                :show-header="false"
                empty-message="Tälle kaudelle ei löytynyt tulevia otteluita."
            ></upcoming-matches-list>
            <upcoming-matches-list
                v-else
                :title="'Käynnissä olevat ottelut'"
                :items="liveMatches"
                :loading="liveLoading"
                :error="liveError"
                :show-score="true"
                :show-header="false"
                empty-message="Pappaliigassa ei ole juuri nyt käynnissä olevia Faceit-otteluita."
            ></upcoming-matches-list>
        </div>
    `
};
