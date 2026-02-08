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
            groupedViewEnabled: true
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
        if (this.seasonsStore && !this.seasonsLoaded) {
            this.seasonsStore.fetchSeasons().catch(err => {
                console.error('[UpcomingMatchesView] seasons fetch failed', err);
            });
        }
    },
    methods: {
        async loadUpcoming() {
            if (!this.upcomingStore || !this.currentSeasonId) return;
            try {
                await this.upcomingStore.fetchUpcomingMatches(this.upcomingParams);
            } catch (error) {
                console.error('[UpcomingMatchesView] upcoming fetch failed', error);
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
                <p class="upcoming-view__subtitle">{{ currentSeasonLabel }}</p>
                <div class="upcoming-view__controls">
                    <button type="button" class="btn-secondary upcoming-view__toggle" @click="toggleGrouping">
                        {{ groupedViewEnabled ? 'Nayta vain aikajarjestys' : 'Nayta paiva- ja divisioonaryhmittely' }}
                    </button>
                </div>
            </header>

            <loading-spinner
                v-if="!currentSeasonId"
                message="Kautta haetaan..."
            ></loading-spinner>
            <upcoming-matches-list
                v-else
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

        </div>
    `
};
