window.TeamsView = {
    name: 'TeamsView',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; }
    },
    data() {
        return {
            loading: true,
            error: null,
            seasons: [],
            selectedSeasonId: '',
            teams: [],
            search: ''
        };
    },
    computed: {
        selectedSeasonLabel() {
            if (!this.selectedSeasonId) {
                return 'Kaikki kaudet';
            }
            const row = this.seasons.find(season => String(this.getSeasonId(season)) === String(this.selectedSeasonId));
            if (!row) {
                return `Kausi ${this.selectedSeasonId}`;
            }
            return this.getSeasonLabel(row);
        },
        filteredTeams() {
            const needle = String(this.search || '').trim().toLowerCase();
            if (!needle) {
                return this.teams;
            }
            return this.teams.filter(team => {
                const name = String(this.getTeamName(team)).toLowerCase();
                return name.includes(needle);
            });
        }
    },
    async mounted() {
        await this.loadSeasons();
        await this.loadTeams();
    },
    watch: {
        selectedSeasonId() {
            this.loadTeams();
        }
    },
    methods: {
        async loadSeasons() {
            try {
                const rows = await window.apiClient.getSeasons();
                const normalized = Array.isArray(rows) ? [...rows] : [];
                this.seasons = normalized.sort((a, b) => Number(this.getSeasonId(b) || 0) - Number(this.getSeasonId(a) || 0));
            } catch (error) {
                console.warn('[TeamsView] seasons fetch failed', error);
                this.seasons = [];
            }
        },
        async loadTeams() {
            this.loading = true;
            this.error = null;
            try {
                const params = { limit: 5000 };
                if (this.selectedSeasonId) {
                    params.season = this.selectedSeasonId;
                }
                const rows = await window.apiClient.getTeams(params);
                this.teams = Array.isArray(rows)
                    ? [...rows].sort((a, b) => this.getTeamName(a).localeCompare(this.getTeamName(b), 'fi'))
                    : [];
            } catch (error) {
                this.error = error?.message || 'Joukkueiden lataus epäonnistui';
            } finally {
                this.loading = false;
            }
        },
        getTeamName(team) {
            return (
                team?.display_name ||
                team?.displayName ||
                team?.team_name ||
                team?.teamName ||
                team?.name ||
                'Tuntematon joukkue'
            );
        },
        getTeamId(team) {
            return team?.team_id || team?.teamId || null;
        },
        getTeamAvatar(team) {
            const fallback = window.PAPPALIIGA_DEFAULT_LOGO || '';
            const src = team?.avatar || fallback;
            try {
                if (window.apiClient && typeof window.apiClient.proxyAvatar === 'function') {
                    return window.apiClient.proxyAvatar(src) || fallback;
                }
                return src;
            } catch (error) {
                return src;
            }
        },
        getSeasonId(season) {
            return season?.id ?? season?.season ?? season?.season_id ?? season?.seasonId ?? null;
        },
        getSeasonLabel(season) {
            return season?.label || season?.name || `Kausi ${this.getSeasonId(season)}`;
        }
    },
    template: `
        <section class="teams-view">
            <header class="teams-view__header">
                <h1 class="title-accent titleUnderlinePage">Joukkueet</h1>
                <p class="teams-view__meta">{{ filteredTeams.length }} / {{ teams.length }} joukkuetta · {{ selectedSeasonLabel }}</p>
                <div class="teams-view__filters">
                    <select v-model="selectedSeasonId" class="teams-view__season">
                        <option value="">Kaikki kaudet</option>
                        <option
                            v-for="season in seasons"
                            :key="getSeasonId(season)"
                            :value="String(getSeasonId(season))"
                        >
                            {{ getSeasonLabel(season) }}
                        </option>
                    </select>
                    <input
                        v-model="search"
                        type="search"
                        class="teams-view__search"
                        placeholder="Hae joukkuetta..."
                        autocomplete="off"
                    />
                </div>
            </header>

            <loading-spinner
                v-if="loading"
                message="Joukkueita ladataan..."
            ></loading-spinner>
            <error-message
                v-else-if="error"
                :message="error"
                @retry="loadTeams"
            ></error-message>
            <div v-else class="teams-list-grid">
                <router-link
                    v-for="team in filteredTeams"
                    :key="getTeamId(team)"
                    :to="{ name: 'team', params: { teamId: getTeamId(team) } }"
                    class="teams-list-card"
                >
                    <img
                        :src="getTeamAvatar(team)"
                        :alt="getTeamName(team)"
                        class="teams-list-card__avatar"
                        loading="lazy"
                    />
                    <span class="teams-list-card__name">{{ getTeamName(team) }}</span>
                </router-link>
            </div>
        </section>
    `
};
