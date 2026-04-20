window.PlayersView = {
    name: 'PlayersView',
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
            players: [],
            search: '',
            seasonsLoadToken: 0,
            playersLoadToken: 0
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
        filteredPlayers() {
            const needle = String(this.search || '').trim().toLowerCase();
            if (!needle) {
                return this.players;
            }
            return this.players.filter(player => {
                const name = String(this.getPlayerName(player)).toLowerCase();
                return name.includes(needle);
            });
        }
    },
    async mounted() {
        await this.loadAll();
    },
    watch: {
        selectedSeasonId() {
            this.loadPlayers();
        }
    },
    methods: {
        async loadAll() {
            const requestToken = ++this.playersLoadToken;
            ++this.seasonsLoadToken;
            this.loading = true;
            this.error = null;
            try {
                const bundle = await window.apiClient.getSeasonsPlayers({ limit: 5000 });
                if (requestToken !== this.playersLoadToken) return;
                const rawSeasons = Array.isArray(bundle.seasons) ? [...bundle.seasons] : [];
                this.seasons = rawSeasons.sort((a, b) => Number(this.getSeasonId(b) || 0) - Number(this.getSeasonId(a) || 0));
                this.players = Array.isArray(bundle.players)
                    ? [...bundle.players].sort((a, b) => this.getPlayerName(a).localeCompare(this.getPlayerName(b), 'fi'))
                    : [];
            } catch (error) {
                if (requestToken !== this.playersLoadToken) return;
                this.error = error?.message || 'Tietojen lataus epäonnistui';
            } finally {
                if (requestToken === this.playersLoadToken) {
                    this.loading = false;
                }
            }
        },
        async loadPlayers() {
            const requestToken = ++this.playersLoadToken;
            this.loading = true;
            this.error = null;
            try {
                const params = { limit: 5000 };
                if (this.selectedSeasonId) {
                    params.season = this.selectedSeasonId;
                }
                const rows = await window.apiClient.getPlayers(params, {
                    force: true,
                    noCache: true,
                    persistCache: false
                });
                if (requestToken !== this.playersLoadToken) {
                    return;
                }
                this.players = Array.isArray(rows)
                    ? [...rows].sort((a, b) => this.getPlayerName(a).localeCompare(this.getPlayerName(b), 'fi'))
                    : [];
            } catch (error) {
                if (requestToken !== this.playersLoadToken) {
                    return;
                }
                this.error = error?.message || 'Pelaajien lataus epäonnistui';
            } finally {
                if (requestToken !== this.playersLoadToken) {
                    return;
                }
                this.loading = false;
            }
        },
        getPlayerName(player) {
            return player?.nickname || player?.name || 'Tuntematon pelaaja';
        },
        getPlayerId(player) {
            return player?.player_id || player?.playerId || null;
        },
        getPreferredChampionshipId(player) {
            const championshipId = player?.championship_id || player?.championshipId || null;
            if (championshipId != null && String(championshipId).trim() !== '') {
                return String(championshipId);
            }
            return null;
        },
        getPlayerRoute(player) {
            const playerId = this.getPlayerId(player);
            if (!playerId) {
                return { name: 'players' };
            }
            const championshipId = this.getPreferredChampionshipId(player);
            if (championshipId) {
                return {
                    name: 'player-detail',
                    params: {
                        championshipId,
                        playerId: String(playerId)
                    }
                };
            }
            return {
                name: 'player',
                params: { playerId: String(playerId) }
            };
        },
        getPlayerAvatar(player) {
            const fallback = window.PAPPALIIGA_DEFAULT_LOGO || '';
            const src = player?.avatar || fallback;
            try {
                if (window.apiClient && typeof window.apiClient.proxyAvatar === 'function') {
                    return window.apiClient.proxyAvatar(src) || fallback;
                }
                return src;
            } catch (error) {
                return src;
            }
        },
        handleAvatarError(event) {
            const fallback = window.PAPPALIIGA_DEFAULT_LOGO || '';
            if (!event?.target || !fallback) return;
            if (event.target.src !== fallback) {
                event.target.src = fallback;
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
        <section class="players-view">
            <header class="teams-view__header">
                <h1 class="title-accent titleUnderlinePage">Pelaajat</h1>
                <p class="teams-view__meta">{{ filteredPlayers.length }} / {{ players.length }} pelaajaa · {{ selectedSeasonLabel }}</p>
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
                        placeholder="Hae pelaajaa..."
                        autocomplete="off"
                    />
                </div>
            </header>

            <loading-spinner
                v-if="loading"
                message="Pelaajia ladataan..."
            ></loading-spinner>
            <error-message
                v-else-if="error"
                :message="error"
                @retry="loadPlayers"
            ></error-message>
            <div v-else class="teams-list-grid">
                <router-link
                    v-for="player in filteredPlayers"
                    :key="getPlayerId(player)"
                    :to="getPlayerRoute(player)"
                    class="teams-list-card"
                >
                    <img
                        :src="getPlayerAvatar(player)"
                        :alt="getPlayerName(player)"
                        class="teams-list-card__avatar"
                        loading="lazy"
                        @error="handleAvatarError"
                    />
                    <span class="teams-list-card__name">{{ getPlayerName(player) }}</span>
                </router-link>
            </div>
        </section>
    `
};
