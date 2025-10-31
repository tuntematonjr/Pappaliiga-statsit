// Player View - Detailed player stats with map breakdown
window.PlayerView = {
    name: 'PlayerView',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; },
        get MapStatsTable() { return window.MapStatsTable; },
        get ProgressBar() { return window.ProgressBar; },
        get SortableTable() { return window.SortableTable; },
        get Masthead() { return window.Masthead; },
        get CopyLink() { return window.CopyLink; }
    },
    template: `
        <div class="player-view">
            <masthead></masthead>
            <loading-spinner v-if="loading" message="Pelaajaa ladataan..."></loading-spinner>
            <error-message v-else-if="error" :message="error" @retry="loadPlayer"></error-message>
            
            <div v-else class="home-section">
                <div class="player-header">
                    <img :src="avatarUrl(playerInfo.avatar) || defaultAvatar" :alt="playerInfo.nickname" class="player-avatar">
                    <div class="player-header-info">
                        <h1>{{ playerInfo.nickname }}</h1>
                        <p v-if="playerInfo.country" class="player-country">{{ playerInfo.country }}</p>
                        <p v-if="playerInfo.faceit_url">
                            <a :href="playerInfo.faceit_url" target="_blank" class="faceit-link">Katso Faceitissä →</a>
                        </p>
                        <copy-link label="Kopioi pelaajan linkki" compact></copy-link>
                    </div>
                </div>

                <div class="season-selector home-section">
                    <label>Kausi/Divisioona:</label>
                    <select v-model="selectedChampionship" @change="loadMapStats">
                        <option v-for="season in seasonStats" :key="season.championship_id" :value="season.championship_id">
                            Season {{ season.season }} - Div {{ season.division_num }} ({{ season.team_name }})
                        </option>
                    </select>
                </div>

                <div v-if="selectedSeason" class="player-season-stats home-section">
                    <h2 class="section-title">Season {{ selectedSeason.season }} - Division {{ selectedSeason.division_num }}</h2>
                    <p class="team-info" style="text-align: center; color: var(--accent); margin-bottom: 20px;">
                        Team: {{ selectedSeason.team_name }}
                    </p>
                    
                    <!-- Key Performance Stats with Progress Bars -->
                    <div class="stat-cards">
                        <div class="stat-card">
                            <h3>Rating (arvo)</h3>
                            <div class="stat-value">{{ selectedSeason.rating.toFixed(2) }}</div>
                            <progress-bar 
                                :value="selectedSeason.rating" 
                                :max="2.0" 
                                :color="selectedSeason.rating >= 1.0 ? 'ok' : 'warn'"
                                height="10px"
                            ></progress-bar>
                        </div>
                        <div class="stat-card">
                            <h3>K/D-suhde</h3>
                            <div class="stat-value">{{ selectedSeason.kd.toFixed(2) }}</div>
                            <progress-bar 
                                :value="selectedSeason.kd" 
                                :max="2.0" 
                                :color="selectedSeason.kd >= 1.0 ? 'ok' : 'warn'"
                                height="10px"
                            ></progress-bar>
                        </div>
                        <div class="stat-card">
                            <h3>ADR</h3>
                            <div class="stat-value">{{ selectedSeason.adr.toFixed(1) }}</div>
                            <progress-bar 
                                :value="selectedSeason.adr" 
                                :max="100" 
                                color="accent"
                                height="10px"
                            ></progress-bar>
                        </div>
                        <div class="stat-card">
                            <h3>HS%</h3>
                            <div class="stat-value">{{ selectedSeason.hs_pct.toFixed(1) }}%</div>
                            <progress-bar 
                                :value="selectedSeason.hs_pct" 
                                :max="100" 
                                color="accent"
                                height="10px"
                            ></progress-bar>
                        </div>
                    </div>
                    
                    <!-- Detailed Stats -->
                    <div class="stat-cards" style="margin-top: 20px;">
                        <div class="stat-card">
                            <h3>Pelatut kartat</h3>
                            <div class="stat-value">{{ selectedSeason.maps_played }}</div>
                        </div>
                        <div class="stat-card">
                            <h3>Kills</h3>
                            <div class="stat-value">{{ selectedSeason.kills }}</div>
                        </div>
                        <div class="stat-card">
                            <h3>Deaths</h3>
                            <div class="stat-value">{{ selectedSeason.deaths }}</div>
                        </div>
                        <div class="stat-card">
                            <h3>MVPs</h3>
                            <div class="stat-value">{{ selectedSeason.mvps }}</div>
                        </div>
                        <div class="stat-card">
                            <h3>Assists</h3>
                            <div class="stat-value">{{ selectedSeason.assists }}</div>
                        </div>
                        <div class="stat-card">
                            <h3>3K-erät</h3>
                            <div class="stat-value">{{ selectedSeason.triple_kills || 0 }}</div>
                        </div>
                        <div class="stat-card">
                            <h3>4K Rounds</h3>
                            <div class="stat-value">{{ selectedSeason.quadro_kills || 0 }}</div>
                        </div>
                        <div class="stat-card">
                            <h3>Aces</h3>
                            <div class="stat-value">{{ selectedSeason.penta_kills || 0 }}</div>
                        </div>
                    </div>
                </div>

                <map-stats-table 
                    v-if="selectedChampionship"
                    :map-stats="mapStats"
                    :loading="mapStatsLoading"
                    :error="mapStatsError"
                    title="Karttakohtainen suoritus"
                    :show-wins="false"
                    :show-rating="true"
                    :show-mvps="true"
                />
            </div>
        </div>
    `,
    data() {
        return {
            loading: true,
            error: null,
            playerInfo: null,
            seasonStats: [],
            selectedChampionship: null,
            mapStats: [],
            mapStatsLoading: false,
            mapStatsError: null,
            defaultAvatar: '/static/pappaliiga-logo-white-bg.png'
        };
    },
    computed: {
        playerId() {
            return this.$route.params.playerId;
        },
        selectedSeason() {
            if (!this.selectedChampionship) return null;
            return this.seasonStats.find(s => s.championship_id === this.selectedChampionship);
        }
    },
    watch: {
        playerId: {
            immediate: true,
            handler() {
                this.loadPlayer();
            }
        }
    },
    methods: {
        avatarUrl(src) {
            try { return window.apiClient.proxyAvatar(src); } catch (e) { return src; }
        },
        async loadPlayer() {
            this.loading = true;
            this.error = null;
            
            try {
                const [playerInfo, seasonStats] = await Promise.all([
                    window.apiClient.getPlayerInfo(this.playerId),
                    window.apiClient.getPlayerSeasonStats(this.playerId)
                ]);
                
                this.playerInfo = playerInfo;
                this.seasonStats = seasonStats;
                
                // Auto-select championship from query param or most recent
                const championshipParam = this.$route.query.championship;
                if (championshipParam) {
                    this.selectedChampionship = parseInt(championshipParam);
                } else if (seasonStats.length > 0) {
                    this.selectedChampionship = seasonStats[0].championship_id;
                }
                
                if (this.selectedChampionship) {
                    await this.loadMapStats();
                }
            } catch (err) {
                this.error = err.message || 'Pelaajan lataus epäonnistui';
            } finally {
                this.loading = false;
            }
        },
        async loadMapStats() {
            if (!this.selectedChampionship) return;
            
            this.mapStatsLoading = true;
            this.mapStatsError = null;
            
            try {
                this.mapStats = await window.apiClient.getPlayerMapStats(this.playerId, this.selectedChampionship);
            } catch (err) {
                this.mapStatsError = err.message || 'Karttatilastojen lataus epäonnistui';
            } finally {
                this.mapStatsLoading = false;
            }
        }
    }
};
