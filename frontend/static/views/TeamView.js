// Team View - Detailed team stats with map breakdown
window.TeamView = {
    name: 'TeamView',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; },
        get MapStatsTable() { return window.MapStatsTable; },
        get ProgressBar() { return window.ProgressBar; },
        get SplitBar() { return window.SplitBar; },
        get SortableTable() { return window.SortableTable; },
        get Masthead() { return window.Masthead; },
        get CopyLink() { return window.CopyLink; }
    },
    template: `
        <div class="team-view">
            <masthead></masthead>
            <loading-spinner v-if="loading" message="Loading team..."></loading-spinner>
            <error-message v-else-if="error" :message="error" @retry="loadTeam"></error-message>
            
            <div v-else class="home-section">
                <div class="team-header">
                    <img :src="avatarUrl(teamInfo.avatar) || defaultAvatar" :alt="teamInfo.display_name || teamInfo.team_name" class="team-avatar">
                    <div class="team-header-info">
                        <h1>{{ teamInfo.display_name || teamInfo.team_name }}</h1>
                        <p v-if="teamInfo.faceit_url">
                            <a :href="teamInfo.faceit_url" target="_blank" class="faceit-link">View on Faceit →</a>
                        </p>
                        <copy-link label="Copy Team Link" compact></copy-link>
                    </div>
                </div>

                <div class="season-selector home-section">
                    <label>Season/Division:</label>
                    <select v-model="selectedChampionship" @change="loadMapStats">
                        <option v-for="season in seasonStats" :key="season.championship_id" :value="season.championship_id">
                            Season {{ season.season }} - Div {{ season.division_num }}
                        </option>
                    </select>
                </div>

                <div v-if="selectedSeason" class="team-season-stats home-section">
                    <h2 class="section-title">Season {{ selectedSeason.season }} - Division {{ selectedSeason.division_num }}</h2>
                    
                    <!-- Win/Loss Split Bar -->
                    <div class="stat-card" style="margin-bottom: 24px;">
                        <h3>Win / Loss Record</h3>
                        <split-bar :wins="selectedSeason.wins" :losses="selectedSeason.losses" :left-text="(selectedSeason.wins+'W')" :right-text="(selectedSeason.losses+'L')" :show-percent="true"></split-bar>
                        <div style="text-align: center; margin-top: 8px; color: var(--muted); font-size: 0.9rem;">
                            Win Rate: {{ (selectedSeason.win_rate * 100).toFixed(1) }}%
                        </div>
                    </div>
                    
                    <!-- Stat Cards Grid -->
                    <div class="stat-cards home-section">
                        <div class="stat-card">
                            <h3>Maps Played</h3>
                            <div class="stat-value">{{ selectedSeason.maps_played }}</div>
                        </div>
                        <div class="stat-card">
                            <h3>K/D Ratio</h3>
                            <div class="stat-value">{{ selectedSeason.kd.toFixed(2) }}</div>
                            <progress-bar 
                                :value="selectedSeason.kd" 
                                :max="2.0" 
                                :color="selectedSeason.kd >= 1.0 ? 'ok' : 'warn'"
                                height="8px"
                                :showShimmer="false"
                            ></progress-bar>
                        </div>
                        <div class="stat-card">
                            <h3>ADR</h3>
                            <div class="stat-value">{{ selectedSeason.adr.toFixed(1) }}</div>
                            <progress-bar 
                                :value="selectedSeason.adr" 
                                :max="100" 
                                color="accent"
                                height="8px"
                                :showShimmer="false"
                            ></progress-bar>
                        </div>
                        <div class="stat-card">
                            <h3>Round Diff</h3>
                            <div class="stat-value" :class="selectedSeason.round_diff >= 0 ? 'positive' : 'negative'">
                                {{ selectedSeason.round_diff >= 0 ? '+' : '' }}{{ selectedSeason.round_diff }}
                            </div>
                        </div>
                    </div>
                </div>

                <map-stats-table 
                    v-if="selectedChampionship"
                    :map-stats="mapStats"
                    :loading="mapStatsLoading"
                    :error="mapStatsError"
                    title="Per-Map Performance"
                    :show-wins="true"
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
            teamInfo: null,
            seasonStats: [],
            selectedChampionship: null,
            mapStats: [],
            mapStatsLoading: false,
            mapStatsError: null,
            defaultAvatar: '/static/pappaliiga-logo-white-bg.png'
        };
    },
    computed: {
        teamId() {
            return this.$route.params.teamId;
        },
        selectedSeason() {
            if (!this.selectedChampionship) return null;
            return this.seasonStats.find(s => s.championship_id === this.selectedChampionship);
        }
    },
    watch: {
        teamId: {
            immediate: true,
            handler() {
                this.loadTeam();
            }
        }
    },
    methods: {
        avatarUrl(src) {
            try { return window.apiClient.proxyAvatar(src); } catch (e) { return src; }
        },
        async loadTeam() {
            this.loading = true;
            this.error = null;
            
            try {
                const [teamInfo, seasonStats] = await Promise.all([
                    window.apiClient.getTeamInfo(this.teamId),
                    window.apiClient.getTeamSeasonStats(this.teamId)
                ]);
                
                this.teamInfo = teamInfo;
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
                this.error = err.message || 'Failed to load team';
            } finally {
                this.loading = false;
            }
        },
        async loadMapStats() {
            if (!this.selectedChampionship) return;
            
            this.mapStatsLoading = true;
            this.mapStatsError = null;
            
            try {
                this.mapStats = await window.apiClient.getTeamMapStats(this.teamId, this.selectedChampionship);
            } catch (err) {
                this.mapStatsError = err.message || 'Failed to load map stats';
            } finally {
                this.mapStatsLoading = false;
            }
        }
    }
};
