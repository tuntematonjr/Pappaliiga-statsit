// Division View - minimal, syntactically-safe implementation
// We'll progressively add features. Keep this file stable to avoid parser/runtime errors.
window.DivisionView = {
    name: 'DivisionView',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; },
        get TeamNav() { return window.TeamNav; },
        get StandingsTable() { return window.StandingsTable; },
        get MapStatsTable() { return window.MapStatsTable; },
        get LeadersNew() { return window.LeadersNew; },
        get Masthead() { return window.Masthead; },
        get CopyLink() { return window.CopyLink; },
        get SortableTable() { return window.SortableTable; }
        ,
        get AllTeamsComparison() { return window.AllTeamsComparison; }
        ,
        get TeamDetail() { return window.TeamDetail; }
    },
    template: `
        <div class="division-view">
            <masthead></masthead>
            <loading-spinner v-if="loading" message="Divisioonaa ladataan..."></loading-spinner>
            <error-message v-else-if="error" :message="error" @retry="loadDivision"></error-message>

            <div v-else-if="division" class="home-section">
                <div class="division-header">
                    <h1>{{ formatDivisionName(division.name) }}</h1>
                    <p class="division-meta">Season {{ division.season }} | Division {{ division.division_num }}</p>
                    <copy-link label="Copy Division Link" compact></copy-link>
                </div>
                
                <!-- All Teams Comparison -->
                <div class="home-section" style="margin: 20px 0;">
                    <all-teams-comparison :championship-id="division.championship_id"></all-teams-comparison>
                </div>

                <team-nav v-if="division.teams && division.teams.length > 0" :teams="division.teams" :championship-id="division.championship_id"></team-nav>


                <div class="division-content home-section" style="margin:20px 0;">
                    <h2 class="section-title">Divisioona tilastot</h2>
                    <div class="stat-cards">
                        <div class="stat-card">
                            <div class="stat-icon">👥</div>
                            <div class="stat-value">{{ division.teams ? division.teams.length : 0 }}</div>
                            <div class="stat-label">Joukkueita</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-icon">👤</div>
                            <div class="stat-value">{{ playerCount }}</div>
                            <div class="stat-label">Pelaajia</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-icon">🗺️</div>
                            <div class="stat-value">{{ division_aggregates.maps_played_total }}</div>
                            <div class="stat-label">Karttoja Pelattu</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-icon">🎯</div>
                            <div class="stat-value">{{ division_aggregates.rounds_played_total }}</div>
                            <div class="stat-label">Erää Pelattu</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-icon">📊</div>
                            <div class="stat-value">{{ division_aggregates.median_adr }}</div>
                            <div class="stat-label">Median ADR</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-icon">⚔️</div>
                            <div class="stat-value">{{ division_aggregates.total_kills }}</div>
                            <div class="stat-label">Total Kills</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-icon">💀</div>
                            <div class="stat-value">{{ division_aggregates.total_deaths }}</div>
                            <div class="stat-label">Total Deaths</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-icon">📈</div>
                            <div class="stat-value">{{ division_aggregates.median_kr }}</div>
                            <div class="stat-label">Median K/R</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-icon">🛡️</div>
                            <div class="stat-value">{{ division_aggregates.median_survival }}</div>
                            <div class="stat-label">Median Survival</div>
                        </div>
                    </div>
                </div>

                <div class="map-stats-section home-section map-stats-table" style="margin: 40px 0;">
                    <h2 class="section-title">Karttatilastot</h2>

                    <div v-if="mapStatsLoading" class="loading">Loading map stats...</div>
                    <div v-else-if="mapStatsError" class="error">{{ mapStatsError }}</div>

                    <sortable-table v-else-if="mapRows.length > 0"
                        :columns="mapColumns"
                        :data="mapRows"
                        :defaultSort="{ column: 'maps_played', order: 'desc' }"
                    >
                        <template v-slot:cell-map_name="{ row }">
                            <div class="map-name">
                                <img v-if="row.logo" :src="row.logo" alt="" class="map-logo" />
                                <span class="map-name-text">{{ row.map_name }}</span>
                            </div>
                        </template>
                    </sortable-table>

                    <p v-if="!mapRows.length" class="no-data">Ei karttatilastoja saatavilla</p>
                </div>

                <div class="leaders-section home-section" style="margin: 40px 0;">
                    <h2 class="section-title">Divarin Sankarit</h2>
                    <leaders-new :categories="leaderCategories"></leaders-new>
                </div>
            </div>
        </div>
    `,
    data() {
        return {
            loading: true,
            error: null,
            division: null,
            playerCount: '-',
            mapStats: [],
            mapStatsLoading: false,
            mapStatsError: null,
            leaderCategories: []
            ,
            division_aggregates: {
                maps_played_total: 0,
                rounds_played_total: 0,
                total_kills: 0,
                total_deaths: 0,
                median_adr: '-',
                median_kr: '-',
                median_survival: '-'
            }
        };
    },
    computed: {
        championshipId() { return this.$route.params.championshipId; }
        ,
        mapColumns() {
            return [
                { key: 'map_name', label: 'Kartta', sortable: true },
                { key: 'maps_played', label: 'Pelattu', sortable: true, numeric: true },
                { key: 'banned', label: 'Banned', sortable: true, numeric: true },
                { key: 'rounds_played', label: 'Rundeja', sortable: true, numeric: true },
                { key: 'rounds_per_map', label: 'R/Map', sortable: true, numeric: true, decimals: 2 },
                { key: 'kills', label: 'Killed', sortable: true, numeric: true },
                { key: 'deaths', label: 'Deaths', sortable: true, numeric: true },
                { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1 },
                { key: 'kr', label: 'K/R', sortable: true, numeric: true, decimals: 2 },
                { key: 'udpr', label: 'UDPR', sortable: true, numeric: true, decimals: 2 },
                { key: 'enemy_flash', label: 'Enemy/Flash', sortable: true, numeric: true, decimals: 2 },
                { key: 'sniper_kills', label: 'Sniper Kills', sortable: true, numeric: true },
                { key: 'assists', label: 'Assists', sortable: true, numeric: true },
                { key: 'k2', label: '2K', sortable: true, numeric: true },
                { key: 'k3', label: '3K', sortable: true, numeric: true },
                { key: 'k4', label: '4K', sortable: true, numeric: true },
                { key: 'ace', label: 'ACE', sortable: true, numeric: true },
                { key: 'pistol_kills', label: 'Pistol Kills', sortable: true, numeric: true }
            ];
        },
        mapRows() {
            if (!this.mapStats || this.mapStats.length === 0) return [];
            return this.mapStats.map(m => {
                const curr = m && m.curr ? m.curr : {};
                const maps_played = Number(curr.maps_played || 0);
                const rounds_played = Number(curr.rounds_played || 0);
                const rounds_per_map = maps_played ? (rounds_played / maps_played) : 0;
                return {
                    map_name: m.map_name || '',
                    logo: curr.logo || null,
                    maps_played: maps_played,
                    banned: Number(curr.banned || 0),
                    rounds_played: rounds_played,
                    rounds_per_map: Number(rounds_per_map.toFixed(2)),
                    kills: Number(curr.kills || 0),
                    deaths: Number(curr.deaths || 0),
                    adr: Number(curr.adr || 0),
                    kr: Number(curr.kr || 0),
                    udpr: Number(curr.udpr || 0),
                    enemy_flash: Number(curr.enemy_flash || 0),
                    sniper_kills: Number(curr.sniper_kills || 0),
                    assists: Number(curr.assists || 0),
                    k2: Number(curr.k2 || 0),
                    k3: Number(curr.k3 || 0),
                    k4: Number(curr.k4 || 0),
                    ace: Number(curr.ace || 0),
                    pistol_kills: Number(curr.pistol_kills || 0)
                };
            });
        }
        
    },
    watch: {
        championshipId: { immediate: true, handler() { this.loadDivision(); } }
    },
    methods: {
        async loadDivision() {
            this.loading = true;
            this.error = null;
            try {
                // Fetch real division data by championship ID
                const division = await window.apiClient.getDivisionById(this.championshipId);
                this.division = division;

                if (typeof division.player_count === 'number') {
                    this.playerCount = division.player_count;
                } else {
                    // Fallback: collect unique players across all teams
                    const uniquePlayers = new Set();
                    (division.teams || []).forEach(team => {
                        if (!team || !Array.isArray(team.players)) {
                            return;
                        }
                        team.players.forEach(player => {
                            if (!player) {
                                return;
                            }
                            const id = player.player_id || player.playerId || player.id;
                            if (id) {
                                uniquePlayers.add(String(id));
                            } else if (player.nickname) {
                                uniquePlayers.add(`nick:${player.nickname}`);
                            }
                        });
                    });
                    this.playerCount = uniquePlayers.size;
                }

                // Map stats: use division.map_stats if present, otherwise fetch separately
                if (division.map_stats && Array.isArray(division.map_stats)) {
                    this.mapStats = division.map_stats;
                } else {
                    // Fetch map stats separately if not included in division payload
                    try {
                        this.mapStats = await window.apiClient.getDivisionMapStats(this.championshipId);
                    } catch (mapErr) {
                        console.warn('Failed to load map stats:', mapErr);
                        this.mapStats = [];
                    }
                }

                // Leader categories: use division.leaders if present, otherwise leave empty or fetch
                if (division.leaders && Array.isArray(division.leaders)) {
                    this.leaderCategories = division.leaders;
                } else {
                    // If backend doesn't provide leaders yet, leave empty (or fetch from separate endpoint)
                    this.leaderCategories = [];
                }

                // Division aggregates: use from API response or compute from mapStats
                if (division.aggregates) {
                    this.division_aggregates = division.aggregates;
                } else if (this.mapStats && this.mapStats.length > 0) {
                    // Compute aggregates from map stats if not provided by API
                    this.division_aggregates = {
                        maps_played_total: this.mapStats.reduce((s, m) => s + (m.curr ? (m.curr.maps_played || 0) : 0), 0),
                        rounds_played_total: this.mapStats.reduce((s, m) => s + (m.curr ? (m.curr.rounds_played || 0) : 0), 0),
                        total_kills: this.mapStats.reduce((s, m) => s + (m.curr ? (m.curr.kills || 0) : 0), 0),
                        total_deaths: this.mapStats.reduce((s, m) => s + (m.curr ? (m.curr.deaths || 0) : 0), 0),
                        median_adr: this.mapStats.length ? Math.round([...this.mapStats.map(m => m.curr ? (m.curr.adr || 0) : 0)].sort((a, b) => a - b)[Math.floor(this.mapStats.length / 2)] * 10) / 10 : 0,
                        median_kr: this.mapStats.length ? Math.round([...this.mapStats.map(m => m.curr ? (m.curr.kr || 0) : 0)].sort((a, b) => a - b)[Math.floor(this.mapStats.length / 2)] * 100) / 100 : 0,
                        median_survival: this.mapStats.length ? Math.round((this.mapStats.reduce((s, m) => s + (m.curr && m.curr.rounds_played && m.curr.maps_played ? (m.curr.rounds_played / m.curr.maps_played) : 0), 0) / this.mapStats.length) * 10) / 10 : 0
                    };
                } else {
                    // Default empty aggregates
                    this.division_aggregates = {
                        maps_played_total: 0,
                        rounds_played_total: 0,
                        total_kills: 0,
                        total_deaths: 0,
                        median_adr: '-',
                        median_kr: '-',
                        median_survival: '-'
                    };
                }

            } catch (err) {
                this.error = err && err.message ? err.message : 'Divisioonan lataus epäonnistui';
            } finally {
                this.loading = false;
            }
        }
        ,
        formatDivisionName(name) {
            if (!name) return '';
            return name.replace(/\s+S\d+$/i, '').trim();
        }
        
    }
};


