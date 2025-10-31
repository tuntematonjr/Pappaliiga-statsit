// Home View - Stats overview and recent activity
window.HomeView = {
    name: 'HomeView',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; },
        get ProgressBar() { return window.ProgressBar; },
        get SortableTable() { return window.SortableTable; },
        get HeroCards() { return window.HeroCards; },
        get Masthead() { return window.Masthead; },
        get SeasonsNav() { return window.SeasonsNav; },
        get StatsGrid() { return window.StatsGrid; },
        get LeadersNew() { return window.LeadersNew; }
    },
    template: `
        <div class="home-view">
            <div class="index-wrapper">
                <!-- Top toolbar (placeholder for future features) -->
                <div class="top-toolbar">
                    <div class="toolbar-left">
                        <div class="toolbar-placeholder">Valitse näkymä &middot; Nopeita linkkejä</div>
                    </div>
                    <div class="toolbar-right">
                        <a class="btn btn-ghost" href="javascript:void(0)">Overview</a>
                        <a class="btn" href="javascript:void(0)">Pages</a>
                    </div>
                </div>

                <masthead></masthead>

                <!-- Hero Section with AFI and Pappaliiga -->
                <hero-cards :cards="heroCardsData"></hero-cards>

                <loading-spinner v-if="loading" message="Tilastoja ladataan..."></loading-spinner>
                <error-message v-else-if="error" :message="error" @retry="loadData"></error-message>

                <div v-else>
                    <!-- All-Time Stats -->
                    <section class="stats-overview home-section">
                        <h2 class="section-title">Kaikki Kaudet Yhteensä</h2>
                        <stats-grid :stats="allTimeStats"></stats-grid>
                    </section>

                    <!-- Season Selector -->
                    <section class="season-selector home-section">
                        <h2 class="section-title">Valitse Kausi</h2>
                        <seasons-nav 
                            :seasons="seasonsData" 
                            :current-season="selectedSeason"
                            @season-change="handleSeasonChange" 
                        ></seasons-nav>

                        <div v-if="seasonStats" class="season-details">
                            <!-- Season Overview -->
                            <div class="stats-overview home-section" style="margin-top: 32px;">
                                <h2 class="section-title">Season {{ selectedSeason }} Yleiskatsaus</h2>
                                <stats-grid :stats="seasonStatsData"></stats-grid>
                            </div>

                            <!-- Season progress bars: overall completion and regular vs playoffs split -->
                            <div class="season-progress">
                                <div class="card stat-card">
                                    <h3 style="margin-bottom:8px; text-align:center">Kausiedistys</h3>
                                    <div style="text-align:center; margin-bottom:12px">
                                        <div style="font-size:1.6rem; font-weight:800; color:var(--accent);">{{ seasonProgress }}%</div>
                                        <div style="font-size:0.85rem; color:var(--muted);">Kokonaisvalmis</div>
                                    </div>
                                    <div class="season-progress-grid">
                                        <div class="col">
                                            <div class="progress-label" style="margin-bottom:8px; color:var(--muted); font-weight:700">Runkosarja</div>
                                            <progress-bar :value="regularProgressPercent" :max="100" :show-shimmer="true" :label="regularPlayed + '/' + regularScheduled + ' · ' + regularProgressPercent + '%'" :show-percentage="true"></progress-bar>
                                        </div>

                                        <div class="col">
                                            <div class="progress-label" style="margin-bottom:8px; color:var(--muted); font-weight:700">Playoffs</div>
                                            <progress-bar :value="playoffProgressPercent" :max="100" :show-shimmer="true" :label="playoffPlayed + '/' + playoffScheduled + ' · ' + playoffProgressPercent + '%'" :show-percentage="true"></progress-bar>
                                        </div>
                                    </div>
                                </div>
                            </div>

                            <!-- Division Links -->
                            <div class="division-links home-section" style="margin-top: 32px;">
                                <h2 class="section-title">Divisioonat</h2>
                                <div class="division-grid">
                                    <router-link
                                        v-for="div in seasonDivisions"
                                        :key="div.championship_id"
                                        :to="'/division/' + div.championship_id"
                                        class="division-link-card"
                                    >
                                            <div class="division-name">{{ formatDivisionName(div.name) }}</div>

                                        <div class="division-progress-row vertical">
                                            <!-- Fallback CSS-only progress bar to ensure visibility even if the Vue component isn't registered -->
                                            <div class="progress-bar progress-base division-inline-bar" :style="{ height: '12px' }" role="progressbar" :aria-valuemin="0" :aria-valuemax="100" :aria-valuenow="getProgress(div)">
                                                <div class="progress-fill" :style="{ width: getProgress(div) + '%' }"></div>
                                            </div>
                                            <div class="division-percent">{{ getProgress(div) }}%</div>
                                            <div class="played-count">{{ div.played_matches }} / {{ div.total_matches }} pelattua</div>
                                        </div>

                                        <div class="division-extra">
                                            <div class="teams-count">👥 {{ div.teams_count }} joukkuetta</div>
                                        </div>
                                        <div class="division-footer">
                                            <div class="division-card-footer">
                                                <!-- Show updated badge only if division is not completed -->
                                                <div v-if="getProgress(div) < 100" class="badge badge-updated">Päivitetty {{ formatDate(div.last_updated) }}</div>
                                                <div v-else class="badge badge-finished">Taputeltu loppuun</div>
                                            </div>
                                        </div>
                                    </router-link>
                                </div>
                            </div>
                        </div>
                    </section>
                </div>
            </div>
        </div>
    `,
    data() {
        return {
            loading: true, // Start with loading true to fetch real data
            error: null,
            // Real data from API (no mock fallbacks)
            overview: null,
            seasons: [],
            selectedSeason: null,
            currentSeason: null,
            seasonStats: null,
            seasonDivisions: [],
            topByRating: [],
            topByKD: [],
            topByADR: []
        };
    },
    computed: {
        heroCardsData() {
            return [
                {
                    title: 'Armafinland',
                    subtitle: 'Yhteisö on avoin kaikille pelaajille ja ryhmille, jotka haluavat kokeilla taktista pelaamista myös Arma-sarjan peleissä. Pelaamme Arma 3 ja Arma Reforger, sekä järjestämme kansainvälisiä TvT-tehtäviä, joissa painotetaan realismia, joukkuepeliä ja yhteistoimintaa. Pelien ulkopuolella meno on rentoa ja mutkatonta, mutta pelissä otetaan tehtävät tosissaan.',
                    logoUrl: 'https://armafinland.fi/logot/images/armafin-logo-200px.png',
                    primaryText: 'Liity AFI Discord',
                    primaryUrl: 'https://armafinland.fi/discord',
                    secondaryText: 'Lue lisää',
                    secondaryUrl: 'https://armafinland.fi/',
                    target: '_blank',
                    variant: 'afi'
                },
                {
                    title: 'Pappaliiga',
                    subtitle: 'Pappaliigan tarkoituksena on tarjota varttuneemmalle väelle mahdollisuus kilpapelaamiseen; tosissaan ja "ei niin tosissaan".',
                    logoUrl: '/static/pappaliiga-logo-white-bg.png',
                    primaryText: 'Liity Pappaliiga Discord',
                    primaryUrl: 'https://discord.gg/qbySKpAYch',
                    secondaryText: 'Pappaliiga.fi',
                    secondaryUrl: 'https://pappaliiga.fi/',
                    target: '_blank',
                    variant: 'pappaliiga'
                }
            ];
        },

        // NOTE: getProgress and formatDate moved to methods because they need parameters

        
        
        allTimeStats() {
            if (!this.overview) return [];
            return [
                { icon: '🎲', label: 'Divisioonaa', value: this.overview.total_divisions || 0 },
                { icon: '👥', label: 'Joukkuetta', value: this.overview.total_teams || 0 },
                { icon: '👤', label: 'Pelaajaa', value: this.overview.total_players || 0 },
                { icon: '⚔️', label: 'Ottelua Pelattu', value: this.overview.total_matches || 0 },
                { icon: '🗺️', label: 'Karttaa Pelattu', value: this.overview.total_maps_played || 0 },
                { icon: '🔄', label: 'Kierrosta Pelattu', value: this.overview.total_rounds || 0 },
                { icon: '💀', label: 'Tappoja', value: this.overview.total_kills || 0 },
                { icon: '☠️', label: 'Kuolemia', value: this.overview.total_deaths || 0 }
            ];
        },
        seasonsData() {
            return this.seasons.map(s => ({
                season: s.season,
                label: `Season ${s.season}`,
                status: s.is_current ? '(Käynnissä)' : '(Loppunut)'
            }));
        },
        seasonStatsData() {
            if (!this.seasonStats) return [];
            // compute regular vs playoff percentage
            const reg = Number(this.seasonStats.regular_matches || 0);
            const po = Number(this.seasonStats.playoff_matches || 0);
            const total = reg + po;
            const regPercent = total > 0 ? Math.round((reg / total) * 100) : 0;

            return [
                { icon: '🎲', label: 'Divisioonaa', value: this.seasonStats.divisions },
                { icon: '👥', label: 'Joukkuetta', value: this.seasonStats.teams },
                { icon: '👤', label: 'Pelaajaa', value: this.seasonStats.players },
                { icon: '⚔️', label: 'Ottelua', value: this.seasonStats.matches },
                { icon: '🗺️', label: 'Karttaa', value: this.seasonStats.maps },
                { icon: '🔄', label: 'Kierrosta pelattu', value: this.seasonStats.rounds_played },
                { icon: '💀', label: 'Tappoja', value: this.seasonStats.total_kills },
                { icon: '☠️', label: 'Kuolemia', value: this.seasonStats.total_deaths }
            ];
        },
        // Percentage of regular matches out of total (used for split bar)
        regularPercent() {
            const reg = Number(this.seasonStats?.regular_matches || 0);
            const po = Number(this.seasonStats?.playoff_matches || 0);
            const total = reg + po;
            return total > 0 ? Math.round((reg / total) * 100) : 0;
        },
        // Season overall progress: matches played vs total scheduled (regular + playoffs)
        seasonProgress() {
            const reg = Number(this.seasonStats?.regular_matches || 0);
            const po = Number(this.seasonStats?.playoff_matches || 0);
            const totalScheduled = reg + po;
            const played = Number(this.seasonStats?.matches || 0);
            return totalScheduled > 0 ? Math.round((played / totalScheduled) * 100) : 0;
        },
        // Regular-season counts and percent
        regularScheduled() {
            return Number(this.seasonStats?.regular_matches || 0);
        },
        regularPlayed() {
            return Number(this.seasonStats?.played_regular_matches || 0);
        },
        regularMissing() {
            return Math.max(0, this.regularScheduled - this.regularPlayed);
        },
        regularProgressPercent() {
            return this.regularScheduled > 0 ? Math.round((this.regularPlayed / this.regularScheduled) * 100) : 0;
        },
        // Playoffs counts and percent
        playoffScheduled() {
            return Number(this.seasonStats?.playoff_matches || 0);
        },
        playoffPlayed() {
            return Number(this.seasonStats?.played_playoff_matches || 0);
        },
        playoffMissing() {
            return Math.max(0, this.playoffScheduled - this.playoffPlayed);
        },
        playoffProgressPercent() {
            return this.playoffScheduled > 0 ? Math.round((this.playoffPlayed / this.playoffScheduled) * 100) : 0;
        },
        // No client-side search; division list rendered directly
        topPlayersCategories() {
            return [
                {
                    title: 'Rating',
                    subtitle: 'Parhaat pelaajat ratingin mukaan',
                    leaders: this.topByRating.map((p, idx) => ({
                        rank: idx + 1,
                        playerName: p.nickname,
                        teamName: p.team_name,
                        teamLogo: p.team_logo,
                        value: p.stat_value.toFixed(2)
                    }))
                },
                {
                    title: 'K/D Ratio',
                    subtitle: 'Parhaat pelaajat K/D-suhteen mukaan',
                    leaders: this.topByKD.map((p, idx) => ({
                        rank: idx + 1,
                        playerName: p.nickname,
                        teamName: p.team_name,
                        teamLogo: p.team_logo,
                        value: p.stat_value.toFixed(2)
                    }))
                },
                {
                    title: 'ADR',
                    subtitle: 'Parhaat pelaajat keskivahingon mukaan',
                    leaders: this.topByADR.map((p, idx) => ({
                        rank: idx + 1,
                        playerName: p.nickname,
                        teamName: p.team_name,
                        teamLogo: p.team_logo,
                        value: p.stat_value.toFixed(1)
                    }))
                }
            ];
        }
    },
    async mounted() {
        await this.loadData();
    },
    methods: {
        async loadData() {
            this.loading = true;
            this.error = null;
            
            try {
                // Fetch all home page data in parallel
                const [overview, seasons, topRating, topKD, topADR] = await Promise.all([
                    window.apiClient.getStatsOverview(),
                    window.apiClient.getSeasons(),
                    window.apiClient.getTopPlayers('rating', { limit: 10, min_maps: 5 }),
                    window.apiClient.getTopPlayers('kd', { limit: 10, min_maps: 5 }),
                    window.apiClient.getTopPlayers('adr', { limit: 10, min_maps: 5 })
                ]);
                
                this.overview = overview;
                this.seasons = seasons;
                this.topByRating = topRating;
                this.topByKD = topKD;
                this.topByADR = topADR;
                
                // Select current season by default
                const currentSeasonData = seasons.find(s => s.is_current);
                if (currentSeasonData) {
                    this.currentSeason = currentSeasonData.season;
                    this.selectedSeason = this.currentSeason;
                } else if (seasons.length > 0) {
                    // Fallback to first season if no current season marked
                    this.currentSeason = seasons[0].season;
                    this.selectedSeason = this.currentSeason;
                }
                
                if (this.selectedSeason) {
                    await this.loadSeasonStats(this.selectedSeason);
                }
            } catch (err) {
                this.error = err.message || 'Tilastojen lataus epäonnistui';
                console.error('HomeView loadData error:', err);
            } finally {
                this.loading = false;
            }
        },

        async handleSeasonChange(season) {
            console.log('Season changed to:', season);
            this.selectedSeason = season;
            await this.loadSeasonStats(season);
        },

        async loadSeasonStats(season) {
            try {
                // Get season-specific data: divisions and stats
                const [divisions, seasonStatsData, topRating, topKD, topADR] = await Promise.all([
                    window.apiClient.getDivisionsBySeason(season),
                    window.apiClient.getSeasonStats(season),
                    window.apiClient.getTopPlayers('rating', { season, limit: 10, min_maps: 5 }),
                    window.apiClient.getTopPlayers('kd', { season, limit: 10, min_maps: 5 }),
                    window.apiClient.getTopPlayers('adr', { season, limit: 10, min_maps: 5 })
                ]);
                
                // Filter to regular season divisions only (not playoffs)
                this.seasonDivisions = divisions.filter(d => !d.is_playoff);
                this.seasonStats = seasonStatsData;
                
                // Update top players for this season
                this.topByRating = topRating;
                this.topByKD = topKD;
                this.topByADR = topADR;
                
                console.log(`Loaded stats for season ${season}`);
            } catch (err) {
                console.error('Failed to load season stats:', err);
                // Don't throw - allow partial page load
            }
        }
        ,
        getProgress(div) {
            const played = Number(div.played_matches || 0);
            const total = Number(div.total_matches || 0);
            return total > 0 ? Math.round((played / total) * 100) : 0;
        },

        // Format division name by stripping trailing season token like ' S11' or ' S9'
        formatDivisionName(name) {
            if (!name) return '';
            // Remove trailing ' S' + digits (case-insensitive) and any surrounding whitespace
            return name.replace(/\s+S\d+$/i, '').trim();
        },

        formatDate(iso) {
            if (!iso) return '-';
            try {
                const d = new Date(iso);
                return d.toLocaleString('fi-FI', { year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' });
            } catch (e) { return iso; }
        }
    }
};

