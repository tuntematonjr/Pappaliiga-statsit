
const TEAM_PLAYER_COLUMNS = [
    { key: 'nickname', label: 'Pelaaja', sortable: true, align: 'left', colClass: 'col-name' },
    { key: 'maps', label: 'Kartat', sortable: true, numeric: true },
    { key: 'rounds', label: 'Erät', sortable: true, numeric: true },
    { key: 'rating', label: 'Rating', sortable: true, numeric: true, decimals: 2 },
    { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2 },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1 },
    { key: 'kr', label: 'K/R', sortable: true, numeric: true, decimals: 2 },
    { key: 'hs_percent', label: 'HS%', sortable: true, numeric: true, decimals: 1 },
    { key: 'damage', label: 'Damage', sortable: true, numeric: true },
    { key: 'assists', label: 'Assistit', sortable: true, numeric: true },
    { key: 'clutches', label: 'Clutchit', sortable: true, numeric: true },
    { key: 'utility', label: 'Utility dmg', sortable: true, numeric: true }
];

const TEAM_MAP_COLUMNS = [
    { key: 'map_name', label: 'Kartta', sortable: true, align: 'left', colClass: 'col-map-name' },
    { key: 'maps_played', label: 'Ottelut', sortable: true, numeric: true },
    { key: 'wins', label: 'Voitot', sortable: true, numeric: true },
    { key: 'losses', label: 'Tappiot', sortable: true, numeric: true },
    { key: 'win_rate', label: 'Voitto%', sortable: true, numeric: true, format: value => `${value.toFixed(1)} %` },
    { key: 'rating', label: 'Rating', sortable: true, numeric: true, decimals: 2 },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1 },
    { key: 'clutches', label: 'Clutchit', sortable: true, numeric: true }
];

window.TeamDetail = {
    name: 'TeamDetail',
    props: {
        teamId: { type: [String, Number], required: true },
        championshipId: { type: [String, Number], default: null }
    },
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; },
        get SplitBar() { return window.SplitBar; },
        get ProgressBar() { return window.ProgressBar; },
        get SortableTable() { return window.SortableTable; },
        get MapStatsTable() { return window.MapStatsTable; },
        get TeamMatches() { return window.TeamMatches; },
        get CopyLink() { return window.CopyLink; }
    },
    data() {
        return {
            loading: true,
            error: null,
            seasons: [],
            selectedChampionship: this.championshipId ? String(this.championshipId) : null,
            team: null,
            teamStats: null,
            players: [],
            mapStats: [],
            mapStatsLoading: false,
            mapStatsError: null,
            matches: [],
            matchesLoading: false,
            matchesError: null,
            defaultAvatar: '/static/pappaliiga-logo-white-bg.png'
        };
    },
    computed: {
        activeChampionship() {
            return this.selectedChampionship;
        },
        seasonOptions() {
            return (this.seasons || []).map(season => ({
                value: String(season.championship_id || season.id || season.championshipId || ''),
                label: season.name || season.season_name || `Kausi ${season.season}`,
                isCurrent: Boolean(season.is_current || season.isCurrent)
            })).filter(option => option.value);
        },
        displayName() {
            return this.team?.display_name || this.team?.team_name || this.team?.name || 'Tuntematon joukkue';
        },
        logoUrl() {
            const src = this.team?.logo || this.team?.avatar || this.team?.team_logo;
            return this.ensureAvatar(src);
        },
        faceitUrl() {
            return this.team?.faceit_url || this.team?.faceit || this.team?.links?.faceit;
        },
        summaryStats() {
            const stats = this.teamStats || {};
            const wins = Number(stats.wins ?? stats.maps_won ?? 0);
            const losses = Number(stats.losses ?? stats.maps_lost ?? 0);
            const matches = Number(stats.matches ?? stats.matches_played ?? stats.series_played ?? 0);
            const roundsDiff = Number(stats.rounds_diff ?? stats.round_diff ?? stats.rounds_delta ?? 0);
            const winRate = matches > 0 ? (wins / matches) * 100 : (stats.win_rate ?? 0);
            return {
                wins,
                losses,
                matches,
                roundsDiff,
                roundsDiffDisplay: roundsDiff > 0 ? `+${roundsDiff}` : `${roundsDiff}`,
                winRate: Number.isFinite(winRate) ? winRate : 0,
                rating: this.safeNumber(stats.rating ?? stats.rating_2 ?? stats.hltv_rating),
                kd: this.safeNumber(stats.kd ?? stats.kd_ratio),
                adr: this.safeNumber(stats.adr ?? stats.average_damage),
                hs: this.safeNumber(stats.hs_percent ?? stats.headshot_percent ?? stats.hs)
            };
        },
        statEntries() {
            const stats = this.summaryStats;
            return [
                { label: 'Ottelut', value: stats.matches },
                { label: 'Eräero', value: stats.roundsDiffDisplay },
                { label: 'Voitto%', value: `${stats.winRate.toFixed(1)} %` },
                { label: 'Rating', value: stats.rating.toFixed(2) },
                { label: 'K/D', value: stats.kd.toFixed(2) },
                { label: 'ADR', value: stats.adr.toFixed(1) },
                { label: 'HS%', value: `${stats.hs.toFixed(1)} %` }
            ];
        },
        playerTableColumns() {
            return TEAM_PLAYER_COLUMNS;
        },
        playerTableRows() {
            if (!Array.isArray(this.players)) return [];
            return this.players.map((player, index) => {
                const maps = Number(player.maps ?? player.maps_played ?? player.map_count ?? 0);
                const rounds = Number(player.rounds ?? player.rounds_played ?? 0);
                const rating = this.safeNumber(player.rating ?? player.rating_2 ?? player.hltv_rating ?? 0);
                const kd = this.safeNumber(player.kd ?? player.kd_ratio ?? 0);
                const adr = this.safeNumber(player.adr ?? player.average_damage ?? 0);
                const kr = this.safeNumber(player.kr ?? player.kills_per_round ?? 0);
                const hs = this.safeNumber(player.hs_percent ?? player.headshot_percent ?? 0);
                const damage = this.safeNumber(player.damage ?? player.total_damage);
                const assists = this.safeNumber(player.assists);
                const clutches = this.safeNumber(player.clutches ?? player.clutch_wins);
                const utility = this.safeNumber(player.utility_damage ?? player.utility);
                return {
                    id: player.player_id || player.id || `player-${index}`,
                    nickname: player.nickname || player.player_name || player.name || 'Tuntematon',
                    maps,
                    rounds,
                    rating,
                    kd,
                    adr,
                    kr,
                    hs_percent: hs,
                    damage,
                    assists,
                    clutches,
                    utility
                };
            });
        },
        mapColumns() {
            return TEAM_MAP_COLUMNS;
        },
        mapStatsTableData() {
            if (!Array.isArray(this.mapStats)) return [];
            return this.mapStats.map((stats, index) => {
                const base = stats.curr || stats;
                const matches = Number(base.matches ?? base.maps ?? base.maps_played ?? 0);
                const wins = Number(base.wins ?? base.maps_won ?? 0);
                const losses = Number(base.losses ?? base.maps_lost ?? 0);
                const winRate = matches > 0 ? (wins / matches) * 100 : this.safeNumber(base.win_rate);
                const rating = this.safeNumber(base.rating ?? base.rating_2 ?? base.hltv_rating);
                const adr = this.safeNumber(base.adr);
                const clutches = this.safeNumber(base.clutches ?? base.clutch_wins);
                const mapName = stats.map_name || base.map_name || stats.map || 'Kartta';
                const logo = base.logo || stats.logo || base.image || null;
                return {
                    map_name: mapName,
                    curr: {
                        map_name: mapName,
                        maps_played: matches,
                        wins,
                        losses,
                        win_rate: winRate,
                        rating,
                        adr,
                        clutches,
                        logo
                    }
                };
            });
        },
        hasPlayers() {
            return this.playerTableRows.length > 0;
        },
        hasMapStats() {
            return this.mapStatsTableData.length > 0;
        }
    },
    watch: {
        championshipId: {
            immediate: false,
            handler(newVal) {
                if (newVal && String(newVal) !== this.selectedChampionship) {
                    this.selectedChampionship = String(newVal);
                }
                if (!newVal) {
                    this.selectedChampionship = null;
                    this.loadTeamData();
                }
            }
        },
        selectedChampionship(newVal, oldVal) {
            if (newVal && newVal !== oldVal) {
                this.loadTeamData();
            }
        }
    },
    async mounted() {
        await this.loadSeasons();
    },
    methods: {
        async loadSeasons() {
            this.loading = true;
            this.error = null;
            try {
                const seasons = await window.apiClient.getTeamSeasons(this.teamId);
                this.seasons = Array.isArray(seasons) ? seasons : [];
                const initialSelection = this.selectedChampionship;
                if (!this.selectedChampionship && this.seasons.length) {
                    const current = this.seasons.find(season => season.is_current) || this.seasons[0];
                    if (current) {
                        this.selectedChampionship = String(current.championship_id || current.championshipId || current.id);
                    }
                }
                if (this.selectedChampionship && this.selectedChampionship === initialSelection) {
                    await this.loadTeamData();
                }
            } catch (err) {
                console.error('TeamDetail seasons fetch failed', err);
                this.error = err?.message || 'Joukkueen kausien haku epäonnistui';
                this.loading = false;
            }
        },
        async loadTeamData() {
            this.loading = true;
            this.error = null;
            try {
                if (this.selectedChampionship) {
                    const details = await window.apiClient.getTeamDetails(this.selectedChampionship, this.teamId);
                    this.team = details.team || details;
                    this.teamStats = details.stats || details.team_stats || details;
                    this.players = details.players || details.roster || [];
                    await Promise.all([
                        this.loadMapStats(),
                        this.loadMatches()
                    ]);
                } else {
                    const info = await window.apiClient.getTeamInfo(this.teamId);
                    this.team = info;
                    this.teamStats = info.stats || info;
                    this.players = info.players || info.roster || [];
                    this.mapStats = [];
                    this.matches = [];
                }
            } catch (err) {
                console.error('TeamDetail load failed', err);
                this.error = err?.message || 'Joukkueen tietojen haku epäonnistui';
            } finally {
                this.loading = false;
            }
        },
        async loadMapStats() {
            this.mapStatsLoading = true;
            this.mapStatsError = null;
            try {
                const maps = await window.apiClient.getTeamMapStats(this.teamId, this.selectedChampionship);
                this.mapStats = Array.isArray(maps) ? maps : [];
            } catch (err) {
                console.error('TeamDetail map stats failed', err);
                this.mapStatsError = err?.message || 'Karttatilastojen haku epäonnistui';
                this.mapStats = [];
            } finally {
                this.mapStatsLoading = false;
            }
        },
        async loadMatches() {
            this.matchesLoading = true;
            this.matchesError = null;
            try {
                const matches = await window.apiClient.getTeamMatches(this.selectedChampionship, this.teamId);
                this.matches = Array.isArray(matches) ? matches : [];
            } catch (err) {
                console.error('TeamDetail matches failed', err);
                this.matchesError = err?.message || 'Otteluiden haku epäonnistui';
                this.matches = [];
            } finally {
                this.matchesLoading = false;
            }
        },
        handleSeasonChange(event) {
            const value = event?.target?.value;
            if (value && value !== this.selectedChampionship) {
                this.selectedChampionship = value;
            }
        },
        ensureAvatar(src) {
            if (!src) return this.defaultAvatar;
            try {
                return window.apiClient.proxyAvatar(src);
            } catch (err) {
                return src || this.defaultAvatar;
            }
        },
        formatNumber(value, decimals = 2) {
            const numeric = this.safeNumber(value);
            return numeric.toFixed(decimals);
        },
        safeNumber(value) {
            const numeric = Number(value);
            if (Number.isFinite(numeric)) return numeric;
            if (value === null || value === undefined) return 0;
            const parsed = Number(String(value).replace(',', '.'));
            return Number.isFinite(parsed) ? parsed : 0;
        }
    },
    template: `
        <div class="team-detail">
            <loading-spinner v-if="loading && !team" message="Joukkuetta ladataan..."></loading-spinner>
            <error-message v-else-if="error" :message="error" @retry="loadTeamData"></error-message>

            <div v-else class="team-detail-content">
                <section class="team-card card">
                    <div class="team-card-left">
                        <img class="team-logo-large" :src="logoUrl" :alt="displayName" loading="lazy" />
                        <div class="team-meta">
                            <h1>{{ displayName }}</h1>
                            <div class="team-tags">
                                <span class="team-tag" v-if="team?.country">{{ team.country }}</span>
                                <span class="team-tag" v-if="team?.division_name">{{ team.division_name }}</span>
                            </div>
                            <div class="team-season-select" v-if="seasonOptions.length">
                                <label for="team-season">Kausi</label>
                                <select id="team-season" :value="selectedChampionship" @change="handleSeasonChange">
                                    <option v-for="option in seasonOptions" :value="option.value" :key="option.value">
                                        {{ option.label }}<span v-if="option.isCurrent"> (nykyinen)</span>
                                    </option>
                                </select>
                            </div>
                        </div>
                    </div>
                    <div class="team-card-actions">
                        <a v-if="faceitUrl" class="btn btn-primary" :href="faceitUrl" target="_blank" rel="noopener">
                            Faceit
                        </a>
                        <copy-link :label="'Kopioi sivun linkki'" class="btn btn-ghost"></copy-link>
                    </div>
                </section>

                <section class="team-summary card">
                    <header class="card-head">
                        <h2 class="title">Joukkueen yhteenveto</h2>
                    </header>
                    <div class="card-content">
                        <split-bar
                            :wins="summaryStats.wins"
                            :losses="summaryStats.losses"
                            height="36px"
                            :show-percent="true"
                        ></split-bar>
                        <div class="team-stat-grid">
                            <div v-for="entry in statEntries" :key="entry.label" class="team-stat-item">
                                <div class="team-stat-label">{{ entry.label }}</div>
                                <div class="team-stat-value">{{ entry.value }}</div>
                            </div>
                        </div>
                    </div>
                </section>

                <section class="team-players card">
                    <header class="card-head">
                        <h2 class="title">Pelaajat</h2>
                    </header>
                    <div class="card-content">
                        <sortable-table
                            v-if="hasPlayers"
                            :columns="playerTableColumns"
                            :data="playerTableRows"
                            :default-sort="{ column: 'rating', order: 'desc', numeric: true }"
                            :compact="false"
                        >
                            <template #cell-hs_percent="{ row }">
                                <span>{{ row.hs_percent.toFixed(1) }} %</span>
                            </template>
                            <template #cell-rating="{ row }">
                                <span>{{ row.rating.toFixed(2) }}</span>
                            </template>
                            <template #cell-kd="{ row }">
                                <span>{{ row.kd.toFixed(2) }}</span>
                            </template>
                            <template #cell-adr="{ row }">
                                <span>{{ row.adr.toFixed(1) }}</span>
                            </template>
                            <template #cell-kr="{ row }">
                                <span>{{ row.kr.toFixed(2) }}</span>
                            </template>
                        </sortable-table>
                        <p v-else class="muted">Ei pelaajatietoja saatavilla.</p>
                    </div>
                </section>

                <section class="team-maps card">
                    <header class="card-head">
                        <h2 class="title">Karttamenestys</h2>
                    </header>
                    <div class="card-content">
                        <loading-spinner v-if="mapStatsLoading" message="Karttatilastoja ladataan..."></loading-spinner>
                        <error-message v-else-if="mapStatsError" :message="mapStatsError" @retry="loadMapStats"></error-message>
                        <map-stats-table
                            v-else
                            :map-stats="mapStatsTableData"
                            :columns-config="mapColumns"
                            :colorize-columns-config="['rating', 'adr', 'win_rate']"
                        ></map-stats-table>
                    </div>
                </section>

                <section class="team-matches-section">
                    <team-matches
                        :matches="matches"
                        :loading="matchesLoading"
                        :error="matchesError"
                    ></team-matches>
                </section>
            </div>
        </div>
    `
};
