
const DIVISION_MAP_COLUMNS = [
    { key: 'map_name', label: 'Kartta', sortable: true, align: 'left', colClass: 'col-name col-map-name' },
    { key: 'maps_played', label: 'Pelattu', sortable: true, numeric: true },
    { key: 'banned', label: 'Bannattu', sortable: true, numeric: true },
    { key: 'rounds_played', label: 'Erät', sortable: true, numeric: true },
    { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2 },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1 },
    { key: 'clutches', label: 'Clutchit', sortable: true, numeric: true },
    { key: 'sniper_kills', label: 'AWP-tap.', sortable: true, numeric: true },
    { key: 'pistol_kills', label: 'Pistoolitap.', sortable: true, numeric: true }
];

const DIVISION_STATS_SCHEMA = [
    { key: ['teams.length', 'team_count', 'aggregates.team_count'], label: 'Joukkueita', icon: null },
    { key: ['player_count', 'aggregates.player_count', 'aggregates.total_players'], label: 'Pelaajia', icon: null },
    { key: ['aggregates.maps_played_total', 'maps_played_total', 'maps_played'], label: 'Karttoja pelattu', icon: null },
    { key: ['aggregates.rounds_played_total', 'rounds_played_total', 'rounds_played'], label: 'Erät', icon: null },
    { key: ['aggregates.median_adr', 'median_adr'], label: 'Median ADR', icon: null, decimals: 1 },
    { key: ['aggregates.total_kills', 'total_kills'], label: 'Kills', icon: null },
    { key: ['aggregates.total_deaths', 'total_deaths'], label: 'Deaths', icon: null },
    { key: ['aggregates.median_survival', 'median_survival'], label: 'Selviytyminen', icon: null, percent: true, decimals: 1 }
];

const DIVISION_LEADER_CONFIG = [
    { key: 'rating', title: 'Rating-koneet', subtitle: 'Rating 2.0', stat: 'rating', group: 'results', decimals: 2 },
    { key: 'kd', title: 'K/D kuninkaat', subtitle: 'K/D-suhde', stat: 'kd', group: 'results', decimals: 2 },
    { key: 'winrate', title: 'Voittoprosentti', subtitle: 'Otteluvoitto-%', stat: 'winrate', group: 'results', percent: true, decimals: 1 },
    { key: 'clutches', title: 'Ratkaisijat', subtitle: 'Voitetut clutchit', stat: 'clutch_wins', group: 'results' },
    { key: 'mvp', title: 'MVP-pisteet', subtitle: 'MVP:t', stat: 'mvps', group: 'results' },
    { key: 'adr', title: 'ADR-tykit', subtitle: 'Keski-dmg', stat: 'adr', group: 'offense', decimals: 1 },
    { key: 'kr', title: 'Tapot / erä', subtitle: 'K/R', stat: 'kr', group: 'offense', decimals: 2 },
    { key: 'kills', title: 'Tapokoneet', subtitle: 'Killit', stat: 'kills', group: 'offense' },
    { key: 'damage', title: 'Dmg-jyrät', subtitle: 'Damage', stat: 'damage', group: 'offense' },
    { key: 'hs', title: 'HS%-kuninkaat', subtitle: 'Headshot-%', stat: 'hs_percent', group: 'offense', percent: true, decimals: 1 },
    { key: 'multi', title: 'Monitapot', subtitle: 'Multi-killit', stat: 'multi_kills', group: 'offense' },
    { key: 'entry', title: 'Entry-voitot', subtitle: 'Avaukset', stat: 'opening_kills', group: 'offense' },
    { key: 'sniper', title: 'AWP-jyrät', subtitle: 'Sniper-tapot', stat: 'sniper_kills', group: 'utility' },
    { key: 'pistol', title: 'Pistoolisankarit', subtitle: 'Pistoolitapot', stat: 'pistol_kills', group: 'utility' },
    { key: 'utility_dmg', title: 'Utility-vaikuttajat', subtitle: 'Utility damage', stat: 'utility_damage', group: 'utility' },
    { key: 'flash', title: 'Flash-tuki', subtitle: 'Flash-assistit', stat: 'flash_assists', group: 'utility' },
    { key: 'assists', title: 'Syöttäjät', subtitle: 'Assistit', stat: 'assists', group: 'utility' },
    { key: 'survival', title: 'Selviytyjät', subtitle: 'Selviytymis-%', stat: 'survival_rate', group: 'utility', percent: true, decimals: 1 },
    { key: 'support', title: 'Tukipelaajat', subtitle: 'Support pisteet', stat: 'support', group: 'utility' },
    { key: 'trade', title: 'Trade-osaajat', subtitle: 'Trade-killit', stat: 'trade_kills', group: 'utility' }
];

const LEADER_GROUP_TITLES = {
    results: 'Tulokset & Plussat',
    offense: 'Offense',
    utility: 'Utility'
};

window.DivisionView = {
    name: 'DivisionView',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; },
        get TeamNav() { return window.TeamNav; },
        get TeamComparisonBoard() { return window.TeamComparisonBoard; },
        get MapsStats() { return window.MapsStats; },
        get LeadersNew() { return window.LeadersNew; },
        get CopyLink() { return window.CopyLink; },
        get SortableTable() { return window.SortableTable; },
        get ProgressBar() { return window.ProgressBar; },
        get StatsGrid() { return window.StatsGrid; }
    },
    data() {
        return {
            loading: false,
            error: null,
            division: null,
            mapStats: [],
            mapStatsError: null,
            leaderGroupsData: [],
            leadersError: null,
            standingsRows: [],
            matches: [],
            matchesLoading: false,
            matchesError: null,
            progress: {
                played: 0,
                total: 0,
                percent: 0
            },
            statsCards: [],
            mapColumns: DIVISION_MAP_COLUMNS,
            standingsColumns: [
                { key: 'rank', label: '#', sortable: true, numeric: true, colClass: 'col-rank' },
                { key: 'team', label: 'Joukkue', sortable: true, align: 'left', colClass: 'col-team' },
                { key: 'matches', label: 'Ottelut', sortable: true, numeric: true },
                { key: 'wins', label: 'Voitot', sortable: true, numeric: true },
                { key: 'losses', label: 'Tappiot', sortable: true, numeric: true },
                { key: 'round_diff', label: 'Erä-ero', sortable: true, numeric: true },
                { key: 'win_rate', label: 'Voitto%', sortable: true, numeric: true },
                { key: 'streak', label: 'Vire', sortable: false }
            ]
        };
    },
    computed: {
        championshipId() {
            return this.$route.params.championshipId || this.$route.query.championship || null;
        },
        divisionTitle() {
            if (!this.division) return '';
            return this.stripSeasonSuffix(this.division.name || this.division.title || '');
        },
        seasonLabel() {
            if (!this.division) return '';
            return this.division.season_name || `Kausi ${this.division.season || ''}`.trim();
        },
        divisionNumber() {
            return this.division ? (this.division.division_num || this.division.number || '') : '';
        },
        progressLabel() {
            return `${this.progress.played}/${this.progress.total} ottelua`;
        },
        progressPercent() {
            return this.progress.percent || 0;
        },
        statsGrid() {
            return this.buildStatsCards(this.division, DIVISION_STATS_SCHEMA);
        },
        teamsForComparison() {
            if (!this.division) return [];
            return this.buildTeamComparison(this.division.teams || []);
        },
        teamsForNav() {
            if (!this.division) return [];
            return (this.division.teams || []).map(team => ({
                team_id: team.team_id || team.id,
                display_name: team.display_name || team.team_name || team.name,
                team_name: team.team_name || team.name,
                avatar: this.ensureAvatar(team.logo || team.avatar || team.team_logo),
                slug: team.slug
            }));
        },
        standingsData() {
            return this.standingsRows;
        },
        matchList() {
            return this.matches;
        },
        leaderGroups() {
            return this.leaderGroupsData;
        }
    },
    watch: {
        '$route.params.championshipId'(next, prev) {
            if (next && next !== prev) {
                this.loadDivision();
            }
        }
    },
    async mounted() {
        await this.loadDivision();
    },
    methods: {
        async loadDivision() {
            if (!this.championshipId) {
                this.error = 'Divisioona puuttuu reitistä';
                return;
            }
            this.loading = true;
            this.error = null;
            this.mapStatsError = null;
            this.leadersError = null;
            try {
                const division = await window.apiClient.getDivisionById(this.championshipId);
                this.division = division;
                this.progress = this.extractProgress(division);
                this.statsCards = this.buildStatsCards(division, DIVISION_STATS_SCHEMA);
                this.standingsRows = this.buildStandings(division);

                if (Array.isArray(division.map_stats) && division.map_stats.length) {
                    this.mapStats = division.map_stats;
                } else {
                    try {
                        this.mapStats = await window.apiClient.getDivisionMapStats(this.championshipId);
                    } catch (mapErr) {
                        console.warn('Division map stats fetch failed', mapErr);
                        this.mapStats = [];
                        this.mapStatsError = mapErr && mapErr.message ? mapErr.message : 'Karttatilastot puuttuvat';
                    }
                }

                this.leaderGroupsData = this.buildLeaderGroups(division.leaders);
                if (!this.leaderGroupsData.length) {
                    const fallback = await this.fetchFallbackLeaders(this.championshipId, division.season);
                    this.leaderGroupsData = fallback;
                    if (!fallback.length) {
                        this.leadersError = 'Johtajalistat eivät ole saatavilla tälle divisioonalle';
                    }
                }

                this.matchesLoading = true;
                try {
                    const matches = await window.apiClient.getDivisionMatches(this.championshipId);
                    this.matches = Array.isArray(matches) ? matches.slice(0, 8) : [];
                    this.matchesError = null;
                } catch (matchErr) {
                    console.warn('Division matches fetch failed', matchErr);
                    this.matchesError = matchErr && matchErr.message ? matchErr.message : 'Ottelutietojen haku epäonnistui';
                } finally {
                    this.matchesLoading = false;
                }
            } catch (err) {
                console.error('Division load failed', err);
                this.error = err && err.message ? err.message : 'Divisioonan lataus epäonnistui';
            } finally {
                this.loading = false;
            }
        },
        buildStatsCards(source, schema) {
            if (!source || !schema) return [];
            return schema.map(def => {
                const rawValue = this.pickValue(source, def.key);
                return {
                    icon: def.icon,
                    label: def.label,
                    value: this.formatMetric(rawValue, def),
                    subtitle: def.subtitle || ''
                };
            });
        },
        pickValue(obj, keySpec) {
            if (!obj) return null;
            const keys = Array.isArray(keySpec) ? keySpec : [keySpec];
            for (const key of keys) {
                const segments = String(key).split('.');
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
                if (found && current !== undefined && current !== null) {
                    return current;
                }
            }
            return null;
        },
        formatMetric(value, options = {}) {
            if (value === undefined || value === null) return '–';
            let numeric = Number(value);
            if (!Number.isFinite(numeric)) {
                numeric = Number(String(value).replace(',', '.'));
            }
            if (!Number.isFinite(numeric)) {
                return value;
            }
            const decimals = options.decimals != null ? options.decimals : (numeric >= 100 ? 0 : 1);
            if (options.percent) {
                if (numeric <= 1 && numeric >= -1) {
                    numeric *= 100;
                }
                return `${numeric.toFixed(decimals)} %`;
            }
            const formatter = new Intl.NumberFormat('fi-FI', {
                minimumFractionDigits: decimals,
                maximumFractionDigits: decimals
            });
            return formatter.format(numeric);
        },
        extractProgress(division) {
            const played = Number(division?.played_matches || division?.matches_played || 0);
            const total = Number(division?.total_matches || division?.matches_total || 0);
            return {
                played,
                total,
                percent: total > 0 ? Math.round((played / total) * 1000) / 10 : 0
            };
        },
        buildTeamComparison(teams) {
            if (!Array.isArray(teams)) return [];
            return teams.map((team, idx) => {
                const matches = Number(team.matches_played ?? team.played ?? 0);
                const wins = Number(team.wins ?? team.maps_won ?? 0);
                const losses = Number(team.losses ?? team.maps_lost ?? 0);
                const winRate = this.safeNumber(team.win_rate ?? (matches ? (wins / matches) * 100 : 0));
                return {
                    team_id: team.team_id || team.id || `team-${idx}`,
                    name: team.display_name || team.team_name || team.name || 'Tuntematon joukkue',
                    logo: this.ensureAvatar(team.logo || team.avatar || team.team_logo),
                    matches_played: matches,
                    wins,
                    losses,
                    rounds_diff: Number(team.rounds_diff ?? team.round_diff ?? 0),
                    win_rate: winRate,
                    kd: this.safeNumber(team.kd),
                    adr: this.safeNumber(team.adr),
                    rating: this.safeNumber(team.rating ?? team.rating_2 ?? team.hltv_rating),
                    rank: team.rank || idx + 1
                };
            }).sort((a, b) => (b.rating || 0) - (a.rating || 0));
        },
        buildStandings(division) {
            const source = (division && division.standings) ? division.standings : (division && division.teams) ? division.teams : [];
            if (!Array.isArray(source)) return [];
            return source.map((team, idx) => {
                const matches = Number(team.matches_played ?? team.played ?? 0);
                const wins = Number(team.wins ?? team.maps_won ?? 0);
                const losses = Number(team.losses ?? team.maps_lost ?? 0);
                const streak = team.streak || team.current_streak || '';
                const winRate = this.safeNumber(team.win_rate ?? (matches ? (wins / matches) * 100 : 0));
                return {
                    id: team.team_id || team.id || `standing-${idx}`,
                    rank: Number(team.rank || idx + 1),
                    team_id: team.team_id || team.id,
                    team: team.display_name || team.team_name || team.name || 'Joukkue',
                    matches,
                    wins,
                    losses,
                    round_diff: Number(team.rounds_diff ?? team.round_diff ?? 0),
                    win_rate: `${winRate.toFixed(1)} %`,
                    win_rate_value: winRate,
                    streak: streak || this.formatStreak(team.last_results)
                };
            });
        },
        formatStreak(result) {
            if (!result) return '–';
            if (typeof result === 'string') return result;
            if (Array.isArray(result)) {
                const recent = result.slice(0, 5).map(code => {
                    if (code === 'W' || code === 'V') return 'V';
                    if (code === 'L' || code === 'T') return 'T';
                    if (code === 'D') return 'R';
                    return '-';
                });
                return recent.join(' ');
            }
            return '–';
        },
        buildLeaderGroups(raw) {
            if (!Array.isArray(raw) || !raw.length) return [];
            const groups = {};
            raw.forEach(category => {
                if (!category) return;
                const statKey = category.statKey || category.stat_key || category.key || category.id;
                const groupKey = category.group || category.groupKey || this.resolveLeaderGroup(statKey);
                const title = category.title || category.categoryTitle || category.statName || 'Tilasto';
                const subtitle = category.subtitle || category.description || '';
                const groupTitle = LEADER_GROUP_TITLES[groupKey] || LEADER_GROUP_TITLES.results;
                const leaders = Array.isArray(category.leaders)
                    ? category.leaders.map((leader, index) => this.normalizeLeader(leader, index))
                    : [];
                if (!groups[groupKey]) {
                    groups[groupKey] = { title: groupTitle, items: [] };
                }
                groups[groupKey].items.push({
                    id: statKey || title,
                    title,
                    subtitle,
                    leaders
                });
            });
            return Object.values(groups).map(group => ({
                title: group.title,
                items: group.items.sort((a, b) => a.title.localeCompare(b.title, 'fi'))
            }));
        },
        resolveLeaderGroup(statKey) {
            if (!statKey) return 'results';
            const config = DIVISION_LEADER_CONFIG.find(item => item.stat === statKey || item.key === statKey);
            return config ? config.group : 'results';
        },
        normalizeLeader(leader, index) {
            if (!leader) return {
                playerName: `Tuntematon ${index + 1}`,
                teamName: '',
                value: '–',
                teamLogo: '/static/pappaliiga-logo-white-bg.png'
            };
            const name = leader.nickname || leader.playerName || leader.player || leader.name || `Tuntematon ${index + 1}`;
            const team = leader.team || leader.teamName || leader.team_name || '';
            const value = leader.value ?? leader.stat_value ?? leader.total ?? leader.score ?? leader.number;
            const logo = leader.team_logo || leader.logo || leader.teamLogo || leader.avatar;
            return {
                playerName: name,
                teamName: team,
                value,
                teamLogo: this.ensureAvatar(logo)
            };
        },
        async fetchFallbackLeaders(championshipId, season) {
            const config = DIVISION_LEADER_CONFIG;
            const uniqueStats = [...new Set(config.map(cat => cat.stat))];
            const statResults = {};
            await Promise.all(uniqueStats.map(async stat => {
                try {
                    statResults[stat] = await window.apiClient.getTopPlayers(stat, { championship: championshipId, season, limit: 4, min_maps: 3 });
                } catch (err) {
                    console.warn('Division fallback leader fetch failed', stat, err);
                    statResults[stat] = [];
                }
            }));
            const groups = {};
            config.forEach(cat => {
                const leaders = (statResults[cat.stat] || []).map((player, index) => ({
                    playerName: player.nickname || player.playerName || player.player || `Tuntematon ${index + 1}`,
                    teamName: player.team_name || player.teamName || player.team || '',
                    value: this.formatFallbackValue(player.stat_value ?? player.value ?? player.total ?? player.score, cat),
                    teamLogo: this.ensureAvatar(player.team_logo || player.teamLogo || player.logo || player.avatar)
                }));
                if (!leaders.length) return;
                if (!groups[cat.group]) {
                    groups[cat.group] = { title: LEADER_GROUP_TITLES[cat.group] || LEADER_GROUP_TITLES.results, items: [] };
                }
                groups[cat.group].items.push({
                    id: cat.key,
                    title: cat.title,
                    subtitle: cat.subtitle || '',
                    leaders
                });
            });
            return Object.values(groups).map(group => ({
                title: group.title,
                items: group.items.sort((a, b) => a.title.localeCompare(b.title, 'fi'))
            }));
        },
        formatFallbackValue(value, config) {
            if (value === undefined || value === null) return '–';
            let numeric = Number(value);
            if (!Number.isFinite(numeric)) {
                numeric = Number(String(value).replace(',', '.'));
            }
            if (!Number.isFinite(numeric)) return value;
            if (config.percent) {
                if (numeric <= 1 && numeric >= -1) {
                    numeric *= 100;
                }
                return `${numeric.toFixed(config.decimals ?? 1)} %`;
            }
            if (config.decimals != null) {
                return numeric.toFixed(config.decimals);
            }
            return numeric >= 1000 ? Math.round(numeric).toString() : numeric.toFixed(2);
        },
        ensureAvatar(src) {
            if (!src) return '/static/pappaliiga-logo-white-bg.png';
            try {
                return window.apiClient.proxyAvatar(src);
            } catch (err) {
                return src;
            }
        },
        safeNumber(value) {
            const numeric = Number(value);
            if (Number.isFinite(numeric)) return numeric;
            const parsed = Number(String(value).replace(',', '.'));
            return Number.isFinite(parsed) ? parsed : 0;
        },
        stripSeasonSuffix(name) {
            if (!name) return '';
            return name.replace(/\s+S\d+$/i, '').trim();
        },
        formatDate(date) {
            if (!date) return '';
            try {
                return new Date(date).toLocaleDateString('fi-FI', {
                    year: 'numeric',
                    month: '2-digit',
                    day: '2-digit'
                });
            } catch (err) {
                return date;
            }
        },
        outcomeBadge(match) {
            if (!match) return { label: '–', tone: 'neutral' };
            const home = Number(match.home_score ?? match.home_rounds ?? match.score_home ?? 0);
            const away = Number(match.away_score ?? match.away_rounds ?? match.score_away ?? 0);
            if (home > away) return { label: `${match.home_team_short || 'Kotijoukkue'} voitti`, tone: 'win' };
            if (away > home) return { label: `${match.away_team_short || 'Vierasjoukkue'} voitti`, tone: 'loss' };
            return { label: 'Tasapeli', tone: 'draw' };
        }
    },
    template: `
        <div class="division-view">
            <div class="division-toolbar">
                <router-link class="chip" to="/">← Etusivu</router-link>
                <copy-link class="chip chip-ghost" :label="'Jaa divisioonalinkki'"></copy-link>
            </div>

            <loading-spinner v-if="loading" message="Divisioonaa ladataan..."></loading-spinner>
            <error-message v-else-if="error" :message="error" @retry="loadDivision"></error-message>

            <div v-else-if="division" class="division-content">
                <header class="division-header card">
                    <div class="header-main">
                        <div class="header-text">
                            <h1>{{ divisionTitle }}</h1>
                            <p class="subtitle">{{ seasonLabel }} · Divisioona {{ divisionNumber }}</p>
                        </div>
                        <div class="header-progress">
                            <div class="progress-chip">
                                <span class="chip-label">{{ progressLabel }}</span>
                                <span class="chip-value">{{ progressPercent.toFixed(1) }} %</span>
                            </div>
                            <progress-bar :value="progressPercent" :max="100" :show-percentage="false"></progress-bar>
                        </div>
                    </div>
                </header>

                <section class="division-stats home-section">
                    <header class="section-header">
                        <h2>Divisioonan tilastot</h2>
                    </header>
                    <stats-grid :stats="statsGrid"></stats-grid>
                </section>

                <section class="division-teams home-section">
                    <team-nav v-if="teamsForNav.length" :teams="teamsForNav" :championship-id="championshipId"></team-nav>
                    <team-comparison-board
                        :teams="teamsForComparison"
                        :loading="false"
                        title="Joukkuevertailu"
                        :subtitle="seasonLabel"
                        :default-sort="{ column: 'rating', order: 'desc', numeric: true }"
                    ></team-comparison-board>
                </section>

                <section class="division-standings home-section">
                    <header class="section-header">
                        <h2>Taulukko</h2>
                    </header>
                    <sortable-table
                        :columns="standingsColumns"
                        :data="standingsData"
                        :default-sort="{ column: 'wins', order: 'desc', numeric: true }"
                        :compact="true"
                    >
                        <template #cell-team="{ row }">
                            <router-link
                                class="table-team-link"
                                :to="{ name: 'team-detail', params: { championshipId: championshipId, teamId: row.team_id } }">
                                {{ row.team }}
                            </router-link>
                        </template>
                        <template #cell-round_diff="{ row }">
                            <span :class="['round-diff', { positive: row.round_diff > 0, negative: row.round_diff < 0 }]">
                                {{ row.round_diff > 0 ? '+' + row.round_diff : row.round_diff }}
                            </span>
                        </template>
                        <template #cell-win_rate="{ row }">
                            <span>{{ row.win_rate }}</span>
                        </template>
                    </sortable-table>
                </section>

                <section class="division-maps home-section">
                    <maps-stats
                        :map-stats="mapStats"
                        :loading="loading && !mapStats.length"
                        :columns="mapColumns"
                        :error="mapStatsError"
                    ></maps-stats>
                </section>

                <section class="division-leaders home-section">
                    <header class="section-header">
                        <h2>Divarin Sankarit</h2>
                    </header>
                    <leaders-new :groups="leaderGroups" :error="leadersError" :loading="loading && !leaderGroups.length"></leaders-new>
                </section>

                <section class="division-matches home-section">
                    <header class="section-header">
                        <h2>Viimeisimmät ottelut</h2>
                    </header>
                    <loading-spinner v-if="matchesLoading" message="Otteluita ladataan..."></loading-spinner>
                    <error-message v-else-if="matchesError" :message="matchesError"></error-message>
                    <ul v-else class="matches-feed">
                        <li v-for="match in matchList" :key="match.match_id || match.id" class="match-card">
                            <div class="match-meta">
                                <span class="match-date">{{ formatDate(match.played_at || match.date) }}</span>
                                <span class="match-map">{{ match.map_name || match.map || 'Tuntematon kartta' }}</span>
                            </div>
                            <div class="match-scoreline">
                                <div class="match-team">
                                    <span class="team-name">{{ match.home_team || match.team_home || match.team1 || 'Kotijoukkue' }}</span>
                                    <span class="team-score">{{ match.home_score ?? match.home_rounds ?? match.score_home ?? '-' }}</span>
                                </div>
                                <div class="match-team">
                                    <span class="team-name">{{ match.away_team || match.team_away || match.team2 || 'Vierasjoukkue' }}</span>
                                    <span class="team-score">{{ match.away_score ?? match.away_rounds ?? match.score_away ?? '-' }}</span>
                                </div>
                                <span :class="['badge', outcomeBadge(match).tone]">{{ outcomeBadge(match).label }}</span>
                            </div>
                        </li>
                        <li v-if="!matchList.length" class="muted">Ei otteluita</li>
                    </ul>
                </section>
            </div>
        </div>
    `
};
