const HOME_MAP_COLUMNS = [
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

const HOME_STATS_SCHEMA = [
    { key: ['team_count', 'teams', 'total_teams', 'aggregates.total_teams'], label: 'Joukkueita', icon: null },
    { key: ['player_count', 'players', 'total_players', 'aggregates.total_players'], label: 'Pelaajia', icon: null },
    { key: ['maps_played_total', 'maps_played', 'maps', 'aggregates.maps_played_total'], label: 'Karttoja pelattu', icon: null },
    { key: ['rounds_played_total', 'rounds_played', 'rounds', 'aggregates.rounds_played_total'], label: 'Erät', icon: null },
    { key: ['median_adr', 'adr_median', 'aggregates.median_adr'], label: 'Median ADR', icon: null, decimals: 1 },
    { key: ['total_kills', 'kills', 'aggregates.total_kills'], label: 'Kills', icon: null },
    { key: ['total_deaths', 'deaths', 'aggregates.total_deaths'], label: 'Deaths', icon: null },
    { key: ['median_survival', 'survival_percent', 'aggregates.median_survival'], label: 'Selviytyminen', icon: null, percent: true, decimals: 1 }
];

const LEADER_GROUP_TITLES = {
    results: 'Tulokset & Plussat',
    offense: 'Offense',
    utility: 'Utility'
};

const FALLBACK_LEADER_CONFIG = [
    { key: 'rating', title: 'Rating-koneet', subtitle: 'Rating 2.0', stat: 'rating', group: 'results', decimals: 2 },
    { key: 'kd', title: 'K/D kuninkaat', subtitle: 'K/D-suhde', stat: 'kd', group: 'results', decimals: 2 },
    { key: 'winrate', title: 'Voittoprosentti', subtitle: 'Otteluvoitto-%', stat: 'winrate', group: 'results', percent: true, decimals: 1 },
    { key: 'clutches', title: 'Ratkaisijat', subtitle: 'Voitetut clutchit', stat: 'clutch_wins', group: 'results' },
    { key: 'mvp', title: 'MVP-pisteet', subtitle: 'MVP:t', stat: 'mvps', group: 'results' },
    { key: 'adr', title: 'ADR-tykit', subtitle: 'Keski-dmg', stat: 'adr', group: 'offense', decimals: 1 },
    { key: 'kr', title: 'Tapot / erä', subtitle: 'K/R', stat: 'kr', group: 'offense', decimals: 2 },
    { key: 'kills', title: 'Tapokoneet', subtitle: 'Kokonaiskillit', stat: 'kills', group: 'offense' },
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

window.HomeView = {
    name: 'HomeView',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; },
        get ProgressBar() { return window.ProgressBar; },
        get HeroCards() { return window.HeroCards; },
        get Masthead() { return window.Masthead; },
        get SeasonsNav() { return window.SeasonsNav; },
        get StatsGrid() { return window.StatsGrid; },
        get MapsStats() { return window.MapsStats; },
        get LeadersNew() { return window.LeadersNew; },
        get TeamComparisonBoard() { return window.TeamComparisonBoard; }
    },
    data() {
        return {
            loading: false,
            error: null,
            seasonError: null,
            overview: null,
            seasons: [],
            selectedSeason: null,
            seasonAggregates: null,
            progress: {
                overall: { played: 0, total: 0, percent: 0 },
                regular: { played: 0, total: 0, percent: 0 },
                playoffs: { played: 0, total: 0, percent: 0 }
            },
            teamComparison: [],
            teamTickerEntries: [],
            mapStats: [],
            mapStatsError: null,
            leaderGroupsData: [],
            leadersError: null,
            divisions: [],
            heroCardsData: [
                {
                    title: 'AFI Esports -hubi',
                    subtitle: 'Arma Finlandin Pappaliiga CS -tilastot ja seuranta yhdestä paikasta.',
                    primaryText: 'AFI sivuille',
                    primaryUrl: 'https://armafinland.fi/',
                    secondaryText: 'Discord-yhteisö',
                    secondaryUrl: 'https://discord.gg/armafinland',
                    variant: 'afi',
                    target: '_blank'
                },
                {
                    title: 'Pappaliiga Legends',
                    subtitle: 'Pappaliigan kilpailulliset kaudet, joukkueet ja sankarit reaaliajassa.',
                    primaryText: 'pappaliiga.fi',
                    primaryUrl: 'https://pappaliiga.fi/',
                    secondaryText: 'Twitter @pappaliiga',
                    secondaryUrl: 'https://twitter.com/pappaliiga',
                    variant: 'pappaliiga',
                    target: '_blank'
                }
            ],
            mapColumns: HOME_MAP_COLUMNS,
            seasonLoading: false
        };
    },
    computed: {
        currentSeasonInfo() {
            if (!Array.isArray(this.seasons)) return null;
            return this.seasons.find(season => String(season.season) === String(this.selectedSeason)) || null;
        },
        overviewStatsCards() {
            return this.buildStatsCards(this.overview, HOME_STATS_SCHEMA);
        },
        seasonStatsCards() {
            return this.buildStatsCards(this.seasonAggregates, HOME_STATS_SCHEMA);
        },
        overallProgressPercent() {
            return this.progress.overall.percent || 0;
        },
        regularProgressPercent() {
            return this.progress.regular.percent || 0;
        },
        playoffProgressPercent() {
            return this.progress.playoffs.percent || 0;
        },
        seasonLabel() {
            if (!this.currentSeasonInfo) return '';
            const label = this.currentSeasonInfo.name || this.currentSeasonInfo.label;
            return label ? `${label}` : `Kausi ${this.currentSeasonInfo.season}`;
        },
        tickerTeams() {
            if (!Array.isArray(this.teamTickerEntries) || !this.teamTickerEntries.length) return [];
            return [...this.teamTickerEntries, ...this.teamTickerEntries];
        },
        seasonSubtitle() {
            if (!this.currentSeasonInfo) return '';
            const phase = this.currentSeasonInfo.phase || (this.currentSeasonInfo.is_playoff ? 'Playoffs' : 'Runkosarja');
            return `${this.seasonLabel} · ${phase}`;
        },
        leaderGroups() {
            return this.leaderGroupsData;
        }
    },
    async mounted() {
        await this.loadInitial();
    },
    methods: {
        async loadInitial() {
            this.loading = true;
            this.error = null;
            try {
                const [overview, seasons] = await Promise.all([
                    window.apiClient.getStatsOverview(),
                    window.apiClient.getSeasons()
                ]);
                this.overview = overview;
                this.seasons = Array.isArray(seasons) ? seasons : [];

                const current = this.seasons.find(season => season.is_current) || this.seasons[0];
                if (current) {
                    this.selectedSeason = current.season;
                    await this.fetchSeasonData(this.selectedSeason);
                }
            } catch (err) {
                console.error('HomeView initial load failed', err);
                this.error = err && err.message ? err.message : 'Etusivun lataus epäonnistui';
            } finally {
                this.loading = false;
            }
        },
        async handleSeasonChange(season) {
            if (String(season) === String(this.selectedSeason)) {
                return;
            }
            this.selectedSeason = season;
            await this.fetchSeasonData(season);
        },
        handleSeasonRetry() {
            if (!this.selectedSeason) return;
            this.fetchSeasonData(this.selectedSeason);
        },
        async fetchSeasonData(season) {
            if (!season) {
                return;
            }
            this.seasonLoading = true;
            this.seasonError = null;
            this.mapStatsError = null;
            this.leadersError = null;
            try {
                const [seasonStats, divisions] = await Promise.all([
                    window.apiClient.getSeasonStats(season),
                    window.apiClient.getDivisionsBySeason(season)
                ]);
                this.seasonAggregates = seasonStats && (seasonStats.aggregates || seasonStats.stats || seasonStats);
                this.mapStats = Array.isArray(seasonStats?.map_stats) ? seasonStats.map_stats : [];
                this.divisions = Array.isArray(divisions) ? divisions.filter(div => !div.is_playoff_secondary) : [];
                this.teamComparison = this.buildTeamComparison(this.divisions);
                this.teamTickerEntries = this.buildTeamTicker(this.teamComparison);
                this.progress = this.extractProgress(seasonStats, this.divisions);

                const initialLeaderGroups = this.buildLeaderGroups(seasonStats?.leaders);
                if (!initialLeaderGroups.length) {
                    const fallbackGroups = await this.fetchFallbackLeaders(season);
                    this.leaderGroupsData = fallbackGroups;
                } else {
                    this.leaderGroupsData = initialLeaderGroups;
                    const totalCategories = initialLeaderGroups.reduce((sum, group) => sum + (group.items?.length || 0), 0);
                    if (totalCategories < 12) {
                        const fallback = await this.fetchFallbackLeaders(season, initialLeaderGroups);
                        this.leaderGroupsData = this.mergeLeaderGroups(initialLeaderGroups, fallback);
                    }
                }
                if (!Array.isArray(this.mapStats) || !this.mapStats.length) {
                    this.mapStatsError = 'Karttatilastoja ei saatavilla';
                }
                if (!this.leaderGroupsData.length) {
                    this.leadersError = 'Johtajalistat eivät ole saatavilla tälle kaudelle';
                }
            } catch (err) {
                console.error('Season data load failed', err);
                this.seasonError = err && err.message ? err.message : 'Kausitilastojen lataus epäonnistui';
            } finally {
                this.seasonLoading = false;
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
                if (!key) continue;
                const segments = String(key).split('.');
                let value = obj;
                for (const seg of segments) {
                    if (value && Object.prototype.hasOwnProperty.call(value, seg)) {
                        value = value[seg];
                    } else {
                        value = undefined;
                        break;
                    }
                }
                if (value !== undefined && value !== null) {
                    return value;
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
                    numeric = numeric * 100;
                }
                return `${numeric.toFixed(decimals)} %`;
            }

            const formatter = new Intl.NumberFormat('fi-FI', {
                minimumFractionDigits: decimals,
                maximumFractionDigits: decimals
            });
            return formatter.format(numeric);
        },
        calcPercent(played, total) {
            const p = Number(played);
            const t = Number(total);
            if (!Number.isFinite(p) || !Number.isFinite(t) || t <= 0) return 0;
            return Math.round((p / t) * 1000) / 10;
        },
        extractProgress(seasonStats, divisions) {
            const safe = (obj, path) => this.pickValue(obj || {}, path);
            const overall = {
                played: safe(seasonStats, ['progress.overall.played', 'matches_played', 'played_matches']) || 0,
                total: safe(seasonStats, ['progress.overall.total', 'matches_total', 'scheduled_matches']) || 0
            };
            if (!overall.total && Array.isArray(divisions)) {
                overall.played = divisions.reduce((sum, div) => sum + Number(div.played_matches || 0), 0);
                overall.total = divisions.reduce((sum, div) => sum + Number(div.total_matches || 0), 0);
            }
            const regular = {
                played: safe(seasonStats, 'progress.regular.played') || overall.played,
                total: safe(seasonStats, 'progress.regular.total') || overall.total
            };
            const playoffs = {
                played: safe(seasonStats, 'progress.playoffs.played') || 0,
                total: safe(seasonStats, 'progress.playoffs.total') || 0
            };
            return {
                overall: { ...overall, percent: this.calcPercent(overall.played, overall.total) },
                regular: { ...regular, percent: this.calcPercent(regular.played, regular.total) },
                playoffs: { ...playoffs, percent: this.calcPercent(playoffs.played, playoffs.total) }
            };
        },
        buildTeamComparison(divisions) {
            if (!Array.isArray(divisions)) return [];
            const teams = [];
            divisions.forEach(div => {
                if (!div || div.is_playoff) return;
                (div.teams || []).forEach(team => {
                    if (!team) return;
                    const matches = Number(team.matches_played ?? team.played ?? team.stats?.matches ?? 0);
                    const wins = Number(team.wins ?? team.maps_won ?? team.stats?.wins ?? 0);
                    const losses = Number(team.losses ?? team.maps_lost ?? team.stats?.losses ?? 0);
                    const winRate = this.safeNumber(team.win_rate ?? (matches ? (wins / matches) * 100 : 0));
                    teams.push({
                        team_id: team.team_id || team.id,
                        name: team.display_name || team.team_name || team.name || 'Tuntematon joukkue',
                        logo: team.logo || team.avatar || team.team_logo || team.image,
                        matches_played: matches,
                        wins,
                        losses,
                        rounds_diff: Number(team.rounds_diff ?? team.round_diff ?? team.rounds_delta ?? 0),
                        win_rate: winRate,
                        kd: this.safeNumber(team.kd ?? team.stats?.kd),
                        adr: this.safeNumber(team.adr ?? team.stats?.adr),
                        rating: this.safeNumber(team.rating ?? team.stats?.rating ?? team.rating_2 ?? team.hltv_rating)
                    });
                });
            });
            return teams
                .sort((a, b) => (b.rating || 0) - (a.rating || 0))
                .map((team, idx) => ({ ...team, rank: idx + 1 }));
        },
        buildTeamTicker(teams) {
            if (!Array.isArray(teams)) return [];
            const seen = new Set();
            const entries = [];
            teams.forEach(team => {
                const key = team.name;
                if (!key || seen.has(key)) return;
                seen.add(key);
                entries.push({
                    name: team.name,
                    logo: this.ensureAvatar(team.logo)
                });
            });
            return entries.slice(0, 18);
        },
        ensureAvatar(src) {
            if (!src) return '/static/pappaliiga-logo-white-bg.png';
            try {
                return window.apiClient.proxyAvatar(src);
            } catch (err) {
                return src;
            }
        },
        buildLeaderGroups(raw) {
            if (!Array.isArray(raw) || !raw.length) {
                return [];
            }
            const groups = {};
            raw.forEach(category => {
                if (!category) return;
                const statKey = category.statKey || category.stat_key || category.key || category.id;
                const groupKey = category.group || category.groupKey || this.resolveLeaderGroup(statKey);
                const title = category.title || category.categoryTitle || category.statName || 'Tilasto';
                const subtitle = category.subtitle || category.description || '';
                const groupTitle = LEADER_GROUP_TITLES[groupKey] || category.groupTitle || LEADER_GROUP_TITLES.results;
                const leaders = Array.isArray(category.leaders) ? category.leaders.map((leader, index) => this.normalizeLeader(leader, index)) : [];
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
            const config = FALLBACK_LEADER_CONFIG.find(item => item.stat === statKey || item.key === statKey);
            return config ? config.group : 'results';
        },
        normalizeLeader(rawLeader, index) {
            if (!rawLeader) return {
                playerName: `Tuntematon ${index + 1}`,
                teamName: '',
                value: '–',
                teamLogo: '/static/pappaliiga-logo-white-bg.png'
            };
            const name = rawLeader.nickname || rawLeader.playerName || rawLeader.player || rawLeader.name || `Tuntematon ${index + 1}`;
            const team = rawLeader.team || rawLeader.teamName || rawLeader.team_name || '';
            const value = rawLeader.value ?? rawLeader.stat_value ?? rawLeader.total ?? rawLeader.score ?? rawLeader.number;
            const logo = rawLeader.team_logo || rawLeader.logo || rawLeader.teamLogo || rawLeader.avatar;
            return {
                playerName: name,
                teamName: team,
                value,
                teamLogo: this.ensureAvatar(logo)
            };
        },
        async fetchFallbackLeaders(season, existingGroups = []) {
            const existingIds = new Set();
            existingGroups.forEach(group => {
                (group.items || []).forEach(cat => existingIds.add(cat.id || cat.title));
            });
            const config = FALLBACK_LEADER_CONFIG.filter(cat => !existingIds.has(cat.key));
            const uniqueStats = [...new Set(config.map(cat => cat.stat))];
            const statResults = {};
            await Promise.all(uniqueStats.map(async stat => {
                try {
                    statResults[stat] = await window.apiClient.getTopPlayers(stat, { season, limit: 4, min_maps: 3 });
                } catch (err) {
                    console.warn('Fallback leader fetch failed', stat, err);
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
                const groupTitle = LEADER_GROUP_TITLES[cat.group] || LEADER_GROUP_TITLES.results;
                if (!groups[cat.group]) {
                    groups[cat.group] = { title: groupTitle, items: [] };
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
        mergeLeaderGroups(primary, fallback) {
            const merged = JSON.parse(JSON.stringify(primary));
            const groupMap = new Map(merged.map(group => [group.title, group]));
            const existingIds = new Set();
            merged.forEach(group => group.items.forEach(cat => existingIds.add(cat.id || cat.title)));

            fallback.forEach(group => {
                if (!group.items || !group.items.length) return;
                if (!groupMap.has(group.title)) {
                    groupMap.set(group.title, { title: group.title, items: [] });
                    merged.push(groupMap.get(group.title));
                }
                const target = groupMap.get(group.title);
                group.items.forEach(cat => {
                    const id = cat.id || cat.title;
                    if (existingIds.has(id)) return;
                    target.items.push(cat);
                    existingIds.add(id);
                });
                target.items.sort((a, b) => a.title.localeCompare(b.title, 'fi'));
            });
            return merged;
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
        safeNumber(value) {
            const numeric = Number(value);
            if (Number.isFinite(numeric)) return numeric;
            if (value === null || value === undefined) return 0;
            const parsed = Number(String(value).replace(',', '.'));
            return Number.isFinite(parsed) ? parsed : 0;
        }
    },
    template: `
        <div class="home-view">
            <div class="home-toolbar">
                <div class="toolbar-label">Tilastot & kausikatsaus</div>
                <div class="toolbar-actions">
                    <span class="badge season-badge" v-if="seasonLabel">Nyt: {{ seasonLabel }}</span>
                </div>
            </div>

            <masthead></masthead>

            <hero-cards :cards="heroCardsData"></hero-cards>

            <loading-spinner v-if="loading" message="Tilastoja ladataan..."></loading-spinner>
            <error-message v-else-if="error" :message="error" @retry="loadInitial"></error-message>

            <div v-else class="home-content">
                <section class="home-section season-select">
                    <header class="section-header">
                        <h2>Kausivalitsin</h2>
                        <p class="subtitle muted">Valitse kausi tarkasteltavaksi ja seuraa etenemistä.</p>
                    </header>
                    <seasons-nav
                        v-if="seasons && seasons.length"
                        :seasons="seasons"
                        :current-season="selectedSeason"
                        @season-change="handleSeasonChange"
                    ></seasons-nav>
                    <error-message
                        v-if="seasonError"
                        class="inline-error"
                        :message="seasonError"
                        @retry="handleSeasonRetry"
                    ></error-message>

                    <div class="season-progress-grid">
                        <div class="card progress-card">
                            <header class="card-head">
                                <h3 class="title">Kokonaisedistyminen</h3>
                                <span class="progress-value">{{ overallProgressPercent.toFixed(1) }} %</span>
                            </header>
                            <div class="card-content">
                                <progress-bar
                                    :value="overallProgressPercent"
                                    :max="100"
                                    :show-percentage="true"
                                    :label="progress.overall.played + '/' + progress.overall.total + ' ottelua'">
                                </progress-bar>
                                <div class="progress-sub-bars">
                                    <div class="progress-sub">
                                        <div class="progress-label">Runkosarja</div>
                                        <progress-bar
                                            :value="regularProgressPercent"
                                            :max="100"
                                            :show-percentage="true"
                                            :label="progress.regular.played + '/' + progress.regular.total">
                                        </progress-bar>
                                    </div>
                                    <div class="progress-sub">
                                        <div class="progress-label">Playoffs</div>
                                        <progress-bar
                                            :value="playoffProgressPercent"
                                            :max="100"
                                            :show-percentage="true"
                                            :label="progress.playoffs.played + '/' + progress.playoffs.total">
                                        </progress-bar>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </section>

                <section class="home-section stats-section">
                    <header class="section-header">
                        <h2>Divisioonan tilastot</h2>
                        <p class="subtitle muted" v-if="seasonSubtitle">{{ seasonSubtitle }}</p>
                    </header>
                    <div class="stat-grid-wrapper">
                        <loading-spinner v-if="seasonLoading && !seasonStatsCards.length" message="Tilastoja ladataan..."></loading-spinner>
                        <stats-grid v-else :stats="seasonStatsCards"></stats-grid>
                    </div>
                </section>

                <section class="home-section comparison-section">
                    <team-comparison-board
                        :teams="teamComparison"
                        :loading="seasonLoading && !teamComparison.length"
                        :title="'Joukkuevertailu'"
                        :subtitle="seasonSubtitle"
                        :default-sort="{ column: 'rating', order: 'desc', numeric: true }"
                    ></team-comparison-board>

                    <div class="team-ticker" v-if="tickerTeams.length">
                        <div class="team-ticker-track">
                            <div class="team-ticker-item" v-for="(team, idx) in tickerTeams" :key="idx">
                                <img :src="team.logo" :alt="team.name" loading="lazy" />
                                <span>{{ team.name }}</span>
                            </div>
                        </div>
                    </div>
                </section>

                <section class="home-section stats-section">
                    <header class="section-header">
                        <h2>Liigan kokonaistilastot</h2>
                        <p class="subtitle muted">Kaikki kaudet ja ottelut yhteensä.</p>
                    </header>
                    <stats-grid :stats="overviewStatsCards"></stats-grid>
                </section>

                <section class="home-section maps-section">
                    <maps-stats
                        :map-stats="mapStats"
                        :loading="seasonLoading && !mapStats.length"
                        :error="mapStatsError"
                        :columns="mapColumns"
                    ></maps-stats>
                </section>

                <section class="home-section leaders-section">
                    <header class="section-header">
                        <h2>Divarin Sankarit</h2>
                        <p class="subtitle muted">Liigan tilastokärjet visualisoituna teemoittain.</p>
                    </header>
                    <leaders-new
                        :groups="leaderGroups"
                        :loading="seasonLoading && !leaderGroups.length"
                        :error="leadersError"
                    ></leaders-new>
                </section>
            </div>
        </div>
    `
};
