const TEAM_PLAYER_COLUMNS = [
    { key: 'player', label: 'Pelaaja', sortable: true, align: 'left' },
    { key: 'maps', label: 'Kartat', sortable: true, numeric: true },
    { key: 'rounds', label: 'Erät', sortable: true, numeric: true },
    { key: 'rating', label: 'Rating', sortable: true, numeric: true, decimals: 2 },
    { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2 },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1 },
    { key: 'kr', label: 'K/R', sortable: true, numeric: true, decimals: 2 },
    { key: 'hs', label: 'HS%', sortable: true, numeric: true, decimals: 1 },
    { key: 'clutches', label: 'Clutchit', sortable: true, numeric: true }
];

const MATCHES_PAGE_SIZE = 8;

function createSegment() {
    return {
        data: null,
        loading: false,
        error: null,
        fetchedAt: null
    };
}

function toNumber(value, fallback = 0) {
    if (value === null || value === undefined) return fallback;
    const numeric = Number(value);
    if (Number.isFinite(numeric)) return numeric;
    const parsed = Number(String(value).replace(',', '.'));
    return Number.isFinite(parsed) ? parsed : fallback;
}

function formatPercent(value, decimals = 1) {
    const numeric = toNumber(value);
    return `${numeric.toFixed(decimals)} %`;
}

function buildMetrics(stats) {
    if (!stats) return [];
    const wins = toNumber(stats.wins ?? stats.matches_won ?? stats.maps_won);
    const losses = toNumber(stats.losses ?? stats.matches_lost ?? stats.maps_lost);
    const matches = toNumber(stats.matches ?? stats.matches_played ?? stats.series_played);
    const roundsDiff = toNumber(stats.rounds_diff ?? stats.round_diff ?? stats.rounds_delta);
    const winRate = matches > 0 ? (wins / matches) * 100 : toNumber(stats.win_rate);
    const rating = toNumber(stats.rating ?? stats.rating_2 ?? stats.hltv_rating);
    const kd = toNumber(stats.kd ?? stats.kd_ratio);
    const adr = toNumber(stats.adr ?? stats.average_damage);
    const hs = toNumber(stats.hs_percent ?? stats.headshot_percent);

    return [
        { key: 'matches', label: 'Ottelut', value: matches },
        { key: 'winrate', label: 'Voitto%', value: formatPercent(winRate) },
        { key: 'roundDiff', label: 'Eräero', value: roundsDiff > 0 ? `+${roundsDiff}` : `${roundsDiff}` },
        { key: 'rating', label: 'Rating', value: rating.toFixed(2) },
        { key: 'kd', label: 'K/D', value: kd.toFixed(2) },
        { key: 'adr', label: 'ADR', value: adr.toFixed(1) },
        { key: 'hs', label: 'HS%', value: `${hs.toFixed(1)} %` }
    ];
}

function buildSparkline(matches) {
    if (!Array.isArray(matches) || !matches.length) {
        return [];
    }
    const tail = matches.slice(-12);
    return tail.map(match => {
        if (match == null) return 0;
        if (typeof match.result === 'string') {
            const normalized = match.result.toLowerCase();
            if (normalized.includes('win') || normalized.includes('voitto')) return 1;
            if (normalized.includes('loss') || normalized.includes('tappio')) return -1;
        }
        const score = toNumber(match.team_score ?? match.score_for ?? match.for);
        const opponent = toNumber(match.opponent_score ?? match.score_against ?? match.against);
        if (score > opponent) return 1;
        if (score < opponent) return -1;
        return 0;
    });
}

function buildMapHighlights(mapStats) {
    if (!Array.isArray(mapStats)) return [];
    return mapStats
        .map(entry => {
            const current = entry.curr || entry;
            const name = current.map_name || entry.map_name || 'Kartta';
            const played = toNumber(current.matches ?? current.maps ?? current.maps_played);
            const wins = toNumber(current.wins ?? current.maps_won);
            const winRate = played ? (wins / played) * 100 : toNumber(current.win_rate);
            return {
                id: name,
                name,
                played,
                winRate: Number.isFinite(winRate) ? winRate : 0,
                rating: toNumber(current.rating),
                adr: toNumber(current.adr)
            };
        })
        .sort((a, b) => b.winRate - a.winRate)
        .slice(0, 3);
}

function buildPlayerRows(players) {
    if (!Array.isArray(players)) return [];
    return players.map((player, index) => {
        const maps = toNumber(player.maps ?? player.maps_played ?? player.map_count);
        const rounds = toNumber(player.rounds ?? player.rounds_played);
        const rating = toNumber(player.rating ?? player.rating_2 ?? player.hltv_rating);
        const kd = toNumber(player.kd ?? player.kd_ratio);
        const adr = toNumber(player.adr ?? player.average_damage);
        const kr = toNumber(player.kr ?? player.kills_per_round);
        const hs = toNumber(player.hs_percent ?? player.headshot_percent);
        const clutches = toNumber(player.clutches ?? player.clutch_wins);
        return {
            id: player.player_id || player.id || `player-${index}`,
            player,
            maps,
            rounds,
            rating,
            kd,
            adr,
            kr,
            hs,
            clutches
        };
    });
}

function paginate(items, page, pageSize) {
    if (!Array.isArray(items) || !items.length) {
        return { items: [], total: 0, totalPages: 0 };
    }
    const total = items.length;
    const totalPages = Math.max(1, Math.ceil(total / pageSize));
    const safePage = Math.min(Math.max(page, 1), totalPages);
    const start = (safePage - 1) * pageSize;
    return {
        items: items.slice(start, start + pageSize),
        total,
        totalPages,
        page: safePage
    };
}

window.TeamDetail = {
    name: 'TeamDetail',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; },
        get TeamView() { return window.TeamView; }
    },
    props: {
        teamId: { type: [String, Number], required: true },
        championshipId: { type: [String, Number], default: null }
    },
    data() {
        const teamStore = typeof window.useTeamStore === 'function' ? window.useTeamStore() : null;
        const seasonsStore = typeof window.useSeasonsStore === 'function' ? window.useSeasonsStore() : null;
        return {
            teamStore,
            seasonsStore,
            selectedChampionship: this.championshipId ? String(this.championshipId) : null,
            activeTab: 'overview',
            matchesPage: 1,
            pageSize: MATCHES_PAGE_SIZE
        };
    },
    computed: {
        teamEntry() {
            if (!this.teamStore || !this.teamId) {
                return null;
            }
            return this.teamStore.getTeamState(this.teamId) || null;
        },
        profileSegment() {
            return this.teamEntry?.profile || createSegment();
        },
        profile() {
            return this.profileSegment.data || null;
        },
        seasonsSegment() {
            return this.teamEntry?.seasonsList || createSegment();
        },
        seasonOptions() {
            const seasons = Array.isArray(this.seasonsSegment.data) ? this.seasonsSegment.data : [];
            return seasons.map(season => {
                const value = String(season.championship_id || season.championshipId || season.id || '');
                return {
                    value,
                    label: season.name || `Kausi ${season.season} · Div ${season.division_num}`,
                    season: season.season,
                    division: season.division_num,
                    championshipId: value,
                    isCurrent: Boolean(season.is_current || season.current)
                };
            }).filter(option => option.value);
        },
        currentChampionshipId() {
            if (this.selectedChampionship) {
                return String(this.selectedChampionship);
            }
            return this.seasonOptions.length ? this.seasonOptions[0].value : null;
        },
        seasonEntry() {
            if (!this.teamEntry || !this.currentChampionshipId) {
                return null;
            }
            return this.teamEntry.seasons?.[this.currentChampionshipId] || null;
        },
        seasonDetailsSegment() {
            return this.seasonEntry?.details || createSegment();
        },
        seasonDetails() {
            return this.seasonDetailsSegment.data || null;
        },
        seasonStats() {
            return this.seasonDetails?.stats || this.seasonDetails?.team_stats || this.seasonDetails || null;
        },
        seasonErrorMessage() {
            if (!this.seasonDetailsSegment.error) return null;
            if (this.seasonDetailsSegment.data) {
                return null;
            }
            return this.seasonDetailsSegment.error;
        },
        mapStatsSegment() {
            return this.seasonEntry?.mapStats || createSegment();
        },
        mapStats() {
            return Array.isArray(this.mapStatsSegment.data)
                ? this.mapStatsSegment.data
                : Array.isArray(this.seasonDetails?.map_stats)
                    ? this.seasonDetails.map_stats
                    : [];
        },
        matchesSegment() {
            return this.seasonEntry?.matches || createSegment();
        },
        matchesList() {
            return Array.isArray(this.matchesSegment.data) ? this.matchesSegment.data : [];
        },
        paginatedMatches() {
            return paginate(this.matchesList, this.matchesPage, this.pageSize);
        },
        playersList() {
            return Array.isArray(this.seasonDetails?.players)
                ? this.seasonDetails.players
                : Array.isArray(this.seasonDetails?.roster)
                    ? this.seasonDetails.roster
                    : Array.isArray(this.profile?.players)
                        ? this.profile.players
                        : [];
        },
        playerRows() {
            return buildPlayerRows(this.playersList);
        },
        playersLoading() {
            return this.seasonDetailsSegment.loading && !this.playerRows.length;
        },
        breadcrumbs() {
            const crumbs = [
                { label: 'Home', to: { name: 'home' } }
            ];
            const season = this.currentSeasonOption;
            if (season) {
                crumbs.push({ label: `Kausi ${season.season}`, to: { name: 'seasons' } });
                if (season.championshipId) {
                    crumbs.push({ label: `Div ${season.division}`, to: { name: 'division', params: { championshipId: season.championshipId } } });
                }
            }
            if (this.profile) {
                crumbs.push({ label: this.profile.display_name || this.profile.team_name || 'Joukkue' });
            }
            return crumbs;
        },
        currentSeasonOption() {
            if (!this.currentChampionshipId) return null;
            return this.seasonOptions.find(option => option.value === this.currentChampionshipId) || null;
        },
        overviewMetrics() {
            return buildMetrics(this.seasonStats || this.profile?.stats);
        },
        sparklinePoints() {
            return buildSparkline(this.matchesList);
        },
        mapHighlights() {
            return buildMapHighlights(this.mapStats);
        },
        loading() {
            return this.profileSegment.loading || this.seasonsSegment.loading || this.seasonDetailsSegment.loading;
        },
        loadError() {
            return this.profileSegment.error || this.seasonsSegment.error;
        }
    },
    watch: {
        teamId: {
            immediate: true,
            handler() {
                this.bootstrap();
            }
        },
        championshipId(newValue) {
            if (newValue) {
                this.selectedChampionship = String(newValue);
                this.matchesPage = 1;
                this.loadSeasonData(this.selectedChampionship, { force: true });
            }
        },
        currentChampionshipId(newValue, oldValue) {
            if (newValue && newValue !== oldValue) {
                this.matchesPage = 1;
                this.loadSeasonData(newValue);
            }
        },
        seasonOptions(newOptions, oldOptions) {
            if (!Array.isArray(newOptions) || !newOptions.length) {
                return;
            }
            if (this.selectedChampionship && newOptions.some(option => option.value === this.selectedChampionship)) {
                return;
            }
            if (!this.selectedChampionship || !oldOptions || !oldOptions.length) {
                this.selectedChampionship = newOptions[0].value;
            }
        }
    },
    methods: {
        async bootstrap() {
            if (!this.teamStore || !this.teamId) return;
            try {
                await Promise.allSettled([
                    this.teamStore.fetchTeamProfile(this.teamId),
                    this.teamStore.fetchTeamSeasons(this.teamId)
                ]);
                if (!this.selectedChampionship && this.seasonOptions.length) {
                    const current = this.seasonOptions.find(option => option.isCurrent) || this.seasonOptions[0];
                    if (current) {
                        this.selectedChampionship = current.value;
                    }
                }
                if (this.currentChampionshipId) {
                    await this.loadSeasonData(this.currentChampionshipId);
                }
            } catch (error) {
                console.error('TeamDetail bootstrap error', error);
            }
        },
        async loadSeasonData(championshipId, options = {}) {
            if (!this.teamStore || !this.teamId || !championshipId) return;
            const tasks = [
                this.teamStore.fetchSeasonDetails(this.teamId, championshipId, options)
            ];
            tasks.push(this.teamStore.fetchSeasonMapStats(this.teamId, championshipId, options));
            tasks.push(this.teamStore.fetchSeasonMatches(this.teamId, championshipId, options));
            await Promise.allSettled(tasks);
        },
        handleSeasonSelect(seasonId) {
            if (!seasonId || seasonId === this.currentChampionshipId) {
                return;
            }
            this.selectedChampionship = seasonId;
            this.matchesPage = 1;
            this.loadSeasonData(seasonId);
        },
        handleTabSelect(tab) {
            this.activeTab = tab;
        },
        handleRefresh() {
            this.teamStore?.fetchTeamProfile(this.teamId, { force: true });
            this.teamStore?.fetchTeamSeasons(this.teamId, { force: true });
            if (this.currentChampionshipId) {
                this.loadSeasonData(this.currentChampionshipId, { force: true });
            }
        },
        handlePageChange(page) {
            this.matchesPage = page;
        }
    },
    template: `
        <div class="team-detail">
            <loading-spinner v-if="loading && !profile" message="Joukkuetta ladataan..."></loading-spinner>
            <error-message v-else-if="loadError && !profile" :message="loadError" @retry="bootstrap"></error-message>
            <team-view
                v-else
                :profile="profile"
                :breadcrumbs="breadcrumbs"
                :season-options="seasonOptions"
                :selected-season="currentChampionshipId"
                :season-loading="seasonDetailsSegment.loading"
                :season-error="seasonErrorMessage"
                :active-tab="activeTab"
                :metrics="overviewMetrics"
                :sparkline="sparklinePoints"
                :map-highlights="mapHighlights"
                :map-stats="mapStats"
                :map-stats-loading="mapStatsSegment.loading"
                :map-stats-error="mapStatsSegment.error"
                :matches="paginatedMatches.items"
                :matches-loading="matchesSegment.loading"
                :matches-error="matchesSegment.error"
                :matches-page="paginatedMatches.page"
                :matches-total-pages="paginatedMatches.totalPages"
                :matches-page-size="pageSize"
                :players="playerRows"
                :player-columns="TEAM_PLAYER_COLUMNS"
                :players-loading="playersLoading"
                :players-error="seasonErrorMessage"
                @select-season="handleSeasonSelect"
                @select-tab="handleTabSelect"
                @refresh="handleRefresh"
                @change-page="handlePageChange"
            ></team-view>
        </div>
    `
};
