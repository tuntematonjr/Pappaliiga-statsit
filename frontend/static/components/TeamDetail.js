// Team detail view that renders stats, maps, matches, players and veto aggregates
// Uses Pinia store (useTeamStore) and apiClient.getTeamPage to fetch data

const PLAYER_COLUMNS = [
    { key: 'nickname', label: 'Pelaaja', sortable: true },
    { key: 'mapsPlayed', label: 'Kartat', sortable: true, numeric: true },
    { key: 'rating', label: 'Rating', sortable: true, numeric: true, decimals: 2 },
    { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2 },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1 },
    { key: 'kr', label: 'K/R', sortable: true, numeric: true, decimals: 2 },
    { key: 'hsPct', label: 'HS%', sortable: true, numeric: true, decimals: 1 },
    { key: 'mk3k', label: '3K', sortable: true, numeric: true },
    { key: 'mk4k', label: '4K', sortable: true, numeric: true },
    { key: 'mk5k', label: 'Ace', sortable: true, numeric: true },
    { key: 'clutchKills', label: 'Clutch', sortable: true, numeric: true }
];

const MAP_COLUMNS = [
    { key: 'mapName', label: 'Kartta', sortable: true },
    { key: 'played', label: 'Pelattu', sortable: true, numeric: true },
    { key: 'winrate', label: 'Win%', sortable: true, numeric: true, decimals: 1 },
    { key: 'wins', label: 'W', sortable: true, numeric: true },
    { key: 'picks', label: 'Picks', sortable: true, numeric: true },
    { key: 'bans', label: 'Bans', sortable: true, numeric: true },
    { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2 },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1 }
];

const VETO_COLUMNS = [
    { key: 'mapName', label: 'Kartta', sortable: true },
    { key: 'timesPicked', label: 'Valittu', sortable: true, numeric: true },
    { key: 'timesBanned', label: 'Bannit', sortable: true, numeric: true },
    { key: 'pickRate', label: 'Pick%', sortable: true, numeric: true, decimals: 1 },
    { key: 'banRate', label: 'Ban%', sortable: true, numeric: true, decimals: 1 }
];

const MATCHES_PAGE_SIZE = 8;

function createSegment() {
    return { data: null, loading: false, error: null, fetchedAt: null };
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
    return `${numeric.toFixed(decimals)}%`;
}

function formatDate(ts) {
    if (!ts) return '';
    const d = new Date(ts * 1000);
    return d.toLocaleDateString('fi-FI', { year: 'numeric', month: 'short', day: 'numeric' });
}

function paginate(items, page, pageSize) {
    if (!Array.isArray(items) || !items.length) {
        return { items: [], total: 0, totalPages: 0, page: 1 };
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

function normalizeSeasonData(pageData) {
    if (!pageData) return null;
    return pageData.seasonData || pageData.season_data || pageData.season || null;
}

function normalizeMap(entry) {
    if (!entry) return null;
    return {
        mapName: entry.mapName || entry.map_name || 'Kartta',
        played: toNumber(entry.played ?? entry.matches ?? entry.maps ?? entry.maps_played),
        wins: toNumber(entry.wins ?? entry.maps_won),
        winrate: toNumber(entry.winrate ?? entry.win_rate ?? entry.winRate),
        picks: toNumber(entry.picks ?? entry.times_picked ?? entry.timesPicked),
        bans: toNumber(entry.times_banned ?? entry.bans ?? entry.ban1) + toNumber(entry.ban2),
        kd: toNumber(entry.kd ?? entry.kd_ratio),
        adr: toNumber(entry.adr ?? entry.average_damage)
    };
}

function normalizePlayer(player, idx = 0) {
    if (!player) return null;
    return {
        playerId: player.player_id || player.id || `player-${idx}`,
        nickname: player.nickname || player.name || 'Pelaaja',
        mapsPlayed: toNumber(player.maps_played ?? player.maps ?? player.map_count),
        rating: toNumber(player.rating ?? player.rating_2 ?? player.hltv_rating),
        kd: toNumber(player.kd ?? player.kd_ratio),
        adr: toNumber(player.adr ?? player.average_damage),
        kr: toNumber(player.kr ?? player.kills_per_round),
        hsPct: toNumber(player.hs_pct ?? player.hs_percent ?? player.headshot_percent),
        mk3k: toNumber(player.mk_3k ?? player['3k']),
        mk4k: toNumber(player.mk_4k ?? player['4k']),
        mk5k: toNumber(player.mk_5k ?? player['5k']),
        clutchKills: toNumber(player.clutch_kills ?? player.clutches ?? player.clutch_wins)
    };
}

function normalizeMatch(match) {
    if (!match) return null;
    const teamScore = toNumber(match.team_score ?? match.score_for ?? match.for ?? match.teamScore);
    const oppScore = toNumber(match.opponent_score ?? match.score_against ?? match.against ?? match.opponentScore);
    const bestOf = toNumber(match.best_of ?? match.bestOf ?? 1);
    const played = toNumber(match.played ?? match.maps ?? match.map_count ?? 1);
    return {
        matchId: match.match_id || match.matchId || match.id,
        team1Name: match.team1_name || match.team1 || match.teamName || match.team,
        team2Name: match.team2_name || match.opponent_name || match.opponent,
        ts: toNumber(match.ts ?? match.started_at ?? match.start_ts ?? match.date),
        bestOf,
        played,
        teamScore,
        oppScore
    };
}

function normalizeVeto(entry) {
    if (!entry) return null;
    return {
        mapName: entry.map_name || entry.mapName || 'Kartta',
        timesPicked: toNumber(entry.times_picked ?? entry.picked),
        timesBanned: toNumber(entry.times_banned ?? entry.banned),
        pickRate: toNumber(entry.pick_rate ?? entry.pickRate),
        banRate: toNumber(entry.ban_rate ?? entry.banRate)
    };
}

function getMatchResult(match) {
    if (!match) return 'pending';
    if (match.teamScore > match.oppScore) return 'win';
    if (match.teamScore < match.oppScore) return 'loss';
    return 'draw';
}

window.TeamDetail = {
    name: 'TeamDetail',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; }
    },
    props: {
        teamId: { type: [String, Number], required: true },
        championshipId: { type: [String, Number], default: null }
    },
    data() {
        const teamStore = typeof window.useTeamStore === 'function' ? window.useTeamStore() : null;
        return {
            teamStore,
            selectedChampionship: this.championshipId ? String(this.championshipId) : null,
            activeTab: 'overview',
            matchesPage: 1,
            matchesPageSize: MATCHES_PAGE_SIZE,
            playerSortKey: 'rating',
            playerSortDesc: true,
            mapSortKey: 'played',
            mapSortDesc: true,
            vetoSortKey: 'timesPicked',
            vetoSortDesc: true
        };
    },
    computed: {
        teamEntry() {
            if (!this.teamStore || !this.teamId) return null;
            return this.teamStore.getTeamState(this.teamId);
        },
        pageSegment() {
            return this.teamEntry?.page || createSegment();
        },
        pageData() {
            return this.pageSegment.data || null;
        },
        teamInfo() {
            return this.pageData?.team || this.pageData?.profile || null;
        },
        seasonOptions() {
            const seasons = Array.isArray(this.pageData?.seasons) ? this.pageData.seasons : [];
            const normalized = seasons.map(season => {
                const value = season.championship_id || season.championshipId || season.id;
                return {
                    value: value ? String(value) : null,
                    label: season.name || `Kausi ${season.season} · Div ${season.division_num}`,
                    season: toNumber(season.season),
                    division: season.division_num
                };
            }).filter(option => option.value);
            return normalized.sort((a, b) => {
                // Newest first: higher season number wins, fallback to numeric value
                const seasonDiff = (b.season || 0) - (a.season || 0);
                if (seasonDiff !== 0) return seasonDiff;
                const av = Number(a.value);
                const bv = Number(b.value);
                if (Number.isFinite(av) && Number.isFinite(bv)) return bv - av;
                return 0;
            });
        },
        currentChampionshipId() {
            if (this.pageData?.currentChampionshipId) return String(this.pageData.currentChampionshipId);
            if (this.selectedChampionship) return String(this.selectedChampionship);
            return this.seasonOptions[0]?.value || null;
        },
        seasonData() {
            return normalizeSeasonData(this.pageData) || null;
        },
        teamStats() {
            return this.seasonData?.teamStats || this.seasonData?.stats || {};
        },
        statBoxes() {
            const s = this.teamStats || {};
            const wins = toNumber(s.wins ?? s.maps_won ?? s.matches_won);
            const losses = toNumber(s.losses ?? s.maps_lost ?? s.matches_lost);
            const matches = toNumber(s.matches ?? s.matches_played ?? s.series_played);
            const roundsDiff = toNumber(s.rounds_diff ?? s.round_diff);
            const winRate = matches ? (wins / matches) * 100 : toNumber(s.win_rate ?? s.match_win_rate);
            const rating = toNumber(s.rating ?? s.rating_2 ?? s.hltv_rating);
            const kd = toNumber(s.kd ?? s.kd_ratio);
            return [
                { key: 'matches', label: 'Ottelut', displayValue: matches || '-' },
                { key: 'wins', label: 'Voitot', displayValue: wins || '-' },
                { key: 'winrate', label: 'Voitto%', displayValue: formatPercent(winRate || 0, 1) },
                { key: 'rounddiff', label: 'Eräero', displayValue: roundsDiff ? roundsDiff : '0' },
                { key: 'rating', label: 'Rating', displayValue: rating ? rating.toFixed(2) : '-' },
                { key: 'kd', label: 'K/D', displayValue: kd ? kd.toFixed(2) : '-' }
            ];
        },
        mapStats() {
            const maps = Array.isArray(this.seasonData?.mapStats) ? this.seasonData.mapStats : (Array.isArray(this.seasonData?.map_stats) ? this.seasonData.map_stats : []);
            return maps.map(normalizeMap).filter(Boolean);
        },
        sortedMaps() {
            const key = this.mapSortKey;
            const desc = this.mapSortDesc ? -1 : 1;
            return [...this.mapStats].sort((a, b) => {
                const av = a?.[key] ?? 0;
                const bv = b?.[key] ?? 0;
                if (av === bv) return 0;
                return av > bv ? desc : -desc;
            });
        },
        matchesList() {
            const matches = this.seasonData?.matchHistory || this.seasonData?.matches || [];
            return Array.isArray(matches) ? matches.map(normalizeMatch).filter(Boolean) : [];
        },
        paginatedMatches() {
            return paginate(this.matchesList, this.matchesPage, this.matchesPageSize);
        },
        players() {
            const players = this.seasonData?.playerStats || this.seasonData?.players || this.seasonData?.roster || [];
            return Array.isArray(players) ? players.map((p, idx) => normalizePlayer(p, idx)).filter(Boolean) : [];
        },
        sortedPlayers() {
            const key = this.playerSortKey;
            const desc = this.playerSortDesc ? -1 : 1;
            return [...this.players].sort((a, b) => {
                const av = a?.[key] ?? 0;
                const bv = b?.[key] ?? 0;
                if (av === bv) return 0;
                return av > bv ? desc : -desc;
            });
        },
        vetoAggregates() {
            const raw = this.seasonData?.vetoAggregates || this.seasonData?.veto_aggregates || [];
            return Array.isArray(raw) ? raw.map(normalizeVeto).filter(Boolean) : [];
        },
        sortedVetoAgg() {
            const key = this.vetoSortKey;
            const desc = this.vetoSortDesc ? -1 : 1;
            return [...this.vetoAggregates].sort((a, b) => {
                const av = a?.[key] ?? 0;
                const bv = b?.[key] ?? 0;
                if (av === bv) return 0;
                return av > bv ? desc : -desc;
            });
        },
        loading() {
            return this.pageSegment.loading;
        },
        loadError() {
            return this.pageSegment.error;
        }
    },
    watch: {
        teamId: {
            immediate: true,
            handler() {
                this.selectedChampionship = this.championshipId ? String(this.championshipId) : null;
                this.bootstrap();
            }
        },
        championshipId(newVal) {
            if (newVal) {
                this.selectedChampionship = String(newVal);
                this.fetchSeason(String(newVal), { force: true });
            }
        }
    },
    methods: {
        async bootstrap() {
            if (!this.teamStore || !this.teamId) return;
            try {
                const data = await this.teamStore.fetchTeamPage(this.teamId, this.selectedChampionship);
                if (data?.currentChampionshipId) {
                    this.selectedChampionship = String(data.currentChampionshipId);
                    this.updateRoute(this.selectedChampionship);
                }
            } catch (err) {
                console.error('TeamDetail bootstrap failed', err);
            }
        },
        async fetchSeason(championshipId, options = {}) {
            if (!this.teamStore || !this.teamId || !championshipId) return;
            try {
                await this.teamStore.fetchTeamPage(this.teamId, championshipId, options);
            } catch (err) {
                console.error('TeamDetail season fetch failed', err);
            }
        },
        selectChampionship(championshipId) {
            if (!championshipId || championshipId === this.currentChampionshipId) return;
            this.selectedChampionship = championshipId;
            this.matchesPage = 1;
            this.fetchSeason(championshipId);
            this.updateRoute(championshipId);
        },
        updateRoute(championshipId) {
            if (!this.$router || !this.$route) return;
            const params = { ...(this.$route.params || {}), teamId: this.teamId };
            const query = { ...(this.$route.query || {}) };
            if (championshipId) {
                query.championship = championshipId;
            } else {
                delete query.championship;
            }
            this.$router.replace({
                name: this.$route.name || 'team',
                params,
                query
            }).catch(() => {});
        },
        selectTab(tab) {
            this.activeTab = tab;
        },
        setMatchPage(page) {
            this.matchesPage = page;
        },
        formatPercent,
        formatDate,
        getMatchResult,
        teamLogo() {
            return this.teamInfo?.logo || this.teamInfo?.avatar || this.teamInfo?.image || '';
        }
    },
    template: `
        <div class="team-detail">
            <loading-spinner v-if="loading && !teamInfo" message="Joukkuetta ladataan..."></loading-spinner>
            <error-message v-else-if="loadError && !teamInfo" :message="loadError" @retry="bootstrap"></error-message>
            <div v-else>
                <header class="team-hero">
                    <div class="team-hero__logo" v-if="teamLogo()">
                        <img :src="teamLogo()" :alt="teamInfo?.teamName || 'Joukkue'" />
                    </div>
                    <div class="team-hero__content">
                        <h1 class="team-hero__title title-accent titleUnderlinePage">
                            {{ teamInfo?.displayName || teamInfo?.teamName || 'Joukkue' }}
                        </h1>
                        <p v-if="seasonOptions.length" class="team-hero__season">
                            Kausi {{ seasonOptions[0]?.season }} · Divisio {{ seasonOptions[0]?.division }}
                        </p>
                    </div>
                    <div v-if="seasonOptions.length" class="team-season-selector">
                        <label class="season-select-label" for="season-select">Valitse kausi</label>
                        <select
                            id="season-select"
                            class="season-select"
                            :value="currentChampionshipId"
                            @change="selectChampionship($event.target.value)"
                        >
                            <option v-for="season in seasonOptions" :key="season.value" :value="season.value">
                                {{ season.label }}
                            </option>
                        </select>
                    </div>
                </header>

                <nav class="team-tabs" role="tablist">
                    <button
                        v-for="tab in ['overview', 'maps', 'matches', 'players', 'veto']"
                        :key="tab"
                        type="button"
                        class="team-tab"
                        :class="{ 'team-tab--active': activeTab === tab }"
                        @click="selectTab(tab)"
                        role="tab"
                    >
                        {{ { overview: 'Yleiskuva', maps: 'Kartat', matches: 'Ottelut', players: 'Pelaajat', veto: 'Veto/Nosto' }[tab] }}
                    </button>
                </nav>

                <section v-if="activeTab === 'overview'" class="team-section">
                    <h2 class="section-title titleUnderline">Kaudenstatistiikka</h2>
                    <div class="stat-boxes-grid">
                        <div v-for="box in statBoxes" :key="box.key" class="stat-box glass-card">
                            <div class="stat-box__label">{{ box.label }}</div>
                            <div class="stat-box__value">{{ box.displayValue }}</div>
                        </div>
                    </div>
                    <h2 class="section-title titleUnderline" style="margin-top: 2rem;">Karttavertailu</h2>
                    <div v-if="mapStats.length" class="maps-preview-grid">
                        <div v-for="map in mapStats.slice(0, 6)" :key="map.mapName" class="map-card glass-card">
                            <h3>{{ map.mapName }}</h3>
                            <div class="map-card__stats">
                                <div>{{ map.played }} pelattu</div>
                                <div class="stat-positive">{{ formatPercent(map.winrate, 1) }} W/R</div>
                                <div>{{ map.wins }}-{{ map.played - map.wins }}</div>
                            </div>
                        </div>
                    </div>
                    <div v-else class="empty-state-container">
                        <div class="empty-state-card">
                            <div class="empty-state-icon">🗺️</div>
                            <h3 class="empty-state-title">Ei karttatietoja</h3>
                            <p class="empty-state-description">Tälle kaudelle ei ole vielä karttavertailua saatavilla.</p>
                        </div>
                    </div>
                </section>

                <section v-if="activeTab === 'maps'" class="team-section">
                    <h2 class="section-title titleUnderline">Kartat - Yksityiskohtainen analyysi</h2>
                    <div v-if="sortedMaps.length" class="table-wrapper">
                        <table class="data-table">
                            <thead>
                                <tr>
                                    <th v-for="col in MAP_COLUMNS" :key="col.key" class="table-header" @click="mapSortKey === col.key ? mapSortDesc = !mapSortDesc : (mapSortKey = col.key, mapSortDesc = true)" style="cursor: pointer;">
                                        {{ col.label }}
                                        <span v-if="mapSortKey === col.key">{{ mapSortDesc ? '↓' : '↑' }}</span>
                                    </th>
                                </tr>
                            </thead>
                            <tbody>
                                <tr v-for="map in sortedMaps" :key="map.mapName" class="table-row">
                                    <td class="table-cell">{{ map.mapName }}</td>
                                    <td class="table-cell numeric">{{ map.played }}</td>
                                    <td class="table-cell numeric">{{ formatPercent(map.winrate, 1) }}</td>
                                    <td class="table-cell numeric">{{ map.wins }}</td>
                                    <td class="table-cell numeric">{{ map.picks }}</td>
                                    <td class="table-cell numeric">{{ map.bans }}</td>
                                    <td class="table-cell numeric">{{ map.kd.toFixed ? map.kd.toFixed(2) : map.kd }}</td>
                                    <td class="table-cell numeric">{{ map.adr.toFixed ? map.adr.toFixed(1) : map.adr }}</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                    <div v-else class="empty-state-container">
                        <div class="empty-state-card">
                            <div class="empty-state-icon">📊</div>
                            <h3 class="empty-state-title">Ei karttatietoja</h3>
                            <p class="empty-state-description">Tälle kaudelle ei ole karttastatistiikkaa saatavilla.</p>
                        </div>
                    </div>
                </section>

                <section v-if="activeTab === 'matches'" class="team-section">
                    <h2 class="section-title titleUnderline">Ottelut ({{ paginatedMatches.total }} yhteensä)</h2>
                    <div v-if="paginatedMatches.items.length" class="matches-list">
                        <div v-for="match in paginatedMatches.items" :key="match.matchId" class="match-card glass-card" :class="'match-card--' + getMatchResult(match)">
                            <div class="match-card__header">
                                <div class="match-date">{{ formatDate(match.ts) }}</div>
                                <div class="match-result">{{ match.teamScore }} - {{ match.oppScore }}</div>
                            </div>
                            <div class="match-card__teams">
                                <span>{{ match.team1Name }}</span>
                                <span class="vs">vs</span>
                                <span>{{ match.team2Name }}</span>
                            </div>
                            <div class="match-card__meta">BO{{ match.bestOf }} · pelattu {{ match.played }}</div>
                        </div>
                    </div>
                    <div v-else class="empty-state-container">
                        <div class="empty-state-card">
                            <div class="empty-state-icon">🎮</div>
                            <h3 class="empty-state-title">Ei otteluita</h3>
                            <p class="empty-state-description">Tälle kaudelle ei ole otteluhistoriaa saatavilla.</p>
                        </div>
                    </div>
                    <div v-if="paginatedMatches.totalPages > 1" class="pagination">
                        <button type="button" @click="setMatchPage(paginatedMatches.page - 1)" :disabled="paginatedMatches.page === 1" class="btn-small">← Edellinen</button>
                        <span>{{ paginatedMatches.page }} / {{ paginatedMatches.totalPages }}</span>
                        <button type="button" @click="setMatchPage(paginatedMatches.page + 1)" :disabled="paginatedMatches.page === paginatedMatches.totalPages" class="btn-small">Seuraava →</button>
                    </div>
                </section>

                <section v-if="activeTab === 'players'" class="team-section">
                    <h2 class="section-title titleUnderline">Pelaajat</h2>
                    <div v-if="sortedPlayers.length" class="table-wrapper">
                        <table class="data-table">
                            <thead>
                                <tr>
                                    <th v-for="col in PLAYER_COLUMNS" :key="col.key" class="table-header" @click="playerSortKey === col.key ? playerSortDesc = !playerSortDesc : (playerSortKey = col.key, playerSortDesc = true)" style="cursor: pointer;">
                                        {{ col.label }}
                                        <span v-if="playerSortKey === col.key">{{ playerSortDesc ? '↓' : '↑' }}</span>
                                    </th>
                                </tr>
                            </thead>
                            <tbody>
                                <tr v-for="player in sortedPlayers" :key="player.playerId" class="table-row">
                                    <td class="table-cell">{{ player.nickname }}</td>
                                    <td class="table-cell numeric">{{ player.mapsPlayed }}</td>
                                    <td class="table-cell numeric">{{ player.rating.toFixed ? player.rating.toFixed(2) : player.rating }}</td>
                                    <td class="table-cell numeric">{{ player.kd.toFixed ? player.kd.toFixed(2) : player.kd }}</td>
                                    <td class="table-cell numeric">{{ player.adr.toFixed ? player.adr.toFixed(1) : player.adr }}</td>
                                    <td class="table-cell numeric">{{ player.kr.toFixed ? player.kr.toFixed(2) : player.kr }}</td>
                                    <td class="table-cell numeric">{{ formatPercent(player.hsPct, 1) }}</td>
                                    <td class="table-cell numeric">{{ player.mk3k }}</td>
                                    <td class="table-cell numeric">{{ player.mk4k }}</td>
                                    <td class="table-cell numeric">{{ player.mk5k }}</td>
                                    <td class="table-cell numeric">{{ player.clutchKills }}</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                    <div v-else class="empty-state-container">
                        <div class="empty-state-card">
                            <div class="empty-state-icon">👤</div>
                            <h3 class="empty-state-title">Ei pelaajatietoja</h3>
                            <p class="empty-state-description">Tälle kaudelle ei ole pelaajatietoja saatavilla.</p>
                        </div>
                    </div>
                </section>

                <section v-if="activeTab === 'veto'" class="team-section">
                    <h2 class="section-title titleUnderline">Ban/Nosto Tilastot</h2>
                    <div v-if="sortedVetoAgg.length" class="table-wrapper">
                        <table class="data-table">
                            <thead>
                                <tr>
                                    <th v-for="col in VETO_COLUMNS" :key="col.key" class="table-header" @click="vetoSortKey === col.key ? vetoSortDesc = !vetoSortDesc : (vetoSortKey = col.key, vetoSortDesc = true)" style="cursor: pointer;">
                                        {{ col.label }}
                                        <span v-if="vetoSortKey === col.key">{{ vetoSortDesc ? '↓' : '↑' }}</span>
                                    </th>
                                </tr>
                            </thead>
                            <tbody>
                                <tr v-for="veto in sortedVetoAgg" :key="veto.mapName" class="table-row">
                                    <td class="table-cell">{{ veto.mapName }}</td>
                                    <td class="table-cell numeric">{{ veto.timesPicked }}</td>
                                    <td class="table-cell numeric">{{ veto.timesBanned }}</td>
                                    <td class="table-cell numeric">{{ formatPercent(veto.pickRate, 1) }}</td>
                                    <td class="table-cell numeric">{{ formatPercent(veto.banRate, 1) }}</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                    <div v-else class="empty-state-container">
                        <div class="empty-state-card">
                            <div class="empty-state-icon">🗳️</div>
                            <h3 class="empty-state-title">Ei ban/nosto historiaa</h3>
                            <p class="empty-state-description">Tälle kaudelle ei ole ban/nosto historiatietoja saatavilla.</p>
                        </div>
                    </div>
                </section>
            </div>
        </div>
    `
};
