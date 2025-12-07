// Comprehensive Team Detail Component with full data display
// Displays all available team data: stats, maps, matches, players, veto history, and advanced metrics

const TEAM_STAT_BOX_SCHEMA = [
    { key: 'matches', label: 'Ottelut', value: 'data.teamStats.matchesPlayed', digits: 0 },
    { key: 'wins', label: 'Voitot', value: 'data.teamStats.wins', digits: 0, suffix: 'W' },
    { key: 'winrate', label: 'Voitto%', value: 'data.teamStats.winRate', digits: 1, percent: true },
    { key: 'rounds', label: 'Erät pelattu', value: 'data.teamStats.roundsWon', digits: 0 },
    { key: 'rounddiff', label: 'Eräero', value: 'data.teamStats.roundsDiff', digits: 0 },
    { key: 'maps_won', label: 'Kartat voitettu', value: 'data.teamStats.mapsWon', digits: 0 }
];

const PLAYER_TABLE_COLUMNS = [
    { key: 'nickname', label: 'Pelaaja', sortable: true, align: 'left', width: '160px' },
    { key: 'maps_played', label: 'Kartat', sortable: true, numeric: true, width: '80px' },
    { key: 'rating', label: 'Rating', sortable: true, numeric: true, decimals: 2, width: '90px' },
    { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2, width: '80px' },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1, width: '80px' },
    { key: 'kr', label: 'K/R', sortable: true, numeric: true, decimals: 2, width: '75px' },
    { key: 'hs_pct', label: 'HS%', sortable: true, numeric: true, decimals: 1, width: '75px' },
    { key: 'mk_3k', label: '3K', sortable: true, numeric: true, width: '60px' },
    { key: 'mk_4k', label: '4K', sortable: true, numeric: true, width: '60px' },
    { key: 'mk_5k', label: 'Ace', sortable: true, numeric: true, width: '60px' },
    { key: 'clutch_kills', label: 'Clutch', sortable: true, numeric: true, width: '75px' }
];

const MAP_COLUMNS = [
    { key: 'map_name', label: 'Kartta', sortable: true, align: 'left', width: '140px' },
    { key: 'played', label: 'Pelattu', sortable: true, numeric: true, width: '75px' },
    { key: 'winrate', label: 'Win%', sortable: true, numeric: true, decimals: 1, width: '75px' },
    { key: 'wins', label: 'W', sortable: true, numeric: true, width: '60px' },
    { key: 'picks', label: 'Picks', sortable: true, numeric: true, width: '65px' },
    { key: 'ban1', label: 'Bans', sortable: true, numeric: true, width: '65px' },
    { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2, width: '75px' },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1, width: '75px' }
];

const VETO_AGGREGATES_COLUMNS = [
    { key: 'map_name', label: 'Kartta', sortable: true, align: 'left', width: '140px' },
    { key: 'times_picked', label: 'Valittu', sortable: true, numeric: true, width: '85px' },
    { key: 'times_banned', label: 'Bannit', sortable: true, numeric: true, width: '85px' },
    { key: 'pick_rate', label: 'Pick%', sortable: true, numeric: true, decimals: 1, width: '80px' },
    { key: 'ban_rate', label: 'Ban%', sortable: true, numeric: true, decimals: 1, width: '80px' }
];

function toCamelCase(obj) {
    if (!obj) return obj;
    if (Array.isArray(obj)) return obj.map(toCamelCase);
    if (obj !== Object(obj)) return obj;
    return Object.keys(obj).reduce((result, key) => {
        const camel = key.replace(/_([a-z])/g, (g) => g[1].toUpperCase());
        result[camel] = toCamelCase(obj[key]);
        return result;
    }, {});
}

function toNumber(v, fallback = 0) {
    if (v === null || v === undefined) return fallback;
    const n = Number(v);
    return Number.isFinite(n) ? n : fallback;
}

window.TeamDetail = {
    name: 'TeamDetail',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; },
        get SortableTable() { return window.SortableTable; },
        get ProgressBar() { return window.ProgressBar; }
    },
    props: {
        teamId: { type: [String, Number], required: true },
        championshipId: { type: [String, Number], default: null }
    },
    data() {
        return {
            pageData: null,
            currentChampionshipId: this.championshipId ? String(this.championshipId) : null,
            loading: false,
            error: null,
            activeTab: 'overview',
            playerSortKey: 'rating',
            playerSortDesc: true,
            mapSortKey: 'played',
            mapSortDesc: true,
            vetoSortKey: 'times_picked',
            vetoSortDesc: true,
            matchesPage: 1,
            matchesPageSize: 10,
            expandedMatch: null
        };
    },
    computed: {
        teamInfo() {
            return this.pageData?.team || null;
        },
        seasonOptions() {
            const seasons = this.pageData?.seasons || [];
            return seasons.map(s => ({
                value: s.championship_id,
                label: `Kausi ${s.season} · Div ${s.division_num}`,
                season: s.season,
                division: s.division_num
            }));
        },
        selectedChampionship() {
            return this.currentChampionshipId || (this.seasonOptions[0]?.value || null);
        },
        seasonData() {
            if (!this.pageData?.season_data) return null;
            return toCamelCase(this.pageData.season_data);
        },
        teamStats() {
            return this.seasonData?.teamStats || {};
        },
        mapStats() {
            return (this.seasonData?.mapStats || []).map(toCamelCase);
        },
        matchHistory() {
            return (this.seasonData?.matchHistory || []).map(toCamelCase);
        },
        playerStats() {
            return (this.seasonData?.playerStats || []).map(toCamelCase);
        },
        vetoHistory() {
            return (this.seasonData?.vetoHistory || []).map(toCamelCase);
        },
        vetoAggregates() {
            return (this.seasonData?.vetoAggregates || []).map(toCamelCase);
        },
        statBoxes() {
            return TEAM_STAT_BOX_SCHEMA.map(schema => {
                const value = this.teamStats[schema.key.replace('data.teamStats.', '')];
                return {
                    ...schema,
                    displayValue: value != null ? Number(value).toFixed(schema.digits) : '–'
                };
            });
        },
        sortedPlayers() {
            const sorted = [...this.playerStats].sort((a, b) => {
                const aVal = toNumber(a[this.playerSortKey]);
                const bVal = toNumber(b[this.playerSortKey]);
                return this.playerSortDesc ? bVal - aVal : aVal - bVal;
            });
            return sorted;
        },
        sortedMaps() {
            const sorted = [...this.mapStats].sort((a, b) => {
                const aVal = toNumber(a[this.mapSortKey]);
                const bVal = toNumber(b[this.mapSortKey]);
                return this.mapSortDesc ? bVal - aVal : aVal - bVal;
            });
            return sorted;
        },
        sortedVetoAgg() {
            const sorted = [...this.vetoAggregates].sort((a, b) => {
                const aVal = toNumber(a[this.vetoSortKey]);
                const bVal = toNumber(b[this.vetoSortKey]);
                return this.vetoSortDesc ? bVal - aVal : aVal - bVal;
            });
            return sorted;
        },
        paginatedMatches() {
            const total = this.matchHistory.length;
            const start = (this.matchesPage - 1) * this.matchesPageSize;
            const end = start + this.matchesPageSize;
            return {
                items: this.matchHistory.slice(start, end),
                total,
                page: this.matchesPage,
                pageSize: this.matchesPageSize,
                totalPages: Math.ceil(total / this.matchesPageSize)
            };
        },
        hasVetoData() {
            return this.vetoHistory.length > 0 || this.vetoAggregates.length > 0;
        }
    },
    watch: {
        teamId: {
            immediate: true,
            async handler(newId) {
                if (newId) {
                    await this.loadTeamData();
                }
            }
        },
        championshipId(newVal) {
            if (newVal) {
                this.currentChampionshipId = String(newVal);
                this.matchesPage = 1;
                this.loadSeasonData();
            }
        }
    },
    methods: {
        async loadTeamData() {
            this.loading = true;
            this.error = null;
            try {
                const enc = encodeURIComponent(String(this.teamId));
                const routes = [
                    `/api/teams/${enc}/page`,
                    `/teams/${enc}/page`
                ];
                
                let data = null;
                for (const route of routes) {
                    try {
                        const resp = await fetch(route);
                        if (!resp.ok) continue;
                        data = await resp.json();
                        break;
                    } catch (e) {
                        continue;
                    }
                }
                
                if (!data) {
                    throw new Error('Failed to load team data');
                }
                
                this.pageData = data;
                if (!this.currentChampionshipId && data.currentChampionshipId) {
                    this.currentChampionshipId = data.currentChampionshipId;
                }
                
                await this.loadSeasonData();
            } catch (err) {
                this.error = String(err.message || 'Failed to load team');
            } finally {
                this.loading = false;
            }
        },
        async loadSeasonData() {
            if (!this.selectedChampionship) return;
            
            try {
                const teamEnc = encodeURIComponent(String(this.teamId));
                const champEnc = encodeURIComponent(this.selectedChampionship);
                const routes = [
                    `/api/teams/${teamEnc}/season/${champEnc}`,
                    `/teams/${teamEnc}/season/${champEnc}`
                ];
                
                let data = null;
                for (const route of routes) {
                    try {
                        const resp = await fetch(route);
                        if (!resp.ok) continue;
                        data = await resp.json();
                        break;
                    } catch (e) {
                        continue;
                    }
                }
                
                if (data) {
                    this.pageData = {
                        ...this.pageData,
                        season_data: data
                    };
                }
            } catch (err) {
                console.error('Failed to load season data:', err);
            }
        },
        selectChampionship(champId) {
            if (champId !== this.currentChampionshipId) {
                this.currentChampionshipId = champId;
            }
        },
        selectTab(tab) {
            this.activeTab = tab;
        },
        isActiveTab(tab) {
            return this.activeTab === tab;
        },
        teamLogo() {
            const src = this.teamInfo?.avatar;
            if (!src) return '/images/default-logo.png';
            try {
                return window.apiClient?.proxyAvatar?.(src) || src;
            } catch {
                return src;
            }
        },
        formatDate(ts) {
            if (!ts) return '';
            try {
                const d = new Date(ts * 1000);
                return d.toLocaleDateString('fi-FI');
            } catch {
                return '';
            }
        },
        formatPercent(v, decimals = 1) {
            return (toNumber(v)).toFixed(decimals);
        },
        getMatchResult(match) {
            const ourTeamId = this.teamInfo?.team_id;
            const isTeam1 = String(match.team1Id) === String(ourTeamId);
            const ourScore = isTeam1 ? match.maps?.[match.played - 1]?.scoreTeam1 : match.maps?.[match.played - 1]?.scoreTeam2;
            const oppScore = isTeam1 ? match.maps?.[match.played - 1]?.scoreTeam2 : match.maps?.[match.played - 1]?.scoreTeam1;
            
            if (ourScore > oppScore) return 'win';
            if (ourScore < oppScore) return 'loss';
            return 'draw';
        },
        toggleMatchExpand(matchId) {
            this.expandedMatch = this.expandedMatch === matchId ? null : matchId;
        },
        setMatchPage(page) {
            this.matchesPage = Math.max(1, Math.min(page, this.paginatedMatches.totalPages));
        }
    },
    template: `
        <div class="team-page-container">
            <loading-spinner v-if="loading && !pageData" message="Joukkuetta ladataan..."></loading-spinner>
            
            <error-message 
                v-else-if="error && !pageData" 
                :message="error" 
                @retry="loadTeamData"
            ></error-message>
            
            <div v-else-if="pageData" class="team-page">
                <!-- HERO HEADER -->
                <header class="team-hero glass-card">
                    <div class="team-hero__logo">
                        <img :src="teamLogo()" :alt="teamInfo?.teamName" />
                    </div>
                    <div class="team-hero__content">
                        <h1 class="team-hero__title title-accent titleUnderlinePage">
                            {{ teamInfo?.displayName || teamInfo?.teamName || 'Team' }}
                        </h1>
                        <p v-if="seasonOptions.length" class="team-hero__season">
                            Kausi {{ seasonOptions[0]?.season }} · Divisio {{ seasonOptions[0]?.division }}
                        </p>
                    </div>
                    
                    <!-- Season selector -->
                    <div v-if="seasonOptions.length > 1" class="team-season-selector">
                        <button
                            v-for="season in seasonOptions"
                            :key="season.value"
                            type="button"
                            class="season-pill"
                            :class="{ 'season-pill--active': selectedChampionship === season.value }"
                            @click="selectChampionship(season.value)"
                        >
                            S{{ season.season }}D{{ season.division }}
                        </button>
                    </div>
                </header>

                <!-- TAB NAVIGATION -->
                <nav class="team-tabs" role="tablist">
                    <button
                        v-for="tab in ['overview', 'maps', 'matches', 'players', 'veto']"
                        :key="tab"
                        type="button"
                        class="team-tab"
                        :class="{ 'team-tab--active': isActiveTab(tab) }"
                        @click="selectTab(tab)"
                        role="tab"
                    >
                        {{ { overview: 'Yleiskuva', maps: 'Kartat', matches: 'Ottelut', players: 'Pelaajat', veto: 'Veto/Nosto' }[tab] }}
                    </button>
                </nav>

                <!-- OVERVIEW TAB -->
                <section v-if="isActiveTab('overview')" class="team-section">
                    <h2 class="section-title titleUnderline">Kaudenstatistiikka</h2>
                    <div class="stat-boxes-grid">
                        <div
                            v-for="box in statBoxes"
                            :key="box.key"
                            class="stat-box glass-card"
                        >
                            <div class="stat-box__label">{{ box.label }}</div>
                            <div class="stat-box__value">{{ box.displayValue }}</div>
                        </div>
                    </div>

                    <h2 class="section-title titleUnderline" style="margin-top: 2rem;">Karttavertailu</h2>
                    <div v-if="mapStats.length" class="maps-preview-grid">
                        <div
                            v-for="map in mapStats.slice(0, 6)"
                            :key="map.mapName"
                            class="map-card glass-card"
                        >
                            <h3>{{ map.mapName }}</h3>
                            <div class="map-card__stats">
                                <div>{{ map.played }} pelattu</div>
                                <div class="stat-positive">{{ formatPercent(map.winrate) }}% W/R</div>
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

                <!-- MAPS TAB -->
                <section v-if="isActiveTab('maps')" class="team-section">
                    <h2 class="section-title titleUnderline">Kartat - Yksityiskohtainen analyysi</h2>
                    <div v-if="sortedMaps.length" class="table-wrapper">
                        <table class="data-table">
                            <thead>
                                <tr>
                                    <th
                                        v-for="col in MAP_COLUMNS"
                                        :key="col.key"
                                        class="table-header"
                                        @click="mapSortKey === col.key ? mapSortDesc = !mapSortDesc : (mapSortKey = col.key, mapSortDesc = true)"
                                        style="cursor: pointer;"
                                    >
                                        {{ col.label }}
                                        <span v-if="mapSortKey === col.key">{{ mapSortDesc ? '↓' : '↑' }}</span>
                                    </th>
                                </tr>
                            </thead>
                            <tbody>
                                <tr v-for="map in sortedMaps" :key="map.mapName" class="table-row">
                                    <td class="table-cell">{{ map.mapName }}</td>
                                    <td class="table-cell numeric">{{ map.played }}</td>
                                    <td class="table-cell numeric">{{ formatPercent(map.winrate) }}%</td>
                                    <td class="table-cell numeric">{{ map.wins }}</td>
                                    <td class="table-cell numeric">{{ map.picks }}</td>
                                    <td class="table-cell numeric">{{ map.ban1 + map.ban2 }}</td>
                                    <td class="table-cell numeric">{{ formatPercent(map.kd, 2) }}</td>
                                    <td class="table-cell numeric">{{ formatPercent(map.adr, 1) }}</td>
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

                <!-- MATCHES TAB -->
                <section v-if="isActiveTab('matches')" class="team-section">
                    <h2 class="section-title titleUnderline">Ottelut ({{ paginatedMatches.total }} yhteensä)</h2>
                    <div v-if="paginatedMatches.items.length" class="matches-list">
                        <div
                            v-for="match in paginatedMatches.items"
                            :key="match.matchId"
                            class="match-card glass-card"
                            :class="'match-card--' + getMatchResult(match)"
                        >
                            <div class="match-card__header">
                                <div class="match-date">{{ formatDate(match.ts) }}</div>
                                <div class="match-result">{{ match.bestOf }}:{{ match.played }}</div>
                            </div>
                            <div class="match-card__teams">
                                <span>{{ match.team1Name }}</span>
                                <span class="vs">vs</span>
                                <span>{{ match.team2Name }}</span>
                            </div>
                        </div>
                    </div>
                    <div v-else class="empty-state-container">
                        <div class="empty-state-card">
                            <div class="empty-state-icon">🎮</div>
                            <h3 class="empty-state-title">Ei otteluita</h3>
                            <p class="empty-state-description">Tälle kaudelle ei ole otteluhistoriaa saatavilla.</p>
                        </div>
                    </div>

                    <!-- Pagination -->
                    <div v-if="paginatedMatches.totalPages > 1" class="pagination">
                        <button
                            type="button"
                            @click="setMatchPage(paginatedMatches.page - 1)"
                            :disabled="paginatedMatches.page === 1"
                            class="btn-small"
                        >
                            ← Edellinen
                        </button>
                        <span>{{ paginatedMatches.page }} / {{ paginatedMatches.totalPages }}</span>
                        <button
                            type="button"
                            @click="setMatchPage(paginatedMatches.page + 1)"
                            :disabled="paginatedMatches.page === paginatedMatches.totalPages"
                            class="btn-small"
                        >
                            Seuraava →
                        </button>
                    </div>
                </section>

                <!-- PLAYERS TAB -->
                <section v-if="isActiveTab('players')" class="team-section">
                    <h2 class="section-title titleUnderline">Pelaajat</h2>
                    <div v-if="sortedPlayers.length" class="table-wrapper">
                        <table class="data-table">
                            <thead>
                                <tr>
                                    <th
                                        v-for="col in PLAYER_TABLE_COLUMNS"
                                        :key="col.key"
                                        class="table-header"
                                        @click="playerSortKey === col.key ? playerSortDesc = !playerSortDesc : (playerSortKey = col.key, playerSortDesc = true)"
                                        style="cursor: pointer;"
                                    >
                                        {{ col.label }}
                                        <span v-if="playerSortKey === col.key">{{ playerSortDesc ? '↓' : '↑' }}</span>
                                    </th>
                                </tr>
                            </thead>
                            <tbody>
                                <tr v-for="player in sortedPlayers" :key="player.playerId" class="table-row">
                                    <td class="table-cell">{{ player.nickname }}</td>
                                    <td class="table-cell numeric">{{ player.mapsPlayed }}</td>
                                    <td class="table-cell numeric">{{ formatPercent(player.rating, 2) }}</td>
                                    <td class="table-cell numeric">{{ formatPercent(player.kd, 2) }}</td>
                                    <td class="table-cell numeric">{{ formatPercent(player.adr, 1) }}</td>
                                    <td class="table-cell numeric">{{ formatPercent(player.kr, 2) }}</td>
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

                <!-- VETO TAB -->
                <section v-if="isActiveTab('veto')" class="team-section">
                    <h2 class="section-title titleUnderline">Ban/Nosto Tilastot</h2>
                    
                    <div v-if="sortedVetoAgg.length" class="table-wrapper">
                        <table class="data-table">
                            <thead>
                                <tr>
                                    <th
                                        v-for="col in VETO_AGGREGATES_COLUMNS"
                                        :key="col.key"
                                        class="table-header"
                                        @click="vetoSortKey === col.key ? vetoSortDesc = !vetoSortDesc : (vetoSortKey = col.key, vetoSortDesc = true)"
                                        style="cursor: pointer;"
                                    >
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
                                    <td class="table-cell numeric">{{ formatPercent(veto.pickRate, 1) }}%</td>
                                    <td class="table-cell numeric">{{ formatPercent(veto.banRate, 1) }}%</td>
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
    const wins = toNumber(stats.maps_won ?? stats.wins ?? stats.matches_won);
    const losses = toNumber(stats.maps_lost ?? stats.losses ?? stats.matches_lost);
    const matches = toNumber(
        stats.matches_played
        ?? stats.matches
        ?? stats.series_played
        ?? stats.match_count
        ?? stats.series_count
        ?? 0
    );
    const roundsDiff = toNumber(stats.rounds_diff ?? stats.round_diff ?? stats.rounds_delta);
    const winRate = matches > 0
        ? (wins / matches) * 100
        : toNumber(stats.map_win_rate ?? stats.win_rate ?? stats.match_win_rate);
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
            const wins = toNumber(current.maps_won ?? current.wins);
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
        return {
            teamStore,
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
        pageSegment() {
            return this.teamEntry?.page || createSegment();
        },
        pageData() {
            return this.pageSegment.data || null;
        },
        profile() {
            return this.pageData?.team || null;
        },
        seasonOptions() {
            const seasons = Array.isArray(this.pageData?.seasons) ? this.pageData.seasons : [];
            return seasons.map(season => {
                const value = String(season.championship_id || season.championshipId || season.id || '');
                return {
                    value,
                    label: season.name || `Kausi ${season.season} · Div ${season.division_num}`,
                    season: season.season,
                    division: season.division_num,
                    championshipId: value,
                    isCurrent: value === this.currentChampionshipId
                };
            }).filter(option => option.value);
        },
        currentChampionshipId() {
            if (this.pageData?.currentChampionshipId) {
                return String(this.pageData.currentChampionshipId);
            }
            if (this.selectedChampionship) {
                return String(this.selectedChampionship);
            }
            return this.seasonOptions.length ? this.seasonOptions[0].value : null;
        },
        seasonDetails() {
            return this.pageData?.season || null;
        },
        seasonStats() {
            return this.seasonDetails?.stats || this.seasonDetails?.team_stats || this.seasonDetails?.teamStats || this.seasonDetails || null;
        },
        seasonErrorMessage() {
            if (!this.pageSegment.error) return null;
            if (this.pageData) {
                return null;
            }
            return this.pageSegment.error;
        },
        mapStats() {
            if (Array.isArray(this.seasonDetails?.map_stats)) {
                return this.seasonDetails.map_stats;
            }
            if (Array.isArray(this.seasonDetails?.mapStats)) {
                return this.seasonDetails.mapStats;
            }
            return [];
        },
        matchesList() {
            return Array.isArray(this.seasonDetails?.matches) ? this.seasonDetails.matches : [];
        },
        paginatedMatches() {
            return paginate(this.matchesList, this.matchesPage, this.pageSize);
        },
        playersList() {
            return Array.isArray(this.seasonDetails?.players)
                ? this.seasonDetails.players
                : Array.isArray(this.seasonDetails?.playerStats)
                    ? this.seasonDetails.playerStats
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
            return this.pageSegment.loading && !this.playerRows.length;
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
        championshipId(newValue) {
            if (newValue) {
                this.selectedChampionship = String(newValue);
                this.matchesPage = 1;
                this.loadSeasonData(this.selectedChampionship, { force: true });
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
                } else if (this.seasonOptions.length && !this.selectedChampionship) {
                    this.selectedChampionship = this.seasonOptions[0].value;
                }
            } catch (error) {
                console.error('TeamDetail bootstrap error', error);
            }
        },
        async loadSeasonData(championshipId, options = {}) {
            if (!this.teamStore || !this.teamId || !championshipId) return;
            await this.teamStore.fetchTeamPage(this.teamId, championshipId, options);
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
            if (this.currentChampionshipId) {
                this.loadSeasonData(this.currentChampionshipId, { force: true });
            } else {
                this.bootstrap();
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
                :season-loading="pageSegment.loading"
                :season-error="seasonErrorMessage"
                :active-tab="activeTab"
                :metrics="overviewMetrics"
                :sparkline="sparklinePoints"
                :map-highlights="mapHighlights"
                :map-stats="mapStats"
                :map-stats-loading="pageSegment.loading"
                :map-stats-error="pageSegment.error"
                :matches="paginatedMatches.items"
                :matches-loading="pageSegment.loading"
                :matches-error="pageSegment.error"
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
