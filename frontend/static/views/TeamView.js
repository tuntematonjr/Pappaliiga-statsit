const DEFAULT_TEAM_LOGO = window.PAPPALIIGA_DEFAULT_LOGO;

window.TeamView = {
    name: 'TeamView',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; },
        get StatPanel() { return window.StatPanel; },
        get SortableTable() { return window.SortableTable; },
        get PlayerRow() { return window.PlayerRow; },
        get CopyLink() { return window.CopyLink; },
        get SparklineChart() { return window.SparklineChart; }
    },
    props: {
        profile: { type: Object, default: null },
        breadcrumbs: { type: Array, default: () => [] },
        seasonOptions: { type: Array, default: () => [] },
        selectedSeason: { type: String, default: null },
        seasonLoading: { type: Boolean, default: false },
        seasonError: { type: String, default: null },
        activeTab: { type: String, default: 'overview' },
        metrics: { type: Array, default: () => [] },
        sparkline: { type: Array, default: () => [] },
        mapHighlights: { type: Array, default: () => [] },
        mapStats: { type: Array, default: () => [] },
        mapStatsLoading: { type: Boolean, default: false },
        mapStatsError: { type: String, default: null },
        matches: { type: Array, default: () => [] },
        matchesLoading: { type: Boolean, default: false },
        matchesError: { type: String, default: null },
        matchesPage: { type: Number, default: 1 },
        matchesTotalPages: { type: Number, default: 0 },
        matchesPageSize: { type: Number, default: 8 },
        players: { type: Array, default: () => [] },
        playerColumns: { type: Array, default: () => [] },
        playersLoading: { type: Boolean, default: false },
        playersError: { type: String, default: null }
    },
    emits: ['select-season', 'select-tab', 'refresh', 'change-page'],
    computed: {
        teamName() {
            return this.profile?.display_name || this.profile?.team_name || this.profile?.name || 'Joukkue';
        },
        crestUrl() {
            const src = this.profile?.avatar || this.profile?.logo || this.profile?.team_logo;
            if (!src) return DEFAULT_TEAM_LOGO;
            try {
                const resolved = window.apiClient.proxyAvatar(src);
                return resolved || DEFAULT_TEAM_LOGO;
            } catch (error) {
                return src || DEFAULT_TEAM_LOGO;
            }
        },
        faceitLink() {
            return this.profile?.faceit_url || this.profile?.faceit || null;
        },
        tabOptions() {
            return [
                { key: 'overview', label: 'Yleiskuva' },
                { key: 'matches', label: 'Ottelut' },
                { key: 'players', label: 'Pelaajat' }
            ];
        },
        hasSeasonPills() {
            return Array.isArray(this.seasonOptions) && this.seasonOptions.length > 1;
        },
        matchesEmpty() {
            return !this.matchesLoading && (!Array.isArray(this.matches) || !this.matches.length);
        },
        playersEmpty() {
            return !this.playersLoading && (!Array.isArray(this.players) || !this.players.length);
        }
    },
    methods: {
        emitSeason(seasonId) {
            this.$emit('select-season', seasonId);
        },
        emitTab(tab) {
            this.$emit('select-tab', tab);
        },
        emitRefresh() {
            this.$emit('refresh');
        },
        emitPage(page) {
            this.$emit('change-page', page);
        },
        isActiveTab(tab) {
            return this.activeTab === tab;
        },
        breadcrumbTo(crumb) {
            return crumb?.to || null;
        },
        formatMatchDate(match) {
            const raw = match?.played_at || match?.date || match?.scheduled_at;
            if (!raw) return '';
            try {
                const date = new Date(raw);
                return date.toLocaleDateString('fi-FI', { day: '2-digit', month: '2-digit', year: 'numeric' });
            } catch (error) {
                return raw;
            }
        },
        matchOpponent(match) {
            return match?.opponent?.name || match?.opponent_name || match?.enemy || match?.opponent || 'Vastustaja';
        },
        matchScoreline(match) {
            const forScore = match?.team_score ?? match?.score_for ?? match?.for ?? match?.score?.for;
            const againstScore = match?.opponent_score ?? match?.score_against ?? match?.against ?? match?.score?.against;
            if (forScore == null || againstScore == null) {
                return match?.scoreline || match?.result || '';
            }
            const formatted = `${forScore} - ${againstScore}`;
            if (match?.result) {
                return `${formatted} · ${match.result}`;
            }
            return formatted;
        }
        return '';
        },
        matchOutcomeClass(match) {
            const result = (match?.result || '').toLowerCase();
            if (result.includes('win') || result.includes('voitto')) return 'match-card__score--win';
            if (result.includes('loss') || result.includes('tappio')) return 'match-card__score--loss';
            if (result.includes('draw') || result.includes('tasapeli')) return 'match-card__score--draw';
            const scoreFor = match?.team_score ?? match?.score_for;
            const scoreAgainst = match?.opponent_score ?? match?.score_against;
            if (scoreFor > scoreAgainst) return 'match-card__score--win';
            if (scoreFor < scoreAgainst) return 'match-card__score--loss';
            return '';
        }
    },
    template: `
        <div class="team-view">
            <header class="team-header glass-card">
                <nav class="team-breadcrumbs" aria-label="Murupolku">
                    <template v-for="(crumb, index) in breadcrumbs" :key="index">
                        <router-link
                            v-if="crumb.to"
                            class="team-breadcrumbs__link"
                            :to="breadcrumbTo(crumb)"
                        >
                            {{ crumb.label }}
                        </router-link>
                        <span v-else class="team-breadcrumbs__current">{{ crumb.label }}</span>
                    </template>
                </nav>
                <div class="team-header__content">
                    <div class="team-header__identity">
                        <div class="team-header__crest">
                            <img :src="crestUrl" :alt="teamName" loading="lazy" />
                        </div>
                        <div class="team-header__meta">
                            <h1 class="title-accent titleUnderlineMain">{{ teamName }}</h1>
                            <div class="team-header__actions">
                                <a v-if="faceitLink" :href="faceitLink" target="_blank" rel="noopener" class="btn-primary">Faceit</a>
                                <copy-link label="Jaa joukkue"></copy-link>
                            </div>
                        </div>
                    </div>
                    <div v-if="seasonOptions.length" class="team-season-pills">
                        <button
                            v-for="season in seasonOptions"
                            :key="season.value"
                            type="button"
                            class="team-season-pill"
                            :class="{ 'team-season-pill--active': selectedSeason === season.value }"
                            @click="emitSeason(season.value)"
                        >
                            {{ season.label }}<span v-if="season.isCurrent" class="team-season-pill__tag">NYT</span>
                        </button>
                    </div>
                    <error-message
                        v-if="seasonError"
                        :message="seasonError"
                        @retry="emitRefresh"
                    ></error-message>
                </div>
                <div class="team-tabs" role="tablist">
                    <button
                        v-for="tab in tabOptions"
                        :key="tab.key"
                        type="button"
                        class="team-tab"
                        :class="{ 'team-tab--active': isActiveTab(tab.key) }"
                        role="tab"
                        @click="emitTab(tab.key)"
                    >
                        {{ tab.label }}
                    </button>
                </div>
            </header>

            <section v-if="isActiveTab('overview')" class="team-section">
                <div class="team-overview">
                    <stat-panel :items="metrics" :columns="3"></stat-panel>
                    <div class="team-overview__sparkline glass-card">
                        <h3 class="title-accent titleUnderlineCard">Viime ottelut</h3>
                        <sparkline-chart
                            v-if="Array.isArray(sparkline) && sparkline.length"
                            :points="sparkline"
                            :width="160"
                            :height="60"
                        ></sparkline-chart>
                        <p v-else class="team-overview__empty">Otteluhistoria ei ole saatavilla.</p>
                    </div>
                    <div class="team-overview__maps">
                        <loading-spinner
                            v-if="mapStatsLoading && !mapHighlights.length"
                            message="Karttatilastoja ladataan..."
                        ></loading-spinner>
                        <error-message
                            v-else-if="mapStatsError && !mapHighlights.length"
                            :message="mapStatsError"
                            @retry="emitRefresh"
                        ></error-message>
                        <article v-else v-for="(map, idx) in mapHighlights" :key="map.id" class="team-map-card glass-card">
                            <h4 class="title-accent titleUnderlineCard">{{ map.name }}</h4>
                            <p class="team-map-card__stat">{{ map.played }} karttaa</p>
                            <p class="team-map-card__metric">{{ map.winRate.toFixed(1) }} %</p>
                            <p class="team-map-card__meta">ADR {{ map.adr.toFixed(1) }} · Rating {{ map.rating.toFixed(2) }}</p>
                        </article>
                        <p v-if="!mapHighlights.length" class="team-overview__empty">Karttatilastoja ei löytynyt.</p>
                    </div>
                </div>
            </section>

            <section v-else-if="isActiveTab('matches')" class="team-section">
                <loading-spinner v-if="matchesLoading" message="Otteluita ladataan..."></loading-spinner>
                <error-message v-else-if="matchesError" :message="matchesError" @retry="emitRefresh"></error-message>
                <div v-else class="team-matches">
                    <p v-if="matchesEmpty" class="team-overview__empty">Ei otteluita tälle kaudelle.</p>
                    <article v-for="(match, index) in matches" :key="match.id || index" class="match-card glass-card">
                        <header class="match-card__header">
                            <span class="match-card__opponent">{{ matchOpponent(match) }}</span>
                            <span class="match-card__date">{{ formatMatchDate(match) }}</span>
                        </header>
                        <div class="match-card__score" :class="matchOutcomeClass(match)">
                            {{ matchScoreline(match) }}
                        </div>
                        <p v-if="match.notes" class="match-card__notes">{{ match.notes }}</p>
                    </article>
                    <div v-if="matchesTotalPages > 1" class="pagination">
                        <button type="button" class="pagination__btn" :disabled="matchesPage <= 1" @click="emitPage(matchesPage - 1)">Edellinen</button>
                        <span class="pagination__info">Sivu {{ matchesPage }} / {{ matchesTotalPages }}</span>
                        <button type="button" class="pagination__btn" :disabled="matchesPage >= matchesTotalPages" @click="emitPage(matchesPage + 1)">Seuraava</button>
                    </div>
                </div>
            </section>

            <section v-else class="team-section">
                <loading-spinner v-if="playersLoading" message="Pelaajia ladataan..."></loading-spinner>
                <error-message v-else-if="playersError" :message="playersError" @retry="emitRefresh"></error-message>
                <div v-else>
                    <sortable-table
                        v-if="!playersEmpty"
                        :columns="playerColumns"
                        :data="players"
                        :default-sort="{ column: 'rating', order: 'desc', numeric: true }"
                        :compact="false"
                    >
                        <template #cell-player="{ row }">
                            <player-row :player="row.player"></player-row>
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
                        <template #cell-hs="{ row }">
                            <span>{{ row.hs.toFixed(1) }} %</span>
                        </template>
                    </sortable-table>
                    <p v-else class="team-overview__empty">Ei pelaajatietoja saatavilla.</p>
                </div>
            </section>
        </div>
    `
};
