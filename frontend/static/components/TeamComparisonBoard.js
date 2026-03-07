// Shared Intl formatters keep numeric output consistent between tabs
const TEAM_BOARD_INT_FORMATTER = new Intl.NumberFormat('fi-FI');
const TEAM_BOARD_PERCENT_FORMATTER = new Intl.NumberFormat('fi-FI', {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1
});

// TeamComparisonBoard - shared comparison table for home & division views
window.TeamComparisonBoard = {
    name: 'TeamComparisonBoard',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; },
        get SortableTable() { return window.SortableTable; },
        get SplitBar() { return window.SplitBar; }
    },
    props: {
        title: {
            type: String,
            default: 'Joukkuevertailu'
        },
        subtitle: {
            type: String,
            default: ''
        },
        teams: {
            type: Array,
            default: () => []
        },
        loading: {
            type: Boolean,
            default: false
        },
        error: {
            type: String,
            default: null
        },
        emptyMessage: {
            type: String,
            default: 'Ei joukkueita'
        },
        defaultSort: {
            type: Object,
            default: () => ({ column: 'wins', order: 'desc', numeric: true })
        },
        stickyHeader: {
            type: Boolean,
            default: true
        },
        showHeader: {
            type: Boolean,
            default: false
        },
        showRank: {
            type: Boolean,
            default: true
        },
        highlightTeamId: {
            type: [String, Number],
            default: null
        },
        championshipId: {
            type: [String, Number],
            default: null
        },
        championshipName: {
            type: String,
            default: null
        },
        championshipSeason: {
            type: [String, Number],
            default: null
        }
    },
    data() {
        return {
            columns: [
                { key: 'team', label: 'Joukkue', sortable: true, align: 'left', colClass: 'col-team col-name', width: '260px', mobilePinned: true, mobilePriority: 1 },
                { key: 'matches', label: 'Ottelut', sortable: true, numeric: true, align: 'center', colClass: 'col-stat col-group-a', width: '90px', mobileLabel: 'Ott.', mobilePriority: 2 },
                { key: 'wins', label: 'Voitot', sortable: true, numeric: true, align: 'center', colClass: 'col-stat col-group-a', width: '88px', mobileHidden: true },
                { key: 'losses', label: 'Tappiot', sortable: true, numeric: true, align: 'center', colClass: 'col-stat col-group-a', width: '88px', mobileHidden: true },
                { key: 'split', label: 'Voittojakauma', sortable: false, colClass: 'col-bar', mobileLabel: 'W-L', mobilePriority: 3 },
                { key: 'win_rate', label: 'Voittoprosentti', sortable: true, numeric: true, align: 'center', colClass: 'col-stat col-group-b', width: '120px', mobileLabel: 'WR%', mobilePriority: 4 },
                { key: 'round_diff', label: 'Erä-ero', sortable: true, numeric: true, align: 'center', colClass: 'col-stat col-group-b', width: '110px', mobileLabel: 'Eraero', mobilePriority: 5 },
                { key: 'kd', label: 'K/D', sortable: true, numeric: true, align: 'center', decimals: 2, colClass: 'col-stat col-group-c', width: '90px', mobileHidden: true },
                { key: 'adr', label: 'ADR', sortable: true, numeric: true, align: 'center', decimals: 1, colClass: 'col-stat col-group-c', width: '100px', mobileHidden: true }
            ]
        };
    },
    computed: {
        rows() {
            if (!Array.isArray(this.teams)) {
                return [];
            }

            return this.teams.map((team, idx) => {
                const wins = Number(team.maps_won ?? team.wins ?? 0);
                const losses = Number(team.maps_lost ?? team.losses ?? 0);
                const matches = Number(
                    team.matches_played
                    ?? team.matches
                    ?? team.series_played
                    ?? team.match_count
                    ?? team.series_count
                    ?? 0
                );
                const roundsDiffRaw = Number(team.rounds_diff ?? team.rounds_delta ?? team.round_diff ?? 0);
                const winRate = this.parseNumber(team.map_win_rate ?? team.win_rate ?? team.winRate ?? (matches ? (wins / matches) * 100 : 0));
                const kd = this.parseNumber(team.kd ?? team.kd_ratio ?? 0);
                const adr = this.parseNumber(team.adr ?? team.average_damage ?? 0);
                const displayName = team.display_name || team.team_name || team.name || `Joukkue ${idx + 1}`;

                return {
                    id: team.team_id || team.id || `team-${idx}`,
                    rank: team.rank ?? idx + 1,
                    name: displayName,
                    logo: this.avatarUrl(team.logo || team.avatar || team.team_logo),
                    matches,
                    wins,
                    losses,
                    round_diff: roundsDiffRaw,
                    win_rate: winRate,
                    kd,
                    adr,
                    split: { wins, losses }
                };
            });
        }
    },
    methods: {
        avatarUrl(src) {
            const fallback = window.PAPPALIIGA_DEFAULT_LOGO;
            if (!src) {
                return fallback;
            }
            try {
                return window.apiClient && typeof window.apiClient.proxyAvatar === 'function'
                    ? window.apiClient.proxyAvatar(src)
                    : (src || fallback);
            } catch (err) {
                console.warn('TeamComparisonBoard avatar fallback failed', err);
                return src || fallback;
            }
        },
        parseNumber(value, fallback = 0) {
            if (value == null) return fallback;
            const num = typeof value === 'number' ? value : Number(String(value).replace(',', '.'));
            return Number.isFinite(num) ? num : fallback;
        },
        formatRoundDiff(value) {
            if (!Number.isFinite(value)) return '0';
            if (value > 0) return `+${value}`;
            return String(value);
        },
        formatPercent(value) {
            let numeric = this.parseNumber(value);
            if (Math.abs(numeric) <= 1 && numeric !== 0) {
                numeric *= 100;
            }
            const formatted = TEAM_BOARD_PERCENT_FORMATTER.format(numeric);
            return `${formatted} %`;
        },
        formatInteger(value) {
            const numeric = this.parseNumber(value);
            return TEAM_BOARD_INT_FORMATTER.format(numeric);
        },
        getTeamUrl(teamId, teamName) {
            if (this.championshipId) {
                return `/team/${this.championshipId}/${teamId}`;
            }
            return `/team/${teamId}`;
        },
        scrollToTeam(teamId, options = {}) {
            const resolvedId = this.resolveTeamId(teamId);
            if (!resolvedId) return;
            const rowEl = this.findRowElement(resolvedId);
            if (!rowEl) return;
            try {
                rowEl.scrollIntoView({ behavior: options.instant ? 'auto' : 'smooth', block: 'center', inline: 'nearest' });
            } catch (error) {
                rowEl.scrollIntoView();
            }
            this.flashRow(rowEl);
        },
        resolveTeamId(teamId) {
            if (teamId == null) return null;
            return String(teamId);
        },
        findRowElement(teamId) {
            const table = this.$refs.tableRef && this.$refs.tableRef.$el;
            if (!table) return null;
            const rows = table.querySelectorAll('tbody tr');
            for (const row of rows) {
                if (row.dataset && String(row.dataset.rowId) === String(teamId)) {
                    return row;
                }
            }
            return null;
        },
        flashRow(rowEl) {
            if (!rowEl) return;
            rowEl.classList.add('table-row--flash');
            setTimeout(() => {
                rowEl.classList.remove('table-row--flash');
            }, 1200);
        }
    },
    template: `
        <section class="team-comparison glass-card division-surface">
            <header v-if="showHeader" class="card-head team-comparison__head">
                <div>
                    <p class="section-eyebrow">TILASTOT</p>
                    <h2 class="title title-accent titleUnderlineCard">{{ title }}</h2>
                    <p v-if="subtitle" class="subtitle muted team-comparison__lede">{{ subtitle }}</p>
                </div>
            </header>

            <div class="card-content">
                <loading-spinner v-if="loading" message="Joukkueita ladataan..."></loading-spinner>
                <error-message v-else-if="error" :message="error"></error-message>

                <template v-else>
                    <sortable-table
                        v-if="rows.length"
                        ref="tableRef"
                        :columns="columns"
                        :data="rows"
                        :defaultSort="defaultSort"
                        :mobile-column-limit="5"
                        class="team-comparison-table"
                        :compact="true"
                        :sticky-header="stickyHeader"
                        :highlight-row-id="highlightTeamId"
                    >
                        <template #cell-team="{ row }">
                            <div class="team-comparison-team">
                                <span v-if="showRank" class="team-rank">{{ row.rank }}</span>
                                <img class="team-logo" :src="row.logo" :alt="row.name" loading="lazy" />
                                <a :href="getTeamUrl(row.id, row.name)" class="team-name team-name--link" :title="'Avaa ' + row.name + ' joukkueen sivu'">
                                    <span class="team-name__label">{{ row.name }}</span>
                                    <span class="team-name__icon" aria-hidden="true">↗</span>
                                </a>
                            </div>
                        </template>

                        <template #cell-matches="{ row }">
                            <span>{{ formatInteger(row.matches) }}</span>
                        </template>

                        <template #cell-wins="{ row }">
                            <span class="stat-pill stat-pill--ok">{{ formatInteger(row.wins) }}</span>
                        </template>

                        <template #cell-losses="{ row }">
                            <span class="stat-pill stat-pill--err">{{ formatInteger(row.losses) }}</span>
                        </template>

                        <template #cell-round_diff="{ row }">
                            <span :class="['round-diff', { positive: row.round_diff > 0, negative: row.round_diff < 0 }]">
                                {{ formatRoundDiff(row.round_diff) }}
                            </span>
                        </template>

                        <template #cell-win_rate="{ row }">
                            <span>{{ formatPercent(row.win_rate) }}</span>
                        </template>

                        <template #cell-split="{ row }">
                            <split-bar
                                :wins="row.split.wins"
                                :losses="row.split.losses"
                                height="30px"
                                :left-text="formatInteger(row.wins) + ' voittoa'"
                                :right-text="formatInteger(row.losses) + ' tappiota'"
                                :show-percent="false"
                            ></split-bar>
                        </template>
                    </sortable-table>

                    <p v-else class="muted empty">{{ emptyMessage }}</p>
                </template>
            </div>
        </section>
    `
};
