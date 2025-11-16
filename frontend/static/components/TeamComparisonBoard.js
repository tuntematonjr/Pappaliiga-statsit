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
            default: () => ({ column: 'rating', order: 'desc', numeric: true })
        }
    },
    data() {
        return {
            columns: [
                { key: 'team', label: 'Joukkue', sortable: true, align: 'left', colClass: 'col-team' },
                { key: 'matches', label: 'Ottelut', sortable: true, numeric: true, colClass: 'col-sm' },
                { key: 'wins', label: 'Voitot', sortable: true, numeric: true, colClass: 'col-sm' },
                { key: 'losses', label: 'Tappiot', sortable: true, numeric: true, colClass: 'col-sm' },
                { key: 'split', label: 'Voittojakauma', sortable: false, colClass: 'col-wide' },
                { key: 'round_diff', label: 'Erä-ero', sortable: true, numeric: true, colClass: 'col-sm' },
                { key: 'win_rate', label: 'Voittoprosentti', sortable: true, numeric: true, colClass: 'col-sm', format: v => `${Number(v).toFixed(1)} %` },
                { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2, colClass: 'col-sm' },
                { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1, colClass: 'col-sm' },
                { key: 'rating', label: 'Rating', sortable: true, numeric: true, decimals: 2, colClass: 'col-sm' }
            ]
        };
    },
    computed: {
        rows() {
            if (!Array.isArray(this.teams)) {
                return [];
            }

            return this.teams.map((team, idx) => {
                const matches = Number(team.matches_played ?? team.matches ?? team.played ?? 0);
                const wins = Number(team.wins ?? team.maps_won ?? 0);
                const losses = Number(team.losses ?? team.maps_lost ?? 0);
                const roundsDiffRaw = Number(team.rounds_diff ?? team.rounds_delta ?? team.round_diff ?? 0);
                const winRate = this.parseNumber(team.win_rate ?? team.winRate ?? (matches ? (wins / matches) * 100 : 0));
                const kd = this.parseNumber(team.kd ?? team.kd_ratio ?? 0);
                const adr = this.parseNumber(team.adr ?? team.average_damage ?? 0);
                const rating = this.parseNumber(team.rating ?? team.hltv_rating ?? team.team_rating ?? 0);
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
                    rating,
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
            const numeric = this.parseNumber(value);
            return `${numeric.toFixed(1)} %`;
        }
    },
    template: `
        <section class="team-comparison card">
            <header class="card-head">
                <div>
                    <h2 class="title title-accent titleUnderlineCard title-delay-0">{{ title }}</h2>
                    <p v-if="subtitle" class="subtitle muted">{{ subtitle }}</p>
                </div>
            </header>

            <div class="card-content">
                <loading-spinner v-if="loading" message="Joukkueita ladataan..."></loading-spinner>
                <error-message v-else-if="error" :message="error"></error-message>

                <template v-else>
                    <sortable-table
                        v-if="rows.length"
                        :columns="columns"
                        :data="rows"
                        :defaultSort="defaultSort"
                        class="team-comparison-table"
                        :compact="true"
                    >
                        <template #cell-team="{ row }">
                            <div class="team-comparison-team">
                                <span class="team-rank">{{ row.rank }}</span>
                                <img class="team-logo" :src="row.logo" :alt="row.name" loading="lazy" />
                                <span class="team-name">{{ row.name }}</span>
                            </div>
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
                                :show-labels="false"
                                :show-percent="true"
                            ></split-bar>
                            <div class="split-labels">
                                <span class="wins">{{ row.wins }} voittoa</span>
                                <span class="losses">{{ row.losses }} tappiota</span>
                            </div>
                        </template>
                    </sortable-table>

                    <p v-else class="muted empty">{{ emptyMessage }}</p>
                </template>
            </div>
        </section>
    `
};
