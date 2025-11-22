// Intl helpers for consistent numeric formatting inside the standings tab
const TEAM_CHART_INT_FORMATTER = new Intl.NumberFormat('fi-FI');
const TEAM_CHART_PERCENT_FORMATTER = new Intl.NumberFormat('fi-FI', {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1
});

// TeamComparisonChart - stacked horizontal bars comparing wins/round diff for teams
window.TeamComparisonChart = {
    name: 'TeamComparisonChart',
    props: {
        teams: {
            type: Array,
            default: () => []
        },
        limit: {
            type: Number,
            default: 10
        },
        showRanks: {
            type: Boolean,
            default: true
        },
        title: {
            type: String,
            default: 'Standings Overview'
        },
        subtitle: {
            type: String,
            default: ''
        },
        highlightTeamId: {
            type: [String, Number],
            default: null
        }
    },
    data() {
        return {
            _chartFlashTimer: null
        };
    },
    computed: {
        normalizedTeams() {
            if (!Array.isArray(this.teams)) {
                return [];
            }
            const rows = this.teams.slice(0, this.limit).map((team, idx) => {
                const wins = Number(team.wins ?? team.maps_won ?? 0);
                const losses = Number(team.losses ?? team.maps_lost ?? 0);
                const matchesPlayed = Number(
                    team.matchesPlayed ?? team.matches_played ?? team.matches ?? (wins + losses)
                );
                const roundDiff = Number(team.roundDiff ?? team.rounds_diff ?? team.round_diff ?? 0);
                const winPctRaw = Number(team.winPct ?? team.winRate ?? team.win_rate ?? 0);
                return {
                    id: this.resolveTeamId(team, idx),
                    name: team.name || team.team_name || team.display_name || `Joukkue ${idx + 1}`,
                    logo: this.avatarUrl(team.logo || team.avatar || team.team_logo),
                    wins,
                    losses,
                    matchesPlayed,
                    roundDiff,
                    winPct: Number.isFinite(winPctRaw) ? winPctRaw : 0,
                    rank: team.rank ?? idx + 1
                };
            });

            if (!rows.length) {
                return [];
            }

            const maxMatches = rows.reduce((max, team) => Math.max(max, team.matchesPlayed || 0), 1);
            const maxRoundDiff = rows.reduce((max, team) => Math.max(max, Math.abs(team.roundDiff || 0)), 0) || 1;

            return rows.map(team => {
                const winsPercent = Math.min(100, Math.max(0, ((team.wins || 0) / maxMatches) * 100));
                const lossesPercent = Math.min(100, Math.max(0, ((team.matchesPlayed - team.wins) / maxMatches) * 100));
                const roundIndicator = ((team.roundDiff || 0) / maxRoundDiff) * 45 + 50; // keep indicator within bar
                return {
                    ...team,
                    winsPercent,
                    lossesPercent,
                    roundIndicator: Math.max(5, Math.min(95, roundIndicator))
                };
            });
        },
        hasTeams() {
            return this.normalizedTeams.length > 0;
        },
        normalizedHighlightId() {
            if (this.highlightTeamId == null) return null;
            return String(this.highlightTeamId);
        }
    },
    beforeUnmount() {
        if (this._chartFlashTimer) {
            clearTimeout(this._chartFlashTimer);
            this._chartFlashTimer = null;
        }
    },
    methods: {
        avatarUrl(src) {
            const fallback = window.PAPPALIIGA_DEFAULT_LOGO;
            if (!src) return fallback;
            try {
                if (window.apiClient && typeof window.apiClient.proxyAvatar === 'function') {
                    const resolved = window.apiClient.proxyAvatar(src);
                    return resolved || fallback;
                }
                return src || fallback;
            } catch (error) {
                return src || fallback;
            }
        },
        resolveTeamId(team, idx) {
            if (!team) return `team-${idx}`;
            const raw = team.team_id || team.id || team.uuid || `team-${idx}`;
            return String(raw);
        },
        formatInteger(value) {
            const numeric = Number(value) || 0;
            return TEAM_CHART_INT_FORMATTER.format(numeric);
        },
        formatPercent(value) {
            let numeric = Number(value) || 0;
            if (Math.abs(numeric) <= 1 && numeric !== 0) {
                numeric *= 100;
            }
            const formatted = TEAM_CHART_PERCENT_FORMATTER.format(numeric);
            return `${formatted} %`;
        },
        isHighlighted(team) {
            if (!team || this.normalizedHighlightId == null) return false;
            if (team.id == null) return false;
            return String(team.id) === this.normalizedHighlightId;
        },
        scrollToTeam(teamId, options = {}) {
            if (teamId == null) return;
            const targetId = String(teamId);
            const rowEl = this.findRowElement(targetId);
            if (!rowEl) return;
            try {
                rowEl.scrollIntoView({ behavior: options.instant ? 'auto' : 'smooth', block: 'center' });
            } catch (error) {
                rowEl.scrollIntoView();
            }
            this.flashRow(rowEl);
        },
        findRowElement(teamId) {
            if (!this.$refs.rowsRef) return null;
            const escapedId = (typeof CSS !== 'undefined' && typeof CSS.escape === 'function')
                ? CSS.escape(teamId)
                : String(teamId).replace(/"/g, '\\"');
            return this.$refs.rowsRef.querySelector(`[data-team-id="${escapedId}"]`);
        },
        flashRow(rowEl) {
            if (!rowEl) return;
            rowEl.classList.remove('team-comparison-chart__row--flash');
            void rowEl.offsetWidth;
            rowEl.classList.add('team-comparison-chart__row--flash');
            if (this._chartFlashTimer) {
                clearTimeout(this._chartFlashTimer);
            }
            this._chartFlashTimer = setTimeout(() => {
                rowEl.classList.remove('team-comparison-chart__row--flash');
            }, 900);
        }
    },
    template: `
        <section class="team-comparison-chart glass-card division-surface" aria-live="polite">
            <header class="card-head team-comparison__head">
                <div>
                    <p class="section-eyebrow team-comparison-chart__label">SARJATAULUKKO</p>
                    <h2 class="title title-accent titleUnderlineCard">{{ title }}</h2>
                    <p v-if="subtitle" class="subtitle muted team-comparison__lede">{{ subtitle }}</p>
                </div>
            </header>

            <div class="card-content team-comparison-chart__body">
                <p v-if="!hasTeams" class="team-comparison-chart__empty">
                    Sarjataulukkoa ei saatu ladattua.
                </p>

                <div v-else class="team-comparison-chart__rows" ref="rowsRef">
                    <div
                        v-for="team in normalizedTeams"
                        :key="team.id || team.name"
                        class="team-comparison-chart__row"
                        :class="{ 'team-comparison-chart__row--active': isHighlighted(team) }"
                        :data-team-id="String(team.id || team.name)"
                    >
                        <div class="team-comparison-chart__meta">
                            <span v-if="showRanks && team.rank" class="team-comparison-chart__rank">#{{ team.rank }}</span>
                            <img
                                :src="team.logo"
                                :alt="team.name + ' logo'"
                                class="team-comparison-chart__logo"
                                width="40"
                                height="40"
                                loading="lazy"
                            >
                            <div>
                                <span class="team-comparison-chart__name">{{ team.name }}</span>
                                <span class="team-comparison-chart__meta-text">{{ formatInteger(team.matchesPlayed) }} ottelua · {{ formatPercent(team.winPct) }}</span>
                            </div>
                        </div>

                        <div
                            class="team-comparison-chart__bar"
                            role="img"
                            :aria-label="team.name + ' ' + team.wins + '-' + team.losses + ', round diff ' + (team.roundDiff >= 0 ? '+' : '') + team.roundDiff"
                        >
                            <span
                                class="team-comparison-chart__segment team-comparison-chart__segment--wins"
                                :style="{ width: team.winsPercent + '%' }"
                            ></span>
                            <span
                                class="team-comparison-chart__segment team-comparison-chart__segment--losses"
                                :style="{ width: team.lossesPercent + '%' }"
                            ></span>
                            <span
                                class="team-comparison-chart__round-indicator"
                                :class="{ 'team-comparison-chart__round-indicator--negative': team.roundDiff < 0 }"
                                :style="{ left: team.roundIndicator + '%' }"
                                :title="'Round diff ' + (team.roundDiff >= 0 ? '+' : '') + team.roundDiff"
                            ></span>
                        </div>

                        <div class="team-comparison-chart__stats">
                            <span class="team-comparison-chart__record">{{ formatInteger(team.wins) }}–{{ formatInteger(team.losses) }}</span>
                            <span
                                class="team-comparison-chart__round"
                                :class="{
                                    'text-ok': team.roundDiff > 0,
                                    'text-err': team.roundDiff < 0
                                }"
                            >
                                {{ team.roundDiff > 0 ? '+' : team.roundDiff < 0 ? '-' : '' }}{{ formatInteger(Math.abs(team.roundDiff)) }}
                            </span>
                        </div>
                    </div>
                </div>
            </div>
        </section>
    `
};

