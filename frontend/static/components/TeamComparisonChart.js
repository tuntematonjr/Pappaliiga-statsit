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
        }
    },
    computed: {
        normalizedTeams() {
            if (!Array.isArray(this.teams)) {
                return [];
            }
            const rows = this.teams.slice(0, this.limit).map(team => ({
                id: team.id || team.team_id,
                name: team.name || team.team_name || 'Joukkue',
                logo: team.logo,
                wins: Number(team.wins ?? 0),
                losses: Number(team.losses ?? 0),
                matchesPlayed: Number(team.matchesPlayed ?? (Number(team.wins ?? 0) + Number(team.losses ?? 0))),
                roundDiff: Number(team.roundDiff ?? team.rounds_diff ?? 0),
                winPct: Number(team.winPct ?? team.winRate ?? 0),
                rank: team.rank ?? null
            }));

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
        }
    },
    template: `
        <section class="team-comparison-chart glass-card" aria-live="polite">
            <header class="team-comparison-chart__header">
                <div>
                    <p class="section-eyebrow">Sarjatilanne</p>
                    <h3 class="title-accent titleUnderlineCard title-delay-0">{{ title }}</h3>
                </div>
            </header>

            <p v-if="!hasTeams" class="team-comparison-chart__empty">
                Sarjataulukkoa ei saatu ladattua.
            </p>

            <div v-else class="team-comparison-chart__rows">
                <div
                    v-for="team in normalizedTeams"
                    :key="team.id || team.name"
                    class="team-comparison-chart__row"
                >
                    <div class="team-comparison-chart__meta">
                        <span v-if="showRanks && team.rank" class="team-comparison-chart__rank">#{{ team.rank }}</span>
                        <img
                            v-if="team.logo"
                            :src="team.logo"
                            :alt="team.name + ' logo'"
                            class="team-comparison-chart__logo"
                            width="36"
                            height="36"
                            loading="lazy"
                        >
                        <div>
                            <span class="team-comparison-chart__name">{{ team.name }}</span>
                            <span class="team-comparison-chart__meta-text">{{ team.matchesPlayed }} ottelua · {{ team.winPct }} %</span>
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
                        <span class="team-comparison-chart__record">{{ team.wins }}–{{ team.losses }}</span>
                        <span
                            class="team-comparison-chart__round"
                            :class="{
                                'text-ok': team.roundDiff > 0,
                                'text-err': team.roundDiff < 0
                            }"
                        >
                            {{ team.roundDiff >= 0 ? '+' : '' }}{{ team.roundDiff }}
                        </span>
                    </div>
                </div>
            </div>
        </section>
    `
};

