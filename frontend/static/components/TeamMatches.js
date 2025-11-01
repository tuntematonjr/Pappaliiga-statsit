// TeamMatches - displays recent matches for a team
window.TeamMatches = {
    name: 'TeamMatches',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; }
    },
    props: {
        title: {
            type: String,
            default: 'Viimeisimmät ottelut'
        },
        matches: {
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
        limit: {
            type: Number,
            default: 6
        }
    },
    computed: {
        visibleMatches() {
            if (!Array.isArray(this.matches)) {
                return [];
            }
            return this.matches.slice(0, this.limit);
        }
    },
    methods: {
        formatDate(dateString) {
            if (!dateString) return 'Ajantasaton';
            try {
                return new Date(dateString).toLocaleDateString('fi-FI', {
                    year: 'numeric',
                    month: '2-digit',
                    day: '2-digit'
                });
            } catch (err) {
                console.warn('TeamMatches formatDate failed', err, dateString);
                return dateString;
            }
        },
        outcome(match) {
            if (!match) return { label: 'Tulos puuttuu', tone: 'neutral' };
            const teamScore = Number(match.team_score ?? match.score_for ?? match.team_rounds ?? 0);
            const oppScore = Number(match.opponent_score ?? match.score_against ?? match.opponent_rounds ?? 0);
            if (Number.isNaN(teamScore) || Number.isNaN(oppScore)) {
                return { label: 'Tulos puuttuu', tone: 'neutral' };
            }
            if (teamScore > oppScore) return { label: 'Voitto', tone: 'win' };
            if (teamScore < oppScore) return { label: 'Tappio', tone: 'loss' };
            return { label: 'Tasapeli', tone: 'draw' };
        },
        mapName(match) {
            return match?.map_name || match?.map || match?.mapName || 'Tuntematon kartta';
        },
        opponent(match) {
            return match?.opponent_name || match?.opponent || match?.opponentTeam || 'Vastustaja';
        }
    },
    template: `
        <section class="team-matches card">
            <header class="card-head">
                <h2 class="title">{{ title }}</h2>
            </header>
            <div class="card-content">
                <loading-spinner v-if="loading" message="Otteluita ladataan..."></loading-spinner>
                <error-message v-else-if="error" :message="error"></error-message>
                <ul v-else class="matches-list" aria-label="Viimeisimmät ottelut">
                    <li v-for="match in visibleMatches"
                        :key="match.match_id || match.id || match.faceit_match_id"
                        class="match-item">
                        <div class="match-meta">
                            <div class="match-date">{{ formatDate(match.played_at || match.date) }}</div>
                            <div class="match-map">{{ mapName(match) }}</div>
                        </div>
                        <div class="match-versus">
                            <span class="label">vs</span>
                            <span class="opponent">{{ opponent(match) }}</span>
                        </div>
                        <div class="match-score">
                            <span class="score">{{ (match.team_score ?? match.score_for ?? '-') }} - {{ (match.opponent_score ?? match.score_against ?? '-') }}</span>
                            <span :class="['badge', outcome(match).tone]">{{ outcome(match).label }}</span>
                        </div>
                    </li>
                    <li v-if="!visibleMatches.length" class="muted empty">Ei otteluita</li>
                </ul>
            </div>
        </section>
    `
};

