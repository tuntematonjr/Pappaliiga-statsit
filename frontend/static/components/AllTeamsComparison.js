// AllTeamsComparison - compact comparison of all teams in a division
window.AllTeamsComparison = {
    name: 'AllTeamsComparison',
    props: {
        championshipId: { type: String, required: false }
    },
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get SortableTable() { return window.SortableTable; }
        ,
        get SplitBar() { return window.SplitBar; }
    },
    data() {
        return {
            teams: [],
            loading: true,
            error: null,
            defaultAvatar: '/static/pappaliiga-logo-white-bg.png'
        };
    },
    computed: {
        // Table columns exactly as requested: logo+name, matches, maps won, maps lost, WR%, K/D, ADR
        tableColumns() {
                return [
                { key: 'name', label: 'Joukkue', sortable: true, colClass: 'col-name', align: 'left' },
                { key: 'matches_played', label: 'Ottelut', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                // Render a combined Maps column with a win/loss split bar (fixed max)
                { key: 'maps', label: 'Kartat', sortable: true, numeric: true, colClass: 'col-maps', align: 'center' },
                { key: 'rounds_diff', label: '\u00B1Rounds', sortable: true, numeric: true, decimals: 0, colClass: 'col-numeric', align: 'center' },
                { key: 'win_rate', label: 'WR%', sortable: true, numeric: true, decimals: 1, colClass: 'col-numeric', align: 'center' },
                { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2, colClass: 'col-numeric', align: 'center' },
                { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1, colClass: 'col-numeric', align: 'center' }
            ];
        }
    },
    methods: {
        avatarUrl(src) {
            if (!src) return this.defaultAvatar;
            try {
                return window.apiClient && window.apiClient.proxyAvatar ? window.apiClient.proxyAvatar(src) : src;
            } catch (e) {
                return src || this.defaultAvatar;
            }
        },
        async fetchTeams() {
            this.loading = true;
            this.error = null;
            try {
                // Prefer existing api-client helper if available
                if (window.apiClient && typeof window.apiClient.getDivisionTeams === 'function') {
                    this.teams = await window.apiClient.getDivisionTeams(this.championshipId || '');
                } else if (window.apiClient && typeof window.apiClient.fetch === 'function') {
                    const res = await window.apiClient.fetch(`/api/championships/${this.championshipId || ''}/teams`);
                    this.teams = await res.json();
                } else {
                    // Fallback: use division data if present on parent view
                    if (this.$parent && this.$parent.division && this.$parent.division.teams) {
                        // map to expected shape
                        this.teams = this.$parent.division.teams.map((t, idx) => {
                            const matches = Number(t.matches_played || t.played || 0);
                            const wins = Number(t.wins || 0);
                            const losses = Number(t.losses || 0);
                            const mapsWon = Number(t.maps_won != null ? t.maps_won : wins);
                            const mapsLost = Number(t.maps_lost != null ? t.maps_lost : losses);
                            const roundsDiff = Number(t.rounds_diff ?? t.rounds_delta ?? 0);
                            return {
                                team_id: t.team_id || t.teamId || `t${idx+1}`,
                                name: t.display_name || t.team_name || t.name || t.teamName || 'Unknown',
                                logo: t.logo || t.avatar || '',
                                matches_played: matches,
                                rounds_diff: roundsDiff,
                                maps_won: mapsWon,
                                maps_lost: mapsLost,
                                maps: mapsWon,
                                wins: wins,
                                losses: losses,
                                win_rate: t.win_rate != null ? Number(t.win_rate) : (matches ? (100 * wins / matches) : 0),
                                kd: t.kd || 0,
                                adr: t.adr || 0
                            };
                        });
                    } else {
                        // No data available
                        this.teams = [];
                    }
                }

                // Ensure numbers
                this.teams = (this.teams || []).map((t, idx) => {
                    const mapsWon = Number(t.maps_won ?? t.mapsWon ?? t.wins ?? 0);
                    const mapsLost = Number(t.maps_lost ?? t.mapsLost ?? t.losses ?? 0);
                    const rawLogo = t.logo ?? t.avatar ?? '';
                    return {
                        rank: idx + 1,
                        ...t,
                        logo: rawLogo,
                        maps_won: mapsWon,
                        maps_lost: mapsLost,
                        maps: mapsWon,
                        win_rate: Number(t.win_rate || 0),
                        kd: Number(t.kd || 0),
                        adr: Number(t.adr || 0),
                        matches_played: Number(t.matches_played || 0)
                    };
                });

            } catch (err) {
                this.error = err && err.message ? err.message : 'Failed to load teams';
            } finally {
                this.loading = false;
            }
        }
    },
    mounted() {
        console.log('AllTeamsComparison mounted, championshipId=', this.championshipId);
        this.fetchTeams().then(()=>console.log('AllTeamsComparison fetched teams', this.teams));
    },
    template: `
        <div class="all-teams-comparison">
            <h3 class="section-title">Joukkuevertailu</h3>

            <loading-spinner v-if="loading" message="Joukkueita ladataan..."></loading-spinner>
            <div v-else-if="error" class="error">{{ error }}</div>

            <div v-else>
                <div>
                    <div v-if="teams && teams.length">
                        <sortable-table :columns="tableColumns" :data="teams" :compact="true" :defaultSort="{ column: 'maps', order: 'desc', numeric: true }">
                            <template v-slot:cell-name="{ row }">
                                <div class="team-cell">
                                    <img :src="avatarUrl(row.logo)" :alt="row.name || 'Team logo'" class="team-cell-logo" />
                                    <span class="team-name-text">{{ row.name }}</span>
                                </div>
                            </template>
                            <template v-slot:cell-maps="{ row }">
                                <div class="maps-wrapper">
                                    <split-bar
                                        :wins="row.maps_won || 0"
                                        :losses="row.maps_lost || 0"
                                        height="40px"
                                        class="maps-bar"
                                        :left-text="(row.maps_won||0) + 'W'"
                                        :right-text="(row.maps_lost||0) + 'L'"
                                        :show-percent="true"
                                    ></split-bar>
                                </div>
                            </template>
                            <template v-slot:cell-rounds_diff="{ row }">
                                <div :class="['rounds-diff', { 'positive': row.rounds_diff > 0, 'negative': row.rounds_diff < 0 }]">
                                    {{ (row.rounds_diff > 0 ? '+' : (row.rounds_diff < 0 ? '-' : '')) + Math.abs(row.rounds_diff) }}
                                </div>
                            </template>
                        </sortable-table>
                    </div>
                    <p v-else class="no-data">Joukkuetietoja ei saatavilla</p>
                </div>
            </div>
        </div>
    `
};
