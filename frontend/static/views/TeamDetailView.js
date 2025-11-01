// TeamDetailView - dedicated page that renders TeamDetail component
window.TeamDetailView = {
    name: 'TeamDetailView',
    components: {
        get TeamDetail() { return window.TeamDetail; }
    },
    computed: {
        championshipId() {
            return this.$route.params.championshipId || this.$route.query.championship || this.$route.query.champ || null;
        },
        teamId() {
            return this.$route.params.teamId || this.$route.query.teamId || null;
        },
        divisionBackLink() {
            const divisionChamp = this.$route.query.championship || this.$route.params.championshipId;
            if (divisionChamp) {
                return { name: 'division', params: { championshipId: divisionChamp } };
            }
            return null;
        }
    },
    mounted() {
        try {
            console.log('TeamDetailView mounted', {
                teamId: this.teamId,
                championshipId: this.championshipId,
                route: this.$route && this.$route.fullPath
            });
        } catch (err) {
            console.warn('TeamDetailView mount log failed', err);
        }
    },
    template: `
        <div class="team-detail-page">
            <div class="page-header">
                <router-link v-if="divisionBackLink" :to="divisionBackLink" class="chip">
                    ← Takaisin divisioonaan
                </router-link>
                <h1>Joukkueen tiedot</h1>
            </div>
            <team-detail :championship-id="championshipId" :team-id="teamId"></team-detail>
        </div>
    `
};
