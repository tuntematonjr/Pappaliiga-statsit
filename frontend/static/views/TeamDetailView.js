// TeamDetailView - dedicated page that renders TeamDetail component
window.TeamDetailView = {
    name: 'TeamDetailView',
    components: {
        get TeamDetail() { return window.TeamDetail; }
    },
    computed: {
        championshipId() { return this.$route.params.championshipId || this.$route.query.champ || null; },
        teamId() { return this.$route.params.teamId; },
        season() { return this.$route.query.season || 'current'; }
    },
    mounted() {
        // Small debug log to help trace why the team page might not open
        try {
            console.log('TeamDetailView mounted', { teamId: this.teamId, championshipId: this.championshipId, route: this.$route && this.$route.fullPath });
        } catch (e) {}
    },
    template: `
        <div class="team-detail-page">
            <div class="page-header">
                <router-link :to="{ name: 'division', params: { slug: $route.query.divisionSlug || '' } }" class="chip">Takaisin divisioonaan</router-link>
                <h1 style="display:inline-block;margin-left:12px;">Joukkueen tiedot</h1>
            </div>
            <team-detail :championship-id="championshipId" :team-id="teamId"></team-detail>
        </div>
    `
};


