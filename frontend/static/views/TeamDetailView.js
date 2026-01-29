window.TeamDetailView = {
    name: 'TeamDetailView',
    components: {
        get TeamDetail() { return window.TeamDetail; },
        get ErrorMessage() { return window.ErrorMessage; }
    },
    computed: {
        teamId() {
            return this.$route.params?.teamId || null;
        },
        championshipId() {
            return this.$route.params?.championshipId || this.$route.query?.championship || null;
        }
    },
    watch: {
        '$route.fullPath'() {
            // Trigger prop update on nested component by forcing re-render
            this.$forceUpdate();
        }
    },
    template: `
        <div class="team-detail-view">
            <team-detail
                v-if="teamId"
                :team-id="teamId"
                :championship-id="championshipId"
            ></team-detail>
            <error-message
                v-else
                message="Joukkuetta ei löytynyt"
            ></error-message>
            <footer class="footer">Pappaliiga Stats</footer>
        </div>
    `
};
