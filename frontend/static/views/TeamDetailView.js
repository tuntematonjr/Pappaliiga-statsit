window.TeamDetailView = {
    name: 'TeamDetailView',
    components: {
        get TeamDetail() { return window.TeamDetail; },
        get ErrorMessage() { return window.ErrorMessage; }
    },
    methods: {
        isLikelyChampionshipUuid(value) {
            if (value === null || value === undefined) return false;
            const normalized = String(value).trim();
            if (!normalized) return false;
            return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(normalized);
        }
    },
    computed: {
        teamId() {
            return this.$route.params?.teamId || null;
        },
        championshipId() {
            const fromQuery = this.$route.query?.championship || null;
            if (this.isLikelyChampionshipUuid(fromQuery)) {
                return String(fromQuery);
            }
            const fromParams = this.$route.params?.championshipId || null;
            if (this.isLikelyChampionshipUuid(fromParams)) {
                return String(fromParams);
            }
            return null;
        }
    },
    template: `
        <div class="team-detail-view">
            <team-detail
                v-if="teamId"
                :key="String(teamId) + '::' + String(championshipId || 'auto')"
                :team-id="teamId"
                :championship-id="championshipId"
            ></team-detail>
            <error-message
                v-else
                message="Joukkuetta ei löytynyt"
            ></error-message>
        </div>
    `
};
