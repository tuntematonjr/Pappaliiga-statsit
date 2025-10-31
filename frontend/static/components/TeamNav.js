// TeamNav - displays a horizontal list of team logos with links
window.TeamNav = {
    name: 'TeamNav',
    props: {
        teams: { type: Array, required: true },
        championshipId: { type: String, required: false },
        defaultAvatar: { type: String, required: false }
    },
    template: `
        <div class="teams-navigation">
            <template v-for="team in teams" :key="team.team_id">
                <router-link v-if="championshipId" class="team-nav-item" :to="{ name: 'team-detail', params: { championshipId: championshipId, teamId: team.team_id } }">
                    <img class="logo nav-logo" :src="avatarUrl(team.avatar) || defaultAvatar" :alt="team.display_name || team.team_name" loading="lazy">
                    <span class="nav-name">{{ team.display_name || team.team_name }}</span>
                </router-link>
                <router-link v-else class="team-nav-item" :to="{ name: 'team', params: { teamId: team.team_id } }">
                    <img class="logo nav-logo" :src="avatarUrl(team.avatar) || defaultAvatar" :alt="team.display_name || team.team_name" loading="lazy">
                    <span class="nav-name">{{ team.display_name || team.team_name }}</span>
                </router-link>
            </template>
        </div>
    `,
    methods: {
        avatarUrl(src) {
            try { return window.apiClient.proxyAvatar(src); } catch (e) { return src; }
        }
    }
};
