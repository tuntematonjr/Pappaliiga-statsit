// TeamNav - displays a horizontal list of team logos with links
const TEAM_NAV_DEFAULT = window.PAPPALIIGA_DEFAULT_LOGO;

window.TeamNav = {
    name: 'TeamNav',
    props: {
        teams: { type: Array, required: true },
        championshipId: { type: String, required: false },
        defaultAvatar: {
            type: String,
            default: TEAM_NAV_DEFAULT
        }
    },
    template: `
        <div class="teams-navigation">
            <template v-for="team in teams" :key="team.team_id">
                <router-link v-if="championshipId" class="team-nav-item" :to="{ name: 'team-detail', params: { championshipId: championshipId, teamId: team.team_id } }">
                    <img class="team-nav-logo" :src="avatarUrl(team)" :alt="team.display_name || team.team_name" loading="lazy">
                    <span class="nav-name">{{ team.display_name || team.team_name }}</span>
                </router-link>
                <router-link v-else class="team-nav-item" :to="{ name: 'team', params: { teamId: team.team_id } }">
                    <img class="team-nav-logo" :src="avatarUrl(team)" :alt="team.display_name || team.team_name" loading="lazy">
                    <span class="nav-name">{{ team.display_name || team.team_name }}</span>
                </router-link>
            </template>
        </div>
    `,
    methods: {
        avatarUrl(team) {
            const candidate = team?.logo || team?.avatar || team?.team_logo || team?.image;
            try {
                const resolved = window.apiClient.proxyAvatar(candidate);
                return resolved || this.defaultAvatar;
            } catch (e) {
                return candidate || this.defaultAvatar;
            }
        }
    }
};
