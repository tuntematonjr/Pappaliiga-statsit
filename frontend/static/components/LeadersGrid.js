// LeadersGrid - displays leader cards (top players per stat category)
window.LeadersGrid = {
    name: 'LeadersGrid',
    props: {
        categories: { type: Array, required: true }
        // Format: [{ categoryTitle, leaders: [{ id?, title, subtitle, playerName, teamName, teamLogo, value }] }]
    },
    template: `
        <section class="leaders-section" aria-label="Top players by category">
            <h2 class="sr-only">Top Players</h2>
            <div v-for="(category, catIdx) in categories" :key="category.id || category.categoryTitle || catIdx" class="leader-category">
                <h3
                    class="category-title title-accent titleUnderlineMain"
                    :class="'title-delay-' + (catIdx % 4)"
                >
                    {{ category.categoryTitle }}
                </h3>
                <div class="leaders-row" role="list">
                    <article v-for="(leader, idx) in safeLeaders(category.leaders)" :key="leaderKey(leader, idx)" class="leader-card" role="listitem">
                        <header class="leader-header">
                            <h4
                                class="leader-title title-accent titleUnderlineCard"
                                :class="'title-delay-' + (idx % 4)"
                            >
                                {{ leader.title || leader.playerName || '—' }}
                            </h4>
                            <span v-if="leader.subtitle" class="leader-subtitle">{{ leader.subtitle }}</span>
                        </header>
                        <div class="leader-info">
                            <div class="leader-player-row">
                                <img class="leader-team-logo" :src="avatarUrl(leader.teamLogo)" :alt="(leader.teamName || 'team') + ' logo'" loading="lazy">
                                <div class="leader-player">{{ leader.playerName || 'Unknown player' }}</div>
                            </div>
                            <div class="leader-team">{{ leader.teamName || '' }}</div>
                            <div class="leader-value" aria-label="value">{{ leader.value != null ? leader.value : '—' }}</div>
                        </div>
                    </article>
                </div>
            </div>
        </section>
    `,
    methods: {
        avatarUrl(src) {
            const fallback = window.PAPPALIIGA_DEFAULT_LOGO;
            if (!src) return fallback;
            try {
                const proxied = window.apiClient && window.apiClient.proxyAvatar
                    ? window.apiClient.proxyAvatar(src)
                    : src;
                return proxied || fallback;
            } catch (e) {
                return fallback;
            }
        },
        leaderKey(leader, idx) {
            // Prefer stable id, fallback to a composite key, then index
            if (leader && leader.id) return leader.id;
            if (leader && leader.playerName) return `${leader.playerName}-${leader.teamName || 'team'}-${leader.value}`;
            return `leader-${idx}`;
        },
        safeLeaders(list) {
            return Array.isArray(list) ? list : [];
        }
    }
};
