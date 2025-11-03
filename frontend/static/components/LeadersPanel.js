// LeadersPanel - modern replacement for LeadersGrid
// Renders categories as card groups optimized for the division page layout
window.LeadersPanel = {
    name: 'LeadersPanel',
    props: {
        categories: { type: Array, default: () => [] }
    },
    template: `
        <section class="leaders-panel" aria-label="Division leaders">
            <div class="leaders-panel-inner">
                <header class="leaders-header">
                    <h2 class="panel-title">Division Leaders</h2>
                    <p class="panel-sub">Top players across requested stats</p>
                </header>

                <div class="leaders-groups">
                    <details v-for="(cat, i) in visibleCategories" :key="cat.id || cat.categoryTitle || i" class="card leaderboard-card" :open="i < 3">
                        <summary class="card-head">
                            <div>
                                <h3 class="title">{{ cat.categoryTitle }}</h3>
                                <div class="hint">Top {{ Math.min((cat.leaders||[]).length, 3) }} players</div>
                            </div>
                        </summary>
                        <div class="card-content">
                            <div class="stat-cards leader-mini-grid">
                                <article v-for="(leader, j) in (cat.leaders || []).slice(0,6)" :key="leaderKey(leader, j)" class="card leader-mini" role="listitem" tabindex="0">
                                    <div class="leader-mini-left">
                                        <img :src="avatarUrl(leader.logo)" class="leader-mini-logo" :alt="(leader.teamName || '') + ' logo'" loading="lazy" />
                                        <div class="leader-mini-meta">
                                            <div class="leader-mini-name">{{ leader.title || leader.playerName || 'Unknown' }}</div>
                                            <div class="leader-mini-sub muted">{{ leader.subtitle || leader.teamName || '' }}</div>
                                        </div>
                                    </div>
                                    <div class="leader-mini-value">{{ formatValue(leader.value) }}</div>
                                </article>
                            </div>
                        </div>
                    </details>
                </div>
            </div>
        </section>
    `,
    computed: {
        visibleCategories() {
            return Array.isArray(this.categories) ? this.categories : [];
        }
    },
    methods: {
        avatarUrl(src) {
            const fallback = window.PAPPALIIGA_DEFAULT_LOGO;
            if (!src) return fallback;
            try {
                const proxied = (window.apiClient && window.apiClient.proxyAvatar) ? window.apiClient.proxyAvatar(src) : src;
                return proxied || fallback;
            } catch (e) {
                return fallback;
            }
        },
        leaderKey(leader, idx) {
            if (leader && leader.id) return leader.id;
            if (leader && leader.title) return `${leader.title}-${leader.subtitle || ''}-${leader.value}`;
            return `leader-${idx}`;
        },
        formatValue(v) {
            if (v == null) return '—';
            return typeof v === 'number' ? (Number.isInteger(v) ? v : Math.round(v * 100) / 100) : v;
        }
    }
};
