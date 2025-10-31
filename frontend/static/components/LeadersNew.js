// LeadersNew - Compact leaders display using base .card system
// 20 stat categories with funny Finnish names, max 3-4 top players per stat
const DEFAULT_AVATAR = '/static/pappaliiga-logo-white-bg.png';

window.LeadersNew = {
    name: 'LeadersNew',
    props: {
        categories: { type: Array, default: () => [] }
    },
    template: `
        <section class="leaders-new" aria-label="Division leaders">
            <div class="leaders-groups">
                <div v-for="(group, gi) in groupedCategories" :key="gi" class="leaders-group">
                    <h2 class="leaders-group-title">{{ group.title }}</h2>
                    <div class="stat-cards leaders-grid">
                        <article v-for="(cat, i) in group.items" 
                                 :key="cat.id || cat.categoryTitle || i" 
                                 class="card leaders-card" 
                                 role="region" 
                                 :aria-label="'Leaders: ' + cat.categoryTitle">
                            <div class="card-head leaders-card-head">
                                <div>
                                    <h3 class="title">{{ cat.categoryTitle }}</h3>
                                    <div class="subtitle muted" v-if="cat.description || cat.subtitle">{{ cat.description || cat.subtitle }}</div>
                                </div>
                            </div>
                            <div class="card-content leaders-card-content">
                                <div class="leader-list" role="list">
                                    <div v-for="(leader, j) in (cat.leaders || []).slice(0, 4)" 
                                         :key="leaderKey(leader, j)" 
                                         class="leader-item" 
                                         role="listitem">
                                        <div class="leader-position">{{ j + 1 }}</div>
                                <img 
                                    :src="avatarUrl(leader.logo)" 
                                    class="leader-avatar" 
                                    :alt="(leader.teamName || 'team') + ' logo'" 
                                    width="36" height="36"
                                    loading="lazy" 
                                    @error="onAvatarError" />
                                        <div class="leader-details">
                                            <div class="leader-name">{{ leaderNameWithTeam(leader, j) }}</div>
                                            <div class="leader-team muted">{{ leader.subtitle || leader.teamName || '' }}</div>
                                        </div>
                                        <div class="leader-value">{{ formatValue(leader.value) }}</div>
                                    </div>
                                </div>
                            </div>
                        </article>
                    </div>
                </div>
            </div>
        </section>
    `,
    computed: {
        visibleCategories() {
            return Array.isArray(this.categories) ? this.categories : [];
        },
        groupedCategories() {
            // Map exact stats to groups. Titles are short humorous Finnish names.
            const groups = [
                { key: 'offense', title: 'Räiskintäosasto', items: [] },
                { key: 'outcome', title: 'Tulostaulu', items: [] },
                { key: 'utility', title: 'Tukipylväät', items: [] }
            ];

            const offense = new Set([
                'Most Kills','Best K/D','Best ADR','Best K/R','Most Sniper Kills','Most Pistol Kills'
            ]);
            const outcome = new Set([
                'Most Rounds','Most MVPs','Best Rating1','Most Total Damage','Most Clutch Kills','Best Clutch WR%','Most Kills','Most Deaths','Most Assists','Best HS%'
            ]);
            const utility = new Set([
                'Most Utility Damage','Most Flashed','Most flashes thrown','Best Enemy/Flash','Best Survival%'
            ]);

            for (const c of this.visibleCategories) {
                const key = (c.statKey || c.categoryTitle || '').toString();
                if (offense.has(key)) groups[0].items.push(c);
                else if (outcome.has(key)) groups[1].items.push(c);
                else if (utility.has(key)) groups[2].items.push(c);
                else groups[1].items.push(c); // fallback to outcome
            }

            // Remove empty groups
            return groups.filter(g => g.items && g.items.length > 0);
        }
    },
    methods: {
        avatarUrl(src) {
            if (!src) return DEFAULT_AVATAR;
            try { return (window.apiClient && window.apiClient.proxyAvatar) ? window.apiClient.proxyAvatar(src) : src; } catch (e) { return src; }
        },
        onAvatarError(e) {
            e.target.src = DEFAULT_AVATAR;
        },
        leaderKey(leader, idx) {
            if (leader && leader.id) return leader.id;
            if (leader && leader.playerName) return `${leader.playerName}-${leader.teamName || ''}-${idx}`;
            return `leader-${idx}`;
        },
        formatValue(v) {
            if (v == null) return 'â€“';
            const num = typeof v === 'number' ? v : Number.parseFloat(String(v).replace(',', '.'));
            if (!Number.isFinite(num)) return v;
            return Number.isInteger(num) ? num : num.toFixed(2);
        },
        leaderNameWithTeam(leader, idx) {
            const base = leader && (leader.title || leader.playerName) ? (leader.title || leader.playerName) : `Tuntematon ${idx + 1}`;
            const team = leader && (leader.teamName || leader.subtitle) ? (leader.teamName || leader.subtitle) : '';
            return team ? `${base} (${team})` : base;
        }
    }
};


