// LeaderCard - renders a single leader category with up to 4 entries
const LEADER_CARD_DEFAULT_AVATAR = window.PAPPALIIGA_DEFAULT_LOGO;

window.LeaderCard = {
    name: 'LeaderCard',
    props: {
        title: { type: String, required: true },
        subtitle: { type: String, default: '' },
        leaders: {
            type: Array,
            default: () => []
        },
        statUnit: { type: String, default: '' },
        statSuffix: { type: String, default: '' },
        dense: { type: Boolean, default: true }
    },
    computed: {
        visibleLeaders() {
            return Array.isArray(this.leaders) ? this.leaders.slice(0, 4) : [];
        },
        titleDelayClass() {
            const seed = (this.title || '').length;
            return `title-delay-${seed % 4}`;
        }
    },
    methods: {
        avatarUrl(src) {
            if (!src) return LEADER_CARD_DEFAULT_AVATAR;
            try {
                return window.apiClient && typeof window.apiClient.proxyAvatar === 'function'
                    ? window.apiClient.proxyAvatar(src) || LEADER_CARD_DEFAULT_AVATAR
                    : (src || LEADER_CARD_DEFAULT_AVATAR);
            } catch (err) {
                console.warn('LeaderCard avatar fallback failed', err);
                return LEADER_CARD_DEFAULT_AVATAR;
            }
        },
        formatValue(value) {
            if (value == null || value === '') {
                return '–';
            }

            if (typeof value === 'number') {
                return this.formatNumber(value);
            }

            const parsed = Number(String(value).replace(',', '.'));
            if (Number.isFinite(parsed)) {
                return this.formatNumber(parsed);
            }
            return value;
        },
        formatNumber(value) {
            if (!Number.isFinite(value)) return '–';
            if (Math.abs(value) >= 1000) {
                return Math.round(value);
            }
            if (Math.abs(value) >= 100) {
                return value.toFixed(1);
            }
            if (Math.abs(value) >= 10) {
                return value.toFixed(2);
            }
            return value.toFixed(2);
        }
    },
    template: `
        <article class="card leader-card" role="region">
            <header class="card-head">
                <div>
                    <h3 :class="['title', 'title-accent', 'titleUnderlineCard', titleDelayClass]">{{ title }}</h3>
                    <p v-if="subtitle" class="subtitle muted">{{ subtitle }}</p>
                </div>
            </header>
            <div class="card-content">
                <div class="leader-entries" :class="{ dense }">
                    <div v-for="(leader, index) in visibleLeaders"
                         :key="leader.id || leader.playerId || leader.playerName || leader.teamName || index"
                         class="leader-entry">
                        <span class="leader-rank">{{ index + 1 }}</span>
                        <img class="leader-logo"
                             :src="avatarUrl(leader.teamLogo || leader.logo || leader.avatar)"
                             :alt="leader.teamName || leader.playerName || 'Logo'"
                             loading="lazy" />
                        <div class="leader-meta">
                            <div class="leader-name">
                                {{ leader.playerName || leader.player || leader.name || 'Tuntematon' }}
                            </div>
                            <div class="leader-team muted">
                                {{ leader.teamName || leader.team || leader.subtitle || '' }}
                            </div>
                        </div>
                        <div class="leader-value">
                            {{ formatValue(leader.value) }}<span v-if="statUnit">{{ statUnit }}</span><span v-if="statSuffix">{{ statSuffix }}</span>
                        </div>
                    </div>
                </div>
            </div>
        </article>
    `
};

