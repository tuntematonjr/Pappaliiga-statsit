// SankariCard - reusable card for Divarin Sankarit rows
window.SankariCard = {
    name: 'SankariCard',
    props: {
        title: { type: String, required: true },
        description: { type: String, default: '' },
        entries: {
            type: Array,
            default: () => []
        }
    },
    computed: {
        visibleEntries() {
            return Array.isArray(this.entries) ? this.entries.slice(0, 4) : [];
        }
    },
    methods: {
        avatarUrl(src) {
            const fallback = window.PAPPALIIGA_DEFAULT_LOGO;
            if (!src) return fallback;
            try {
                const proxied = window.apiClient && window.apiClient.proxyAvatar
                    ? window.apiClient.proxyAvatar(src)
                    : src;
                return proxied || fallback;
            } catch (error) {
                return fallback;
            }
        },
        entryKey(entry, idx) {
            return entry?.id || entry?.playerId || entry?.nickname || `sankari-${idx}`;
        },
        infoLine(entry) {
            const maps = entry?.maps ?? entry?.mapsPlayed;
            const rounds = entry?.rounds ?? entry?.roundsPlayed;
            if (maps != null && rounds != null) {
                return `${maps} karttaa / ${rounds} kierrosta`;
            }
            if (rounds != null) {
                return `${rounds} kierrosta`;
            }
            return '';
        },
        displayValue(entry) {
            if (entry == null) return '–';
            if (entry.displayValue != null && entry.displayValue !== '') return entry.displayValue;
            if (entry.rawValue != null && entry.rawValue !== '') return entry.rawValue;
            return entry.value != null ? entry.value : '–';
        }
    },
    template: `
        <article class="sankari-card glass-card division-surface">
            <div class="sankari-card__content">
                <header class="sankari-card__head">
                    <h3 class="title-accent titleUnderlineCard">{{ title }}</h3>
                    <p v-if="description" class="sankari-card__desc">{{ description }}</p>
                </header>
                <ol class="sankari-card__list">
                    <li
                        v-for="(entry, idx) in visibleEntries"
                        :key="entryKey(entry, idx)"
                        class="sankari-card__row"
                        tabindex="0"
                    >
                        <span class="sankari-card__rank">#{{ idx + 1 }}</span>
                        <div class="sankari-card__meta">
                            <div class="sankari-card__player">
                                <img
                                    class="sankari-card__avatar"
                                    :src="avatarUrl(entry?.avatar)"
                                    :alt="(entry?.nickname || 'Pelaaja') + ' avatar'"
                                    loading="lazy"
                                />
                                <div>
                                    <div class="sankari-card__name">{{ entry?.nickname || entry?.name || 'Tuntematon' }}</div>
                                    <div class="sankari-card__team muted">{{ entry?.teamName || entry?.team || '' }}</div>
                                </div>
                            </div>
                            <div v-if="infoLine(entry)" class="sankari-card__hint muted">{{ infoLine(entry) }}</div>
                        </div>
                        <div class="sankari-card__value">{{ displayValue(entry) }}</div>
                    </li>
                </ol>
            </div>
        </article>
    `
};
