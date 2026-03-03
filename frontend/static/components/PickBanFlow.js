window.PickBanFlow = {
    name: 'PickBanFlow',
    props: {
        entry: {
            type: Object,
            default: null
        },
        mapCatalog: {
            type: Array,
            default: () => []
        },
        matchMaps: {
            type: Array,
            default: () => []
        }
    },
    data() {
        return {
            imageErrors: {}
        };
    },
    computed: {
        steps() {
            return Array.isArray(this.entry?.steps) ? this.entry.steps : [];
        },
        formatLabel() {
            const format = this.entry?.format;
            if (!format) return '';
            return String(format).toUpperCase();
        },
        catalogLookup() {
            const lookup = {};
            this.mapCatalog.forEach(item => {
                const candidates = [
                    item?.map_id,
                    item?.pretty_name,
                    item?.map_name,
                    item?.name,
                    item?.mapName
                ];
                candidates.forEach(candidate => {
                    const key = this.mapKey(candidate);
                    if (!key) return;
                    if (!lookup[key]) lookup[key] = item;
                    if (key.startsWith('de_')) {
                        const shortKey = key.slice(3);
                        if (shortKey && !lookup[shortKey]) lookup[shortKey] = item;
                    } else {
                        const prefixed = `de_${key}`;
                        if (!lookup[prefixed]) lookup[prefixed] = item;
                    }
                });
            });
            return lookup;
        },
        mapImageLookup() {
            const imageUtils = window.MapImageUtils;
            if (!imageUtils || typeof imageUtils.buildMapImageLookup !== 'function') {
                return {};
            }
            const fromMatchMaps = imageUtils.buildMapImageLookup(this.matchMaps || [], {});
            const merged = imageUtils.buildMapImageLookup(this.mapCatalog || [], fromMatchMaps);
            const withAliases = { ...merged };
            Object.keys(merged).forEach(key => {
                if (!key) return;
                if (key.startsWith('de_')) {
                    const shortKey = key.slice(3);
                    if (shortKey && !withAliases[shortKey]) withAliases[shortKey] = merged[key];
                } else {
                    const prefixed = `de_${key}`;
                    if (!withAliases[prefixed]) withAliases[prefixed] = merged[key];
                }
            });
            return withAliases;
        }
    },
    methods: {
        championshipId() {
            return this.$route?.params?.championshipId || this.$route?.query?.championship || null;
        },
        teamRoute(teamId, teamName = '') {
            const championshipId = this.championshipId();
            if (!championshipId || !teamId) return null;
            return {
                name: 'team-detail',
                params: { championshipId: String(championshipId), teamId: String(teamId) }
            };
        },
        mapKey(name) {
            if (!name) return null;
            return String(name).trim().toLowerCase();
        },
        beautifyMapName(raw) {
            if (!raw) return 'Kartta';
            const value = String(raw).trim();
            const lower = value.toLowerCase();
            if (lower === 'forfeit') return 'Forfeit';
            const core = lower.startsWith('de_') ? lower.slice(3) : lower;
            const parts = core.split(/[_-]/).filter(Boolean);
            if (!parts.length) return value;
            return parts.map(part => part.charAt(0).toUpperCase() + part.slice(1)).join(' ');
        },
        resolveMapImage(step) {
            if (!step) return null;
            const mapName = step?.mapName || step?.map_name || step?.map || '';
            const key = this.mapKey(mapName);
            if (!key) return null;
            if (this.imageErrors[key]) return null;

            // Prefer large image for veto cards.
            const directLarge = step?.image_lg || step?.imageLg || null;
            if (directLarge) {
                return window.apiClient && typeof window.apiClient.proxyAvatar === 'function'
                    ? window.apiClient.proxyAvatar(directLarge)
                    : directLarge;
            }

            const catalogEntry = this.catalogLookup[key];
            const catalogLarge = catalogEntry?.image_lg || catalogEntry?.imageLg || null;
            if (catalogLarge) {
                return window.apiClient && typeof window.apiClient.proxyAvatar === 'function'
                    ? window.apiClient.proxyAvatar(catalogLarge)
                    : catalogLarge;
            }

            const imageUtils = window.MapImageUtils;
            if (imageUtils && typeof imageUtils.resolveMapImage === 'function') {
                const resolved = imageUtils.resolveMapImage(
                    {
                        ...step,
                        map_name: mapName,
                        mapName
                    },
                    {
                        mapCatalog: this.mapCatalog,
                        mapImageLookup: this.mapImageLookup,
                        apiClient: window.apiClient
                    }
                );
                if (resolved) return resolved;
            }

            const entry = this.catalogLookup[key];
            const url = entry?.image_sm || entry?.imageSm || null;
            if (!url) return null;
            return window.apiClient && typeof window.apiClient.proxyAvatar === 'function'
                ? window.apiClient.proxyAvatar(url)
                : url;
        },
        onImageError(mapName) {
            const key = this.mapKey(mapName);
            if (!key) return;
            this.imageErrors = { ...this.imageErrors, [key]: true };
        }
    },
    template: `
        <div class="pick-ban-flow">
            <div class="veto-panel-header">
                <div class="veto-panel-heading">
                    <h4 class="veto-panel-title">Pick/Ban</h4>
                    <p class="veto-panel-subtitle">Ottelun veto-polku kartta kerrallaan</p>
                </div>
                <span v-if="formatLabel" class="veto-panel-format">{{ formatLabel }}</span>
            </div>
            <div v-if="steps.length" class="veto-steps">
                <div v-for="step in steps" :key="step.step + step.mapName" class="veto-step" :class="'veto-step--' + step.action">
                    <div class="veto-step__top">
                        <div class="veto-step__order">#{{ step.step }}</div>
                        <div class="veto-step__title">{{ step.label }}</div>
                    </div>
                    <div v-if="resolveMapImage(step)" class="veto-step__map-image">
                        <img :src="resolveMapImage(step)" @error="onImageError(step.mapName)" alt="" />
                    </div>
                    <div class="veto-step__map">{{ beautifyMapName(step.mapName) }}</div>
                    <div v-if="step.action !== 'overflow' && step.action !== 'decider'" class="veto-step__actor">
                        <router-link
                            v-if="teamRoute(step.teamId, step.teamName)"
                            :to="teamRoute(step.teamId, step.teamName)"
                            class="team-link"
                        >{{ step.teamName || 'Järjestelmä' }}</router-link>
                        <span v-else>{{ step.teamName || 'Järjestelmä' }}</span>
                    </div>
                </div>
            </div>
            <div v-else class="pick-ban-flow__empty">No veto data available</div>
        </div>
    `
};
