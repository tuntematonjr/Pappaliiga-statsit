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
                const key = this.mapKey(item?.map_id || item?.pretty_name);
                if (!key) return;
                lookup[key] = item;
            });
            return lookup;
        }
    },
    methods: {
        mapKey(name) {
            if (!name) return null;
            return String(name).trim().toLowerCase();
        },
        resolveMapImage(step) {
            if (!step) return null;
            
            // First try direct image from veto step if available, prefer small variant for veto
            const direct = step?.image_sm || step?.image_lg || step?.imageSm || step?.imageLg;
            if (direct) {
                try {
                    return window.apiClient && typeof window.apiClient.proxyAvatar === 'function'
                        ? window.apiClient.proxyAvatar(direct)
                        : direct;
                } catch (error) {
                    return direct;
                }
            }
            
            // Fallback to catalog lookup by map name, use small variant for veto display
            const mapName = step?.mapName || step?.map_name;
            const key = this.mapKey(mapName);
            if (!key) return null;
            if (this.imageErrors[key]) return null;
            const entry = this.catalogLookup[key];
            const url = entry?.image_sm || entry?.image_lg || null;
            if (!url) return null;
            try {
                return window.apiClient && typeof window.apiClient.proxyAvatar === 'function'
                    ? window.apiClient.proxyAvatar(url)
                    : url;
            } catch (error) {
                return url;
            }
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
                <div>
                    <h4 class="veto-panel-title">BO2/BO3 veto-polku</h4>
                    <span class="section-sub">Jokainen askel, joukkue ja decider/overflow korostettu</span>
                </div>
                <span v-if="formatLabel" class="veto-panel-format">{{ formatLabel }}</span>
            </div>
            <div v-if="steps.length" class="veto-steps">
                <div v-for="step in steps" :key="step.step + step.mapName" class="veto-step" :class="'veto-step--' + step.action">
                    <div class="veto-step__order">#{{ step.step }}</div>
                    <div class="veto-step__title">{{ step.label }}</div>
                    <div v-if="resolveMapImage(step)" class="veto-step__map-image">
                        <img :src="resolveMapImage(step)" @error="onImageError(step.mapName)" alt="" />
                    </div>
                    <div class="veto-step__map">{{ step.mapName }}</div>
                    <div class="veto-step__actor">{{ step.teamName || 'Järjestelmä' }}</div>
                </div>
            </div>
            <div v-else class="pick-ban-flow__empty">No veto data available</div>
        </div>
    `
};
