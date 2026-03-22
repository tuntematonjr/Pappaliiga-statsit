// StatPanel - reusable metric grid with glass cards
window.StatPanel = {
    name: 'StatPanel',
    props: {
        items: {
            type: Array,
            default: () => []
        },
        columns: {
            type: Number,
            default: 4
        },
        dense: {
            type: Boolean,
            default: false
        },
        titleTag: {
            type: String,
            default: 'span'
        }
    },
    computed: {
        panelClasses() {
            return [
                'stat-panel',
                this.dense ? 'stat-panel--dense' : null,
                `stat-panel--cols-${Math.min(Math.max(this.columns, 1), 6)}`
            ].filter(Boolean);
        }
    },
    methods: {
        resolveKey(item, index) {
            return item?.key || `${item?.label || 'stat'}-${index}`;
        }
    },
    template: `
        <div class="stat-panel" :class="panelClasses" role="list">
            <article
                v-for="(item, index) in items"
                :key="resolveKey(item, index)"
                class="stat-panel__item glass-card"
                role="listitem"
                tabindex="0"
                :aria-label="item.label + ' ' + item.value"
            >
                <component :is="titleTag" class="stat-panel__label">
                    {{ item.label }}
                </component>
                <p class="stat-panel__value">
                    {{ item.value }}
                    <span v-if="item.suffix" class="stat-panel__suffix">{{ item.suffix }}</span>
                </p>
                <p v-if="item.caption" class="stat-panel__caption">{{ item.caption }}</p>
            </article>
        </div>
    `
};

