window.PlayerCompareModal = {
    name: 'PlayerCompareModal',
    props: {
        visible: { type: Boolean, default: false },
        basePlayer: { type: Object, default: null },
        comparePlayer: { type: Object, default: null },
        metrics: { type: Array, default: () => [] },
        loading: { type: Boolean, default: false },
        error: { type: String, default: null }
    },
    emits: ['close', 'submit'],
    data() {
        return {
            candidateId: ''
        };
    },
    watch: {
        visible(newVal) {
            if (newVal) {
                this.candidateId = '';
            }
        }
    },
    methods: {
        handleClose() {
            this.$emit('close');
        },
        handleSubmit() {
            if (!this.candidateId) return;
            this.$emit('submit', this.candidateId.trim());
        },
        displayValue(metric, side) {
            const value = side === 'base' ? metric.base : metric.compare;
            if (value == null || Number.isNaN(value)) {
                return '–';
            }
            if (typeof metric.format === 'function') {
                return metric.format(value);
            }
            if (metric.percent) {
                return `${Number(value).toFixed(metric.decimals ?? 1)} %`;
            }
            const decimals = metric.decimals ?? (Number(value) >= 100 ? 0 : 1);
            return Number(value).toFixed(decimals);
        }
    },
    template: `
        <transition name="fade">
            <div v-if="visible" class="compare-modal" role="dialog" aria-modal="true">
                <div class="compare-modal__backdrop" @click="handleClose"></div>
                <div class="compare-modal__content glass-card">
                    <header class="compare-modal__header">
                        <h3 class="title-accent titleUnderlineSection">Vertaa pelaajaa</h3>
                        <button type="button" class="compare-modal__close" @click="handleClose">×</button>
                    </header>

                    <div class="compare-modal__body">
                        <form class="compare-modal__form" @submit.prevent="handleSubmit">
                            <label class="compare-modal__label">
                                Vastustajan ID
                                <input
                                    v-model="candidateId"
                                    type="text"
                                    placeholder="Syötä pelaajan ID"
                                />
                            </label>
                            <button type="submit" class="btn-primary" :disabled="!candidateId || loading">
                                Hae vertailu
                            </button>
                        </form>

                        <loading-spinner v-if="loading" message="Pelaajaa verrataan..."></loading-spinner>
                        <error-message v-else-if="error" :message="error"></error-message>

                        <div v-else-if="metrics.length" class="compare-modal__table">
                            <div class="compare-modal__row compare-modal__row--header">
                                <span>Mittari</span>
                                <span>{{ basePlayer?.nickname || 'Pelaaja' }}</span>
                                <span>{{ comparePlayer?.nickname || 'Vertailu' }}</span>
                            </div>
                            <div v-for="metric in metrics" :key="metric.key" class="compare-modal__row">
                                <span class="compare-modal__metric">{{ metric.label }}</span>
                                <span>{{ displayValue(metric, 'base') }}</span>
                                <span>{{ displayValue(metric, 'compare') }}</span>
                            </div>
                        </div>
                        <p v-else class="compare-modal__empty">Syötä pelaajan ID aloittaaksesi vertailun.</p>
                    </div>
                </div>
            </div>
        </transition>
    `
};

