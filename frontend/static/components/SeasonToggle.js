// SeasonToggle - segmented season picker for active/archive seasons
window.SeasonToggle = {
    name: 'SeasonToggle',
    components: {
        get LoadingSpinner() {
            return window.LoadingSpinner;
        },
        get ErrorMessage() {
            return window.ErrorMessage;
        }
    },
    props: {
        activeSeasons: {
            type: Array,
            default: () => []
        },
        archivedSeasons: {
            type: Array,
            default: () => []
        },
        modelValue: {
            type: String,
            default: null
        },
        segment: {
            type: String,
            default: 'active'
        },
        loading: {
            type: Boolean,
            default: false
        },
        error: {
            type: String,
            default: ''
        }
    },
    emits: ['update:modelValue', 'update:segment', 'segment-change', 'select', 'retry'],
    data() {
        return {
            internalSegment: this.segment || 'active'
        };
    },
    watch: {
        segment(newValue) {
            if (newValue && newValue !== this.internalSegment) {
                this.internalSegment = newValue;
            }
        }
    },
    computed: {
        currentSegment() {
            return this.segment || this.internalSegment || 'active';
        },
        segmentLabel() {
            return this.currentSegment === 'archived' ? 'Arkistoidut kaudet' : 'Käynnissä olevat kaudet';
        },
        seasonOptions() {
            const pool = this.currentSegment === 'archived' ? this.archivedSeasons : this.activeSeasons;
            return Array.isArray(pool) ? pool : [];
        },
        hasOptions() {
            return this.seasonOptions.length > 0;
        }
    },
    methods: {
        changeSegment(segment) {
            const normalized = segment === 'archived' ? 'archived' : 'active';
            if (normalized === this.currentSegment) {
                return;
            }
            this.internalSegment = normalized;
            this.$emit('update:segment', normalized);
            this.$emit('segment-change', normalized);
        },
        handleSelect(season) {
            if (!season) return;
            this.$emit('update:modelValue', season.key);
            this.$emit('select', season);
        },
        handleRetry() {
            this.$emit('retry');
        },
        isSelected(season) {
            return season && String(season.key) === String(this.modelValue);
        }
    },
    template: `
        <div class="season-toggle glass-card">
            <div class="season-toggle__header">
                <div class="season-toggle__tabs" role="tablist" aria-label="Season filters">
                    <button
                        type="button"
                        class="season-toggle__tab"
                        :class="{ 'season-toggle__tab--active': currentSegment === 'active' }"
                        role="tab"
                        :aria-selected="currentSegment === 'active'"
                        @click="changeSegment('active')"
                    >
                        Käynnissä
                    </button>
                    <button
                        type="button"
                        class="season-toggle__tab"
                        :class="{ 'season-toggle__tab--active': currentSegment === 'archived' }"
                        role="tab"
                        :aria-selected="currentSegment === 'archived'"
                        @click="changeSegment('archived')"
                    >
                        Arkisto
                    </button>
                </div>
                <button
                    type="button"
                    class="season-toggle__refresh"
                    @click="handleRetry"
                    :disabled="loading"
                    aria-label="Päivitä kausilista"
                >
                    Päivitä
                </button>
            </div>

            <loading-spinner
                v-if="loading"
                message="Kausia ladataan..."
            ></loading-spinner>

            <error-message
                v-else-if="error"
                :message="error"
                @retry="handleRetry"
            ></error-message>

            <ul v-else class="season-toggle__list" :aria-label="segmentLabel">
                <li
                    v-for="season in seasonOptions"
                    :key="season.key"
                    class="season-toggle__item"
                >
                    <button
                        type="button"
                        class="season-toggle__chip"
                        :class="{ 'season-toggle__chip--active': isSelected(season) }"
                        @click="handleSelect(season)"
                        :aria-pressed="isSelected(season)"
                    >
                        <span class="season-toggle__chip-label">{{ season.label }}</span>
                        <span v-if="season.phase" class="season-toggle__chip-meta">{{ season.phase }}</span>
                        <span v-else-if="season.seasonNumber" class="season-toggle__chip-meta">Kausi {{ season.seasonNumber }}</span>
                    </button>
                </li>
                <li v-if="!hasOptions" class="season-toggle__empty">
                    Ei kausia saatavilla tällä välilehdellä.
                </li>
            </ul>
        </div>
    `
};

