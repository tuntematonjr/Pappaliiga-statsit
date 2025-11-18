// Pill-based season selector (formerly SeasonToggle)
window.SeasonToggle = {
    name: 'SeasonSelector',
    components: {
        get LoadingSpinner() {
            return window.LoadingSpinner;
        },
        get ErrorMessage() {
            return window.ErrorMessage;
        }
    },
    props: {
        seasons: {
            type: Array,
            default: () => []
        },
        modelValue: {
            type: [String, Number],
            default: null
        },
        loading: {
            type: Boolean,
            default: false
        },
        error: {
            type: String,
            default: ''
        },
        showAllLink: {
            type: Boolean,
            default: true
        },
        showHeading: {
            type: Boolean,
            default: true
        },
        flat: {
            type: Boolean,
            default: false
        }
    },
    emits: ['update:modelValue', 'select', 'retry', 'focus-selector'],
    data() {
        return {
            useCompactDropdown: false,
            canScrollLeft: false,
            canScrollRight: false,
            scrollRaf: null
        };
    },
    computed: {
        sortedSeasons() {
            return [...this.seasons].sort((a, b) => {
                const aId = Number.isFinite(a?.id) ? a.id : Number.NEGATIVE_INFINITY;
                const bId = Number.isFinite(b?.id) ? b.id : Number.NEGATIVE_INFINITY;
                if (aId !== bId) {
                    return bId - aId;
                }
                const aNum = Number.isFinite(a?.seasonNumber) ? a.seasonNumber : Number.NEGATIVE_INFINITY;
                const bNum = Number.isFinite(b?.seasonNumber) ? b.seasonNumber : Number.NEGATIVE_INFINITY;
                if (aNum !== bNum) {
                    return bNum - aNum;
                }
                const aLabel = a?.label || '';
                const bLabel = b?.label || '';
                return aLabel.localeCompare(bLabel, 'fi');
            });
        },
        hasSingleSeason() {
            return this.sortedSeasons.length <= 1;
        },
        selectedSeason() {
            return this.sortedSeasons.find(season => String(season.key) === String(this.modelValue)) || null;
        },
        showDropdown() {
            return this.useCompactDropdown && !this.hasSingleSeason;
        }
    },
    mounted() {
        this.handleResize();
        window.addEventListener('resize', this.handleResize);
        this.$nextTick(this.updateOverflowState);
    },
    beforeUnmount() {
        window.removeEventListener('resize', this.handleResize);
        if (this.scrollRaf) {
            cancelAnimationFrame(this.scrollRaf);
            this.scrollRaf = null;
        }
    },
    watch: {
        seasons: {
            immediate: true,
            handler() {
                this.$nextTick(() => {
                    this.handleResize();
                    this.updateOverflowState();
                });
            }
        }
    },
    methods: {
        getPillButtons() {
            const refs = this.$refs.pillButtons;
            if (Array.isArray(refs)) {
                return refs;
            }
            return refs ? [refs] : [];
        },
        handleResize() {
            if (typeof window === 'undefined') {
                this.useCompactDropdown = false;
                return;
            }
            const width = window.innerWidth || 0;
            this.useCompactDropdown = width <= 360 && this.sortedSeasons.length > 3;
            this.$nextTick(this.updateOverflowState);
        },
        handleRetry() {
            this.$emit('retry');
        },
        focusSelector() {
            const container = this.$refs.selectorHeader;
            // Focus the selector header without causing the page to scroll to top.
            if (container && typeof container.focus === 'function') {
                try {
                    container.focus({ preventScroll: true });
                } catch (e) {
                    // Fallback for environments where focus options are not supported
                    try { container.focus(); } catch (e2) { /* ignore */ }
                }
            }
            const buttons = this.getPillButtons();
            if (buttons.length && typeof buttons[0].focus === 'function') {
                try {
                    buttons[0].focus({ preventScroll: true });
                } catch (e) {
                    try { buttons[0].focus(); } catch (e2) { /* ignore */ }
                }
            } else if (this.$refs.selectorDropdown && typeof this.$refs.selectorDropdown.focus === 'function') {
                try {
                    this.$refs.selectorDropdown.focus({ preventScroll: true });
                } catch (e) {
                    try { this.$refs.selectorDropdown.focus(); } catch (e2) { /* ignore */ }
                }
            }
            this.$emit('focus-selector');
        },
        handleSelect(season) {
            if (!season || String(season.key) === String(this.modelValue)) {
                return;
            }
            this.$emit('update:modelValue', season.key);
            this.$emit('select', season);
        },
        handleDropdownChange(event) {
            const value = event?.target?.value;
            if (!value) return;
            const season = this.sortedSeasons.find(entry => String(entry.key) === String(value));
            if (season) {
                this.handleSelect(season);
            }
        },
        isSelected(season) {
            return season && String(season.key) === String(this.modelValue);
        },
        pillLabel(season) {
            if (!season) return '';
            const shortLabel = season.shortLabel || season.label;
            const fallback = season.seasonNumber || season.id;
            return shortLabel || `S${fallback}`;
        },
        handleScroll() {
            if (this.scrollRaf) {
                cancelAnimationFrame(this.scrollRaf);
            }
            this.scrollRaf = requestAnimationFrame(() => {
                this.updateOverflowState();
            });
        },
        updateOverflowState() {
            const scroller = this.$refs.pillScroller;
            if (!scroller) {
                this.canScrollLeft = false;
                this.canScrollRight = false;
                return;
            }
            const { scrollLeft, scrollWidth, clientWidth } = scroller;
            this.canScrollLeft = scrollLeft > 4;
            this.canScrollRight = scrollLeft + clientWidth < scrollWidth - 4;
        },
        scrollBy(direction) {
            const scroller = this.$refs.pillScroller;
            if (!scroller) return;
            const delta = direction === 'left' ? -scroller.clientWidth : scroller.clientWidth;
            scroller.scrollBy({ left: delta, behavior: 'smooth' });
            setTimeout(this.updateOverflowState, 320);
        },
        handleKeydown(event, index) {
            if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) {
                return;
            }
            event.preventDefault();
            const buttons = this.getPillButtons();
            if (!buttons.length) return;
            let nextIndex = index;
            if (event.key === 'ArrowLeft') {
                nextIndex = Math.max(0, index - 1);
            } else if (event.key === 'ArrowRight') {
                nextIndex = Math.min(buttons.length - 1, index + 1);
            } else if (event.key === 'Home') {
                nextIndex = 0;
            } else if (event.key === 'End') {
                nextIndex = buttons.length - 1;
            }
            const target = buttons[nextIndex];
            if (target && typeof target.focus === 'function') {
                target.focus();
            }
        }
    },
    template: `
        <div
            class="season-selector"
            :class="{ 'glass-card': !flat, 'season-selector--flat': flat }"
            role="region"
            aria-label="Season selector"
        >
            <header
                v-if="showHeading"
                class="season-selector__header"
                ref="selectorHeader"
                tabindex="-1"
            >
                <div>
                    <span class="section-eyebrow">Season Selector</span>
                    <h3 class="title-accent titleUnderlineCard">Select Season</h3>
                </div>
                <button
                    v-if="showAllLink && !loading && seasons.length > 1"
                    type="button"
                    class="season-selector__all-link"
                    @click="focusSelector"
                >
                    All Seasons
                </button>
            </header>

            <div class="season-selector__body">
                <loading-spinner
                    v-if="loading && !seasons.length"
                    message="Kausia ladataan..."
                ></loading-spinner>

                <error-message
                    v-else-if="error"
                    :message="error"
                    @retry="handleRetry"
                ></error-message>

                <div v-else>
                    <div
                        v-if="showDropdown"
                        class="season-selector__dropdown"
                    >
                        <label for="season-selector-dropdown" class="sr-only">Select season</label>
                        <select
                            id="season-selector-dropdown"
                            class="season-selector__select"
                            :value="modelValue || (sortedSeasons[0] && sortedSeasons[0].key)"
                            :disabled="hasSingleSeason"
                            @change="handleDropdownChange"
                            ref="selectorDropdown"
                        >
                            <option
                                v-for="season in sortedSeasons"
                                :key="season.key"
                                :value="season.key"
                            >
                                {{ season.label }}
                            </option>
                        </select>
                    </div>

                    <div
                        v-else
                        class="season-selector__pill-bar"
                    >
                        <button
                            type="button"
                            class="season-selector__chevron"
                            v-if="canScrollLeft"
                            aria-label="Scroll seasons left"
                            @click="scrollBy('left')"
                        >
                            &lsaquo;
                        </button>

                        <div
                            class="season-selector__pills"
                            ref="pillScroller"
                            role="tablist"
                            aria-label="Season list"
                            @scroll="handleScroll"
                        >
                            <button
                                v-for="(season, index) in sortedSeasons"
                                :key="season.key"
                                type="button"
                            class="season-pill"
                            :class="{ 'season-pill--active': isSelected(season), 'season-pill--disabled': hasSingleSeason }"
                            role="tab"
                            :aria-selected="isSelected(season)"
                            :aria-label="season.label || pillLabel(season)"
                            :tabindex="isSelected(season) ? 0 : -1"
                            :disabled="hasSingleSeason"
                            ref="pillButtons"
                                @click="handleSelect(season)"
                                @keydown="handleKeydown($event, index)"
                            >
                                <span class="season-pill__short">{{ season.shortLabel || pillLabel(season) }}</span>
                                <span class="season-pill__full">{{ season.label }}</span>
                            </button>
                        </div>

                        <button
                            type="button"
                            class="season-selector__chevron"
                            v-if="canScrollRight"
                            aria-label="Scroll seasons right"
                            @click="scrollBy('right')"
                        >
                            &rsaquo;
                        </button>
                    </div>
                </div>
            </div>
        </div>
    `
};
