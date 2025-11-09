(function () {
    'use strict';

    const FILTER_STATES = Object.freeze(['all', 'waiting', 'active', 'finished']);
    const FILTER_ORDER = Object.freeze(['all', 'active', 'finished', 'waiting']);
    const SORT_OPTIONS = Object.freeze(['tier', 'progress', 'alphabetical']);
    const STATUS_LABELS = Object.freeze({ waiting: 'Waiting', active: 'Active', finished: 'Finished' });
    const CTA_LABELS = Object.freeze({ waiting: 'Open Division', active: 'Open Division', finished: 'View Results' });
    const STORAGE_KEY = 'pappaliiga:last-division';
    const isDevEnv = typeof window !== 'undefined' && ['localhost', '127.0.0.1'].includes(window.location.hostname);

    function getStoredDivisionId() {
        try {
            if (typeof window === 'undefined' || !window.localStorage) return null;
            return window.localStorage.getItem(STORAGE_KEY);
        } catch (error) {
            return null;
        }
    }

    function inferTierFromDivisionId(divisionId) {
        const numeric = Number(divisionId);
        if (numeric >= 1 && numeric <= 5) return 1;
        if (numeric >= 6 && numeric <= 10) return 2;
        if (numeric >= 11 && numeric <= 15) return 3;
        if (numeric >= 16 && numeric <= 20) return 4;
        return 5;
    }

    function normalizeStatus(value, fallback = 'waiting') {
        const normalized = String(value || fallback).toLowerCase();
        return ['waiting', 'active', 'finished'].includes(normalized) ? normalized : fallback;
    }

    function storeDivisionId(id) {
        try {
            if (typeof window !== 'undefined' && window.localStorage && id) {
                window.localStorage.setItem(STORAGE_KEY, id);
            }
        } catch (error) {
            // no-op
        }
    }

    function formatProgressLabel(played, total) {
        const safePlayed = Number.isFinite(played) ? played : 0;
        const safeTotal = Number.isFinite(total) ? total : 0;
        const percent = safeTotal > 0 ? Math.round((safePlayed / safeTotal) * 100) : 0;
        return `${safePlayed}/${safeTotal} · ${percent}%`;
    }

    function stateClass(status) {
        return STATUS_LABELS[status] ? status : 'waiting';
    }

    function inferTierMeta(card) {
        if (card?.tierMeta) {
            return card.tierMeta;
        }
        const value = Number(card?.tier) || 5;
        if (value === 1) return { id: 1, label: 'Tier 1 (Div 1-5)', range: 'Div 1-5', order: 1 };
        if (value === 2) return { id: 2, label: 'Tier 2 (Div 6-10)', range: 'Div 6-10', order: 2 };
        if (value === 3) return { id: 3, label: 'Tier 3 (Div 11-15)', range: 'Div 11-15', order: 3 };
        if (value === 4) return { id: 4, label: 'Tier 4 (Div 16-20)', range: 'Div 16-20', order: 4 };
        return { id: 5, label: 'Tier 5 (Div 21-25)', range: 'Div 21-25', order: 5 };
    }

    function buildCardModel(division) {
        if (!division) {
            return null;
        }
        const tierMeta = inferTierMeta(division);
        const seasonStatus = division.season?.status || 'waiting';
        const seasonMatchesPlayed = Number(division.season?.matches_played) || 0;
        const seasonMatchesTotal = Number(division.season?.matches_total) || 0;
        const playoffsMatchesPlayed = Number(division.playoffs?.matches_played) || 0;
        const playoffsMatchesTotal = Number(division.playoffs?.matches_total) || 7;
        const hrefId = division.slug || division.id;
        const seasonLabel = division.seasonNumber ? `S${division.seasonNumber}` : '';
        const nameSuffix = seasonLabel ? ` ${seasonLabel}` : '';
        const titleBase = division.name || (division.divisionId ? `Division ${division.divisionId}` : 'Division');
        const title = `${titleBase}${nameSuffix}`;
        return {
            id: division.id,
            divisionNumber: division.divisionId,
            title,
            tierMeta,
            tier: tierMeta.id,
            state: seasonStatus,
            season: {
                teams: Number(division.season?.teams) || 0,
                matchesPlayed: seasonMatchesPlayed,
                matchesTotal: seasonMatchesTotal,
                percent: seasonMatchesTotal > 0 ? Math.round((seasonMatchesPlayed / seasonMatchesTotal) * 100) : 0,
                status: seasonStatus,
                progressLabel: formatProgressLabel(seasonMatchesPlayed, seasonMatchesTotal),
                winner: division.season?.winner || null
            },
            playoffs: {
                teams: Number(division.playoffs?.teams) || 8,
                matchesPlayed: playoffsMatchesPlayed,
                matchesTotal: playoffsMatchesTotal || 7,
                percent: playoffsMatchesTotal > 0 ? Math.round((playoffsMatchesPlayed / playoffsMatchesTotal) * 100) : 0,
                status: division.playoffs?.status || 'waiting',
                progressLabel: formatProgressLabel(playoffsMatchesPlayed, playoffsMatchesTotal || 7),
                winner: division.playoffs?.winner || null
            },
            slug: hrefId,
            href: `/division/${hrefId}`,
            searchIndex: [division.name, division.id, division.divisionId].map(value => (value ? String(value).toLowerCase() : '')).join(' ')
        };
    }

    const SeasonBar = {
        name: 'SeasonBar',
        props: {
            seasonOptions: { type: Array, default: () => [] },
            selectedSeason: { type: [String, Number], default: '' },
            seasonLoading: { type: Boolean, default: false },
            filterState: { type: String, default: 'all' },
            searchQuery: { type: String, default: '' },
            sortMode: { type: String, default: 'tier' }
        },
        emits: ['change-season', 'change-filter', 'change-search', 'change-sort', 'reset-filters'],
        data() {
            return {
                filters: FILTER_ORDER
            };
        },
        template: `
            <div class="division-season-bar" role="region" aria-label="Season controls">
                <div class="division-season-bar__section">
                    <label class="division-season-bar__label" for="season-filter">Season</label>
                    <select
                        id="season-filter"
                        class="division-season-bar__select"
                        :value="selectedSeason"
                        :disabled="seasonLoading || !seasonOptions.length"
                        @change="$emit('change-season', $event.target.value)"
                    >
                        <template v-if="seasonOptions.length">
                            <optgroup v-for="group in seasonOptions" :label="group.label" :key="group.id">
                                <option v-for="option in group.options" :value="option.key" :key="option.key">{{ option.label }}</option>
                            </optgroup>
                        </template>
                        <option v-else disabled>Ei kausia</option>
                    </select>
                </div>
                <div class="division-season-bar__section">
                    <span class="division-season-bar__label">Status</span>
                    <div class="division-season-bar__filters">
                        <button
                            v-for="state in filters"
                            :key="state"
                            type="button"
                            class="season-filter-chip"
                            :class="{ 'season-filter-chip--active': filterState === state }"
                            :aria-pressed="filterState === state"
                            @click="$emit('change-filter', state)"
                        >
                            {{ state === 'all' ? 'All' : state.charAt(0).toUpperCase() + state.slice(1) }}
                        </button>
                    </div>
                </div>
                <label class="division-season-bar__section division-season-bar__search">
                    <span class="division-season-bar__label sr-only">Search divisions</span>
                    <input
                        type="search"
                        class="division-season-bar__input"
                        placeholder="Search divisions"
                        :value="searchQuery"
                        @input="$emit('change-search', $event.target.value)"
                    >
                </label>
                <div class="division-season-bar__section">
                    <label class="division-season-bar__label" for="division-sort">Sort</label>
                    <select
                        id="division-sort"
                        class="division-season-bar__select"
                        :value="sortMode"
                        @change="$emit('change-sort', $event.target.value)"
                    >
                        <option value="tier">By tier</option>
                        <option value="progress">By progress</option>
                        <option value="alphabetical">Alphabetical</option>
                    </select>
                </div>
                <div class="division-season-bar__section division-season-bar__reset">
                    <button type="button" class="season-filter-reset" @click="$emit('reset-filters')">Reset</button>
                </div>
            </div>
        `
    };

    const DivisionProgressBar = {
        name: 'DivisionProgressBar',
        props: {
            value: { type: Number, default: 0 },
            max: { type: Number, default: 100 },
            state: { type: String, default: 'waiting' },
            label: { type: String, default: '' },
            ariaLabel: { type: String, default: '' }
        },
        computed: {
            percent() {
                const safeMax = this.max || 0;
                if (!safeMax) return 0;
                return Math.min(100, Math.max(0, Math.round((this.value / safeMax) * 100)));
            },
            stateClass() {
                return `division-progress--${stateClass(this.state)}`;
            }
        },
        template: `
            <div
                class="division-progress"
                :class="stateClass"
                role="progressbar"
                :aria-valuenow="percent"
                aria-valuemin="0"
                aria-valuemax="100"
                :aria-label="ariaLabel || label"
                :title="label"
            >
                <div class="division-progress__track">
                    <div class="division-progress__fill" :style="{ width: percent + '%' }"></div>
                    <span class="division-progress__label">{{ label }}</span>
                </div>
            </div>
        `
    };

    const DivisionCard = {
        name: 'DivisionCard',
        components: { DivisionProgressBar },
        props: {
            division: { type: Object, required: true }
        },
        emits: ['remember'],
        data() {
            return {
                playoffsOpen: this.division.playoffs.status !== 'waiting'
            };
        },
        watch: {
            division: {
                deep: true,
                handler(newValue) {
                    this.playoffsOpen = newValue?.playoffs?.status !== 'waiting';
                }
            }
        },
        computed: {
            statusLabel() {
                return STATUS_LABELS[this.division.season.status] || STATUS_LABELS.waiting;
            },
            ctaLabel() {
                return CTA_LABELS[this.division.season.status] || CTA_LABELS.waiting;
            },
            seasonRows() {
                const rows = [];
                if (this.division.season.teams > 0) {
                    rows.push({ key: 'teams', label: 'Teams', value: this.division.season.teams });
                }
                if (this.division.season.matchesTotal > 0) {
                    rows.push({ key: 'matches', label: 'Matches', value: `${this.division.season.matchesPlayed}/${this.division.season.matchesTotal}` });
                }
                return rows;
            },
            showWinnerStrip() {
                const seasonWinner = this.division.season.winner;
                const playoffsWinner = this.division.playoffs.winner;
                if (this.division.season.status !== 'finished' || !seasonWinner) {
                    return false;
                }
                if (this.division.playoffs.status !== 'finished') {
                    return true;
                }
                return playoffsWinner === seasonWinner;
            }
        },
        methods: {
            togglePlayoffs() {
                this.playoffsOpen = !this.playoffsOpen;
            },
            handleCTA() {
                storeDivisionId(this.division.id);
                this.$emit('remember', this.division.id);
            },
            stateClass
        },
        template: `
            <article class="division-card" :class="'division-card--' + stateClass(division.season.status)">
                <header class="division-card__header">
                    <div>
                        <p class="division-card__eyebrow">Division {{ division.divisionNumber || '–' }}</p>
                        <h3>{{ division.title }}</h3>
                        <p v-if="showWinnerStrip" class="division-card__winner-banner">Winner: {{ division.season.winner }}</p>
                    </div>
                    <span class="division-card__badge" :class="'division-card__badge--' + stateClass(division.season.status)">{{ statusLabel }}</span>
                </header>
                <section class="division-card__section">
                    <division-progress-bar
                        :value="division.season.matchesPlayed"
                        :max="division.season.matchesTotal || 100"
                        :state="division.season.status"
                        :label="division.season.progressLabel"
                        :aria-label="division.title + ' season progress'"
                    ></division-progress-bar>
                    <ul class="division-card__facts" role="list">
                        <li v-for="row in seasonRows" :key="row.key">
                            <span class="division-card__fact-label">{{ row.label }}</span>
                            <span class="division-card__fact-value">{{ row.value }}</span>
                        </li>
                    </ul>
                </section>
                <a class="division-card__action division-card__action--primary" :href="division.href" @click="handleCTA">
                    {{ ctaLabel }}
                </a>
                <section class="division-card__playoffs">
                    <header class="division-card__playoffs-header">
                        <div>
                            <p class="division-card__playoffs-label">Playoffs</p>
                            <p class="division-card__playoffs-sub">Playoffs · Teams: 8 · Matches: {{ division.playoffs.matchesPlayed }}/7</p>
                        </div>
                        <button
                            type="button"
                            class="division-card__playoffs-toggle"
                            :aria-controls="'playoffs-' + division.id"
                            :aria-expanded="playoffsOpen"
                            @click="togglePlayoffs"
                        >
                            {{ playoffsOpen ? 'Hide' : 'Show' }}
                        </button>
                        <span class="sr-only" aria-live="polite">
                            {{ playoffsOpen ? 'Playoffs expanded' : 'Playoffs collapsed' }}
                        </span>
                    </header>
                    <div
                        v-if="playoffsOpen"
                        class="division-card__playoffs-content"
                        :class="'division-card__playoffs-status--' + stateClass(division.playoffs.status)"
                        :id="'playoffs-' + division.id"
                    >
                        <division-progress-bar
                            :value="division.playoffs.matchesPlayed"
                            :max="division.playoffs.matchesTotal || 7"
                            :state="division.playoffs.status"
                            :label="division.playoffs.progressLabel"
                            :aria-label="division.title + ' playoffs progress'"
                        ></division-progress-bar>
                        <div v-if="division.playoffs.status === 'finished' && division.playoffs.winner" class="playoffs-winner-card">
                            Winner: {{ division.playoffs.winner }}
                        </div>
                        <p v-else class="division-card__playoffs-hint">{{ division.playoffs.status === 'active' ? 'Ongoing' : 'Awaiting start' }}</p>
                    </div>
                </section>
            </article>
        `
    };

    const TierSection = {
        name: 'TierSection',
        components: { DivisionCard },
        props: {
            tier: { type: Object, required: true },
            expanded: { type: Boolean, default: true },
            registerContentRef: { type: Function, default: null }
        },
        emits: ['toggle', 'remember'],
        methods: {
            setRef(el) {
                if (typeof this.registerContentRef === 'function') {
                    this.registerContentRef(el);
                }
            }
        },
        template: `
            <section class="tier-section">
                <button type="button" class="tier-section__toggle" :aria-expanded="expanded" @click="$emit('toggle')">
                    <div class="tier-section__header">
                        <div>
                            <span class="tier-section__title">{{ tier.label }}</span>
                            <span class="tier-section__subtitle">{{ tier.range }}</span>
                        </div>
                        <div class="tier-section__pills">
                            <span class="tier-section__pill">Divisions {{ tier.finishedCount }} / {{ tier.totalCount }}<span class="tier-section__pill-track" :style="{ '--progress': tier.divisionPercent }"></span></span>
                            <span class="tier-section__pill">Playoffs {{ tier.playoffsFinished }} / {{ tier.totalCount }}<span class="tier-section__pill-track" :style="{ '--progress': tier.playoffsPercent }"></span></span>
                        </div>
                    </div>
                    <span class="tier-section__chevron" :class="{ 'is-open': expanded }" aria-hidden="true">
                        <svg viewBox="0 0 24 24"><path d="M7 10l5 5 5-5H7z"></path></svg>
                    </span>
                </button>
                <transition name="accordion">
                    <div v-show="expanded" class="tier-section__body" :ref="setRef">
                        <div v-if="tier.visibleDivisions.length" class="tier-section__grid">
                            <division-card
                                v-for="division in tier.visibleDivisions"
                                :key="division.id"
                                :division="division"
                                @remember="$emit('remember', $event)"
                            ></division-card>
                        </div>
                        <p v-else class="tier-section__empty">No matches for current filters.</p>
                    </div>
                </transition>
            </section>
        `
    };

    window.DivisionCardList = {
        name: 'DivisionCardList',
        components: { SeasonBar, TierSection, DivisionCard, DivisionProgressBar },
        props: {
            divisions: { type: Array, default: () => [] },
            emptyMessage: { type: String, default: 'Ei divisioonia saatavilla' },
            filterState: { type: String, default: 'all' },
            searchQuery: { type: String, default: '' },
            sortMode: { type: String, default: 'tier' },
            seasonOptions: { type: Array, default: () => [] },
            selectedSeason: { type: [String, Number], default: '' },
            seasonLoading: { type: Boolean, default: false },
            offlineMessage: { type: String, default: '' },
            dataBadge: { type: String, default: '' },
            warningMessage: { type: String, default: '' },
            isLoading: { type: Boolean, default: false }
        },
        emits: ['change-season', 'change-filter', 'change-search', 'change-sort', 'reset-filters'],
        data() {
            return {
                expandedTiers: {},
                visibleTiers: {},
                observer: null,
                tierRefs: {},
                preferredDivisionId: getStoredDivisionId(),
                initializedExpansion: false
            };
        },
        computed: {
            cardModels() {
                if (!Array.isArray(this.divisions)) {
                    return [];
                }
                const mapped = this.divisions.map(buildCardModel).filter(Boolean);
                if (!mapped.length && this.divisions.length) {
                    if (isDevEnv) {
                        console.warn('[DivisionCardList] Falling back to raw division rendering. Normalized set empty.');
                    }
                    return this.divisions
                        .map((entry, index) => {
                            if (!entry) return null;
                            const safeId = entry.id || entry.division_id || entry.divisionId || entry.slug || `division-${index}`;
                            const rawSeason = entry.season && typeof entry.season === 'object' ? entry.season : {};
                            const rawPlayoffs = entry.playoffs && typeof entry.playoffs === 'object' ? entry.playoffs : {};
                            const fallbackSeason = {
                                teams: Number(rawSeason.teams ?? entry.teams ?? 0),
                                matches_played: Number(rawSeason.matches_played ?? rawSeason.matchesPlayed ?? entry.matches_played ?? entry.matchesPlayed ?? 0),
                                matches_total: Number(rawSeason.matches_total ?? rawSeason.matchesTotal ?? entry.matches_total ?? entry.matchesTotal ?? 0),
                                status: normalizeStatus(rawSeason.status || entry.status),
                                winner: rawSeason.winner || entry.winner || null
                            };
                            const fallbackPlayoffs = {
                                teams: Number(rawPlayoffs.teams ?? 8) || 8,
                                matches_played: Number(rawPlayoffs.matches_played ?? rawPlayoffs.matchesPlayed ?? 0),
                                matches_total: Number(rawPlayoffs.matches_total ?? rawPlayoffs.matchesTotal ?? 7) || 7,
                                status: normalizeStatus(rawPlayoffs.status, 'waiting'),
                                winner: rawPlayoffs.winner || null
                            };
                            const fallbackDivisionId = (entry.divisionId ?? entry.division_id ?? Number(entry.id)) || index;
                            const fallbackTierSource = entry.divisionId ?? entry.division_id ?? entry.id;
                            return buildCardModel({
                                id: String(safeId),
                                divisionId: fallbackDivisionId,
                                name: entry.name || `Division ${(entry.division_id ?? entry.divisionId ?? index)}`,
                                tier: Number(entry.tier) || inferTierFromDivisionId(fallbackTierSource),
                                season: fallbackSeason,
                                playoffs: fallbackPlayoffs,
                                slug: entry.slug || String(safeId),
                                seasonNumber: entry.seasonNumber ?? entry.season_number ?? null
                            });
                        })
                        .filter(Boolean);
                }
                return mapped;
            },
            filterStateNormalized() {
                const state = String(this.filterState || 'all').toLowerCase();
                return FILTER_STATES.includes(state) ? state : 'all';
            },
            searchQueryNormalized() {
                return String(this.searchQuery || '').trim().toLowerCase();
            },
            sortModeNormalized() {
                const mode = String(this.sortMode || 'tier').toLowerCase();
                return SORT_OPTIONS.includes(mode) ? mode : 'tier';
            },
            tierBlueprints() {
                const buckets = new Map();
                this.cardModels.forEach(card => {
                    const key = `tier-${card.tier}`;
                    if (!buckets.has(key)) {
                        buckets.set(key, {
                            id: key,
                            label: card.tierMeta?.label || `Tier ${card.tier}`,
                            range: card.tierMeta?.range || '',
                            order: card.tierMeta?.order || card.tier,
                            allDivisions: [],
                            finishedCount: 0,
                            playoffsFinished: 0
                        });
                    }
                    const bucket = buckets.get(key);
                    bucket.allDivisions.push(card);
                    if (card.season.status === 'finished') {
                        bucket.finishedCount += 1;
                    }
                    if (card.playoffs.status === 'finished') {
                        bucket.playoffsFinished += 1;
                    }
                });
                return Array.from(buckets.values()).sort((a, b) => a.order - b.order);
            },
            filteredTierGroups() {
                const filterState = this.filterStateNormalized;
                const search = this.searchQueryNormalized;
                const comparator = this.getSorter();
                let visibleCount = 0;
                const tierCount = this.tierBlueprints.length;
                const groups = this.tierBlueprints.map(bucket => {
                    const visible = bucket.allDivisions
                        .filter(card => filterState === 'all' || card.season.status === filterState)
                        .filter(card => {
                            if (!search) return true;
                            return card.searchIndex.includes(search);
                        })
                        .sort(comparator);
                    visibleCount += visible.length;
                    const totalCount = bucket.allDivisions.length;
                    return {
                        ...bucket,
                        totalCount,
                        divisionPercent: totalCount ? Math.round((bucket.finishedCount / totalCount) * 100) : 0,
                        playoffsPercent: totalCount ? Math.round((bucket.playoffsFinished / totalCount) * 100) : 0,
                        visibleDivisions: visible
                    };
                });
                if (isDevEnv) {
                    const rawCount = Array.isArray(this.divisions) ? this.divisions.length : 0;
                    console.info(
                        `divisions raw ${rawCount} → normalized ${this.cardModels.length} → filtered ${visibleCount} ` +
                            `(tiers=${tierCount}, status=${this.filterStateNormalized}, search="${this.searchQuery}", sort=${this.sortModeNormalized})`
                    );
                }
                return groups;
            },
            hasVisibleDivisions() {
                return this.filteredTierGroups.some(group => group.visibleDivisions.length > 0);
            }
        },
       watch: {
           divisions: {
               immediate: true,
               handler() {
                   this.initializedExpansion = false;
                   this.$nextTick(() => this.ensureInitialExpansion(true));
               }
           },
            cardModels(newValue) {
                if (isDevEnv) {
                    console.info('[DivisionCardList] cardModels updated', {
                        count: Array.isArray(newValue) ? newValue.length : 0
                    });
                }
            },
            filteredTierGroups(newValue) {
                if (isDevEnv) {
                    const totalVisible = Array.isArray(newValue)
                        ? newValue.reduce((sum, tier) => sum + tier.visibleDivisions.length, 0)
                        : 0;
                    console.info('[DivisionCardList] filteredTierGroups updated', {
                        tiers: Array.isArray(newValue) ? newValue.length : 0,
                        visibleDivisions: totalVisible
                    });
                }
                this.$nextTick(() => this.ensureAtLeastOneExpanded());
            }
       },
        mounted() {
            this.initObserver();
            this.$nextTick(() => {
                this.ensureInitialExpansion(true);
                this.observeTiers();
            });
        },
        beforeUnmount() {
            this.teardownObserver();
        },
        methods: {
            handleSeasonChange(value) {
                this.$emit('change-season', value);
            },
            handleFilterChange(value) {
                this.$emit('change-filter', value);
            },
            handleSearch(value) {
                this.$emit('change-search', value);
            },
            handleSort(value) {
                this.$emit('change-sort', value);
            },
            handleReset() {
                this.$emit('reset-filters');
            },
            getSorter() {
                if (this.sortModeNormalized === 'progress') {
                    return (a, b) => b.season.percent - a.season.percent;
                }
                if (this.sortModeNormalized === 'alphabetical') {
                    return (a, b) => (a.title || '').localeCompare(b.title || '', 'fi');
                }
                return (a, b) => {
                    if (a.tier !== b.tier) {
                        return a.tier - b.tier;
                    }
                    return (a.divisionNumber || 0) - (b.divisionNumber || 0);
                };
            },
            initObserver() {
                if (typeof window === 'undefined' || !('IntersectionObserver' in window)) {
                    this.observer = null;
                    return;
                }
                this.observer = new IntersectionObserver(entries => {
                    entries.forEach(entry => {
                        if (entry.isIntersecting) {
                            const tierId = entry.target?.dataset?.tierId;
                            if (tierId) {
                                this.markTierVisible(tierId);
                                this.observer.unobserve(entry.target);
                            }
                        }
                    });
                }, { rootMargin: '0px 0px 200px 0px', threshold: 0.2 });
            },
            teardownObserver() {
                if (this.observer) {
                    this.observer.disconnect();
                    this.observer = null;
                }
            },
            registerTierRef(id, el) {
                if (!id) return;
                if (!el) {
                    delete this.tierRefs[id];
                    return;
                }
                this.tierRefs[id] = el;
                el.dataset.tierId = id;
                if (this.observer) {
                    this.observer.observe(el);
                } else {
                    this.markTierVisible(id);
                }
            },
            observeTiers() {
                if (!this.observer) {
                    Object.keys(this.tierRefs).forEach(id => this.markTierVisible(id));
                    return;
                }
                Object.values(this.tierRefs).forEach(el => {
                    if (el) {
                        this.observer.observe(el);
                    }
                });
            },
            markTierVisible(id) {
                if (!id || this.visibleTiers[id]) {
                    return;
                }
                this.visibleTiers = { ...this.visibleTiers, [id]: true };
            },
            ensureInitialExpansion(force = false) {
                if (this.initializedExpansion && !force) {
                    this.ensureAtLeastOneExpanded();
                    return;
                }
                const groups = this.filteredTierGroups;
                if (!groups.length) {
                    this.expandedTiers = {};
                    return;
                }
                let target = null;
                if (this.preferredDivisionId) {
                    target = groups.find(group => group.allDivisions.some(div => div.id === this.preferredDivisionId)) || null;
                }
                if (!target) {
                    target = groups[0];
                }
                if (target) {
                    this.expandedTiers = { [target.id]: true };
                    this.markTierVisible(target.id);
                } else {
                    this.expandedTiers = {};
                }
                this.initializedExpansion = true;
            },
            ensureAtLeastOneExpanded() {
                const groups = this.filteredTierGroups;
                if (!groups.length) {
                    this.expandedTiers = {};
                    return;
                }
                const allowed = new Set(groups.map(group => group.id));
                const active = Object.keys(this.expandedTiers).filter(id => this.expandedTiers[id] && allowed.has(id));
                if (active.length) {
                    const nextState = {};
                    active.forEach(id => {
                        nextState[id] = true;
                        this.markTierVisible(id);
                    });
                    this.expandedTiers = nextState;
                    return;
                }
                const fallback = groups[0];
                if (fallback) {
                    this.expandedTiers = { [fallback.id]: true };
                    this.markTierVisible(fallback.id);
                }
            },
            toggleTier(id) {
                if (!id) return;
                const next = { ...this.expandedTiers, [id]: !this.expandedTiers[id] };
                this.expandedTiers = next;
                if (next[id]) {
                    this.markTierVisible(id);
                }
            },
            rememberDivision(id) {
                if (!id) return;
                this.preferredDivisionId = id;
                storeDivisionId(id);
            }
        },
        template: `
            <div class="division-hub">
                <season-bar
                    :season-options="seasonOptions"
                    :selected-season="selectedSeason"
                    :season-loading="seasonLoading"
                    :filter-state="filterState"
                    :search-query="searchQuery"
                    :sort-mode="sortMode"
                    @change-season="handleSeasonChange"
                    @change-filter="handleFilterChange"
                    @change-search="handleSearch"
                    @change-sort="handleSort"
                    @reset-filters="handleReset"
                ></season-bar>
                <div v-if="offlineMessage" class="offline-banner" role="status" aria-live="polite">{{ offlineMessage }}</div>
                <div v-if="dataBadge" class="dev-data-badge badge-small" role="status" aria-live="polite">{{ dataBadge }}</div>
                <div v-if="warningMessage" class="inline-toast" role="status" aria-live="polite">{{ warningMessage }}</div>
                <template v-if="isLoading">
                    <div class="division-hub__skeletons" role="status" aria-live="polite">
                        <article v-for="n in 3" :key="n" class="division-card division-card--skeleton"></article>
                    </div>
                </template>
                <template v-else-if="filteredTierGroups.length">
                    <tier-section
                        v-for="tier in filteredTierGroups"
                        :key="tier.id"
                        :tier="tier"
                        :expanded="!!expandedTiers[tier.id]"
                        :register-content-ref="el => registerTierRef(tier.id, el)"
                        @toggle="toggleTier(tier.id)"
                        @remember="rememberDivision"
                    ></tier-section>
                </template>
                <p v-else class="division-hub__empty">{{ emptyMessage }}</p>
            </div>
        `
    };
})();
