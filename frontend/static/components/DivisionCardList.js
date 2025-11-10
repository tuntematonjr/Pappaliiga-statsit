(function () {
    'use strict';

    const FILTER_STATES = Object.freeze(['all', 'waiting', 'active', 'finished']);
    const FILTER_ORDER = Object.freeze(['all', 'active', 'finished', 'waiting']);
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
            searchQuery: { type: String, default: '' }
        },
        emits: ['change-season', 'change-filter', 'change-search', 'reset-filters'],
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
                playoffsOpen: false
            };
        },
        watch: {
            division: {
                deep: true,
                handler() {
                    this.playoffsOpen = false;
                }
            }
        },
        computed: {
            statusLabel() {
                return STATUS_LABELS[this.division.season.status] || STATUS_LABELS.waiting;
            },
            ctaLabel() {
                if (this.division.season.status === 'finished') {
                    return CTA_LABELS.finished;
                }
                return CTA_LABELS.waiting;
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
            stateClass,
            statusText(value) {
                return STATUS_LABELS[value] || STATUS_LABELS.waiting;
            }
        },
        template: `
            <article class="division-card" role="listitem" :class="'division-card--' + stateClass(division.season.status)">
                <header class="division-card__header">
                    <div>
                        <p class="division-card__eyebrow">Division {{ division.divisionNumber || '0' }}</p>
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
                    <ul v-if="seasonRows.length" class="division-card__facts" role="list">
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
                            <p class="division-card__playoffs-sub">
                                Teams: {{ division.playoffs.teams }} · Matches: {{ division.playoffs.matchesPlayed }}/{{ division.playoffs.matchesTotal }}
                                <span class="division-card__playoffs-badge" :class="'division-card__playoffs-badge--' + stateClass(division.playoffs.status)">
                                    {{ statusText(division.playoffs.status) }}
                                </span>
                            </p>
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
                        <p v-else class="division-card__playoffs-hint">
                            {{ division.playoffs.status === 'active' ? 'Ongoing series' : 'Bracket announcement coming soon' }}
                        </p>
                    </div>
                </section>
            </article>
        `
    };

    window.DivisionCardList = {
        name: 'DivisionCardList',
        components: { SeasonBar, DivisionCard, DivisionProgressBar },
        props: {
            divisions: { type: Array, default: () => [] },
            emptyMessage: { type: String, default: 'Ei divisioonia saatavilla' },
            filterState: { type: String, default: 'all' },
            searchQuery: { type: String, default: '' },
            seasonOptions: { type: Array, default: () => [] },
            selectedSeason: { type: [String, Number], default: '' },
            seasonLoading: { type: Boolean, default: false },
            offlineMessage: { type: String, default: '' },
            dataBadge: { type: String, default: '' },
            warningMessage: { type: String, default: '' },
            isLoading: { type: Boolean, default: false }
        },
        emits: ['change-season', 'change-filter', 'change-search', 'reset-filters'],
        data() {
            return {
                preferredDivisionId: getStoredDivisionId(),
                renderCount: 0,
                renderBatchSize: 8,
                sentinelObserver: null
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
            filteredCards() {
                const filterState = this.filterStateNormalized;
                const search = this.searchQueryNormalized;
                const sorted = this.cardModels
                    .filter(card => filterState === 'all' || card.season.status === filterState)
                    .filter(card => {
                        if (!search) return true;
                        return card.searchIndex.includes(search);
                    })
                    .sort((a, b) => {
                        if (a.tier !== b.tier) {
                            return a.tier - b.tier;
                        }
                        return (a.divisionNumber || 0) - (b.divisionNumber || 0);
                    });
                if (isDevEnv) {
                    console.info(
                        `[DivisionCardList] filtered ${sorted.length} cards (status=${filterState}, search="${this.searchQuery}")`
                    );
                }
                return sorted;
            },
            visibleCards() {
                const limit = this.renderCount || this.renderBatchSize;
                return this.filteredCards.slice(0, limit);
            },
            hasMoreCards() {
                return this.visibleCards.length < this.filteredCards.length;
            },
            hasVisibleDivisions() {
                return this.filteredCards.length > 0;
            }
        },
        watch: {
            divisions: {
                immediate: true,
                handler() {
                    this.resetVirtualWindow();
                }
            },
            filterState() {
                this.resetVirtualWindow();
            },
            searchQuery() {
                this.resetVirtualWindow();
            },
            cardModels(newValue) {
                if (isDevEnv) {
                    console.info('[DivisionCardList] cardModels updated', {
                        count: Array.isArray(newValue) ? newValue.length : 0
                    });
                }
            },
            filteredCards(newValue) {
                if (isDevEnv) {
                    const totalVisible = Array.isArray(newValue) ? newValue.length : 0;
                    console.info('[DivisionCardList] filteredCards updated', {
                        visibleDivisions: totalVisible
                    });
                }
                this.$nextTick(() => this.observeSentinel());
            }
        },
        mounted() {
            this.initSentinelObserver();
            this.$nextTick(() => this.resetVirtualWindow());
        },
        beforeUnmount() {
            this.teardownSentinelObserver();
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
            handleReset() {
                this.$emit('reset-filters');
            },
            resetVirtualWindow() {
                if (!Array.isArray(this.filteredCards) || !this.filteredCards.length) {
                    this.renderCount = this.renderBatchSize;
                    this.$nextTick(() => this.observeSentinel());
                    return;
                }
                this.renderCount = Math.min(this.renderBatchSize, this.filteredCards.length);
                this.$nextTick(() => this.observeSentinel());
            },
            increaseRenderCount() {
                if (!this.hasMoreCards) {
                    return;
                }
                const next = Math.min(this.renderCount + this.renderBatchSize, this.filteredCards.length);
                if (next !== this.renderCount) {
                    this.renderCount = next;
                }
            },
            initSentinelObserver() {
                if (typeof window === 'undefined' || !('IntersectionObserver' in window)) {
                    this.sentinelObserver = null;
                    this.renderCount = this.filteredCards.length || this.renderBatchSize;
                    return;
                }
                this.sentinelObserver = new IntersectionObserver(entries => {
                    entries.forEach(entry => {
                        if (entry.isIntersecting) {
                            this.increaseRenderCount();
                        }
                    });
                }, { root: null, rootMargin: '0px 0px 200px 0px', threshold: 0 });
            },
            observeSentinel() {
                if (!this.sentinelObserver) {
                    return;
                }
                const el = this.$refs?.sentinel;
                this.sentinelObserver.disconnect();
                if (el) {
                    this.sentinelObserver.observe(el);
                }
            },
            teardownSentinelObserver() {
                if (this.sentinelObserver) {
                    this.sentinelObserver.disconnect();
                    this.sentinelObserver = null;
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
                    @change-season="handleSeasonChange"
                    @change-filter="handleFilterChange"
                    @change-search="handleSearch"
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
                <template v-else-if="hasVisibleDivisions">
                    <div class="division-list" role="list">
                        <division-card
                            v-for="division in visibleCards"
                            :key="division.id"
                            :division="division"
                            @remember="rememberDivision"
                        ></division-card>
                    </div>
                    <div
                        v-if="hasMoreCards"
                        ref="sentinel"
                        class="division-list__sentinel"
                        aria-hidden="true"
                    ></div>
                </template>
                <p v-else class="division-hub__empty">{{ emptyMessage }}</p>
            </div>
        `
    };
})();
