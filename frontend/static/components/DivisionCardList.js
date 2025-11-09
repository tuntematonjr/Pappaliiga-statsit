/**
 * DivisionCardList Component - Season 11 hub rewrite
 *
 * Groups divisions into tier-based accordions, exposes search/filter controls,
 * and renders responsive cards with explicit state colors.
 */
(function () {
    'use strict';

    // ============================================================================
    // CONSTANTS
    // ============================================================================

    const STATUS_LABELS = Object.freeze({
        upcoming: 'Alkamaton',
        running: 'Käynnissä',
        ended: 'Päättynyt'
    });

    const STATUS_ACTIONS = Object.freeze({
        upcoming: 'Open division',
        running: 'Open division',
        ended: 'Open division'
    });

    const STATE_MAP = Object.freeze({
        running: 'active',
        live: 'active',
        ended: 'finished',
        completed: 'finished',
        finished: 'finished',
        upcoming: 'waiting'
    });

    const STATE_LABELS = Object.freeze({
        active: 'Active',
        finished: 'Finished',
        waiting: 'Waiting'
    });

    const FILTER_STATES = Object.freeze(['all', 'active', 'finished', 'waiting']);

    const PROGRESS_TOOLTIP = 'Computed as matches played divided by total scheduled matches.';

    const STORAGE_KEY = 'pappaliiga:last-division';

    const TIER_BUCKETS = Object.freeze([
        { id: 'tier-1', label: 'Tier 1 (Div 1-5)', range: 'Div 1-5', min: 1, max: 5, order: 1 },
        { id: 'tier-2', label: 'Tier 2 (Div 6-10)', range: 'Div 6-10', min: 6, max: 10, order: 2 },
        { id: 'tier-3', label: 'Tier 3 (Div 11-15)', range: 'Div 11-15', min: 11, max: 15, order: 3 },
        { id: 'tier-4', label: 'Tier 4 (Div 16-20)', range: 'Div 16-20', min: 16, max: 20, order: 4 },
        { id: 'tier-5', label: 'Tier 5 (Div 21-25)', range: 'Div 21-25', min: 21, max: 25, order: 5 }
    ]);

    const EXTRA_TIER = Object.freeze({ id: 'tier-extra', label: 'Tier 6 (Div 26+)', range: 'Div 26+', order: 6 });
    const PLAYOFF_TIER = Object.freeze({ id: 'tier-playoffs', label: 'Playoffs', range: 'Bracket', order: 7 });

    // ============================================================================
    // UTILITY HELPERS
    // ============================================================================

    function safeNumber(value, fallback = 0) {
        if (value === null || value === undefined) return fallback;
        const num = Number(value);
        return Number.isFinite(num) ? num : fallback;
    }

    function safeString(value, fallback = '') {
        if (value === null || value === undefined) return fallback;
        const str = String(value).trim();
        return str || fallback;
    }

    function coalesce(...values) {
        for (let i = 0; i < values.length; i += 1) {
            const value = values[i];
            if (value !== null && value !== undefined) {
                return value;
            }
        }
        return undefined;
    }

    function parseDate(dateValue) {
        if (!dateValue) return null;
        const date = new Date(dateValue);
        return Number.isNaN(date.getTime()) ? null : date;
    }

    function formatDate(dateString) {
        const date = parseDate(dateString);
        if (!date) return null;
        return date.toLocaleDateString('fi-FI', {
            day: 'numeric',
            month: 'numeric',
            year: 'numeric'
        });
    }

    function formatDateTime(dateString) {
        const date = parseDate(dateString);
        if (!date) return '';
        return date.toLocaleDateString('fi-FI', {
            day: 'numeric',
            month: 'numeric',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
    }

    function formatDateRange(startDate, endDate) {
        const start = formatDate(startDate);
        const end = formatDate(endDate);

        if (start && end) return `${start} - ${end}`;
        if (start) return `Alkaen ${start}`;
        if (end) return `Päättyy ${end}`;
        return 'Ajankohta vahvistuu';
    }

    function formatMatchesLabel(played, total) {
        const playedMatches = safeNumber(played);
        const totalMatches = safeNumber(total);
        if (!totalMatches) {
            return playedMatches > 0 ? `${playedMatches} matches` : 'Ei otteluita';
        }
        return `${playedMatches} / ${totalMatches} matches`;
    }

    function formatProgressLabel(played, total) {
        const playedMatches = safeNumber(played);
        const totalMatches = safeNumber(total);
        if (!totalMatches) return '0%';
        const percent = Math.round((playedMatches / totalMatches) * 100);
        return `${playedMatches} / ${totalMatches} · ${percent}%`;
    }

    function normalizeStatusValue(status) {
        if (!status && status !== 0) return 'upcoming';
        const value = String(status).toLowerCase();
        if (value === 'live') return 'running';
        if (value === 'completed') return 'ended';
        if (value === 'finished') return 'ended';
        if (value === 'running' || value === 'upcoming' || value === 'ended') return value;
        return 'upcoming';
    }

    function resolveState(status) {
        const normalized = normalizeStatusValue(status);
        return STATE_MAP[normalized] || 'waiting';
    }

    function stripSeasonFromName(name, seasonNumber) {
        let result = safeString(name);
        if (!result) return result;

        const seasonValue = Number.isFinite(Number(seasonNumber))
            ? String(Number(seasonNumber))
            : null;

        const patterns = [
            /\s*(?:season|kausi)\s*#?\d+\s*$/i,
            /\s*-?\s*(?:season|kausi)\s*#?\d+\s*$/i,
            /\s*S\d+\s*$/i,
            /\s*\(S\d+\)\s*$/i
        ];

        if (seasonValue) {
            patterns.push(new RegExp(`\\s*(?:season|kausi)\\s*${seasonValue}\\s*$`, 'i'));
            patterns.push(new RegExp(`\\s*-\\s*(?:season|kausi)\\s*${seasonValue}\\s*$`, 'i'));
            patterns.push(new RegExp(`\\s*S${seasonValue}\\s*$`, 'i'));
            patterns.push(new RegExp(`\\s*\\(S${seasonValue}\\)\\s*$`, 'i'));
        }

        patterns.forEach(pattern => {
            result = result.replace(pattern, '');
        });

        const cleaned = result.replace(/\s{2,}/g, ' ').trim();
        return cleaned || name;
    }

    function computeMapPoolSize(raw) {
        if (!raw) return null;
        const pool = raw.mapPool || raw.map_pool || raw.maps || raw.map_list || raw.mapList;
        if (Array.isArray(pool)) {
            return pool.length;
        }
        const count = safeNumber(
            coalesce(
                raw.mapCount,
                raw.map_count,
                raw.mapsCount,
                raw.maps_count,
                raw.map_total,
                raw.mapsTotal
            ),
            null
        );
        return count || null;
    }

    function determineTierMeta(divisionNum, isPlayoff) {
        if (isPlayoff) {
            return { ...PLAYOFF_TIER };
        }
        if (!Number.isFinite(divisionNum)) {
            return { ...EXTRA_TIER };
        }
        if (divisionNum === 0) {
            return { ...TIER_BUCKETS[0] };
        }
        const bucket = TIER_BUCKETS.find(entry => divisionNum >= entry.min && divisionNum <= entry.max);
        return bucket ? { ...bucket } : { ...EXTRA_TIER };
    }

    function buildSearchIndex(parts) {
        return parts
            .map(part => safeString(part).toLowerCase())
            .filter(Boolean)
            .join(' ');
    }

    function getStoredDivisionId() {
        try {
            if (typeof window === 'undefined' || !window.localStorage) {
                return null;
            }
            return window.localStorage.getItem(STORAGE_KEY);
        } catch (error) {
            return null;
        }
    }

    function storeDivisionId(id) {
        if (!id) return;
        try {
            if (typeof window === 'undefined' || !window.localStorage) {
                return;
            }
            window.localStorage.setItem(STORAGE_KEY, id);
        } catch (error) {
            // ignore storage failures
        }
    }

    // ============================================================================
    // DATA NORMALIZATION
    // ============================================================================

    function normalizeApiDivision(apiDiv) {
        if (!apiDiv) return null;

        const id = safeString(
            coalesce(
                apiDiv.championshipId,
                apiDiv.championship_id,
                apiDiv.key,
                apiDiv.id
            )
        );
        if (!id) {
            console.warn('[DivisionCardList] Division missing key:', apiDiv);
            return null;
        }

        const name = safeString(apiDiv.name);
        const slug = safeString(apiDiv.slug);
        const raw = apiDiv.raw || apiDiv;
        const season = safeNumber(coalesce(raw.season, apiDiv.season), null);
        const divisionNum = safeNumber(
            coalesce(
                raw.divisionNum,
                raw.division_num,
                apiDiv.divisionNum,
                apiDiv.division_num
            ),
            null
        );

        const explicitIsPlayoff = coalesce(
            apiDiv.isPlayoff,
            apiDiv.is_playoff,
            raw.isPlayoff,
            raw.is_playoff,
            apiDiv.kind === 'playoffs',
            raw.kind === 'playoffs'
        );
        const normalizedName = name.toLowerCase();
        const isPlayoff = typeof explicitIsPlayoff === 'boolean'
            ? explicitIsPlayoff
            : normalizedName.includes('playoff') || normalizedName.includes('pudotus');

        const parentId = safeString(
            coalesce(
                raw.parentChampionshipId,
                raw.parent_championship_id,
                apiDiv.parentChampionshipId,
                apiDiv.parent_championship_id
            ),
            null
        );

        const teamsCount = safeNumber(apiDiv.teamsCount || apiDiv.teams_count);
        const totalMatches = safeNumber(coalesce(apiDiv.totalMatches, apiDiv.total_matches));
        const playedMatches = safeNumber(coalesce(apiDiv.playedMatches, apiDiv.played_matches));
        const finishedMatches = safeNumber(coalesce(apiDiv.finishedMatches, apiDiv.finished_matches));
        const liveMatches = safeNumber(coalesce(apiDiv.liveMatches, apiDiv.live_matches));
        const upcomingMatches = safeNumber(coalesce(apiDiv.upcomingMatches, apiDiv.upcoming_matches));

        const startDate = apiDiv.start || apiDiv.start_date || apiDiv.startDate;
        const endDate = apiDiv.end || apiDiv.end_date || apiDiv.endDate;
        const updatedDate = apiDiv.updated || apiDiv.updated_at || apiDiv.updatedAt;

        const status = normalizeStatusValue(apiDiv.status);
        const state = resolveState(status);
        const tierMeta = determineTierMeta(divisionNum, isPlayoff);

        const rawProgress = totalMatches > 0 ? Math.min(100, Math.round((playedMatches / totalMatches) * 100)) : 0;
        const progressPercent = status === 'ended' ? 100 : rawProgress;

        const winnerTeamId = safeString(apiDiv.winnerTeamId || apiDiv.winner_team_id, null);
        const winnerTeamName = safeString(apiDiv.winnerTeamName || apiDiv.winner_team_name || apiDiv.winner, null);

        const href = `/division/${slug || id}`;
        const displayName = stripSeasonFromName(name, season);
        const mapPoolSize = computeMapPoolSize(raw);
        const searchIndex = buildSearchIndex([
            id,
            slug,
            name,
            displayName,
            divisionNum != null ? `division ${divisionNum}` : '',
            raw.code,
            raw.title
        ]);

        return {
            id,
            name,
            displayName,
            slug,
            season,
            divisionNum,
            isPlayoff,
            parentId,
            tierId: tierMeta.id,
            tierLabel: tierMeta.label,
            tierRangeLabel: tierMeta.range,
            tierOrder: tierMeta.order,
            mapPoolSize,
            teams: {
                count: teamsCount,
                label: teamsCount ? `${teamsCount} teams` : 'Ei joukkueita'
            },
            matches: {
                total: totalMatches,
                played: playedMatches,
                finished: finishedMatches,
                live: liveMatches,
                upcoming: upcomingMatches,
                label: formatMatchesLabel(playedMatches, totalMatches)
            },
            dates: {
                start: startDate,
                end: endDate,
                updated: updatedDate,
                label: formatDateRange(startDate, endDate),
                updatedLabel: formatDateTime(updatedDate)
            },
            progress: {
                percent: progressPercent,
                label: formatProgressLabel(playedMatches, totalMatches)
            },
            status,
            statusLabel: STATUS_LABELS[status] || STATUS_LABELS.upcoming,
            state,
            stateLabel: STATE_LABELS[state] || STATE_LABELS.waiting,
            actionLabel: STATUS_ACTIONS[status] || STATUS_ACTIONS.upcoming,
            winner: winnerTeamId ? {
                id: winnerTeamId,
                name: winnerTeamName
            } : null,
            href,
            raw,
            searchIndex
        };
    }

    // ============================================================================
    // VUE COMPONENT
    // ============================================================================

    window.DivisionCardList = {
        name: 'DivisionCardList',
        props: {
            divisions: {
                type: Array,
                default: () => []
            },
            seasonLabel: {
                type: String,
                default: ''
            },
            emptyMessage: {
                type: String,
                default: 'Ei divisioonia saatavilla'
            },
            filterState: {
                type: String,
                default: 'all'
            },
            searchQuery: {
                type: String,
                default: ''
            },
            sortMode: {
                type: String,
                default: 'tier'
            }
        },
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
            normalizedDivisions() {
                if (!Array.isArray(this.divisions)) return [];
                return this.divisions.map(normalizeApiDivision).filter(Boolean);
            },
            filterStateNormalized() {
                const value = String(this.filterState || 'all').toLowerCase();
                return FILTER_STATES.includes(value) ? value : 'all';
            },
            searchQueryNormalized() {
                return safeString(this.searchQuery).toLowerCase();
            },
            sortModeNormalized() {
                const value = String(this.sortMode || 'tier').toLowerCase();
                if (value === 'progress' || value === 'alphabetical') {
                    return value;
                }
                return 'tier';
            },
            filteredDivisions() {
                const query = this.searchQueryNormalized;
                const filterState = this.filterStateNormalized;
                return this.normalizedDivisions.filter(division => {
                    if (filterState !== 'all' && division.state !== filterState) {
                        return false;
                    }
                    if (query && division.searchIndex.indexOf(query) === -1) {
                        return false;
                    }
                    return true;
                });
            },
            tierGroups() {
                const buckets = new Map();
                this.filteredDivisions.forEach(division => {
                    const key = division.tierId || EXTRA_TIER.id;
                    if (!buckets.has(key)) {
                        buckets.set(key, {
                            id: key,
                            label: division.tierLabel || EXTRA_TIER.label,
                            rangeLabel: division.tierRangeLabel || EXTRA_TIER.range,
                            order: division.tierOrder || EXTRA_TIER.order,
                            divisions: [],
                            finishedCount: 0
                        });
                    }
                    const bucket = buckets.get(key);
                    bucket.divisions.push(division);
                    if (division.state === 'finished') {
                        bucket.finishedCount += 1;
                    }
                });

                const comparator = this.getSorter();
                return Array.from(buckets.values())
                    .sort((a, b) => a.order - b.order)
                    .map(group => {
                        group.divisions.sort(comparator);
                        group.totalCount = group.divisions.length;
                        group.progressPercent = group.totalCount
                            ? Math.round((group.finishedCount / group.totalCount) * 100)
                            : 0;
                        return group;
                    });
            },
            hasDivisions() {
                return this.tierGroups.length > 0;
            }
        },
        watch: {
            normalizedDivisions: {
                immediate: true,
                handler() {
                    this.initializedExpansion = false;
                    this.$nextTick(() => this.ensureInitialExpansion(true));
                }
            },
            filteredDivisions() {
                this.$nextTick(() => this.ensureAtLeastOneExpanded());
            },
            tierGroups() {
                this.$nextTick(() => this.observeTiers());
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
                                if (this.observer) {
                                    this.observer.unobserve(entry.target);
                                }
                            }
                        }
                    });
                }, {
                    root: null,
                    rootMargin: '0px 0px 200px 0px',
                    threshold: 0.2
                });
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
            getSorter() {
                if (this.sortModeNormalized === 'progress') {
                    return (a, b) => {
                        if (b.progress.percent !== a.progress.percent) {
                            return b.progress.percent - a.progress.percent;
                        }
                        return (a.displayName || '').localeCompare(b.displayName || '', 'fi');
                    };
                }
                if (this.sortModeNormalized === 'alphabetical') {
                    return (a, b) => (a.displayName || '').localeCompare(b.displayName || '', 'fi');
                }
                return (a, b) => {
                    if (a.tierOrder !== b.tierOrder) {
                        return a.tierOrder - b.tierOrder;
                    }
                    if (Number.isFinite(a.divisionNum) && Number.isFinite(b.divisionNum)) {
                        return a.divisionNum - b.divisionNum;
                    }
                    return (a.displayName || '').localeCompare(b.displayName || '', 'fi');
                };
            },
            ensureInitialExpansion(force = false) {
                if (this.initializedExpansion && !force) {
                    this.ensureAtLeastOneExpanded();
                    return;
                }
                const groups = this.tierGroups;
                if (!groups.length) {
                    this.expandedTiers = {};
                    return;
                }
                let target = null;
                if (this.preferredDivisionId) {
                    target = groups.find(group => group.divisions.some(div => div.id === this.preferredDivisionId)) || null;
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
                const groups = this.tierGroups;
                if (!groups.length) {
                    this.expandedTiers = {};
                    return;
                }
                const allowed = new Set(groups.map(group => group.id));
                const activeIds = Object.keys(this.expandedTiers).filter(id => this.expandedTiers[id] && allowed.has(id));
                if (activeIds.length) {
                    const nextState = {};
                    activeIds.forEach(id => {
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
            isTierExpanded(id) {
                return !!this.expandedTiers[id];
            },
            shouldRenderTier(id) {
                return this.visibleTiers[id] || this.isTierExpanded(id);
            },
            rememberDivision(id) {
                if (!id) return;
                this.preferredDivisionId = id;
                storeDivisionId(id);
            },
            navigateToDivision(division) {
                if (!division || !division.href || typeof window === 'undefined') return;
                this.rememberDivision(division.id);
                window.location.assign(division.href);
            },
            handleCardKey(event, division) {
                if (!division) return;
                if (event && (event.key === 'Enter' || event.key === ' ')) {
                    this.navigateToDivision(division);
                }
            },
            getProgressDisplay(division) {
                const matches = division.matches || {};
                if (matches.total > 0) {
                    const percent = Math.round(division.progress.percent);
                    return `${matches.played} / ${matches.total} · ${percent}%`;
                }
                return division.state === 'waiting' ? '0% · Scheduling' : `${Math.round(division.progress.percent)}%`;
            },
            getActivityLine(division) {
                const dates = division.dates || {};
                if (division.state === 'waiting') {
                    return 'Scheduling';
                }
                if (division.state === 'finished') {
                    return dates.label || 'Taputeltu loppuun';
                }
                if (dates.updatedLabel) {
                    return `Viimeksi päivitetty ${dates.updatedLabel}`;
                }
                return dates.label || 'Ajankohta vahvistuu';
            },
            getQuickFacts(division) {
                const matches = division.matches || {};
                const facts = [
                    {
                        key: 'matches',
                        icon: 'matches',
                        label: 'Matches',
                        value: matches.label || 'Ei otteluita'
                    },
                    {
                        key: 'teams',
                        icon: 'teams',
                        label: 'Teams',
                        value: division.teams?.label || 'Ei joukkueita'
                    }
                ];
                facts.push({
                    key: 'maps',
                    icon: 'maps',
                    label: 'Map pool',
                    value: division.mapPoolSize ? `${division.mapPoolSize} maps` : 'N/A'
                });
                return facts;
            },
            divisionCardActions(division) {
                const href = division.href || '#';
                if (division.state === 'finished') {
                    return [
                        { label: 'View Stats', href: `${href}#stats`, primary: true },
                        { label: 'Results', href: `${href}#results`, primary: false }
                    ];
                }
                if (division.state === 'waiting') {
                    return [
                        { label: 'Open Division', href, primary: true }
                    ];
                }
                return [
                    { label: 'Open Division', href, primary: true },
                    { label: 'View Stats', href: `${href}#stats`, primary: false }
                ];
            },
            tierProgressLabel(tier) {
                return `${tier.finishedCount} / ${tier.totalCount} divisions finished`;
            },
            progressTooltip() {
                return PROGRESS_TOOLTIP;
            }
        },
        template: `
            <div class="division-hub">
                <template v-if="hasDivisions">
                    <section
                        v-for="tier in tierGroups"
                        :key="tier.id"
                        class="division-tier"
                    >
                        <button
                            type="button"
                            class="division-tier__toggle"
                            :aria-expanded="isTierExpanded(tier.id)"
                            @click="toggleTier(tier.id)"
                        >
                            <div class="division-tier__title">
                                <span class="division-tier__label">{{ tier.label }}</span>
                                <span class="division-tier__range">{{ tier.rangeLabel }}</span>
                            </div>
                            <div class="division-tier__meta">
                                <span class="division-tier__progress-label">{{ tierProgressLabel(tier) }}</span>
                                <div
                                    class="division-tier__progress"
                                    role="progressbar"
                                    :aria-valuenow="tier.progressPercent"
                                    aria-valuemin="0"
                                    aria-valuemax="100"
                                >
                                    <span class="division-tier__progress-fill" :style="{ width: tier.progressPercent + '%' }"></span>
                                </div>
                            </div>
                            <span class="division-tier__chevron" :class="{ 'is-open': isTierExpanded(tier.id) }" aria-hidden="true">
                                <svg viewBox="0 0 24 24" focusable="false">
                                    <path d="M7 10l5 5 5-5H7z"></path>
                                </svg>
                            </span>
                        </button>
                        <transition name="accordion">
                            <div
                                v-show="isTierExpanded(tier.id)"
                                class="division-tier__content"
                                :data-tier-id="tier.id"
                                :ref="el => registerTierRef(tier.id, el)"
                            >
                                <div
                                    v-if="shouldRenderTier(tier.id)"
                                    class="division-hub__grid"
                                    role="list"
                                    :aria-label="tier.label"
                                >
                                    <article
                                        v-for="division in tier.divisions"
                                        :key="division.id"
                                        class="division-hub-card"
                                        :class="'division-hub-card--' + division.state"
                                        role="listitem"
                                        tabindex="0"
                                        :aria-label="division.displayName || division.name"
                                        @click="navigateToDivision(division)"
                                        @keydown.enter.prevent="handleCardKey($event, division)"
                                        @keydown.space.prevent="handleCardKey($event, division)"
                                    >
                                        <header class="division-hub-card__header">
                                            <div>
                                                <p class="division-hub-card__eyebrow">Division {{ division.divisionNum != null ? division.divisionNum : '-' }}</p>
                                                <h3>{{ division.displayName || division.name }}</h3>
                                            </div>
                                            <span class="division-hub-card__badge" :class="'division-hub-card__badge--' + division.state">
                                                {{ division.stateLabel }}
                                            </span>
                                        </header>

                                        <div class="division-hub-card__body">
                                            <template v-if="division.state === 'finished'">
                                                <div class="division-hub-card__finished">
                                                    <p>Taputeltu loppuun</p>
                                                    <p class="division-hub-card__winner">
                                                        Winner: {{ division.winner?.name || 'Vahvistuu' }}
                                                    </p>
                                                </div>
                                            </template>
                                            <template v-else>
                                                <div
                                                    class="division-hub-card__progress"
                                                    :class="{ 'division-hub-card__progress--waiting': division.state === 'waiting' }"
                                                    role="progressbar"
                                                    :aria-valuenow="Math.round(division.progress.percent)"
                                                    aria-valuemin="0"
                                                    aria-valuemax="100"
                                                    :title="progressTooltip()"
                                                >
                                                    <div class="division-hub-card__progress-fill" :style="{ width: division.progress.percent + '%' }"></div>
                                                    <span class="division-hub-card__progress-marker" aria-hidden="true"></span>
                                                    <span class="division-hub-card__progress-text">{{ getProgressDisplay(division) }}</span>
                                                </div>
                                            </template>
                                            <p class="division-hub-card__activity">{{ getActivityLine(division) }}</p>
                                            <ul class="division-hub-card__facts" role="list">
                                                <li
                                                    v-for="fact in getQuickFacts(division)"
                                                    :key="fact.key"
                                                    class="division-hub-card__fact"
                                                >
                                                    <span class="division-hub-card__fact-icon" aria-hidden="true">
                                                        <svg v-if="fact.icon === 'matches'" viewBox="0 0 24 24" focusable="false">
                                                            <path d="M5 4h14v2H5zM5 11h14v2H5zM5 18h14v2H5z"></path>
                                                        </svg>
                                                        <svg v-else-if="fact.icon === 'teams'" viewBox="0 0 24 24" focusable="false">
                                                            <path d="M9 11.75A3.75 3.75 0 1 1 9 4.25a3.75 3.75 0 0 1 0 7.5zm0 2.5c2.5 0 7.5 1.25 7.5 3.75V20H1.5v-2c0-2.5 5-3.75 7.5-3.75zM17 10c-1.1 0-2-.9-2-2s.9-2 2-2 2 .9 2 2-.9 2-2 2zm2.5.5c.83 0 1.5.67 1.5 1.5V14h-4v-1.5c0-.83.67-1.5 1.5-1.5z"></path>
                                                        </svg>
                                                        <svg v-else viewBox="0 0 24 24" focusable="false">
                                                            <path d="M4 5h16v2H4zm2 4h12v2H6zm-2 4h16v2H4zm2 4h12v2H6z"></path>
                                                        </svg>
                                                    </span>
                                                    <span class="division-hub-card__fact-label">{{ fact.label }}</span>
                                                    <span class="division-hub-card__fact-value">{{ fact.value }}</span>
                                                </li>
                                            </ul>
                                        </div>
                                        <footer class="division-hub-card__actions">
                                            <a
                                                v-for="action in divisionCardActions(division)"
                                                :key="action.label"
                                                class="btn-chip"
                                                :class="{ 'btn-chip--primary': action.primary }"
                                                :href="action.href"
                                                @click.stop="rememberDivision(division.id)"
                                            >
                                                {{ action.label }}
                                            </a>
                                        </footer>
                                    </article>
                                </div>
                            </div>
                        </transition>
                    </section>
                </template>
                <p v-else class="division-hub__empty">{{ emptyMessage }}</p>
            </div>
        `
    };
})();
