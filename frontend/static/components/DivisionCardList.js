(function () {
    'use strict';

    const DivisionStatus = Object.freeze({
        NOT_STARTED: 'ei-alkanut',
        REGULAR_ACTIVE: 'runkosarja-kaynnissa',
        PLAYOFFS_ACTIVE: 'playoffit-kaynnissa',
        COMPLETE: 'taputeltu-loppuun'
    });
    const DEFAULT_STATUS_LABELS = Object.freeze({
        [DivisionStatus.NOT_STARTED]: 'Ei alkanut',
        [DivisionStatus.REGULAR_ACTIVE]: 'Runkosarja käynnissä',
        [DivisionStatus.PLAYOFFS_ACTIVE]: 'Playoffit käynnissä',
        [DivisionStatus.COMPLETE]: 'Taputeltu loppuun'
    });
    const STATUS_META =
        (typeof window !== 'undefined' && window.PAPPALIIGA_DIVISION_STATUS_META) || {};
    const STATUS_LABELS = Object.freeze({
        [DivisionStatus.NOT_STARTED]:
            STATUS_META[DivisionStatus.NOT_STARTED]?.label ||
            DEFAULT_STATUS_LABELS[DivisionStatus.NOT_STARTED],
        [DivisionStatus.REGULAR_ACTIVE]:
            STATUS_META[DivisionStatus.REGULAR_ACTIVE]?.label ||
            DEFAULT_STATUS_LABELS[DivisionStatus.REGULAR_ACTIVE],
        [DivisionStatus.PLAYOFFS_ACTIVE]:
            STATUS_META[DivisionStatus.PLAYOFFS_ACTIVE]?.label ||
            DEFAULT_STATUS_LABELS[DivisionStatus.PLAYOFFS_ACTIVE],
        [DivisionStatus.COMPLETE]:
            STATUS_META[DivisionStatus.COMPLETE]?.label ||
            DEFAULT_STATUS_LABELS[DivisionStatus.COMPLETE]
    });
    const STATUS_ICONS = Object.freeze({
        [DivisionStatus.NOT_STARTED]: STATUS_META[DivisionStatus.NOT_STARTED]?.icon || null,
        [DivisionStatus.REGULAR_ACTIVE]: STATUS_META[DivisionStatus.REGULAR_ACTIVE]?.icon || null,
        [DivisionStatus.PLAYOFFS_ACTIVE]: STATUS_META[DivisionStatus.PLAYOFFS_ACTIVE]?.icon || null,
        [DivisionStatus.COMPLETE]: STATUS_META[DivisionStatus.COMPLETE]?.icon || null
    });
    const STATUS_ORDER = Array.isArray(
        typeof window !== 'undefined' && window.PAPPALIIGA_DIVISION_STATUS_ORDER
    )
        ? window.PAPPALIIGA_DIVISION_STATUS_ORDER
        : [
              DivisionStatus.NOT_STARTED,
              DivisionStatus.REGULAR_ACTIVE,
              DivisionStatus.PLAYOFFS_ACTIVE,
              DivisionStatus.COMPLETE
          ];
    const FILTER_STATES = Object.freeze(['all', ...STATUS_ORDER]);
    const FILTER_ORDER = FILTER_STATES;
    const FILTER_LABELS = Object.freeze({
        all: 'Kaikki',
        [DivisionStatus.NOT_STARTED]: STATUS_LABELS[DivisionStatus.NOT_STARTED],
        [DivisionStatus.REGULAR_ACTIVE]: STATUS_LABELS[DivisionStatus.REGULAR_ACTIVE],
        [DivisionStatus.PLAYOFFS_ACTIVE]: STATUS_LABELS[DivisionStatus.PLAYOFFS_ACTIVE],
        [DivisionStatus.COMPLETE]: STATUS_LABELS[DivisionStatus.COMPLETE]
    });
    const STORAGE_KEY = 'pappaliiga:last-division';
    const isDevEnv = typeof window !== 'undefined' && ['localhost', '127.0.0.1'].includes(window.location.hostname);
    const verboseCardLogs = Boolean(
        typeof window !== 'undefined' && window.PAPPALIIGA_DEBUG_CARD_MODEL === true
    );
    const verboseListLogs = Boolean(
        typeof window !== 'undefined' && window.PAPPALIIGA_DEBUG_DIVISION_LIST === true
    );
    const loggedCardDebugIds = new Set();

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

    function storeDivisionId(id) {
        try {
            if (typeof window !== 'undefined' && window.localStorage && id) {
                window.localStorage.setItem(STORAGE_KEY, id);
            }
        } catch (error) {
            // no-op
        }
    }

    function clampMatchCount(value) {
        const numeric = Number(value);
        if (!Number.isFinite(numeric) || numeric < 0) {
            return 0;
        }
        return Math.round(numeric);
    }

    function getProgressState(played, total) {
        const safePlayed = clampMatchCount(played);
        const safeTotal = clampMatchCount(total);
        if (safeTotal > 0 && safePlayed >= safeTotal) {
            return 'finished';
        }
        if (safePlayed > 0) {
            return 'active';
        }
        return 'not-started';
    }

    function getDivisionStatus(regularPlayed, regularTotal, playoffPlayed, playoffTotal) {
        const regularPlayedSafe = clampMatchCount(regularPlayed);
        const regularTotalSafe = clampMatchCount(regularTotal);
        const playoffPlayedSafe = clampMatchCount(playoffPlayed);
        const playoffTotalSafe = clampMatchCount(playoffTotal);
        const hasPlayoffs = playoffTotalSafe > 0;

        if (
            regularPlayedSafe === regularTotalSafe &&
            (!hasPlayoffs || playoffPlayedSafe === playoffTotalSafe)
        ) {
            return DivisionStatus.COMPLETE;
        }

        if (hasPlayoffs && playoffPlayedSafe > 0 && playoffPlayedSafe < playoffTotalSafe) {
            return DivisionStatus.PLAYOFFS_ACTIVE;
        }

        if (
            hasPlayoffs &&
            regularTotalSafe > 0 &&
            regularPlayedSafe === regularTotalSafe &&
            playoffPlayedSafe === 0
        ) {
            return DivisionStatus.PLAYOFFS_ACTIVE;
        }

        if (
            regularPlayedSafe > 0 &&
            regularTotalSafe > 0 &&
            regularPlayedSafe < regularTotalSafe &&
            playoffPlayedSafe === 0
        ) {
            return DivisionStatus.REGULAR_ACTIVE;
        }

        if (regularPlayedSafe === 0 && playoffPlayedSafe === 0) {
            return DivisionStatus.NOT_STARTED;
        }

        if (regularPlayedSafe > 0 && regularTotalSafe === 0) {
            return DivisionStatus.REGULAR_ACTIVE;
        }

        return DivisionStatus.NOT_STARTED;
    }

    function buildProgressCopy(played, total) {
        const safePlayed = clampMatchCount(played);
        const safeTotal = clampMatchCount(total);
        const hasTotal = safeTotal > 0;
        const playedValue = hasTotal ? Math.min(safePlayed, safeTotal) : safePlayed;
        const percent = hasTotal && safeTotal > 0 ? Math.round((playedValue / safeTotal) * 100) : 0;
        const remaining = hasTotal ? Math.max(safeTotal - playedValue, 0) : 0;

        const base = hasTotal
            ? `${playedValue} / ${safeTotal} ottelua Ottelut`
            : safePlayed > 0
              ? `${safePlayed} ottelua Ottelut`
              : 'Ei otteluita';

        return {
            label: hasTotal ? `${playedValue} / ${safeTotal} Ottelut` : base,
            tooltip: hasTotal ? `${base} · ${remaining} jäljellä` : base,
            percent,
            remaining
        };
    }

    function stateClass(value) {
        const normalized = value ? String(value).trim().toLowerCase() : '';
        if (!normalized) {
            return DivisionStatus.NOT_STARTED;
        }
        return normalized.replace(/[^a-z0-9]+/g, '-');
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

    function cleanDivisionName(rawName) {
        if (!rawName) return '';
        // Remove season suffix like "S11", "S12", etc.
        return String(rawName).replace(/\s+S\d+$/i, '').trim();
    }

    function slugifyFallback(value) {
        if (!value) return '';
        const base = String(value);
        const normalized = typeof base.normalize === 'function' ? base.normalize('NFD') : base;
        return normalized
            .replace(/[\u0300-\u036f]/g, '')
            .replace(/[^a-zA-Z0-9]+/g, '-')
            .replace(/^-+|-+$/g, '')
            .toLowerCase();
    }

    function resolveDivisionSlug(division, fallbackName) {
        const normalizer = typeof window !== 'undefined' ? window.divisionNormalizer : null;
        const resolved = normalizer?.getDivisionSlug ? normalizer.getDivisionSlug(division) : null;
        if (resolved) return resolved;
        const name = fallbackName || division?.name || '';
        return slugifyFallback(name) || null;
    }

    function resolveDivisionSeason(division) {
        if (!division) return null;
        const direct = division.seasonNumber ?? division.season_number;
        if (direct != null) return direct;
        const seasonValue = division.season;
        if (typeof seasonValue === 'number' || typeof seasonValue === 'string') {
            return seasonValue;
        }
        if (seasonValue && typeof seasonValue === 'object') {
            return (
                seasonValue.season ??
                seasonValue.season_number ??
                seasonValue.seasonNumber ??
                seasonValue.id ??
                null
            );
        }
        return null;
    }

    function buildDivisionHref(slug, divisionId, name, season, isPlayoffs) {
        const base = slug || (divisionId != null ? String(divisionId) : '');
        if (!base) return '/division';
        const path = isPlayoffs ? `/division/${base}/playoffs` : `/division/${base}`;
        const params = new URLSearchParams();
        if (divisionId != null) {
            params.set('championship', String(divisionId));
        }
        if (name) {
            params.set('championship_name', String(name));
        }
        if (season != null) {
            params.set('championship_season', String(season));
        }
        const query = params.toString();
        return query ? `${path}?${query}` : path;
    }

    function buildCardModel(division) {
        if (!division) {
            return null;
        }
        
        if (verboseCardLogs && division.tier === 0) {
            const divisionId = String(division.id || division.divisionId || division.division_id || '');
            if (divisionId && !loggedCardDebugIds.has(divisionId)) {
                loggedCardDebugIds.add(divisionId);
                console.log('[buildCardModel] Processing division:', {
                    id: divisionId,
                    name: division.name,
                    season: division.season,
                    status: division.status,
                    bestPlayer: division.bestPlayer || division.best_player,
                    mvpTeam: division.mvpTeam || division.mvp_team
                });
            }
        }
        
        const tierMeta = inferTierMeta(division);
        
        // Handle both camelCase and snake_case from API
        const divisionId = division.divisionId || division.division_id || division.id;
        const divisionNum = division.division_num || division.divisionNum || division.tier;
        
        // Season data
        const seasonMatchesPlayed = clampMatchCount(
            division.season?.matches_played ?? division.season?.matchesPlayed ?? 0
        );
        const seasonMatchesTotal = clampMatchCount(
            division.season?.matches_total ?? division.season?.matchesTotal ?? 0
        );
        const playoffsMatchesPlayed = clampMatchCount(
            division.playoffs?.matches_played ?? division.playoffs?.matchesPlayed ?? 0
        );
        const playoffsMatchesTotal = clampMatchCount(
            division.playoffs?.matches_total ?? division.playoffs?.matchesTotal ?? 0
        );
        let playoffTeams = Number(division.playoffs?.teams ?? 0);
        if (!Number.isFinite(playoffTeams) || playoffTeams < 0) {
            playoffTeams = 0;
        }
        const playoffsConfigured = Boolean(
            playoffsMatchesTotal > 0 ||
            playoffTeams > 0 ||
            division.playoffs?.winner ||
            division.playoffs?.winner_team
        );
        const combinedStatus = getDivisionStatus(
            seasonMatchesPlayed,
            seasonMatchesTotal,
            playoffsMatchesPlayed,
            playoffsMatchesTotal
        );
        const seasonCopy = buildProgressCopy(seasonMatchesPlayed, seasonMatchesTotal);
        const playoffCopy = playoffsConfigured
            ? buildProgressCopy(playoffsMatchesPlayed, playoffsMatchesTotal)
            : { label: 'Ei playoffeja', tooltip: 'Ei playoffeja', percent: 0, remaining: 0 };
        const hrefId = resolveDivisionSlug(division, division.name) || divisionId;
        const playoffsHrefId =
            (typeof window !== 'undefined' && window.divisionNormalizer?.getPlayoffsHrefId
                ? window.divisionNormalizer.getPlayoffsHrefId(division)
                : null) ||
            null;
        // Clean the division name - remove season suffix
        const cleanName = cleanDivisionName(division.name);
        const title = cleanName || (divisionNum ? `Division ${divisionNum}` : 'Division');
        const seasonValue = resolveDivisionSeason(division);
        
        // Extract best player and MVP team info
        const bestPlayer = division.meta?.mvp_player || division.best_player || division.bestPlayer;
        const mvpTeam = division.meta?.winner_team || division.mvp_team || division.mvpTeam;
        const winners = division.winners || [];
        
        return {
            id: divisionId,
            divisionNumber: divisionNum,
            title,
            tierMeta,
            tier: tierMeta.id,
            state: combinedStatus,
            season: {
                teams: null,
                matchesPlayed: seasonMatchesPlayed,
                matchesTotal: seasonMatchesTotal,
                percent: seasonCopy.percent,
                progressState: getProgressState(seasonMatchesPlayed, seasonMatchesTotal),
                isFinished: seasonMatchesTotal > 0 && seasonMatchesPlayed >= seasonMatchesTotal,
                progressLabel: seasonCopy.label,
                progressTooltip: seasonCopy.tooltip,
                winner: division.season?.winner || division.meta?.winner_team || null
            },
            playoffs: {
                teams: playoffTeams,
                matchesPlayed: playoffsMatchesPlayed,
                matchesTotal: playoffsMatchesTotal,
                percent: playoffCopy.percent,
                hasChampionship: playoffsConfigured,
                progressState: getProgressState(playoffsMatchesPlayed, playoffsMatchesTotal),
                isFinished:
                    playoffsConfigured && playoffsMatchesTotal > 0
                        ? playoffsMatchesPlayed >= playoffsMatchesTotal
                        : false,
                progressLabel: playoffCopy.label,
                progressTooltip: playoffCopy.tooltip,
                winner: division.playoffs?.winner_team || division.playoffs?.winner || null,
                href: playoffsConfigured && playoffsHrefId
                    ? buildDivisionHref(playoffsHrefId, playoffsHrefId, title, seasonValue, true)
                    : ''
            },
            bestPlayer: bestPlayer ? {
                name: bestPlayer.name || bestPlayer.nickname,
                rating: Number(bestPlayer.rating || 0).toFixed(2)
            } : null,
            mvpTeam: mvpTeam,
            winners: winners,
            slug: hrefId,
            href: buildDivisionHref(hrefId, divisionId, title, seasonValue, false),
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
            showSeasonPicker: { type: Boolean, default: true }
        },
        emits: ['change-season', 'change-filter', 'change-search', 'reset-filters'],
        data() {
            return {
                filters: FILTER_ORDER
            };
        },
        computed: {
            statusMeta() {
                return STATUS_META;
            }
        },
        template: `
            <div class="division-season-bar" role="region" aria-label="Season controls">
                <div
                    v-if="showSeasonPicker"
                    class="division-season-bar__section"
                >
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
                    <span class="division-season-bar__label">Tila</span>
                    <div class="division-season-bar__filters">
                        <button
                            v-for="state in filters"
                            :key="state"
                            type="button"
                            class="season-filter-chip"
                            :class="{ 'season-filter-chip--active': filterState === state }"
                            :data-status="state !== 'all' ? state : null"
                            :aria-pressed="filterState === state"
                            @click="$emit('change-filter', state)"
                        >
                            <span
                                v-if="state !== 'all' && statusMeta[state]?.icon"
                                class="season-filter-chip__icon"
                                aria-hidden="true"
                            >
                                <svg
                                    :viewBox="statusMeta[state].icon.viewBox"
                                    role="presentation"
                                    focusable="false"
                                >
                                    <path
                                        v-for="(path, idx) in statusMeta[state].icon.paths"
                                        :key="idx"
                                        :d="path.d"
                                        fill="currentColor"
                                    ></path>
                                </svg>
                            </span>
                            <span class="season-filter-chip__label">{{ FILTER_LABELS[state] || state }}</span>
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
            state: { type: String, default: 'not-started' },
            label: { type: String, default: '' },
            ariaLabel: { type: String, default: '' },
            animationDelay: { type: Number, default: 0 },
            tooltip: { type: String, default: '' },
            showPercent: { type: Boolean, default: false }
        },
        computed: {
            percent() {
                const safeMax = Number.isFinite(this.max) ? this.max : 0;
                const safeValue = Number.isFinite(this.value) ? this.value : 0;
                if (safeMax <= 0) return 0;
                return Math.min(100, Math.max(0, Math.round((safeValue / safeMax) * 100)));
            },
            stateClass() {
                return `division-progress--${stateClass(this.state)}`;
            },
            fillStyle() {
                return {
                    width: this.percent + '%',
                    '--shimmer-delay': `${this.animationDelay}s`
                };
            },
            tooltipText() {
                return this.tooltip || this.label;
            },
            displayLabel() {
                if (!this.showPercent) return this.label;
                if (!this.label) return `${this.percent}%`;
                return `${this.percent}% · ${this.label}`;
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
                :title="tooltipText"
                :data-tooltip="tooltipText"
            >
                <div class="division-progress__track">
                    <div class="division-progress__fill" :style="fillStyle"></div>
                    <span class="division-progress__label">{{ displayLabel }}</span>
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
        computed: {
            playoffsPendingStart() {
                return Boolean(
                    this.division.playoffs?.hasChampionship &&
                    this.division.season?.isFinished &&
                    this.division.playoffs.matchesPlayed === 0 &&
                    !this.division.playoffs.isFinished
                );
            },
            displayStatusState() {
                return this.playoffsPendingStart ? DivisionStatus.NOT_STARTED : this.division.state;
            },
            statusLabel() {
                return STATUS_LABELS[this.displayStatusState] || STATUS_LABELS[DivisionStatus.NOT_STARTED];
            },
            statusIcon() {
                return STATUS_ICONS[this.displayStatusState] || null;
            },
            playoffsHasStarted() {
                if (!this.division.playoffs?.hasChampionship) {
                    return false;
                }
                return Boolean(
                    this.division.playoffs.matchesPlayed > 0 ||
                    this.division.playoffs.isFinished ||
                    this.division.playoffs.winner
                );
            },
            isDivisionComplete() {
                if (this.division.state === DivisionStatus.COMPLETE) {
                    return true;
                }
                if (this.division.playoffs?.hasChampionship) {
                    return Boolean(
                        this.division.playoffs.winner ||
                        (this.division.playoffs.matchesTotal > 0 &&
                            this.division.playoffs.matchesPlayed >= this.division.playoffs.matchesTotal)
                    );
                }
                return Boolean(
                    this.division.season.winner ||
                    (this.division.season.matchesTotal > 0 &&
                        this.division.season.matchesPlayed >= this.division.season.matchesTotal)
                );
            },
            progressStage() {
                if (this.isDivisionComplete) {
                    return 'complete';
                }
                if (this.playoffsHasStarted) {
                    return 'playoffs';
                }
                return 'regular';
            },
            progressModel() {
                if (this.progressStage === 'complete') {
                    return {
                        value: 100,
                        max: 100,
                        state: 'finished',
                        label: '100%',
                        tooltip: 'Divisioona valmis',
                        showPercent: false
                    };
                }
                if (this.progressStage === 'playoffs') {
                    return {
                        value: this.division.playoffs.matchesPlayed,
                        max: this.division.playoffs.matchesTotal || this.division.playoffs.matchesPlayed || 0,
                        state: this.division.playoffs.progressState,
                        label: this.division.playoffs.progressLabel,
                        tooltip: this.division.playoffs.progressTooltip,
                        showPercent: true
                    };
                }
                return {
                    value: this.division.season.matchesPlayed,
                    max: this.division.season.matchesTotal || this.division.season.matchesPlayed || 0,
                    state: this.division.season.progressState,
                    label: this.division.season.progressLabel,
                    tooltip: this.division.season.progressTooltip,
                    showPercent: true
                };
            },
            showPlayoffsCTA() {
                if (!this.division.playoffs?.hasChampionship) {
                    return false;
                }
                if (!this.division.playoffs.href) {
                    return false;
                }
                return this.playoffsHasStarted;
            },
            seasonRows() {
                const rows = [];
                const hasBestPlayer = Boolean(this.division.bestPlayer);
                const hasMvpTeam = Boolean(this.division.mvpTeam);
                if (hasBestPlayer) {
                    rows.push({ key: 'bestPlayer', label: 'Paras pelaaja', value: `${this.division.bestPlayer.name} (${this.division.bestPlayer.rating})` });
                }
                if (hasMvpTeam) {
                    rows.push({ key: 'mvpTeam', label: 'MVP-joukkue', value: this.division.mvpTeam });
                }
                return rows;
            },
            showWinnerStrip() {
                const seasonWinner = this.division.season.winner;
                const playoffsWinner = this.division.playoffs.winner;
                if (!this.division.season.isFinished || !seasonWinner) {
                    return false;
                }
                if (!this.division.playoffs.hasChampionship) {
                    return true;
                }
                if (!this.division.playoffs.isFinished) {
                    return true;
                }
                return playoffsWinner === seasonWinner;
            }
        },
        methods: {
            handleCTA() {
                storeDivisionId(this.division.id);
                this.$emit('remember', this.division.id);
            },
            handlePlayoffsCTA() {
                storeDivisionId(this.division.id);
                this.$emit('remember', this.division.id);
            },
            stateClass
        },
        template: `
            <article class="division-card" role="listitem" :class="'division-card--' + stateClass(displayStatusState)">
                <header class="division-card__header division-card__header--centered">
                    <div class="division-card__title-row division-card__title-row--centered">
                        <h3
                            class="division-card__title division-card__title--hero title-accent titleUnderlineCard"
                            :title="division.title"
                        >
                            <span class="division-card__title-text">{{ division.title }}</span>
                        </h3>
                    </div>
                    <p v-if="showWinnerStrip" class="division-card__winner-banner division-card__winner-banner--centered">
                        Voittaja: {{ division.season.winner }}
                    </p>
                </header>
                <div class="division-card__body">
                    <section class="division-card__block division-card__block--regular">
                        <div class="division-card__stat-lines">
                            <p class="division-card__stat-line">Joukkueet: {{ division.season.teams != null ? division.season.teams : '–' }}</p>
                        </div>
                        <division-progress-bar
                            :value="progressModel.value"
                            :max="progressModel.max"
                            :state="progressModel.state"
                            :label="progressModel.label"
                            :tooltip="progressModel.tooltip"
                            :show-percent="progressModel.showPercent"
                            :aria-label="division.title + ' eteneminen'"
                            :animation-delay="(division.divisionNumber * 0.15) % 2"
                        ></division-progress-bar>
                        <div class="division-card__status-row division-card__status-row--centered">
                            <span
                                class="division-card__badge"
                                :class="'division-card__badge--' + stateClass(displayStatusState)"
                                :data-status="displayStatusState"
                            >
                                <span v-if="statusIcon" class="status-pill__icon" aria-hidden="true">
                                    <svg
                                        :viewBox="statusIcon.viewBox"
                                        role="presentation"
                                        focusable="false"
                                    >
                                        <path
                                            v-for="(path, idx) in statusIcon.paths"
                                            :key="idx"
                                            :d="path.d"
                                            fill="currentColor"
                                        ></path>
                                    </svg>
                                </span>
                                <span class="status-pill__label">{{ statusLabel }}</span>
                            </span>
                        </div>
                        <p v-if="division.playoffs.isFinished && division.playoffs.winner" class="division-card__note division-card__note--centered">
                            Playoff-voittaja: {{ division.playoffs.winner }}
                        </p>
                        <ul v-if="seasonRows.length" class="division-card__facts division-card__facts--centered" role="list">
                            <li v-for="row in seasonRows" :key="row.key">
                                <span class="division-card__fact-label">{{ row.label }}</span>
                                <span class="division-card__fact-value">{{ row.value }}</span>
                            </li>
                        </ul>
                    </section>
                    <div class="division-card__action-row division-card__action-row--split">
                        <a class="division-card__action division-card__action--primary" :href="division.href" @click="handleCTA">
                            Avaa divisioona
                        </a>
                        <a
                            v-if="showPlayoffsCTA"
                            class="division-card__action division-card__action--ghost"
                            :href="division.playoffs.href"
                            @click="handlePlayoffsCTA"
                        >
                            Näytä playoffit
                        </a>
                    </div>
                </div>
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
            isLoading: { type: Boolean, default: false },
            showSeasonPicker: { type: Boolean, default: true },
            showControls: { type: Boolean, default: true }
        },
        emits: ['change-season', 'change-filter', 'change-search', 'reset-filters'],
        data() {
            return {
                preferredDivisionId: getStoredDivisionId(),
                renderCount: 0,
                renderBatchSize: 8,
                sentinelObserver: null,
                teamCountOverrides: {},
                teamCountRequests: {}
            };
        },
        computed: {
            cardModels() {
                if (!Array.isArray(this.divisions)) {
                    return [];
                }
                const overrides = this.teamCountOverrides || {};
                const mapped = this.divisions.map(buildCardModel).filter(Boolean);
                mapped.forEach(card => {
                    const override = overrides[card.id];
                    if (Number.isFinite(override)) {
                        card.season.teams = override;
                    }
                });
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
                                winner: rawSeason.winner || entry.winner || null
                            };
                            const fallbackPlayoffs = {
                                teams: Number(rawPlayoffs.teams ?? 0) || 0,
                                matches_played: Number(rawPlayoffs.matches_played ?? rawPlayoffs.matchesPlayed ?? 0),
                                matches_total: Number(rawPlayoffs.matches_total ?? rawPlayoffs.matchesTotal ?? 0) || 0,
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
                                seasonNumber: entry.seasonNumber ?? entry.season_number ?? null,
                                raw: entry
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
                    .filter(card => filterState === 'all' || card.state === filterState)
                    .filter(card => {
                        if (!search) return true;
                        return card.searchIndex.includes(search);
                    })
                    .sort((a, b) => {
                        // Sort by division number (division_num from API)
                        const aNum = Number(a.divisionNumber) || 0;
                        const bNum = Number(b.divisionNumber) || 0;
                        return aNum - bNum;
                    });
                if (verboseListLogs) {
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
                    this.teamCountOverrides = {};
                    this.teamCountRequests = {};
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
                if (verboseListLogs) {
                    console.info('[DivisionCardList] cardModels updated', {
                        count: Array.isArray(newValue) ? newValue.length : 0
                    });
                }
            },
            filteredCards(newValue) {
                if (verboseListLogs) {
                    const totalVisible = Array.isArray(newValue) ? newValue.length : 0;
                    console.info('[DivisionCardList] filteredCards updated', {
                        visibleDivisions: totalVisible
                    });
                }
                this.$nextTick(() => this.observeSentinel());
            },
            visibleCards: {
                immediate: true,
                handler(newValue) {
                    this.ensureTeamCounts(newValue);
                }
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
            ensureTeamCounts(cards) {
                if (typeof window === 'undefined' || !window.apiClient) {
                    return;
                }
                if (!Array.isArray(cards) || !cards.length) {
                    return;
                }
                cards.forEach(card => {
                    const divisionId = card?.id;
                    if (!divisionId) return;
                    if (this.teamCountOverrides[divisionId] !== undefined) return;
                    if (this.teamCountRequests[divisionId]) return;
                    this.teamCountRequests[divisionId] = true;
                    window.apiClient
                        .getDivisionTeamCount(divisionId)
                        .then(count => {
                            if (!Number.isFinite(count)) return;
                            this.teamCountOverrides[divisionId] = count;
                        })
                        .catch(error => {
                            if (isDevEnv) {
                                console.warn('[DivisionCardList] team count fetch failed', { divisionId, error });
                            }
                        })
                        .finally(() => {
                            delete this.teamCountRequests[divisionId];
                        });
                });
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
                    v-if="showControls"
                    :season-options="seasonOptions"
                    :selected-season="selectedSeason"
                    :season-loading="seasonLoading"
                    :filter-state="filterState"
                    :search-query="searchQuery"
                    :show-season-picker="showSeasonPicker"
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
