/**
 * DivisionCardList Component - Simplified Rewrite
 * 
 * Displays division cards with playoff divisions nested inside parent cards.
 * Uses parentChampionshipId from API for reliable parent-child relationships.
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
        upcoming: 'Avaa divisioona',
        running: 'Avaa divisioona',
        ended: 'Avaa divisioona'
    });

    const PLAYOFF_STATUS_LABELS = Object.freeze({
        upcoming: 'Ei vielä alkanut',
        running: 'Käynnissä',
        ended: 'Taputeltu loppuun'
    });

    // ============================================================================
    // UTILITY FUNCTIONS
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
        return isNaN(date.getTime()) ? null : date;
    }

    // ============================================================================
    // FORMATTING FUNCTIONS
    // ============================================================================

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
        if (!date) return '–';
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

        if (start && end) return `${start} – ${end}`;
        if (start) return `Alkaen ${start}`;
        if (end) return `Päättyy ${end}`;
        return 'Ajankohta vahvistuu';
    }

    function formatTeamsLabel(count) {
        const num = safeNumber(count);
        if (num === 0) return 'Ei joukkueita';
        return `${num} ${num === 1 ? 'joukkue' : 'joukkuetta'}`;
    }

    function formatMatchesLabel(played, total) {
        const p = safeNumber(played);
        const t = safeNumber(total);
        
        if (t === 0) return p > 0 ? `${p} ottelua` : 'Ei otteluita';
        return `${p} / ${t} pelattu`;
    }

    function formatProgressLabel(played, total) {
        const p = safeNumber(played);
        const t = safeNumber(total);
        
        if (t === 0) return '';
        return `${p} / ${t} ottelua`;
    }

    function normalizeStatusValue(status) {
        if (!status && status !== 0) return 'upcoming';
        const value = String(status).toLowerCase();
        if (value === 'live') return 'running';
        if (value === 'completed' || value === 'finished') return 'ended';
        if (value === 'running' || value === 'upcoming' || value === 'ended') return value;
        return 'upcoming';
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
        const season = safeNumber(
            coalesce(
                raw.season,
                apiDiv.season
            ),
            null
        );
        const divisionNum = safeNumber(
            coalesce(
                raw.divisionNum,
                raw.division_num,
                apiDiv.divisionNum,
                apiDiv.division_num
            ),
            null
        );
        const phase = safeString(apiDiv.phase);

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

        // Teams
        const teamsCount = safeNumber(apiDiv.teamsCount || apiDiv.teams_count);

        // Matches
        const totalMatches = safeNumber(coalesce(apiDiv.totalMatches, apiDiv.total_matches));
        const playedMatches = safeNumber(coalesce(apiDiv.playedMatches, apiDiv.played_matches));
        const finishedMatches = safeNumber(coalesce(apiDiv.finishedMatches, apiDiv.finished_matches));
        const liveMatches = safeNumber(coalesce(apiDiv.liveMatches, apiDiv.live_matches));
        const upcomingMatches = safeNumber(coalesce(apiDiv.upcomingMatches, apiDiv.upcoming_matches));

        // Dates
        const startDate = apiDiv.start || apiDiv.start_date || apiDiv.startDate;
        const endDate = apiDiv.end || apiDiv.end_date || apiDiv.endDate;
        const updatedDate = apiDiv.updated || apiDiv.updated_at || apiDiv.updatedAt;

        // Status & Progress
        const status = normalizeStatusValue(apiDiv.status);
        const progressPercent = safeNumber(coalesce(apiDiv.progressPercent, apiDiv.progress_percent));
        const kind = safeString(apiDiv.kind, isPlayoff ? 'playoffs' : 'regular');

        // Winner
        const winnerTeamId = safeString(apiDiv.winnerTeamId || apiDiv.winner_team_id, null);
        const winnerTeamName = safeString(apiDiv.winnerTeamName || apiDiv.winner_team_name || apiDiv.winner, null);

        // Build href
        const href = `/division/${slug || id}`;

        const displayName = stripSeasonFromName(name, season);

        return {
            id,
            name,
            displayName,
            slug,
            season,
            divisionNum,
            isPlayoff,
            parentId,
            teams: {
                count: teamsCount,
                label: formatTeamsLabel(teamsCount)
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
                percent: status === 'ended' ? 100 : progressPercent,
                label: formatProgressLabel(playedMatches, totalMatches)
            },
            status,
            statusLabel: STATUS_LABELS[status] || STATUS_LABELS.upcoming,
            actionLabel: STATUS_ACTIONS[status] || STATUS_ACTIONS.upcoming,
            kind,
            winner: winnerTeamId ? {
                id: winnerTeamId,
                name: winnerTeamName
            } : null,
            href
        };
    }

    function groupDivisions(normalizedDivisions) {
        if (!Array.isArray(normalizedDivisions)) {
            return { divisionGroups: [], orphanedPlayoffs: [] };
        }

        const regularDivisions = [];
        const playoffDivisions = [];
        normalizedDivisions.forEach(division => {
            if (division.isPlayoff) {
                playoffDivisions.push(division);
            } else {
                regularDivisions.push(division);
            }
        });

        const divisionGroups = regularDivisions.map(regular => ({
            main: regular,
            playoff: null
        }));

        const groupById = new Map();
        divisionGroups.forEach(group => {
            groupById.set(group.main.id, group);
        });

        const orphanedPlayoffs = [];

        playoffDivisions.forEach(playoff => {
            let parentGroup = playoff.parentId ? groupById.get(playoff.parentId) : null;

            if (!parentGroup && playoff.divisionNum && playoff.season) {
                parentGroup = divisionGroups.find(group =>
                    group.main.divisionNum === playoff.divisionNum &&
                    group.main.season === playoff.season
                );
            }

            if (parentGroup && !parentGroup.playoff) {
                parentGroup.playoff = playoff;
            } else {
                orphanedPlayoffs.push(playoff);
            }
        });

        return { divisionGroups, orphanedPlayoffs };
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
            }
        },
        data() {
            return {};
        },
        computed: {
            normalizedDivisions() {
                if (!Array.isArray(this.divisions)) return [];
                return this.divisions.map(normalizeApiDivision).filter(Boolean);
            },
            groupedDivisions() {
                const { divisionGroups, orphanedPlayoffs } = groupDivisions(this.normalizedDivisions);
                return [...divisionGroups, ...orphanedPlayoffs.map(p => ({ main: p, playoff: null, isOrphaned: true }))];
            },
            hasDivisions() {
                return this.groupedDivisions.length > 0;
            }
        },
        methods: {
            getStatItems(division) {
                if (!division) return [];
                const fallback = 'Tietoja päivitetään';
                const teams = division.teams || {};
                const matches = division.matches || {};
                const dates = division.dates || {};

                return [
                    {
                        key: 'teams',
                        icon: 'teams',
                        label: 'Joukkueet',
                        value: teams.label || fallback,
                        title: `${teams.count || 0} joukkuetta`,
                        muted: false
                    },
                    {
                        key: 'matches',
                        icon: 'matches',
                        label: 'Ottelut',
                        value: matches.label || fallback,
                        title: matches.label || fallback,
                        muted: false
                    },
                    {
                        key: 'dates',
                        icon: 'dates',
                        label: 'Ajanjakso',
                        value: dates.label || fallback,
                        title: dates.label || fallback,
                        muted: false
                    },
                    {
                        key: 'updated',
                        icon: 'updated',
                        label: 'Päivitetty',
                        value: dates.updatedLabel || fallback,
                        title: dates.updatedLabel || fallback,
                        muted: true
                    }
                ];
            },
            getProgressPercent(progress) {
                return Math.min(100, Math.max(0, progress.percent || 0));
            },
            describeProgress(division) {
                if (!division) {
                    return {
                        state: 'upcoming',
                        percent: 0,
                        percentLabel: '0%',
                        caption: 'Edistyminen',
                        tooltip: '',
                        ariaLabel: 'Edistyminen'
                    };
                }
                const state = normalizeStatusValue(division.status);
                const progressData = division.progress || {};
                const matches = division.matches || {};
                const dates = division.dates || {};
                const percent = this.getProgressPercent(progressData);
                const caption = progressData.label || formatMatchesLabel(
                    matches.played,
                    matches.total
                ) || 'Ei otteluita';
                const tooltipParts = [];
                if (caption) tooltipParts.push(caption);
                if (dates.label) tooltipParts.push(dates.label);
                return {
                    state,
                    percent,
                    percentLabel: `${Math.round(percent)}%`,
                    caption,
                    tooltip: tooltipParts.join(' • '),
                    ariaLabel: caption
                };
            },
            getPlayoffStatusLabel(status) {
                return PLAYOFF_STATUS_LABELS[status] || STATUS_LABELS[status] || 'Unknown';
            },
            describePlayoff(playoff) {
                if (!playoff) {
                    return null;
                }
                const state = normalizeStatusValue(playoff.status);
                const matches = playoff.matches || {};
                const dates = playoff.dates || {};
                const progressData = playoff.progress || {};
                const percent = this.getProgressPercent(progressData);
                const matchesCaption = matches.label || formatMatchesLabel(
                    matches.played,
                    matches.total
                ) || 'Ei otteluita';
                const progressCaption = progressData.label || matchesCaption;
                const tooltipParts = [];
                if (progressCaption) tooltipParts.push(progressCaption);
                if (dates.label) tooltipParts.push(dates.label);
                let winnerName = null;
                if (playoff.winner && playoff.winner.name) {
                    winnerName = playoff.winner.name;
                }
                const playoffAria = progressCaption || 'Playoffien eteneminen';
                const displayTitle = playoff.displayName || playoff.name || 'Pudotuspelit';
                return {
                    id: playoff.id,
                    name: playoff.name,
                    title: displayTitle,
                    state,
                    statusLabel: this.getPlayoffStatusLabel(state),
                    matchesCaption,
                    hasDates: Boolean(dates.label),
                    dateLabel: dates.label,
                    progressPercent: percent,
                    progressPercentLabel: `${Math.round(percent)}%`,
                    progressCaption,
                    progressAriaLabel: playoffAria,
                    progressTooltip: tooltipParts.join(' • '),
                    winner: winnerName,
                    href: playoff.href
                };
            }
        },
        template: `
            <div class="division-card-list-wrapper">
                <div
                    v-if="hasDivisions"
                    class="division-card-list"
                    role="list"
                    :aria-label="'Divisioonat ' + seasonLabel"
                >
                    <article
                        v-for="group in groupedDivisions"
                        :key="group.main.id"
                        class="division-card"
                        :class="[
                            'division-card--status-' + group.main.status,
                            'division-card--kind-' + group.main.kind,
                            { 'division-card--muted': group.main.status === 'upcoming' },
                            { 'division-card--orphaned': group.isOrphaned },
                            { 'division-card--has-playoff': !!group.playoff }
                        ]"
                        role="listitem"
                        :aria-labelledby="'division-title-' + group.main.id"
                    >
                        <div class="division-card__inner">
                            <div v-if="group.isOrphaned" class="division-card__warning">
                                ⚠️ Pudotuspelien päädivisioona puuttuu tai sitä ei löytynyt
                            </div>

                            <header class="division-card__top">
                                <span
                                    class="division-card__badge"
                                    :class="'badge-' + (group.main.kind || 'regular')"
                                    aria-hidden="true"
                                >
                                    <svg
                                        v-if="group.main.kind === 'masters'"
                                        viewBox="0 0 24 24"
                                        aria-hidden="true"
                                        focusable="false"
                                    >
                                        <path d="M4 4l3 6 5-2 5 2 3-6h2l-1.5 14h-15L2 4h2z"></path>
                                        <path d="M7 20h10l-5 3-5-3z"></path>
                                    </svg>
                                    <svg
                                        v-else-if="group.main.kind === 'playoffs'"
                                        viewBox="0 0 24 24"
                                        aria-hidden="true"
                                        focusable="false"
                                    >
                                        <path d="M5 3h4l1 4h4l1-4h4l-2 12h-10L5 3z"></path>
                                        <path d="M9 17h6l-3 4-3-4z"></path>
                                    </svg>
                                    <svg v-else viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                                        <path d="M12 2l8 4v6c0 4.97-3.31 9.58-8 11-4.69-1.42-8-6.03-8-11V6l8-4z"></path>
                                    </svg>
                                </span>
                                <div class="division-card__titles">
                                    <h3 class="division-card__name" :id="'division-title-' + group.main.id">
                                        {{ group.main.displayName || group.main.name }}
                                    </h3>
                                    <span class="division-card__chip" :class="'chip-' + group.main.status">
                                        <span v-if="group.main.status === 'running'" class="chip__dot" aria-hidden="true"></span>
                                        {{ group.main.statusLabel }}
                                    </span>
                                </div>
                            </header>

                            <ul class="division-card__stats" role="list">
                                <li
                                    v-for="stat in getStatItems(group.main)"
                                    :key="stat.key"
                                    class="division-card__stat"
                                    role="listitem"
                                    :class="{ 'division-card__stat--muted': stat.muted }"
                                    :title="stat.title"
                                >
                                    <span class="division-card__icon" aria-hidden="true">
                                        <svg
                                            v-if="stat.icon === 'teams'"
                                            viewBox="0 0 24 24"
                                            aria-hidden="true"
                                            focusable="false"
                                        >
                                            <path d="M9 12c-2.21 0-4-1.79-4-4s1.79-4 4-4 4 1.79 4 4-1.79 4-4 4zm6 0c-1.86 0-3.41-1.28-3.86-3h2.08c.36 1.19 1.47 2 2.78 2 1.65 0 3-1.35 3-3s-1.35-3-3-3c-1.31 0-2.42.81-2.78 2h-2.08C11.59 3.28 13.14 2 15 2c2.76 0 5 2.24 5 5s-2.24 5-5 5zm-6 2c2.67 0 8 1.34 8 4v2H1v-2c0-2.66 5.33-4 8-4zm0 2c-2.33 0-6 1.17-6 2v.01h12V18c0-.83-3.67-2-6-2zm6.92-2.74C18.46 15.37 20 17.28 20 19v1h-4v-1c0-1.54-.85-3.03-2.23-4.24.37-.05.74-.08 1.15-.08.36 0 .71.03 1.05.08z"></path>
                                        </svg>
                                        <svg
                                            v-else-if="stat.icon === 'matches'"
                                            viewBox="0 0 24 24"
                                            aria-hidden="true"
                                            focusable="false"
                                        >
                                            <path d="M4 5h16v4H4z"></path>
                                            <path d="M6 5h2v14H6zm10 0h2v14h-2z"></path>
                                            <path d="M9 5h6v6H9z"></path>
                                        </svg>
                                        <svg
                                            v-else-if="stat.icon === 'dates'"
                                            viewBox="0 0 24 24"
                                            aria-hidden="true"
                                            focusable="false"
                                        >
                                            <path d="M7 2v2H5a2 2 0 0 0-2 2v2h18V6a2 2 0 0 0-2-2h-2V2h-2v2H9V2H7zm13 8H4v10a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V10zm-9 3h2v4h-2v-4z"></path>
                                        </svg>
                                        <svg
                                            v-else
                                            viewBox="0 0 24 24"
                                            aria-hidden="true"
                                            focusable="false"
                                        >
                                            <path d="M12 1a11 11 0 1 0 11 11A11 11 0 0 0 12 1zm0 2a9 9 0 1 1-9 9 9 9 0 0 1 9-9zm-.5 3h1.5v5.25l3.5 2.1-.75 1.23L11.5 13V6z"></path>
                                        </svg>
                                    </span>
                                    <div class="division-card__stat-text">
                                        <span class="division-card__stat-label">{{ stat.label }}</span>
                                        <span class="division-card__stat-value">{{ stat.value }}</span>
                                    </div>
                                </li>
                            </ul>

                            <div v-if="group.main.winner" class="division-card__winner">
                                <span class="division-card__winner-label">Voittaja</span>
                                <span class="division-card__winner-value">{{ group.main.winner.name }}</span>
                            </div>

                            <template v-for="progress in [describeProgress(group.main)]" :key="group.main.id + '-progress'">
                                <div
                                    v-if="progress.state === 'running'"
                                    class="division-card__progress division-card__progress--active"
                                    role="group"
                                    :aria-label="progress.ariaLabel"
                                    :title="progress.tooltip"
                                >
                                    <div class="division-card__progress-meta">
                                        <span class="division-card__progress-percent">{{ progress.percentLabel }}</span>
                                        <span class="division-card__progress-caption">{{ progress.caption }}</span>
                                    </div>
                                    <div class="division-card__progress-track">
                                        <span
                                            class="division-card__progress-fill"
                                            :style="{ width: progress.percent + '%' }"
                                        ></span>
                                    </div>
                                </div>
                                <div
                                    v-else-if="progress.state === 'upcoming'"
                                    class="division-card__progress division-card__progress--upcoming"
                                    role="status"
                                    aria-live="polite"
                                >
                                    <span class="division-card__progress-empty">Ei vielä alkanut</span>
                                </div>
                                <div
                                    v-else
                                    class="division-card__progress division-card__progress--ended"
                                    role="status"
                                    aria-live="polite"
                                >
                                    <span class="division-card__progress-check" aria-hidden="true">
                                        <svg viewBox="0 0 16 16" focusable="false">
                                            <path d="M6.6 11.2 3.4 8l1.2-1.2 2 2L11.4 4 12.6 5.2l-6 6z"></path>
                                        </svg>
                                    </span>
                                    <span class="division-card__progress-done">Taputeltu loppuun</span>
                                </div>
                            </template>

                            <template v-if="group.playoff">
                                <div
                                    v-for="playoff in [describePlayoff(group.playoff)]"
                                    :key="group.playoff.id + '-playoff'"
                                    class="division-card__playoff"
                                    :class="'division-card__playoff--' + playoff.state"
                                >
                                    <header class="division-card__playoff-header">
                                        <span class="division-card__playoff-title">{{ playoff.title }}</span>
                                        <span class="division-card__playoff-chip" :class="'playoff-chip--' + playoff.state">
                                            {{ playoff.statusLabel }}
                                        </span>
                                    </header>
                                    <p class="division-card__playoff-line">{{ playoff.matchesCaption }}</p>
                                    <p v-if="playoff.hasDates" class="division-card__playoff-dates">
                                        {{ playoff.dateLabel }}
                                    </p>
                                    <div
                                        v-if="playoff.state === 'running'"
                                        class="division-card__playoff-progress"
                                        role="group"
                                        :aria-label="playoff.progressAriaLabel"
                                        :title="playoff.progressTooltip"
                                    >
                                        <div class="division-card__playoff-progress-meta">
                                            <span class="division-card__playoff-progress-percent">{{ playoff.progressPercentLabel }}</span>
                                            <span class="division-card__playoff-progress-caption">{{ playoff.progressCaption }}</span>
                                        </div>
                                        <div class="division-card__playoff-progress-track">
                                            <span
                                                class="division-card__playoff-progress-fill"
                                                :style="{ width: playoff.progressPercent + '%' }"
                                            ></span>
                                        </div>
                                    </div>
                                    <div
                                        v-else-if="playoff.state === 'upcoming'"
                                        class="division-card__playoff-placeholder"
                                        role="status"
                                        aria-live="polite"
                                    >
                                        Ei vielä alkanut
                                    </div>
                                    <div v-else class="division-card__playoff-result">
                                        <p class="division-card__playoff-winner" v-if="playoff.winner">
                                            Voittaja: {{ playoff.winner }}
                                        </p>
                                        <p v-else>Taputeltu loppuun</p>
                                    </div>
                                    <div class="division-card__playoff-actions">
                                        <a
                                            v-if="playoff.href"
                                            :href="playoff.href"
                                            class="division-card__playoff-button"
                                            :aria-label="'Avaa playoff-sarja: ' + playoff.title"
                                        >
                                            Avaa playoff-sarja
                                        </a>
                                        <span
                                            v-else
                                            class="division-card__playoff-button division-card__playoff-button--disabled"
                                            aria-disabled="true"
                                        >
                                            Avaa playoff-sarja
                                        </span>
                                    </div>
                                </div>
                            </template>
                            <div v-else class="division-card__playoff division-card__playoff--placeholder" aria-hidden="true"></div>

                            <footer class="division-card__footer">
                                <a
                                    :href="group.main.href"
                                    class="division-card__cta"
                                    :aria-label="group.main.actionLabel + ': ' + (group.main.displayName || group.main.name)"
                                >
                                    {{ group.main.actionLabel }}
                                </a>
                            </footer>
                        </div>
                    </article>
                </div>
                <p v-else class="division-card-list__empty">
                    {{ emptyMessage }}
                </p>
            </div>
        `
    };
})();
