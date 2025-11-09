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

    // ============================================================================
    // DATA NORMALIZATION
    // ============================================================================

    function normalizeApiDivision(apiDiv) {
        if (!apiDiv) return null;

        // The API returns 'key' as the unique identifier (derived from championship_id)
        const id = safeString(apiDiv.key);
        if (!id) {
            console.warn('[DivisionCardList] Division missing key:', apiDiv);
            return null;
        }

        const name = safeString(apiDiv.name);
        const slug = safeString(apiDiv.slug);
        
        // season and divisionNum are not normalized by the store, get from raw
        const raw = apiDiv.raw || apiDiv;
        const season = safeNumber(
            raw.season || 
            apiDiv.season, 
            null
        );
        const divisionNum = safeNumber(
            raw.divisionNum || 
            raw.division_num || 
            apiDiv.divisionNum || 
            apiDiv.division_num, 
            null
        );
        const phase = safeString(apiDiv.phase);
        
        // Check if this is a playoff division by looking at the name
        // Playoff divisions have "Playoff" or "Playoffs" in their name
        const isPlayoff = name.toLowerCase().includes('playoff');
        
        // Parent championship ID (kept for potential future use)
        const parentId = safeString(
            raw.parentChampionshipId || 
            raw.parent_championship_id || 
            apiDiv.parentChampionshipId || 
            apiDiv.parent_championship_id, 
            null
        );

        // Teams
        const teamsCount = safeNumber(apiDiv.teamsCount || apiDiv.teams_count);

        // Matches
        const totalMatches = safeNumber(apiDiv.totalMatches || apiDiv.total_matches);
        const playedMatches = safeNumber(apiDiv.playedMatches || apiDiv.played_matches);
        const finishedMatches = safeNumber(apiDiv.finishedMatches || apiDiv.finished_matches);
        const liveMatches = safeNumber(apiDiv.liveMatches || apiDiv.live_matches);
        const upcomingMatches = safeNumber(apiDiv.upcomingMatches || apiDiv.upcoming_matches);

        // Dates
        const startDate = apiDiv.start || apiDiv.start_date || apiDiv.startDate;
        const endDate = apiDiv.end || apiDiv.end_date || apiDiv.endDate;
        const updatedDate = apiDiv.updated || apiDiv.updated_at || apiDiv.updatedAt;

        // Status & Progress
        const status = safeString(apiDiv.status, 'upcoming');
        const progressPercent = safeNumber(apiDiv.progressPercent || apiDiv.progress_percent);
        const kind = safeString(apiDiv.kind, isPlayoff ? 'playoffs' : 'regular');

        // Winner
        const winnerTeamId = safeString(apiDiv.winnerTeamId || apiDiv.winner_team_id, null);
        const winnerTeamName = safeString(apiDiv.winnerTeamName || apiDiv.winner_team_name || apiDiv.winner, null);

        // Build href
        const href = `/division/${slug || id}`;

        return {
            id,
            name,
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

        const regularDivisions = normalizedDivisions.filter(d => !d.isPlayoff);
        const playoffDivisions = normalizedDivisions.filter(d => d.isPlayoff);

        // Match playoffs to regular divisions by division number and season
        // This is more reliable than parent_championship_id
        const divisionGroups = regularDivisions.map(regular => {
            const playoff = playoffDivisions.find(p => 
                p.divisionNum === regular.divisionNum && 
                p.season === regular.season
            );
            return { main: regular, playoff: playoff || null };
        });

        // Find orphaned playoffs (no matching division number/season)
        const orphanedPlayoffs = playoffDivisions.filter(p => {
            if (!p.divisionNum && !p.season) return true;
            return !regularDivisions.some(r => 
                r.divisionNum === p.divisionNum && 
                r.season === p.season
            );
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
            return {
                expandedPlayoffs: new Set()
            };
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
            togglePlayoff(divisionId) {
                if (this.expandedPlayoffs.has(divisionId)) {
                    this.expandedPlayoffs.delete(divisionId);
                } else {
                    this.expandedPlayoffs.add(divisionId);
                }
                // Trigger reactivity
                this.expandedPlayoffs = new Set(this.expandedPlayoffs);
            },
            isPlayoffExpanded(divisionId) {
                return this.expandedPlayoffs.has(divisionId);
            },
            getStatIcon(type) {
                const icons = { teams: '👥', matches: '🎮', dates: '📅', updated: '🔄' };
                return icons[type] || '';
            },
            getProgressPercent(progress) {
                return Math.min(100, Math.max(0, progress.percent || 0));
            },
            getPlayoffStatusLabel(status) {
                return PLAYOFF_STATUS_LABELS[status] || STATUS_LABELS[status] || 'Unknown';
            }
        },
        template: `
            <div class="division-card-list-wrapper">
                <div v-if="hasDivisions" class="division-card-list" role="list" :aria-label="'Divisioonat ' + seasonLabel">
                    <article
                        v-for="group in groupedDivisions"
                        :key="group.main.id"
                        class="division-card"
                        :class="[
                            'division-card--status-' + group.main.status,
                            'division-card--kind-' + group.main.kind,
                            { 'division-card--muted': group.main.status === 'upcoming' },
                            { 'division-card--orphaned': group.isOrphaned }
                        ]"
                        role="listitem"
                        :aria-labelledby="'division-title-' + group.main.id"
                    >
                        <div class="division-card__inner">
                            <!-- Warning for orphaned playoffs -->
                            <div v-if="group.isOrphaned" class="division-card__warning">
                                ⚠️ Pudotuspelien päädivisioona puuttuu
                            </div>

                            <!-- Top section -->
                            <div class="division-card__top">
                                <div class="division-card__badge" :class="'division-card__badge--' + group.main.status">
                                    <span>{{ group.main.statusLabel }}</span>
                                    <span v-if="group.main.matches.live > 0" class="division-card__badge-pulse" aria-hidden="true"></span>
                                </div>
                                <div class="division-card__titles">
                                    <h3 class="division-card__title" :id="'division-title-' + group.main.id">
                                        {{ group.main.name }}
                                    </h3>
                                    <div v-if="group.main.winner" class="division-card__winner">
                                        🏆 {{ group.main.winner.name }}
                                    </div>
                                </div>
                            </div>

                            <!-- Stats grid -->
                            <div class="division-card__stats">
                                <div class="division-card__stat">
                                    <span class="division-card__stat-icon" aria-hidden="true">{{ getStatIcon('teams') }}</span>
                                    <span class="division-card__stat-label">Joukkueet:</span>
                                    <span class="division-card__stat-value" :title="group.main.teams.count + ' joukkuetta'">
                                        {{ group.main.teams.label }}
                                    </span>
                                </div>
                                <div class="division-card__stat">
                                    <span class="division-card__stat-icon" aria-hidden="true">{{ getStatIcon('matches') }}</span>
                                    <span class="division-card__stat-label">Ottelut:</span>
                                    <span class="division-card__stat-value" :title="'Ottelut ' + group.main.matches.label">
                                        {{ group.main.matches.label }}
                                    </span>
                                </div>
                                <div class="division-card__stat">
                                    <span class="division-card__stat-icon" aria-hidden="true">{{ getStatIcon('dates') }}</span>
                                    <span class="division-card__stat-label">Ajanjakso:</span>
                                    <span class="division-card__stat-value" :title="group.main.dates.label">
                                        {{ group.main.dates.label }}
                                    </span>
                                </div>
                                <div class="division-card__stat division-card__stat--muted">
                                    <span class="division-card__stat-icon" aria-hidden="true">{{ getStatIcon('updated') }}</span>
                                    <span class="division-card__stat-label">Päivitetty:</span>
                                    <span class="division-card__stat-value" :title="'Päivitetty ' + group.main.dates.updatedLabel">
                                        {{ group.main.dates.updatedLabel }}
                                    </span>
                                </div>
                            </div>

                            <!-- Progress bar -->
                            <div class="division-card__progress">
                                <div
                                    class="division-card__progress-bar"
                                    role="progressbar"
                                    :aria-valuenow="getProgressPercent(group.main.progress)"
                                    aria-valuemin="0"
                                    aria-valuemax="100"
                                    :aria-label="group.main.progress.label || 'Edistyminen'"
                                >
                                    <div
                                        class="division-card__progress-fill"
                                        :style="{ width: getProgressPercent(group.main.progress) + '%' }"
                                    ></div>
                                </div>
                                <div v-if="group.main.progress.label" class="division-card__progress-label">
                                    {{ group.main.progress.label }}
                                </div>
                            </div>

                            <!-- Action button -->
                            <div class="division-card__action">
                                <a
                                    :href="group.main.href"
                                    class="division-card__button"
                                    :aria-label="group.main.actionLabel + ': ' + group.main.name"
                                >
                                    <span>{{ group.main.actionLabel }}</span>
                                    <span class="division-card__button-arrow" aria-hidden="true">→</span>
                                </a>
                            </div>
                        </div>

                        <!-- Playoff section -->
                        <div v-if="group.playoff" class="division-card__playoff" :data-parent-id="group.main.id">
                            <button
                                class="division-card__playoff-toggle"
                                :aria-expanded="isPlayoffExpanded(group.main.id) ? 'true' : 'false'"
                                :aria-controls="'playoff-content-' + group.main.id"
                                @click.prevent.stop="togglePlayoff(group.main.id)"
                            >
                                {{ isPlayoffExpanded(group.main.id) ? '▲ PUDOTUSPELIT' : '▼ PUDOTUSPELIT' }}
                            </button>
                            <div
                                :id="'playoff-content-' + group.main.id"
                                class="division-card__playoff-content"
                                :class="{ 'division-card__playoff-content--expanded': isPlayoffExpanded(group.main.id) }"
                            >
                                <div class="division-card__playoff-inner">
                                    <div class="division-card__playoff-header">
                                        <span class="division-card__playoff-badge">
                                            {{ getPlayoffStatusLabel(group.playoff.status) }}
                                        </span>
                                        <span class="division-card__playoff-title">
                                            {{ group.playoff.name }}
                                        </span>
                                    </div>
                                    <div class="division-card__playoff-info">
                                        {{ group.playoff.teams.label }} • {{ group.playoff.matches.label }}
                                    </div>
                                    <div v-if="group.playoff.matches.total > 0" class="division-card__playoff-progress-bar">
                                        <div
                                            :style="{ width: getProgressPercent(group.playoff.progress) + '%' }"
                                        ></div>
                                    </div>
                                    <div class="division-card__playoff-action">
                                        <a
                                            :href="group.playoff.href"
                                            class="division-card__playoff-button"
                                            :aria-label="'Avaa pudotuspelit: ' + group.playoff.name"
                                        >
                                            Avaa pudotuspelit →
                                        </a>
                                    </div>
                                </div>
                            </div>
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
