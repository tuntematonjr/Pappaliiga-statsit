(function () {
    const STATUS_COPY = Object.freeze({
        upcoming: { label: 'Alkamaton', action: 'Katso tiedot', secondary: 'Sarjan aikataulu' },
        running: { label: 'Käynnissä', action: 'Avaa sarja', secondary: 'Tilastot & ottelut' },
        ended: { label: 'Päättynyt', action: 'Avaa arkisto', secondary: 'Tilastot & historia' }
    });

    function toNumber(value, fallback = 0) {
        if (value === null || value === undefined) {
            return fallback;
        }
        const numeric = Number(value);
        if (Number.isFinite(numeric)) {
            return numeric;
        }
        const parsed = Number(String(value).replace(',', '.'));
        return Number.isFinite(parsed) ? parsed : fallback;
    }

    function extractNumber(value) {
        if (value === null || value === undefined) {
            return null;
        }
        const match = String(value).match(/(\d+)/);
        return match ? Number(match[1]) : null;
    }

    function coerceDate(value) {
        if (!value) return null;
        if (value instanceof Date) {
            return Number.isFinite(value.getTime()) ? value : null;
        }
        const timestamp = typeof value === 'number' ? value : Date.parse(value);
        if (!Number.isFinite(timestamp)) {
            return null;
        }
        const date = new Date(timestamp);
        return Number.isFinite(date.getTime()) ? date : null;
    }

    function formatDate(date) {
        if (!date) return '';
        try {
            return new Intl.DateTimeFormat('fi-FI', {
                day: 'numeric',
                month: 'numeric',
                year: 'numeric'
            }).format(date);
        } catch (error) {
            return date.toISOString().slice(0, 10);
        }
    }

    function formatDateTime(date) {
        if (!date) return '';
        try {
            return new Intl.DateTimeFormat('fi-FI', {
                day: 'numeric',
                month: 'numeric',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            }).format(date);
        } catch (error) {
            return date.toISOString();
        }
    }

    function formatDateRange(status, startDate, endDate) {
        const start = startDate ? formatDate(startDate) : '';
        const end = endDate ? formatDate(endDate) : '';
        if (start && end) {
            return `${start} – ${end}`;
        }
        if (status === 'running') {
            if (start) {
                return `Käynnissä · ${start} alkaen`;
            }
            return 'Käynnissä';
        }
        if (status === 'upcoming') {
            if (start) {
                return `Alkaa ${start}`;
            }
            return 'Alkamisaika vahvistuu';
        }
        if (status === 'ended') {
            if (end) {
                return `Päättyi ${end}`;
            }
            return 'Päättynyt';
        }
        return start || end || 'Ajankohta vahvistuu';
    }

    function parseTeams(division) {
        let count = 0;
        if (Array.isArray(division?.teams)) {
            count = division.teams.length;
        } else if (division?.teams && typeof division.teams === 'object' && division.teams.count != null) {
            count = toNumber(division.teams.count);
        } else {
            count =
                toNumber(division?.teamCount ?? division?.team_count ?? division?.team_total ?? division?.teamcount ?? 0);
            if (!count && division?.raw) {
                count = toNumber(
                    division.raw.teamCount ??
                        division.raw.team_count ??
                        division.raw.team_total ??
                        division.raw.teams_count ??
                        0
                );
            }
        }
        const label = count ? `${count}` : '–';
        return { count, label };
    }

    function parseMatches(division) {
        const matchesSource = division?.matches || division?.raw?.matches || {};
        const played = toNumber(
            matchesSource.played ??
                matchesSource.completed ??
                matchesSource.played_matches ??
                matchesSource.playedGames ??
                matchesSource.played ??
                division?.matchesPlayed ??
                division?.matches_played ??
                division?.played_matches ??
                division?.raw?.matches_played ??
                0
        );
        const total = toNumber(
            matchesSource.total ??
                matchesSource.scheduled ??
                matchesSource.total_matches ??
                matchesSource.max ??
                matchesSource.count ??
                division?.matchesTotal ??
                division?.matches_total ??
                division?.total_matches ??
                division?.schedule_count ??
                division?.raw?.total_matches ??
                0
        );
        const label = total > 0 ? `${played} / ${total}` : played ? `${played}` : '–';
        return { played, total, label };
    }

    function computePercent(division, matches) {
        const direct = toNumber(
            division?.progressPercent ??
                division?.progress_percent ??
                division?.progress ??
                division?.completion ??
                division?.raw?.progress_percent ??
                division?.raw?.progress,
            NaN
        );
        if (Number.isFinite(direct)) {
            return Math.max(0, Math.min(100, Math.round(direct)));
        }
        if (matches.total > 0) {
            return Math.max(0, Math.min(100, Math.round((matches.played / matches.total) * 100)));
        }
        return 0;
    }

    function determineKind(division, name) {
        const explicit = (division?.kind || division?.tier || division?.category || '').toString().toLowerCase();
        if (explicit.includes('master') || explicit.includes('mestaruus')) {
            return 'masters';
        }
        if (explicit.includes('playoff') || explicit.includes('bracket') || explicit.includes('fina')) {
            return 'playoffs';
        }
        if (explicit) {
            return 'division';
        }
        if (division?.raw?.is_playoff || division?.is_playoff || division?.isPlayoff) {
            return 'playoffs';
        }
        const divisionNumber =
            division?.division_num ?? division?.divisionNum ?? division?.division ?? division?.raw?.division_num;
        if (divisionNumber === 0) {
            return 'masters';
        }
        if ((name || '').toLowerCase().includes('mestaruus')) {
            return 'masters';
        }
        return 'division';
    }

    function determineStatus(division, context) {
        const raw = (division?.status || division?.state || division?.phase || '').toString().toLowerCase();
        if (raw) {
            if (['running', 'live', 'active', 'ongoing', 'käynnissä', 'current'].some(token => raw.includes(token))) {
                return 'running';
            }
            if (['ended', 'finished', 'completed', 'done', 'päättynyt', 'closed'].some(token => raw.includes(token))) {
                return 'ended';
            }
            if (
                ['upcoming', 'pending', 'scheduled', 'future', 'planned', 'not started', 'alkamaton'].some(token =>
                    raw.includes(token)
                )
            ) {
                return 'upcoming';
            }
        }

        const percent = context.progressPercent;
        const matches = context.matches;
        const now = Date.now();
        if (percent >= 100 && matches.total > 0) {
            return 'ended';
        }
        if (matches.played > 0 && matches.played < matches.total) {
            return 'running';
        }
        if (matches.total > 0 && matches.played >= matches.total) {
            return 'ended';
        }
        if (context.endDate && context.endDate.getTime() < now) {
            return 'ended';
        }
        if (context.startDate && context.startDate.getTime() > now) {
            return 'upcoming';
        }
        if (context.startDate && context.startDate.getTime() <= now) {
            return 'running';
        }
        return 'upcoming';
    }

    function isExternalHref(href) {
        return typeof href === 'string' && /^https?:\/\//i.test(href);
    }

    function pickHref(division) {
        if (typeof division?.href === 'string' && division.href) return division.href;
        if (typeof division?.url === 'string' && division.url) return division.url;
        if (typeof division?.link === 'string' && division.link) return division.link;
        if (typeof division?.slug === 'string' && division.slug && division.slug.startsWith('/')) return division.slug;
        return null;
    }

    function parseWinner(division) {
        const winner =
            division?.winner || division?.champion || division?.championTeam || division?.raw?.winner || division?.raw?.champion;
        if (typeof winner === 'string' && winner.trim()) {
            return winner.trim();
        }
        if (winner && typeof winner === 'object') {
            if (typeof winner.name === 'string' && winner.name.trim()) {
                return winner.name.trim();
            }
            if (typeof winner.team_name === 'string' && winner.team_name.trim()) {
                return winner.team_name.trim();
            }
        }
        if (division?.topTeam && typeof division.topTeam.name === 'string' && division.topTeam.name.trim()) {
            return division.topTeam.name.trim();
        }
        return 'TBD';
    }

    function deriveOrder(division, index, kind) {
        const nameNumber = extractNumber(division?.name);
        const numericCandidates = [
            division?.order,
            division?.rank,
            division?.position,
            division?.division_num,
            division?.divisionNum,
            division?.division,
            division?.raw?.division_num,
            nameNumber
        ];
        let primary = Number.POSITIVE_INFINITY;
        for (let i = 0; i < numericCandidates.length; i += 1) {
            const candidate = toNumber(numericCandidates[i], NaN);
            if (Number.isFinite(candidate)) {
                primary = candidate;
                break;
            }
        }
        if (!Number.isFinite(primary)) {
            primary = 999;
        }
        let secondary = 0;
        if (kind === 'masters') {
            secondary = -1;
        } else if (kind === 'playoffs') {
            secondary = 1;
        }
        return { primary, secondary, tertiary: index };
    }

    function buildAriaLabel(name, statusLabel, teams, matchesLabel) {
        const parts = [name, statusLabel];
        if (teams) {
            parts.push(`${teams} joukkuetta`);
        }
        if (matchesLabel) {
            parts.push(`Ottelut ${matchesLabel}`);
        }
        return parts.join('. ');
    }

    function normalizeDivision(division, index) {
        if (!division) return null;
        const name = division.name || division.title || division.label || 'Divisioona';
        const teams = parseTeams(division);
        const matches = parseMatches(division);
        const progressPercent = computePercent(division, matches);
        const startDate = coerceDate(
            division.start ||
                division.start_date ||
                division.startDate ||
                division.scheduled_start ||
                division.raw?.start_date ||
                division.raw?.start
        );
        const endDate = coerceDate(
            division.end ||
                division.end_date ||
                division.endDate ||
                division.scheduled_end ||
                division.raw?.end_date ||
                division.raw?.end
        );
        const updatedAt = coerceDate(
            division.updated ||
                division.updated_at ||
                division.last_updated ||
                division.lastUpdate ||
                division.raw?.updated_at ||
                division.raw?.last_updated
        );
        const kind = determineKind(division, name);
        const status = determineStatus(division, {
            progressPercent,
            matches,
            startDate,
            endDate
        });
        const statusCopy = STATUS_COPY[status] || STATUS_COPY.upcoming;
        const dateLabel = formatDateRange(status, startDate, endDate);
        const updatedLabel = updatedAt ? formatDateTime(updatedAt) : '–';
        const updatedTitle = updatedAt ? updatedAt.toISOString() : '';
        const statItems = [
            {
                key: 'teams',
                icon: 'teams',
                label: 'Joukkueet',
                value: teams.label,
                title: teams.count ? `${teams.count} joukkuetta` : 'Joukkueet'
            },
            {
                key: 'matches',
                icon: 'matches',
                label: 'Ottelut',
                value: matches.label || '–',
                title: matches.total ? `Ottelut ${matches.label}` : 'Ottelut'
            },
            {
                key: 'dates',
                icon: 'dates',
                label: 'Ajanjakso',
                value: dateLabel || 'Ajankohta vahvistuu',
                title: dateLabel || 'Ajankohta vahvistuu'
            },
            {
                key: 'updated',
                icon: 'updated',
                label: 'Päivitetty',
                value: updatedLabel,
                title: updatedAt ? `Päivitetty ${updatedLabel}` : 'Päivitetty',
                muted: true
            }
        ];
        const href = pickHref(division);
        const order = deriveOrder(division, index, kind);
        const ariaLabel = buildAriaLabel(name, statusCopy.label, teams.count, matches.label);
        const showProgress = status !== 'upcoming' || progressPercent > 0;

        const card = {
            key: division.key || division.id || division.uid || `division-${index}`,
            name,
            kind,
            status,
            statusLabel: statusCopy.label,
            actionLabel: statusCopy.action,
            secondaryLabel: division.secondaryLabel || statusCopy.secondary || '',
            statItems,
            teamsCount: teams.count,
            matchesPlayed: matches.played,
            matchesTotal: matches.total,
            matchesLabel: matches.label,
            progressPercent: status === 'ended' ? 100 : progressPercent,
            progressLabel: matches.total
                ? `${matches.played} / ${matches.total} ottelua`
                : matches.played
                ? `${matches.played} ottelua`
                : '',
            showProgress,
            showMutedAccent: status === 'upcoming',
            dateLabel,
            updatedLabel,
            updatedTitle,
            timelineLabel: dateLabel,
            winner: status === 'ended' ? parseWinner(division) : null,
            href,
            route: division.route || null,
            isExternal: division.external === true || isExternalHref(href),
            detailHref: division.detailHref || division.detailsHref || null,
            ariaLabel,
            order,
            startDate,
            endDate,
            updatedAt
        };

        card.progressPercent = Math.max(0, Math.min(100, Math.round(card.progressPercent)));

        if (!card.secondaryLabel) {
            card.secondaryLabel =
                card.status === 'running'
                    ? 'Tilastot & ottelut'
                    : card.status === 'ended'
                    ? 'Sarjan historia'
                    : 'Lisätiedot';
        }
        if (!card.winner && card.status === 'ended') {
            card.winner = 'TBD';
        }
        return card;
    }

    const DivisionCard = {
        name: 'DivisionCard',
        props: {
            card: {
                type: Object,
                required: true
            }
        },
        computed: {
            rootTag() {
                if (this.card?.route) {
                    return 'router-link';
                }
                if (this.card?.href) {
                    return 'a';
                }
                return 'div';
            },
            navAttrs() {
                if (this.rootTag === 'router-link') {
                    return { to: this.card.route };
                }
                if (this.rootTag === 'a') {
                    const attrs = { href: this.card.href };
                    if (this.card.isExternal) {
                        attrs.target = '_blank';
                        attrs.rel = 'noopener noreferrer';
                    }
                    return attrs;
                }
                return { role: 'article' };
            },
            tabIndex() {
                return this.rootTag === 'div' ? 0 : null;
            },
            progressStyle() {
                return { width: `${this.card.progressPercent}%` };
            },
            statusClass() {
                return `division-card--status-${this.card.status}`;
            },
            kindClass() {
                return `division-card--kind-${this.card.kind}`;
            },
            mutedClass() {
                return { 'division-card--muted': this.card.showMutedAccent };
            },
            showProgress() {
                return this.card.showProgress;
            },
            hasWinner() {
                return this.card.status === 'ended' && Boolean(this.card.winner);
            }
        },
        methods: {
            handleKeypress(event) {
                if (this.rootTag !== 'div') {
                    return;
                }
                if (event.key === 'Enter' || event.key === ' ') {
                    event.preventDefault();
                    if (this.card?.href) {
                        if (this.card.isExternal) {
                            window.open(this.card.href, '_blank', 'noopener');
                        } else {
                            window.open(this.card.href, '_self');
                        }
                    } else if (this.card?.route && this.$router) {
                        this.$router.push(this.card.route);
                    }
                }
            }
        },
        template: `
            <component
                :is="rootTag"
                class="division-card"
                :class="[statusClass, kindClass, mutedClass]"
                v-bind="navAttrs"
                :tabindex="tabIndex"
                :aria-label="card.ariaLabel"
                @keydown.space="handleKeypress"
                @keydown.enter="handleKeypress"
            >
                <span v-if="card.status === 'ended'" class="division-card__ribbon" aria-hidden="true">Päättynyt</span>
                <div class="division-card__inner">
                    <header class="division-card__top">
                        <span class="division-card__badge" :class="'badge-' + card.kind" aria-hidden="true">
                            <svg v-if="card.kind === 'masters'" viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                                <path d="M4 4l3 6 5-2 5 2 3-6h2l-1.5 14h-15L2 4h2z"></path>
                                <path d="M7 20h10l-5 3-5-3z"></path>
                            </svg>
                            <svg v-else-if="card.kind === 'playoffs'" viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                                <path d="M5 3h4l1 4h4l1-4h4l-2 12h-10L5 3z"></path>
                                <path d="M9 17h6l-3 4-3-4z"></path>
                            </svg>
                            <svg v-else viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                                <path d="M12 2l8 4v6c0 4.97-3.31 9.58-8 11-4.69-1.42-8-6.03-8-11V6l8-4z"></path>
                            </svg>
                        </span>
                        <div class="division-card__titles">
                            <h3 class="division-card__name" :title="card.name">{{ card.name }}</h3>
                            <span class="division-card__chip" :class="'chip-' + card.status">
                                <span v-if="card.status === 'running'" class="chip__dot" aria-hidden="true"></span>
                                {{ card.statusLabel }}
                            </span>
                        </div>
                    </header>

                    <ul class="division-card__stats" role="list">
                        <li
                            v-for="item in card.statItems"
                            :key="item.key"
                            class="division-card__stat"
                            role="listitem"
                            :class="{ 'division-card__stat--muted': item.muted }"
                            :title="item.title"
                        >
                            <span class="division-card__icon">
                                <svg v-if="item.icon === 'teams'" viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                                    <path d="M9 12c-2.21 0-4-1.79-4-4s1.79-4 4-4 4 1.79 4 4-1.79 4-4 4zm6 0c-1.86 0-3.41-1.28-3.86-3h2.08c.36 1.19 1.47 2 2.78 2 1.65 0 3-1.35 3-3s-1.35-3-3-3c-1.31 0-2.42.81-2.78 2h-2.08C11.59 3.28 13.14 2 15 2c2.76 0 5 2.24 5 5s-2.24 5-5 5zm-6 2c2.67 0 8 1.34 8 4v2H1v-2c0-2.66 5.33-4 8-4zm0 2c-2.33 0-6 1.17-6 2v.01h12V18c0-.83-3.67-2-6-2zm6.92-2.74C18.46 15.37 20 17.28 20 19v1h-4v-1c0-1.54-.85-3.03-2.23-4.24.37-.05.74-.08 1.15-.08.36 0 .71.03 1.05.08z"></path>
                                </svg>
                                <svg v-else-if="item.icon === 'matches'" viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                                    <path d="M4 5h16v4H4z"></path>
                                    <path d="M6 5h2v14H6zm10 0h2v14h-2z"></path>
                                    <path d="M9 5h6v6H9z"></path>
                                </svg>
                                <svg v-else-if="item.icon === 'dates'" viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                                    <path d="M7 2v2H5a2 2 0 0 0-2 2v2h18V6a2 2 0 0 0-2-2h-2V2h-2v2H9V2H7zm13 8H4v10a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V10zm-9 3h2v4h-2v-4z"></path>
                                </svg>
                                <svg v-else-if="item.icon === 'updated'" viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                                    <path d="M12 1a11 11 0 1 0 11 11A11 11 0 0 0 12 1zm0 2a9 9 0 1 1-9 9 9 9 0 0 1 9-9zm-.5 3h1.5v5.25l3.5 2.1-.75 1.23L11.5 13V6z"></path>
                                </svg>
                            </span>
                            <div class="division-card__stat-text">
                                <span class="division-card__stat-label">{{ item.label }}</span>
                                <span class="division-card__stat-value">{{ item.value }}</span>
                            </div>
                        </li>
                    </ul>

                    <div
                        v-if="showProgress"
                        class="division-card__progress"
                        role="group"
                        :aria-label="card.progressLabel || 'Kausi etenee'"
                    >
                        <div class="division-card__progress-bar">
                            <span class="division-card__progress-fill" :style="progressStyle"></span>
                        </div>
                        <span class="division-card__progress-label">
                            {{ card.status === 'ended' ? '100 %' : card.progressPercent + ' %' }}
                        </span>
                    </div>

                    <p v-if="hasWinner" class="division-card__winner">
                        <span class="division-card__winner-label">Voittaja:</span>
                        <span class="division-card__winner-value">{{ card.winner }}</span>
                    </p>

                    <footer class="division-card__footer">
                        <span class="division-card__cta">{{ card.actionLabel }}</span>
                        <span
                            v-if="card.secondaryLabel"
                            class="division-card__secondary"
                            :class="{ 'division-card__secondary--link': Boolean(card.detailHref) }"
                            :title="card.detailHref || null"
                        >
                            {{ card.secondaryLabel }}
                        </span>
                    </footer>
                </div>
            </component>
        `
    };

    window.DivisionCard = DivisionCard;

    window.DivisionCardList = {
        name: 'DivisionCardList',
        components: {
            DivisionCard
        },
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
                default: 'Ei divisioonia'
            }
        },
        computed: {
            normalizedDivisions() {
                if (!Array.isArray(this.divisions)) {
                    return [];
                }
                return this.divisions
                    .map((division, index) => normalizeDivision(division, index))
                    .filter(Boolean);
            },
            hasDivisions() {
                return this.normalizedDivisions.length > 0;
            },
            orderedDivisions() {
                return this.normalizedDivisions
                    .slice()
                    .sort((a, b) => {
                        if (a.order.primary !== b.order.primary) {
                            return a.order.primary - b.order.primary;
                        }
                        if (a.order.secondary !== b.order.secondary) {
                            return a.order.secondary - b.order.secondary;
                        }
                        return a.order.tertiary - b.order.tertiary;
                    });
            }
        },
        template: `
            <div class="division-card-list-wrapper">
                <div
                    v-if="hasDivisions"
                    class="division-card-list"
                    role="list"
                    :aria-label="'Divisioonat ' + (seasonLabel || '')"
                >
                    <division-card
                        v-for="card in orderedDivisions"
                        :key="card.key"
                        :card="card"
                        role="listitem"
                    ></division-card>
                </div>
                <p v-else class="division-card-list__empty">
                    {{ emptyMessage }}
                </p>
            </div>
        `
    };
})();
