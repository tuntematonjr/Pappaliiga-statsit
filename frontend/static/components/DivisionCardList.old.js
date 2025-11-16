(function () {
    const STATUS_COPY = Object.freeze({
        upcoming: { label: 'Alkamaton', action: 'Avaa divisioona' },
        running: { label: 'Käynnissä', action: 'Avaa divisioona' },
        ended: { label: 'Päättynyt', action: 'Avaa divisioona' }
    });

    const PLAYOFF_STATUS_LABELS = Object.freeze({
        upcoming: 'Ei vielä alkanut',
        running: 'Käynnissä',
        ended: 'Taputeltu loppuun'
    });

    const PLAYOFF_SERIES_CAPTION = '8 joukkuetta — 7 ottelua';
    const DEFAULT_PLAYOFF_MATCHES = 7;

    function pickString(source, keys) {
        if (!source) return '';
        for (const key of keys) {
            if (Object.prototype.hasOwnProperty.call(source, key)) {
                const value = source[key];
                if (value !== undefined && value !== null && value !== '') {
                    const text = String(value).trim();
                    if (text) {
                        return text;
                    }
                }
            }
        }
        return '';
    }

    function normaliseKey(value) {
        return value ? String(value).trim().toLowerCase() : '';
    }

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

    function deriveBaseSlug(slug) {
        if (!slug) return '';
        let base = String(slug).trim().toLowerCase();
        base = base.replace(/\s+/g, '-');
        base = base.replace(/-(?:playoffs?|po|pudotuspelit).*/g, '');
        base = base.replace(/-(?:rk|runko|regular).*/g, '');
        base = base.replace(/\-+/g, '-');
        base = base.replace(/^-|-$/g, '');
        return base;
    }

    function deriveSeasonDivisionKey(season, divisionNum) {
        if (!Number.isFinite(season) || !Number.isFinite(divisionNum)) {
            return '';
        }
        return `${season}-${divisionNum}`;
    }

    function derivePlayoffParentKey(raw, normalized, fallbackKey) {
        const parentId = pickString(raw, [
            'parent_championship_id',
            'parentChampionshipId',
            'main_championship_id',
            'mainChampionshipId',
            'root_championship_id',
            'rootChampionshipId',
            'base_championship_id',
            'baseChampionshipId'
        ]);
        if (parentId) {
            return normaliseKey(parentId);
        }

        const slug = normalized.slug || pickString(raw, ['slug', 'code', 'identifier']);
        const baseSlug = deriveBaseSlug(slug);
        if (baseSlug) {
            return normaliseKey(baseSlug);
        }

        const seasonDivisionKey = deriveSeasonDivisionKey(normalized.season, normalized.divisionNumber);
        if (seasonDivisionKey) {
            return normaliseKey(seasonDivisionKey);
        }

        return normaliseKey(fallbackKey);
    }

    function toSortableTime(date) {
        if (date instanceof Date) {
            const value = date.getTime();
            if (Number.isFinite(value)) {
                return value;
            }
        }
        return Number.NEGATIVE_INFINITY;
    }

    function pickLatestPlayoff(playoffs) {
        if (!Array.isArray(playoffs) || playoffs.length === 0) {
            return null;
        }
        return playoffs.reduce((selected, candidate) => {
            if (!candidate) return selected;
            if (!selected) return candidate;

            const candidateStart = toSortableTime(candidate.startDate);
            const selectedStart = toSortableTime(selected.startDate);
            if (candidateStart !== selectedStart) {
                return candidateStart > selectedStart ? candidate : selected;
            }

            const candidateUpdated = toSortableTime(candidate.updatedAt);
            const selectedUpdated = toSortableTime(selected.updatedAt);
            if (candidateUpdated !== selectedUpdated) {
                return candidateUpdated > selectedUpdated ? candidate : selected;
            }

            // Fall back to keeping the existing selection to preserve original ordering
            return selected;
        }, null);
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
        const raw = division || {};
        const rawData = raw.raw || {};
        const teamsNode = raw.teams;
        let count = 0;

        if (Array.isArray(teamsNode)) {
            count = teamsNode.length;
        } else if (teamsNode && typeof teamsNode === 'object' && teamsNode.count != null) {
            count = toNumber(teamsNode.count);
        }

        const candidateKeys = [
            'teamsCount',
            'teams_count',
            'team_count',
            'teamCount',
            'team_total',
            'teamTotal',
            'teams',
            'teamcount',
            'teamCountTotal',
            'teams_total'
        ];
        const rawCandidateKeys = [
            'teamsCount',
            'teams_count',
            'team_count',
            'teamCount',
            'team_total',
            'teamTotal',
            'teams',
            'teamcount',
            'teamCountTotal',
            'teams_total'
        ];

        if (!count) {
            for (const key of candidateKeys) {
                const value = raw[key];
                if (value != null) {
                    count = toNumber(value);
                    if (count) break;
                }
            }
        }

        if (!count) {
            for (const key of rawCandidateKeys) {
                const value = rawData[key];
                if (value != null) {
                    count = toNumber(value);
                    if (count) break;
                }
            }
        }

        const label = count ? `${count}` : '–';
        return { count, label };
    }

    function parseMatches(division) {
        const raw = division || {};
        const rawData = raw.raw || {};

        const matchesSource = raw.matches || rawData.matches || {};
        const extraSource = rawData?.aggregates || rawData?.stats || {};
        const playedCandidateKeys = [
            'played',
            'playedMatches',
            'played_matches',
            'matchesPlayed',
            'matches_played',
            'matches',
            'completed',
            'finished_matches',
            'finishedMatches',
            'played_games',
            'playedGames',
            'gamesPlayed'
        ];
        const totalCandidateKeys = [
            'total',
            'totalMatches',
            'matchesTotal',
            'matches_total',
            'total_matches',
            'matches',
            'scheduled',
            'scheduled_matches',
            'schedule_count',
            'max',
            'count'
        ];

        const rawMatchNodes = [matchesSource, raw, rawData, extraSource];

        function pickFirstNumber(keys, sources) {
            for (const source of sources) {
                if (!source || typeof source !== 'object') continue;
                for (const key of keys) {
                    if (Object.prototype.hasOwnProperty.call(source, key)) {
                        const numeric = toNumber(source[key]);
                        if (numeric) return numeric;
                    }
                }
            }
            return 0;
        }

        const played = pickFirstNumber(playedCandidateKeys, rawMatchNodes);
        const total = pickFirstNumber(totalCandidateKeys, rawMatchNodes);
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
        const playoffFlags = [
            division?.raw?.is_playoff,
            division?.raw?.is_playoffs,
            division?.is_playoff,
            division?.is_playoff_secondary,
            division?.isPlayoff,
            division?.is_playoffs,
            division?.isPlayoffs,
            division?.stage && String(division.stage).toLowerCase().includes('playoff'),
            division?.phase && String(division.phase).toLowerCase().includes('playoff')
        ];
        if (playoffFlags.some(Boolean)) {
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

    function sanitizeDivisionName(rawName, division, kind) {
        const fallback = kind === 'masters' || division?.division_num === 0 ? 'Mestaruussarja' : 'Divisioona';
        let name = (rawName || '').trim();
        if (!name || /arkisto/i.test(name)) {
            return fallback;
        }

        if (kind === 'masters' || division?.division_num === 0) {
            name = 'Mestaruussarja';
        }

        const seasonPatterns = [
            /\bKausi\s*\d+\b/gi,
            /\bSeason\s*\d+\b/gi,
            /\bS\s*\d+\b/gi,
            /\bS\d+\b/gi,
            /\((?:\s*(?:Kausi|Season|S)\s*\d+[^\)]*)\)/gi
        ];
        seasonPatterns.forEach(pattern => {
            name = name.replace(pattern, '');
        });

        name = name.replace(/\s+Divisioona\s*$/i, ' Divisioona');
        name = name.replace(/[·•\-–—]+/g, ' ');
        name = name.replace(/\s{2,}/g, ' ').trim();

        if (!name) {
            return fallback;
        }
        return name;
    }

    function deriveOrder(division, index, kind, displayName) {
        if (kind === 'masters' || division?.division_num === 0) {
            return { primary: -100, secondary: 0, tertiary: index };
        }
        const nameNumber = extractNumber(displayName || division?.name);
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
        if (kind === 'playoffs') {
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

    function buildPlayoffDescriptor(playoffCard) {
        if (!playoffCard) return null;
        const status = playoffCard.status;
        const statusLabel = PLAYOFF_STATUS_LABELS[status] || playoffCard.statusLabel || '';
        const startDate = playoffCard.startDate instanceof Date ? playoffCard.startDate : null;
        const endDate = playoffCard.endDate instanceof Date ? playoffCard.endDate : null;
        const startLabel = startDate ? formatDate(startDate) : '';
        const endLabel = endDate ? formatDate(endDate) : '';

        let dateLabel = '';
        if (startLabel && endLabel) {
            dateLabel = `${startLabel} – ${endLabel}`;
        } else if (status === 'running' && startLabel) {
            dateLabel = `${startLabel} –`;
        } else if (status === 'ended' && endLabel) {
            dateLabel = `Päättyi ${endLabel}`;
        } else if (startLabel) {
            dateLabel = startLabel;
        }
        if (!dateLabel && status === 'upcoming') {
            dateLabel = 'Ei vielä alkanut';
        }

        const matchesTotalRaw =
            Number.isFinite(playoffCard.matchesTotal) && playoffCard.matchesTotal > 0
                ? playoffCard.matchesTotal
                : null;
        const matchesTotal = matchesTotalRaw || DEFAULT_PLAYOFF_MATCHES;
        const matchesPlayedRaw = Number.isFinite(playoffCard.matchesPlayed) ? playoffCard.matchesPlayed : 0;
        const matchesPlayed = Math.max(0, Math.min(matchesTotal, matchesPlayedRaw));

        let progressPercent = Number.isFinite(playoffCard.progressPercent) ? playoffCard.progressPercent : NaN;
        if (!Number.isFinite(progressPercent)) {
            progressPercent = matchesTotal > 0 ? Math.round((matchesPlayed / matchesTotal) * 100) : 0;
        }
        progressPercent = Math.max(0, Math.min(100, Math.round(progressPercent)));

        const progressLabel =
            matchesTotal > 0 ? `${matchesPlayed} / ${matchesTotal} ottelua` : `${matchesPlayed} ottelua`;
        const progressTooltip = progressLabel;
        const progressAriaLabel = status === 'running' ? `Playoff-sarja etenee: ${progressLabel}` : '';

        const winner =
            status === 'ended'
                ? (playoffCard.winner && String(playoffCard.winner).trim()) || 'TBD'
                : null;

        let link = null;
        if (playoffCard.route) {
            link = { type: 'route', to: playoffCard.route };
        } else if (playoffCard.href) {
            link = { type: 'href', href: playoffCard.href, external: playoffCard.isExternal };
        }
        return {
            status,
            statusLabel,
            dateLabel,
            hasDates: Boolean(dateLabel),
            winner,
            matchesCaption: PLAYOFF_SERIES_CAPTION,
            link,
            isUpcoming: status === 'upcoming',
            isRunning: status === 'running',
            isEnded: status === 'ended',
            progressPercent,
            progressPercentText: `${progressPercent} %`,
            progressLabel,
            progressTooltip,
            progressAriaLabel,
            matchesPlayed,
            matchesTotal
        };
    }

    function normalizeDivision(division, index) {
        if (!division) return null;
        const rawName = division.name || division.title || division.label || '';
        const kind = determineKind(division, rawName);
        const name = sanitizeDivisionName(rawName, division, kind);

        const slugValue = pickString(division, ['slug', 'code', 'identifier', 'championship_slug', 'championshipSlug']);
        const slugBase = deriveBaseSlug(slugValue);
        const championshipId = pickString(division, ['championship_id', 'championshipId', 'id', 'championshipID']);
        const seasonNumber = toNumber(
            division.season ??
                division.season_number ??
                division.seasonNumber ??
                division?.raw?.season ??
                division?.raw?.season_number ??
                division?.raw?.seasonNumber,
            NaN
        );
        const divisionNumber = toNumber(
            division.division_num ??
                division.divisionNum ??
                division.division ??
                division?.raw?.division_num ??
                division?.raw?.divisionNum ??
                division?.raw?.division,
            NaN
        );
        const canonicalKey = normaliseKey(
            slugBase ||
                championshipId ||
                deriveSeasonDivisionKey(seasonNumber, divisionNumber) ||
                name ||
                `division-${index}`
        );

        const teams = parseTeams(division);
        const matches = parseMatches(division);
        const progressPercent = computePercent(division, matches);
        const startDate = coerceDate(
            division.start ||
                division.start_date ||
                division.startDate ||
                division.start_ts ||
                division.scheduled_start ||
                division.first_started_at ||
                division.first_started_ts ||
                division.firstScheduledAt ||
                division.raw?.start_date ||
                division.raw?.start ||
                division.raw?.first_started_at
        );
        const endDate = coerceDate(
            division.end ||
                division.end_date ||
                division.endDate ||
                division.end_ts ||
                division.scheduled_end ||
                division.last_finished_at ||
                division.last_finished_ts ||
                division.lastScheduledAt ||
                division.raw?.end_date ||
                division.raw?.end ||
                division.raw?.last_finished_at
        );
        const updatedAt = coerceDate(
            division.updated ||
                division.updated_at ||
                division.last_updated ||
                division.lastUpdate ||
                division.lastActivityAt ||
                division.last_activity_at ||
                division.updated_ts ||
                division.last_activity_ts ||
                division.raw?.updated_at ||
                division.raw?.last_updated
        );
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
        const order = deriveOrder(division, index, kind, name);
        const parentKey = derivePlayoffParentKey(
            division,
            {
                slug: slugValue,
                season: Number.isFinite(seasonNumber) ? seasonNumber : null,
                divisionNumber: Number.isFinite(divisionNumber) ? divisionNumber : null
            },
            canonicalKey
        );
        const ariaLabel = buildAriaLabel(name, statusCopy.label, teams.count, matches.label);

        const card = {
            key: division.key || division.id || division.uid || `division-${index}`,
            name,
            kind,
            status,
            statusLabel: statusCopy.label,
            actionLabel: statusCopy.action,
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
            updatedAt,
            season: Number.isFinite(seasonNumber) ? seasonNumber : null,
            divisionNumber: Number.isFinite(divisionNumber) ? divisionNumber : null,
            slug: slugValue || null,
            slugBase: slugBase || null,
            championshipId: championshipId || null,
            lookupKey: canonicalKey,
            parentKey,
            hasPlayoffs: false,
            playoff: null
        };

        card.progressPercent = Math.max(0, Math.min(100, Math.round(card.progressPercent)));

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
            hasWinner() {
                return this.card.status === 'ended' && Boolean(this.card.winner);
            },
            isUpcoming() {
                return this.card.status === 'upcoming';
            },
            isRunning() {
                return this.card.status === 'running';
            },
            isEnded() {
                return this.card.status === 'ended';
            },
            progressCaption() {
                if (!this.isRunning) return '';
                const played = this.card.matchesPlayed ?? 0;
                const total = this.card.matchesTotal ?? 0;
                if (total > 0) {
                    return `${played} / ${total} ottelua`;
                }
                if (played > 0) {
                    return `${played} ottelua Ottelut`;
                }
                return 'Seuranta käynnissä';
            },
            progressTooltip() {
                if (!this.isRunning) return '';
                const played = this.card.matchesPlayed ?? 0;
                const total = this.card.matchesTotal ?? 0;
                if (total > 0) {
                    return `${played} / ${total} ottelua Ottelut`;
                }
                return `${played} ottelua Ottelut`;
            },
            progressPercentText() {
                const percent = Math.max(0, Math.min(100, Math.round(this.card.progressPercent || 0)));
                return `${percent} %`;
            },
            progressAriaLabel() {
                if (this.isRunning) {
                    return this.card.progressLabel || this.progressCaption || 'Kausi etenee';
                }
                if (this.isUpcoming) {
                    return 'Ei vielä alkanut';
                }
                return 'Taputeltu loppuun';
            },
            hasPlayoffs() {
                return Boolean(this.card.playoff);
            },
            playoffInfo() {
                return this.card.playoff || null;
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
                        v-if="isRunning"
                        class="division-card__progress division-card__progress--active"
                        role="group"
                        :aria-label="progressAriaLabel"
                        :title="progressTooltip"
                    >
                        <div class="division-card__progress-meta">
                            <span class="division-card__progress-percent">{{ progressPercentText }}</span>
                            <span class="division-card__progress-caption">{{ progressCaption }}</span>
                        </div>
                        <div class="division-card__progress-track">
                            <span class="division-card__progress-fill" :style="progressStyle"></span>
                        </div>
                    </div>
                    <div
                        v-else-if="isUpcoming"
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

                    <div
                        v-if="hasPlayoffs"
                        class="division-card__playoff"
                        :class="'division-card__playoff--' + playoffInfo.status"
                    >
                        <header class="division-card__playoff-header">
                            <span class="division-card__playoff-title">Playoffs</span>
                            <span class="division-card__playoff-chip" :class="'playoff-chip--' + playoffInfo.status">
                                {{ playoffInfo.statusLabel }}
                            </span>
                        </header>
                        <p class="division-card__playoff-line">{{ playoffInfo.matchesCaption }}</p>
                        <p v-if="playoffInfo.hasDates" class="division-card__playoff-dates">
                            {{ playoffInfo.dateLabel }}
                        </p>
                        <div
                            v-if="playoffInfo.isRunning"
                            class="division-card__playoff-progress"
                            role="group"
                            :aria-label="playoffInfo.progressAriaLabel"
                            :title="playoffInfo.progressTooltip"
                        >
                            <div class="division-card__playoff-progress-meta">
                                <span class="division-card__playoff-progress-percent">{{ playoffInfo.progressPercentText }}</span>
                                <span class="division-card__playoff-progress-caption">{{ playoffInfo.progressLabel }}</span>
                            </div>
                            <div class="division-card__playoff-progress-track">
                                <span
                                    class="division-card__playoff-progress-fill"
                                    :style="{ width: playoffInfo.progressPercent + '%' }"
                                ></span>
                            </div>
                        </div>
                        <div
                            v-else-if="playoffInfo.isUpcoming"
                            class="division-card__playoff-placeholder"
                            role="status"
                            aria-live="polite"
                        >
                            Ei vielä alkanut
                        </div>
                        <div v-else class="division-card__playoff-result">
                            <p class="division-card__playoff-winner">Voittaja: {{ playoffInfo.winner }}</p>
                        </div>
                        <div class="division-card__playoff-actions">
                            <router-link
                                v-if="playoffInfo.link && playoffInfo.link.type === 'route'"
                                :to="playoffInfo.link.to"
                                class="division-card__playoff-button"
                            >
                                Avaa playoff-sarja
                            </router-link>
                            <a
                                v-else-if="playoffInfo.link && playoffInfo.link.type === 'href'"
                                :href="playoffInfo.link.href"
                                class="division-card__playoff-button"
                                :target="playoffInfo.link.external ? '_blank' : null"
                                :rel="playoffInfo.link.external ? 'noopener noreferrer' : null"
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
                    <div v-else class="division-card__playoff division-card__playoff--placeholder" aria-hidden="true"></div>

                    <footer class="division-card__footer">
                        <span class="division-card__cta">{{ card.actionLabel }}</span>
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

                const groups = new Map();

                this.divisions.forEach((division, index) => {
                    const normalized = normalizeDivision(division, index);
                    if (!normalized) {
                        return;
                    }

                    // For playoff divisions, use parentKey to group with their parent
                    // For regular divisions, use their own lookupKey
                    let key;
                    if (normalized.kind === 'playoffs') {
                        // Playoff division - use parent key to find the parent bucket
                        key = normaliseKey(
                            normalized.parentKey ||
                                normalized.slugBase ||
                                deriveSeasonDivisionKey(normalized.season, normalized.divisionNumber) ||
                                normalized.lookupKey
                        );
                    } else {
                        // Regular division - use its own lookupKey
                        key = normaliseKey(
                            normalized.lookupKey ||
                                normalized.slug ||
                                normalized.championshipId ||
                                deriveSeasonDivisionKey(normalized.season, normalized.divisionNumber) ||
                                normalized.key
                        );
                    }
                    
                    if (!key) {
                        key = `__division_${index}`;
                    }

                    if (!groups.has(key)) {
                        groups.set(key, { main: null, playoffs: [] });
                    }
                    const bucket = groups.get(key);
                    if (normalized.kind === 'playoffs') {
                        bucket.playoffs.push(normalized);
                    } else if (!bucket.main) {
                        bucket.main = normalized;
                    } else {
                        // multiple regular entries; create a separate bucket with unique key
                        const altKey = `${key}::${bucket.playoffs.length + 1}`;
                        groups.set(altKey, { main: normalized, playoffs: [] });
                    }
                });

                const result = [];
                groups.forEach(bucket => {
                    const main = bucket.main;
                    
                    // Handle orphaned playoff divisions (no parent regular division found)
                    if (!main && bucket.playoffs.length > 0) {
                        // Skip orphaned playoffs - they should have a parent division
                        return;
                    }
                    
                    if (!main) {
                        return;
                    }

                    const playoffCard = pickLatestPlayoff(bucket.playoffs);
                    if (playoffCard) {
                        main.hasPlayoffs = true;
                        main.playoff = buildPlayoffDescriptor(playoffCard);
                    } else {
                        main.hasPlayoffs = false;
                        main.playoff = null;
                    }

                    // Don't render playoff divisions as standalone cards - they should be attached to parents
                    if (main.kind === 'playoffs') {
                        return;
                    }

                    // Skip divisions with playoff-like slugs that are standalone
                    if (main.slug && /-(?:po|playoffs?)$/i.test(main.slug)) {
                        return;
                    }

                    result.push(main);
                });

                return result;
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
