(function () {
    const DEFAULT_TEAM_LOGO = window.PAPPALIIGA_DEFAULT_LOGO;
    const MIN_VALID_MATCH_EPOCH_MS = Date.UTC(2001, 0, 1);

    function isLikelyPlaceholderMatchTsMs(value) {
        if (!Number.isFinite(value) || value <= 0) return true;
        return value < MIN_VALID_MATCH_EPOCH_MS;
    }

    function coerceEpochMs(value) {
        if (value === null || value === undefined || value === 0) return null;
        const numeric = Number(value);
        if (Number.isFinite(numeric) && numeric > 0) {
            const ms = Math.abs(numeric) < 1_000_000_000_000 ? numeric * 1000 : numeric;
            return isLikelyPlaceholderMatchTsMs(ms) ? null : ms;
        }
        const parsed = Date.parse(String(value));
        if (!Number.isFinite(parsed) || parsed <= 0) return null;
        return isLikelyPlaceholderMatchTsMs(parsed) ? null : parsed;
    }

    function getScheduledTs(raw) {
        if (!raw || typeof raw !== 'object') return null;
        const scheduledRaw = raw.scheduled_ts
            ?? raw.scheduledTs
            ?? raw.scheduled_at
            ?? raw.scheduledAt
            ?? raw.scheduled
            ?? raw.start_ts
            ?? raw.startTs
            ?? raw.start_at
            ?? raw.startAt
            ?? raw.date
            ?? raw.datetime
            ?? raw.ts
            ?? null;
        return coerceEpochMs(scheduledRaw);
    }

    function normalizeTeam(raw, fallbackLabel) {
        if (!raw || typeof raw !== 'object') {
            return { id: null, name: fallbackLabel, avatar: DEFAULT_TEAM_LOGO };
        }
        const id = raw.id ?? raw.team_id ?? raw.teamId ?? null;
        const name = raw.name ?? raw.team_name ?? raw.teamName ?? fallbackLabel;
        const avatar = raw.avatar ?? raw.logo ?? raw.team_logo ?? raw.teamLogo ?? DEFAULT_TEAM_LOGO;
        return { id, name, avatar };
    }

    function normalizeUpcomingMatch(raw) {
        if (!raw || typeof raw !== 'object') return null;
        const matchId = raw.match_id ?? raw.matchId ?? null;
        const championshipId = raw.championship_id ?? raw.championshipId ?? null;
        const divisionName = raw.division_name ?? raw.divisionName ?? raw.name ?? null;
        const divisionSlug = raw.division_slug ?? raw.divisionSlug ?? null;
        const divisionNum = raw.division_num ?? raw.divisionNum ?? null;
        const season = raw.season ?? raw.season_id ?? raw.seasonId ?? null;
        const isPlayoffs = Boolean(raw.is_playoffs ?? raw.isPlayoffs ?? raw.is_playoff ?? raw.isPlayoff);
        const status = raw.status ?? null;
        const scheduledTs = getScheduledTs(raw);
        const team1 = normalizeTeam(
            raw.team1 || {
                team_id: raw.team1_id ?? raw.team1Id ?? null,
                team_name: raw.team1_name ?? raw.team1Name ?? null,
                avatar: raw.team1_avatar ?? raw.team1Avatar ?? raw.t1_avatar ?? raw.t1Avatar
            },
            'Joukkue 1'
        );
        const team2 = normalizeTeam(
            raw.team2 || {
                team_id: raw.team2_id ?? raw.team2Id ?? null,
                team_name: raw.team2_name ?? raw.team2Name ?? null,
                avatar: raw.team2_avatar ?? raw.team2Avatar ?? raw.t2_avatar ?? raw.t2Avatar
            },
            'Joukkue 2'
        );
        const faceitUrl = raw.faceit_url ?? raw.faceitUrl ?? '';

        return {
            matchId,
            championshipId,
            divisionName,
            divisionSlug,
            divisionNum,
            season,
            isPlayoffs,
            status,
            scheduledTs,
            team1,
            team2,
            faceitUrl
        };
    }

    function formatDateTime(ts) {
        if (!ts) return 'Aika tarkentuu';
        try {
            return new Date(ts).toLocaleString('fi-FI', {
                day: 'numeric',
                month: 'short',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            });
        } catch (error) {
            return 'Aika tarkentuu';
        }
    }

    const existingMatchTimeUtils = (typeof window !== 'undefined' && window.matchTimeUtils && typeof window.matchTimeUtils === 'object')
        ? window.matchTimeUtils
        : {};
    window.matchTimeUtils = Object.freeze({
        ...existingMatchTimeUtils,
        coerceEpochMs,
        getScheduledTs,
        formatDateTime
    });

    window.UpcomingMatchesList = {
        name: 'UpcomingMatchesList',
        components: {
            get LoadingSpinner() { return window.LoadingSpinner; },
            get ErrorMessage() { return window.ErrorMessage; }
        },
        props: {
            title: { type: String, default: 'Tulevat ottelut' },
            subtitle: { type: String, default: '' },
            items: { type: Array, default: () => [] },
            loading: { type: Boolean, default: false },
            error: { type: String, default: '' },
            emptyMessage: { type: String, default: 'Ei tulevia otteluita.' },
            showDivision: { type: Boolean, default: false },
            showFaceit: { type: Boolean, default: true },
            showHeader: { type: Boolean, default: true },
            groupByDayDivision: { type: Boolean, default: false },
            showWeekSeparators: { type: Boolean, default: true },
            separatorGranularity: { type: String, default: 'week' }
        },
        computed: {
            normalizedItems() {
                if (!Array.isArray(this.items)) return [];
                return this.items
                    .map(normalizeUpcomingMatch)
                    .filter(Boolean)
                    .sort((a, b) => {
                        const at = a.scheduledTs ?? Number.POSITIVE_INFINITY;
                        const bt = b.scheduledTs ?? Number.POSITIVE_INFINITY;
                        if (at !== bt) return at - bt;
                        return String(a.matchId || '').localeCompare(String(b.matchId || ''));
                    });
            },
            dayDivisionGroups() {
                if (!this.groupByDayDivision) return [];
                const dayMap = new Map();
                for (const match of this.normalizedItems) {
                    const dayKey = this.dayBucket(match.scheduledTs);
                    if (!dayMap.has(dayKey)) {
                        dayMap.set(dayKey, {
                            key: dayKey,
                            ts: match.scheduledTs ?? Number.POSITIVE_INFINITY,
                            label: this.dayLabel(match.scheduledTs),
                            divisionMap: new Map()
                        });
                    }
                    const dayGroup = dayMap.get(dayKey);
                    const divisionKey = this.divisionGroupKey(match);
                    if (!dayGroup.divisionMap.has(divisionKey)) {
                        dayGroup.divisionMap.set(divisionKey, {
                            key: divisionKey,
                            label: this.divisionLabel(match),
                            sortOrder: this.divisionSortOrder(match),
                            matches: []
                        });
                    }
                    dayGroup.divisionMap.get(divisionKey).matches.push(match);
                }
                return Array.from(dayMap.values())
                    .sort((a, b) => a.ts - b.ts)
                    .map(day => ({
                        key: day.key,
                        label: day.label,
                        divisions: Array.from(day.divisionMap.values()).sort((a, b) => {
                            if (a.sortOrder !== b.sortOrder) return a.sortOrder - b.sortOrder;
                            return String(a.label || '').localeCompare(String(b.label || ''), 'fi');
                        })
                    }));
            }
        },
        methods: {
            formatDateTime,
            dayBucket(ts) {
                if (!ts || !Number.isFinite(Number(ts))) return 'unknown-day';
                const date = new Date(Number(ts));
                if (Number.isNaN(date.getTime())) return 'unknown-day';
                const year = date.getFullYear();
                const month = String(date.getMonth() + 1).padStart(2, '0');
                const day = String(date.getDate()).padStart(2, '0');
                return `${year}-${month}-${day}`;
            },
            dayLabel(ts) {
                if (!ts || !Number.isFinite(Number(ts))) return 'Päivä avoin';
                const date = new Date(Number(ts));
                if (Number.isNaN(date.getTime())) return 'Päivä avoin';
                try {
                    return date.toLocaleDateString('fi-FI', {
                        weekday: 'long',
                        day: 'numeric',
                        month: 'long'
                    });
                } catch (error) {
                    return this.dayBucket(ts);
                }
            },
            weekBucket(ts) {
                if (!ts || !Number.isFinite(Number(ts))) return 'unknown';
                const date = new Date(Number(ts));
                if (Number.isNaN(date.getTime())) return 'unknown';
                const day = date.getDay();
                const mondayOffset = (day + 6) % 7;
                const monday = new Date(date);
                monday.setHours(0, 0, 0, 0);
                monday.setDate(monday.getDate() - mondayOffset);
                return monday.toISOString().slice(0, 10);
            },
            weekNumber(ts) {
                if (!ts || !Number.isFinite(Number(ts))) return null;
                const date = new Date(Number(ts));
                if (Number.isNaN(date.getTime())) return null;
                const utcDate = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
                const dayNum = utcDate.getUTCDay() || 7;
                utcDate.setUTCDate(utcDate.getUTCDate() + 4 - dayNum);
                const yearStart = new Date(Date.UTC(utcDate.getUTCFullYear(), 0, 1));
                const weekNo = Math.ceil((((utcDate - yearStart) / 86400000) + 1) / 7);
                return Number.isFinite(weekNo) ? weekNo : null;
            },
            weekLabel(ts) {
                const weekNo = this.weekNumber(ts);
                if (!weekNo) return 'Ajankohta avoin';
                return `Viikko ${weekNo}`;
            },
            separatorBucket(ts) {
                return this.separatorGranularity === 'day' ? this.dayBucket(ts) : this.weekBucket(ts);
            },
            separatorLabel(ts) {
                return this.separatorGranularity === 'day' ? this.dayLabel(ts) : this.weekLabel(ts);
            },
            hasWeekChange(index, match) {
                if (index === 0) return true;
                const previous = this.normalizedItems[index - 1];
                return this.separatorBucket(previous?.scheduledTs) !== this.separatorBucket(match?.scheduledTs);
            },
            resolveAvatar(src) {
                if (!src) return DEFAULT_TEAM_LOGO;
                try {
                    if (window.apiClient && typeof window.apiClient.proxyAvatar === 'function') {
                        const resolved = window.apiClient.proxyAvatar(src);
                        return resolved || DEFAULT_TEAM_LOGO;
                    }
                    return src || DEFAULT_TEAM_LOGO;
                } catch (error) {
                    return src || DEFAULT_TEAM_LOGO;
                }
            },
            divisionLabel(item) {
                if (!item) return '';
                const base = item.divisionName || (item.divisionNum != null ? `Divisioona ${item.divisionNum}` : 'Divisioona');
                return item.isPlayoffs ? `${base} (Playoffs)` : base;
            },
            divisionGroupKey(item) {
                if (!item) return 'division-unknown';
                return String(
                    item.championshipId ??
                    item.divisionSlug ??
                    item.divisionNum ??
                    item.divisionName ??
                    'division-unknown'
                );
            },
            divisionSortOrder(item) {
                if (!item || typeof item !== 'object') return Number.POSITIVE_INFINITY;
                const direct = Number(item.divisionNum);
                if (Number.isFinite(direct)) return direct;
                const label = this.divisionLabel(item);
                const match = String(label).match(/(\d+)/);
                if (match) {
                    const parsed = Number(match[1]);
                    if (Number.isFinite(parsed)) return parsed;
                }
                return Number.POSITIVE_INFINITY;
            },
            divisionRoute(item) {
                if (!item?.championshipId) return null;
                return {
                    name: 'division',
                    params: { championshipId: item.championshipId }
                };
            },
            divisionGroupRoute(divisionGroup) {
                if (!divisionGroup || !Array.isArray(divisionGroup.matches) || !divisionGroup.matches.length) return null;
                return this.divisionRoute(divisionGroup.matches[0]);
            },
            teamRoute(item, team) {
                if (!item?.championshipId || !team?.id) return null;
                return {
                    name: 'team-detail',
                    params: { championshipId: item.championshipId, teamId: team.id }
                };
            }
        },
        template: `
            <section class="upcoming-matches glass-card">
                <header v-if="showHeader" class="upcoming-matches__header">
                    <div>
                        <h2 class="title-accent titleUnderlineSection">{{ title }}</h2>
                        <p v-if="subtitle" class="upcoming-matches__subtitle">{{ subtitle }}</p>
                    </div>
                </header>

                <loading-spinner
                    v-if="loading"
                    message="Tulevia otteluita haetaan..."
                ></loading-spinner>
                <error-message
                    v-else-if="error"
                    :message="error"
                ></error-message>

                <div v-else>
                    <p v-if="!normalizedItems.length" class="upcoming-matches__empty">{{ emptyMessage }}</p>
                    <div v-else-if="groupByDayDivision" class="upcoming-matches__groups">
                        <section v-for="dayGroup in dayDivisionGroups" :key="dayGroup.key" class="upcoming-day-group">
                            <div class="upcoming-matches__week-separator upcoming-matches__week-separator--day">
                                <span>{{ dayGroup.label }}</span>
                            </div>
                            <div v-for="division in dayGroup.divisions" :key="dayGroup.key + '-' + division.key" class="upcoming-division-group">
                                <div class="upcoming-matches__week-separator upcoming-matches__week-separator--division">
                                    <router-link
                                        v-if="divisionGroupRoute(division)"
                                        :to="divisionGroupRoute(division)"
                                        class="upcoming-matches__group-link"
                                    >{{ division.label }}</router-link>
                                    <span v-else class="upcoming-matches__group-label">{{ division.label }}</span>
                                </div>
                                <ul class="upcoming-matches__list upcoming-matches__list--grouped">
                                    <li v-for="(match, index) in division.matches" :key="'match-' + (match.matchId || (dayGroup.key + '-' + division.key + '-' + index))" class="upcoming-match-card">
                                        <div class="upcoming-match-card__teams">
                                            <div class="upcoming-match-card__team upcoming-match-card__team--left">
                                                <img :src="resolveAvatar(match.team1.avatar)" :alt="match.team1.name" class="upcoming-match-card__logo">
                                                <router-link
                                                    v-if="teamRoute(match, match.team1)"
                                                    :to="teamRoute(match, match.team1)"
                                                    class="upcoming-match-card__team-name"
                                                >{{ match.team1.name }}</router-link>
                                                <span v-else class="upcoming-match-card__team-name">{{ match.team1.name }}</span>
                                            </div>
                                            <span class="upcoming-match-card__vs">vs</span>
                                            <div class="upcoming-match-card__team upcoming-match-card__team--right">
                                                <img :src="resolveAvatar(match.team2.avatar)" :alt="match.team2.name" class="upcoming-match-card__logo">
                                                <router-link
                                                    v-if="teamRoute(match, match.team2)"
                                                    :to="teamRoute(match, match.team2)"
                                                    class="upcoming-match-card__team-name"
                                                >{{ match.team2.name }}</router-link>
                                                <span v-else class="upcoming-match-card__team-name">{{ match.team2.name }}</span>
                                            </div>
                                        </div>
                                        <div class="upcoming-match-card__actions">
                                            <div class="upcoming-match-card__date">{{ formatDateTime(match.scheduledTs) }}</div>
                                            <a
                                                v-if="showFaceit && match.faceitUrl"
                                                class="upcoming-match-card__link btn-secondary"
                                                :href="match.faceitUrl"
                                                target="_blank"
                                                rel="noopener"
                                            >Faceit Lobby</a>
                                        </div>
                                    </li>
                                </ul>
                            </div>
                        </section>
                    </div>
                    <ul v-else class="upcoming-matches__list">
                        <template v-for="(match, index) in normalizedItems">
                            <li
                                v-if="showWeekSeparators && hasWeekChange(index, match)"
                                :key="'separator-' + separatorBucket(match.scheduledTs) + '-' + index"
                                class="upcoming-matches__week-separator"
                            >
                                <span>{{ separatorLabel(match.scheduledTs) }}</span>
                            </li>
                            <li :key="'match-' + (match.matchId || index)" class="upcoming-match-card">
                            <div class="upcoming-match-card__teams">
                                <div class="upcoming-match-card__team upcoming-match-card__team--left">
                                    <img :src="resolveAvatar(match.team1.avatar)" :alt="match.team1.name" class="upcoming-match-card__logo">
                                    <router-link
                                        v-if="teamRoute(match, match.team1)"
                                        :to="teamRoute(match, match.team1)"
                                        class="upcoming-match-card__team-name"
                                    >{{ match.team1.name }}</router-link>
                                    <span v-else class="upcoming-match-card__team-name">{{ match.team1.name }}</span>
                                </div>
                                <span class="upcoming-match-card__vs">vs</span>
                                <div class="upcoming-match-card__team upcoming-match-card__team--right">
                                    <img :src="resolveAvatar(match.team2.avatar)" :alt="match.team2.name" class="upcoming-match-card__logo">
                                    <router-link
                                        v-if="teamRoute(match, match.team2)"
                                        :to="teamRoute(match, match.team2)"
                                        class="upcoming-match-card__team-name"
                                    >{{ match.team2.name }}</router-link>
                                    <span v-else class="upcoming-match-card__team-name">{{ match.team2.name }}</span>
                                </div>
                            </div>

                            <div v-if="showDivision" class="upcoming-match-card__division">
                                <router-link v-if="divisionRoute(match)" :to="divisionRoute(match)" class="upcoming-match-card__division-link">
                                    {{ divisionLabel(match) }}
                                </router-link>
                                <span v-else class="upcoming-match-card__division-link">{{ divisionLabel(match) }}</span>
                            </div>

                            <div class="upcoming-match-card__actions">
                                <div class="upcoming-match-card__date">{{ formatDateTime(match.scheduledTs) }}</div>
                                <a
                                    v-if="showFaceit && match.faceitUrl"
                                    class="upcoming-match-card__link btn-secondary"
                                    :href="match.faceitUrl"
                                    target="_blank"
                                    rel="noopener"
                                >Faceit Lobby</a>
                            </div>
                            </li>
                        </template>
                    </ul>
                </div>
            </section>
        `
    };
})();
