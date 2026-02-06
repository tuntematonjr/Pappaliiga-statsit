(function () {
    const DEFAULT_TEAM_LOGO = window.PAPPALIIGA_DEFAULT_LOGO;

    function coerceEpochMs(value) {
        if (value === null || value === undefined || value === 0) return null;
        const numeric = Number(value);
        if (!Number.isFinite(numeric) || numeric <= 0) return null;
        return Math.abs(numeric) < 1_000_000_000_000 ? numeric * 1000 : numeric;
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
        const scheduledRaw = raw.scheduled_ts ?? raw.scheduledTs ?? raw.scheduled_at ?? raw.scheduledAt ?? raw.ts ?? null;
        const scheduledTs = coerceEpochMs(scheduledRaw);
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

    function statusLabel(status) {
        if (!status) return 'Tulossa';
        const value = String(status).toLowerCase();
        if (['scheduled', 'configured', 'pending', 'ready'].includes(value)) return 'Tulossa';
        if (['live', 'ongoing', 'in_progress', 'started'].includes(value)) return 'Käynnissä';
        if (['finished', 'closed', 'over', 'completed'].includes(value)) return 'Pelattu';
        return 'Tulossa';
    }

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
            showHeader: { type: Boolean, default: true }
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
            }
        },
        methods: {
            formatDateTime,
            statusLabel,
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
            divisionRoute(item) {
                if (!item?.championshipId) return null;
                return {
                    name: item.isPlayoffs ? 'division-playoffs' : 'division',
                    params: { championshipId: item.championshipId },
                    query: {
                        championship: item.championshipId,
                        ...(item.divisionName ? { championship_name: item.divisionName } : {}),
                        ...(item.season != null ? { championship_season: item.season } : {})
                    }
                };
            },
            teamRoute(item, team) {
                if (!item?.championshipId || !team?.id) return null;
                return {
                    name: 'team-detail',
                    params: { championshipId: item.championshipId, teamId: team.id },
                    query: {
                        championship: item.championshipId,
                        ...(item.divisionName ? { championship_name: item.divisionName } : {}),
                        ...(item.season != null ? { championship_season: item.season } : {}),
                        ...(team.name ? { team_name: team.name } : {})
                    }
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
                    <div class="upcoming-matches__count" v-if="normalizedItems.length">
                        {{ normalizedItems.length }} ottelua
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
                    <ul v-else class="upcoming-matches__list">
                        <li v-for="match in normalizedItems" :key="match.matchId" class="upcoming-match-card">
                            <div class="upcoming-match-card__meta">
                                <div class="upcoming-match-card__date">{{ formatDateTime(match.scheduledTs) }}</div>
                                <div class="upcoming-match-card__status">{{ statusLabel(match.status) }}</div>
                            </div>

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

                            <a
                                v-if="showFaceit && match.faceitUrl"
                                class="upcoming-match-card__link btn-secondary"
                                :href="match.faceitUrl"
                                target="_blank"
                                rel="noopener"
                            >Faceit Linkki</a>
                        </li>
                    </ul>
                </div>
            </section>
        `
    };
})();
