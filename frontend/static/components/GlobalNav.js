// GlobalNav.js - Context-aware breadcrumb navigation
(function () {
    const { computed } = Vue;
    const { useRoute } = VueRouter;

    window.GlobalNav = {
        name: 'GlobalNav',
        template: `
            <div class="global-nav" v-if="breadcrumbs.length > 0">
                <div class="layout-boundary">
                    <div class="breadcrumbs">
                        <template v-for="(crumb, index) in breadcrumbs" :key="crumb.key">
                            <router-link 
                                v-if="!crumb.disabled"
                                :to="crumb.to" 
                                class="breadcrumb-link"
                                :class="{ 'breadcrumb-link--active': index === breadcrumbs.length - 1 }">
                                <span class="breadcrumb-icon" v-if="crumb.icon">{{ crumb.icon }}</span>
                                <span class="breadcrumb-text">{{ crumb.label }}</span>
                            </router-link>
                            <span v-else class="breadcrumb-current">
                                <span class="breadcrumb-icon" v-if="crumb.icon">{{ crumb.icon }}</span>
                                <span class="breadcrumb-text">{{ crumb.label }}</span>
                            </span>
                            <span v-if="index < breadcrumbs.length - 1" class="breadcrumb-separator">›</span>
                        </template>
                    </div>
                </div>
            </div>
        `,
        setup() {
            const route = useRoute();
            const divisionStore = typeof window.useDivisionStore === 'function' ? window.useDivisionStore() : null;
            const teamStore = typeof window.useTeamStore === 'function' ? window.useTeamStore() : null;
            const playerStore = typeof window.usePlayerStore === 'function' ? window.usePlayerStore() : null;
            const seasonsStore = typeof window.useSeasonsStore === 'function' ? window.useSeasonsStore() : null;

            function beautifyDivisionLabel(value) {
                if (!value) return '';
                const text = String(value).replace(/[-_]+/g, ' ').trim();
                if (!text) return '';
                return text.charAt(0).toUpperCase() + text.slice(1);
            }

            function looksLikeRawId(value) {
                const text = String(value || '').trim();
                if (!text) return false;
                // Championship IDs are UUID-like; do not surface them as breadcrumb labels.
                return /^[0-9a-f]{8}(?:[-\s]?[0-9a-f]{4}){3}[-\s]?[0-9a-f]{12}$/i.test(text)
                    || /^[0-9a-f-]{24,}$/i.test(text);
            }

            function formatSeasonLabel(value) {
                if (value === null || value === undefined) return '';
                const text = String(value).trim();
                if (!text) return '';
                if (/^kausi\s+/i.test(text)) return text;
                if (/^s\d+/i.test(text)) return text.toUpperCase();
                return `Kausi ${text}`;
            }

            function normalizeIdLike(value) {
                return String(value || '').replace(/[^a-z0-9]/gi, '').toLowerCase();
            }

            function looksLikeChampionshipIdLabel(label, championshipId) {
                const normalizedLabel = normalizeIdLike(label);
                const normalizedId = normalizeIdLike(championshipId);
                return Boolean(normalizedLabel) && Boolean(normalizedId) && normalizedLabel === normalizedId;
            }

            function formatDivisionBreadcrumbLabel(name, seasonValue, isPlayoffs = false, divisionNum = null) {
                const normalizer = typeof window !== 'undefined' ? window.divisionNormalizer : null;
                if (normalizer?.buildDivisionBreadcrumbMeta) {
                    return normalizer.buildDivisionBreadcrumbMeta({
                        name,
                        season: seasonValue,
                        isPlayoffs,
                        divisionNum
                    }).label;
                }
                const safeName = String(name || '').trim() || 'Divisioona';
                const seasonLabel = seasonValue ? formatSeasonLabel(seasonValue) : '';
                const base = seasonLabel ? `${safeName} (${seasonLabel})` : safeName;
                if (isPlayoffs && !/playoffs?/i.test(base)) return `${base} (Playoffs)`;
                return base;
            }

            function readTeamContext(params) {
                const teamId = params?.teamId;
                if (!teamStore || !teamId || typeof teamStore.getTeamState !== 'function') return null;
                const entry = teamStore.getTeamState(teamId);
                if (!entry) return null;
                const page = entry?.page?.data || null;
                const seasons = Array.isArray(page?.seasons) ? page.seasons : [];
                const selectedChampionshipId =
                    params?.championshipId
                    || entry?.selectedChampionship
                    || page?.currentChampionshipId
                    || null;
                const seasonRow = selectedChampionshipId
                    ? (seasons.find(season => String(season?.championshipId || '') === String(selectedChampionshipId)) || null)
                    : null;
                return {
                    page,
                    seasonRow,
                    selectedChampionshipId: selectedChampionshipId ? String(selectedChampionshipId) : null
                };
            }

            function resolveDivisionSeason(params) {
                const lookupKey = params?.championshipId;
                if (divisionStore && lookupKey && typeof divisionStore.getDivisionState === 'function') {
                    const entry = divisionStore.getDivisionState(lookupKey);
                    const season = entry?.details?.data?.season;
                    if (season != null) return String(season);
                }
                const teamContext = readTeamContext(params);
                if (teamContext?.seasonRow?.season != null) {
                    return String(teamContext.seasonRow.season);
                }
                return '';
            }

            function resolveDivisionName(params) {
                const lookupKey = params?.championshipId;
                if (divisionStore && lookupKey && typeof divisionStore.getDivisionState === 'function') {
                    const entry = divisionStore.getDivisionState(lookupKey);
                    const name = entry?.details?.data?.name;
                    if (name) return String(name);
                }
                const teamContext = readTeamContext(params);
                if (teamContext?.seasonRow) {
                    const row = teamContext.seasonRow;
                    if (row?.name) return String(row.name);
                    const divisionNum = row?.divisionNum;
                    if (divisionNum != null) {
                        const normalizer = typeof window !== 'undefined' ? window.divisionNormalizer : null;
                        if (normalizer?.buildDivisionBreadcrumbMeta) {
                            return normalizer.buildDivisionBreadcrumbMeta({
                                name: null,
                                divisionNum,
                                season: row?.season,
                                isPlayoffs: Boolean(row?.isPlayoffs)
                            }).name;
                        }
                        return `${divisionNum} Divisioona`;
                    }
                }
                if (lookupKey) {
                    if (looksLikeRawId(lookupKey)) {
                        return '';
                    }
                    return beautifyDivisionLabel(lookupKey);
                }
                return 'Divisioona';
            }

            function resolveTeamName(params) {
                const teamId = params?.teamId;
                if (teamStore && teamId && typeof teamStore.getTeamState === 'function') {
                    const entry = teamStore.getTeamState(teamId);
                    const team = entry?.page?.data?.team || null;
                    const name =
                        team?.displayName ||
                        team?.display_name ||
                        team?.teamName ||
                        team?.team_name ||
                        team?.name ||
                        null;
                    if (name) return String(name);
                }
                if (teamId) {
                    return `Joukkue ${teamId}`;
                }
                return 'Joukkue';
            }

            function resolveCurrentSeasonLabel() {
                if (!seasonsStore) return '';
                const current = seasonsStore.currentSeason || seasonsStore.seasons?.find(season => season?.isActive) || seasonsStore.newestSeason || null;
                if (!current) return '';
                const label = current.label || current.shortLabel || null;
                if (label) return label;
                const seasonId = current.id ?? current.seasonNumber ?? null;
                return seasonId != null ? `Kausi ${seasonId}` : '';
            }

            function readPlayerContext(playerId, preferredChampionshipId = null) {
                if (!playerStore || !playerId || typeof playerStore.getPlayerState !== 'function') return null;
                const entry = playerStore.getPlayerState(playerId);
                if (!entry) return null;
                const profile = entry?.profile?.data || null;
                const seasons = Array.isArray(entry?.seasons?.data) ? entry.seasons.data : [];
                const defaultBundle = entry?.bundle?.__default__?.data || null;
                const selectedChampionshipIdRaw =
                    preferredChampionshipId
                    || defaultBundle?.selected_championship_id
                    || defaultBundle?.selectedChampionshipId
                    || '';
                const selectedChampionshipId = String(selectedChampionshipIdRaw || '');
                const selectedSeason = selectedChampionshipId
                    ? (seasons.find(season =>
                        String(
                            season?.championship_id
                            || season?.championshipId
                            || season?.id
                            || ''
                        ) === selectedChampionshipId
                    ) || null)
                    : (seasons[0] || null);
                return { profile, selectedSeason, selectedChampionshipId };
            }

            function resolvePlayerName(params) {
                const playerId = params?.playerId;
                const context = readPlayerContext(playerId, params?.championshipId || null);
                const nickname =
                    context?.profile?.nickname
                    || context?.profile?.name
                    || context?.profile?.player_name
                    || null;
                return nickname ? String(nickname) : `Pelaaja ${playerId}`;
            }

            function resolvePlayerTeamContext(params) {
                const playerId = params?.playerId;
                const context = readPlayerContext(playerId, params?.championshipId || null);
                const seasonRow = context?.selectedSeason || null;
                const championshipId =
                    params?.championshipId
                    || seasonRow?.championship_id
                    || seasonRow?.championshipId
                    || seasonRow?.id
                    || context?.selectedChampionshipId
                    || null;
                const teamId =
                    seasonRow?.team_id
                    || seasonRow?.teamId
                    || null;
                const teamName =
                    seasonRow?.team_name
                    || seasonRow?.teamName
                    || seasonRow?.team
                    || null;
                const season = seasonRow?.season != null ? String(seasonRow.season) : null;
                const division = seasonRow?.division_num != null ? String(seasonRow.division_num) : null;
                const isPlayoffs = Boolean(
                    seasonRow?.is_playoffs
                    || seasonRow?.isPlayoffs
                    || seasonRow?.is_playoff
                );
                return {
                    championshipId: championshipId ? String(championshipId) : null,
                    teamId: teamId ? String(teamId) : null,
                    teamName: teamName ? String(teamName) : null,
                    season,
                    division,
                    isPlayoffs
                };
            }

            const breadcrumbs = computed(() => {
                const crumbs = [];
                const routeName = route.name;
                const params = route.params;

                // Always add home first
                crumbs.push({
                    key: 'home',
                    label: 'Etusivu',
                    icon: '🏠',
                    to: { name: 'home' },
                    disabled: routeName === 'home'
                });

                if (routeName === 'season-upcoming') {
                    const seasonLabel = resolveCurrentSeasonLabel();
                    crumbs.push({
                        key: 'season-upcoming',
                        label: seasonLabel ? `Tulevat ottelut (${seasonLabel})` : 'Tulevat ottelut',
                        icon: '📅',
                        to: { name: 'season-upcoming' },
                        disabled: true
                    });
                }

                // Division/Championship context
                if (params.championshipId) {
                    const teamContext = readTeamContext(params);
                    const teamSeason = teamContext?.seasonRow || null;
                    const championshipName = resolveDivisionName(params);
                    const seasonValue = resolveDivisionSeason(params);
                    const isPlayoffs = Boolean(teamSeason?.isPlayoffs);
                    const fullLabel = formatDivisionBreadcrumbLabel(
                        championshipName,
                        seasonValue,
                        isPlayoffs,
                        teamSeason?.divisionNum ?? null
                    );
                    
                    crumbs.push({
                        key: 'division',
                        label: fullLabel,
                        icon: '🏆',
                        to: { 
                            name: 'division', 
                            params: { championshipId: params.championshipId }
                        },
                        disabled: routeName === 'division'
                    });
                }

                // Team context
                if (params.teamId) {
                    const teamName = resolveTeamName(params);
                    
                    // If championship isn't in params, derive it from current team context.
                    const teamContext = readTeamContext(params);
                    if (!params.championshipId && teamContext?.selectedChampionshipId) {
                        const championshipIdFromQuery = teamContext.selectedChampionshipId;
                        const championshipName = resolveDivisionName({ championshipId: championshipIdFromQuery });
                        const seasonValue = resolveDivisionSeason({ championshipId: championshipIdFromQuery });
                        const isPlayoffs = Boolean(teamContext?.seasonRow?.isPlayoffs);
                        const fullLabel = formatDivisionBreadcrumbLabel(championshipName, seasonValue, isPlayoffs);
                        
                        crumbs.push({
                            key: 'division',
                            label: fullLabel,
                            icon: '🏆',
                            to: { 
                                name: 'division', 
                                params: { championshipId: championshipIdFromQuery }
                            },
                            disabled: false
                        });
                    }
                    
                    // Add team breadcrumb
                    crumbs.push({
                        key: 'team',
                        label: teamName,
                        icon: '👥',
                        to: params.championshipId 
                            ? { 
                                name: 'team-detail', 
                                params: { 
                                    championshipId: params.championshipId,
                                    teamId: params.teamId 
                                }
                            }
                            : { 
                                name: 'team', 
                                params: { teamId: params.teamId }
                            },
                        disabled: routeName === 'team' || routeName === 'team-detail'
                    });
                }

                // Player context
                if (params.playerId && (routeName === 'player' || routeName === 'player-detail')) {
                    const playerName = resolvePlayerName(params);
                    const playerContext = resolvePlayerTeamContext(params);
                    const isPlayoffs = playerContext.isPlayoffs;

                    if (playerContext.championshipId) {
                        const championshipName = resolveDivisionName({ championshipId: playerContext.championshipId });
                        const championshipLooksLikeId = looksLikeChampionshipIdLabel(championshipName, playerContext.championshipId);
                        const seasonValue = resolveDivisionSeason({ championshipId: playerContext.championshipId }) || playerContext.season;
                        const fullLabel = formatDivisionBreadcrumbLabel(
                            championshipLooksLikeId ? null : championshipName,
                            seasonValue,
                            isPlayoffs,
                            playerContext.division
                        );
                        crumbs.push({
                            key: 'division',
                            label: fullLabel,
                            icon: '🏆',
                            to: {
                                name: 'division',
                                params: { championshipId: playerContext.championshipId }
                            },
                            disabled: false
                        });
                    }

                    if (playerContext.teamId || playerContext.teamName) {
                        const teamName = playerContext.teamName || `Joukkue ${playerContext.teamId || ''}`.trim();
                        const teamTo = playerContext.teamId
                            ? (playerContext.championshipId
                                ? {
                                    name: 'team-detail',
                                    params: {
                                        championshipId: playerContext.championshipId,
                                        teamId: playerContext.teamId
                                    }
                                }
                                : {
                                    name: 'team',
                                    params: { teamId: playerContext.teamId }
                                })
                            : null;
                        crumbs.push({
                            key: 'team',
                            label: teamName,
                            icon: '👥',
                            to: teamTo || { name: 'home' },
                            disabled: !teamTo
                        });
                    }

                    crumbs.push({
                        key: 'player',
                        label: playerName,
                        icon: '🎮',
                        to: playerContext.championshipId
                            ? {
                                name: 'player-detail',
                                params: {
                                    championshipId: playerContext.championshipId,
                                    playerId: params.playerId
                                }
                            }
                            : { 
                                name: 'player', 
                                params: { playerId: params.playerId }
                            },
                        disabled: true
                    });
                }

                // Seasons page
                if (routeName === 'seasons') {
                    crumbs.push({
                        key: 'seasons',
                        label: 'Kaudet',
                        icon: '📅',
                        to: { name: 'seasons' },
                        disabled: true
                    });
                }

                return crumbs;
            });

            return {
                breadcrumbs
            };
        }
    };
})();
