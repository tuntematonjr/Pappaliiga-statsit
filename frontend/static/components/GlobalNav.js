// GlobalNav.js - Context-aware breadcrumb navigation
(function () {
    const { computed } = Vue;
    const { useRoute, useRouter } = VueRouter;

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
            const router = useRouter();
            const divisionStore = typeof window.useDivisionStore === 'function' ? window.useDivisionStore() : null;
            const teamStore = typeof window.useTeamStore === 'function' ? window.useTeamStore() : null;
            const seasonsStore = typeof window.useSeasonsStore === 'function' ? window.useSeasonsStore() : null;

            function beautifyDivisionLabel(value) {
                if (!value) return '';
                const text = String(value).replace(/[-_]+/g, ' ').trim();
                if (!text) return '';
                return text.charAt(0).toUpperCase() + text.slice(1);
            }

            function formatSeasonLabel(value) {
                if (value === null || value === undefined) return '';
                const text = String(value).trim();
                if (!text) return '';
                if (/^kausi\s+/i.test(text)) return text;
                if (/^s\d+/i.test(text)) return text.toUpperCase();
                return `Kausi ${text}`;
            }

            function resolveDivisionSeason(params, query) {
                if (query?.championship_season) {
                    return String(query.championship_season);
                }
                const lookupKey = query?.championship || params?.championshipId;
                if (divisionStore && lookupKey && typeof divisionStore.getDivisionState === 'function') {
                    const entry = divisionStore.getDivisionState(lookupKey);
                    const season = entry?.details?.data?.season;
                    if (season != null) return String(season);
                }
                return '';
            }

            function resolveDivisionName(params, query) {
                if (query?.championship_name) {
                    return String(query.championship_name);
                }
                const lookupKey = query?.championship || params?.championshipId;
                if (divisionStore && lookupKey && typeof divisionStore.getDivisionState === 'function') {
                    const entry = divisionStore.getDivisionState(lookupKey);
                    const name = entry?.details?.data?.name;
                    if (name) return String(name);
                }
                if (lookupKey) {
                    return beautifyDivisionLabel(lookupKey);
                }
                return 'Divisioona';
            }

            function resolveTeamName(params, query) {
                if (query?.team_name) {
                    return String(query.team_name);
                }
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

            function resolveTeamSeason(params, query) {
                if (query?.championship_season) {
                    return String(query.championship_season);
                }
                const teamId = params?.teamId;
                if (teamStore && teamId && typeof teamStore.getTeamState === 'function') {
                    const entry = teamStore.getTeamState(teamId);
                    const page = entry?.page?.data || null;
                    const selected = entry?.selectedChampionship || page?.currentChampionshipId || query?.championship || null;
                    const seasons = Array.isArray(page?.seasons) ? page.seasons : [];
                    if (selected && seasons.length) {
                        const match = seasons.find(season => String(season.championshipId) === String(selected));
                        if (match?.season != null) {
                            return String(match.season);
                        }
                    }
                }
                return '';
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

            const breadcrumbs = computed(() => {
                const crumbs = [];
                const routeName = route.name;
                const params = route.params;
                const query = route.query;

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
                    const championshipName = resolveDivisionName(params, query);
                    const seasonValue = resolveDivisionSeason(params, query);
                    const seasonLabel = seasonValue ? formatSeasonLabel(seasonValue) : '';
                    const fullLabel = seasonLabel ? `${championshipName} (${seasonLabel})` : championshipName;
                    const isPlayoffs = routeName === 'division-playoffs';
                    
                    crumbs.push({
                        key: 'division',
                        label: isPlayoffs ? `${fullLabel} (Playoffs)` : fullLabel,
                        icon: '🏆',
                        to: { 
                            name: isPlayoffs ? 'division-playoffs' : 'division', 
                            params: { championshipId: params.championshipId },
                            query: {
                                ...(query.championship ? { championship: query.championship } : {}),
                                ...(query.championship_name ? { championship_name: query.championship_name } : {}),
                                ...(query.championship_season ? { championship_season: query.championship_season } : {})
                            }
                        },
                        disabled: routeName === 'division' || routeName === 'division-playoffs'
                    });
                }

                // Team context
                if (params.teamId) {
                    const teamName = resolveTeamName(params, query);
                    
                    // If championship isn't in params but we have it in query or store, add division breadcrumb
                    if (!params.championshipId && (query?.championship || query?.team_championship)) {
                        const championshipIdFromQuery = query.championship || query.team_championship;
                        const championshipName = resolveDivisionName({ championshipId: championshipIdFromQuery }, query);
                        const seasonValue = resolveDivisionSeason({ championshipId: championshipIdFromQuery }, query);
                        const seasonLabel = seasonValue ? formatSeasonLabel(seasonValue) : '';
                        const fullLabel = seasonLabel ? `${championshipName} (${seasonLabel})` : championshipName;
                        
                        crumbs.push({
                            key: 'division',
                            label: fullLabel,
                            icon: '🏆',
                            to: { 
                                name: 'division', 
                                params: { championshipId: championshipIdFromQuery },
                                query: {
                                    ...(query.championship ? { championship: query.championship } : {}),
                                    ...(query.championship_name ? { championship_name: query.championship_name } : {}),
                                    ...(query.championship_season ? { championship_season: query.championship_season } : {})
                                }
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
                                },
                                query: {
                                    ...(query.championship ? { championship: query.championship } : {}),
                                    ...(query.championship_name ? { championship_name: query.championship_name } : {}),
                                    ...(query.championship_season ? { championship_season: query.championship_season } : {}),
                                    ...(query.team_name ? { team_name: query.team_name } : {}),
                                    ...(teamName ? { team_name: teamName } : {})
                                }
                            }
                            : { 
                                name: 'team', 
                                params: { teamId: params.teamId },
                                query: teamName ? { team_name: teamName } : {}
                            },
                        disabled: routeName === 'team' || routeName === 'team-detail'
                    });
                }

                // Player context
                if (params.playerId && routeName === 'player') {
                    const playerName = query.player_name || `Pelaaja ${params.playerId}`;
                    crumbs.push({
                        key: 'player',
                        label: playerName,
                        icon: '🎮',
                        to: { 
                            name: 'player', 
                            params: { playerId: params.playerId },
                            query: query.player_name ? { player_name: query.player_name } : {}
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
