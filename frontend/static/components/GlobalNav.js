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
                if (params?.championshipId) {
                    return beautifyDivisionLabel(params.championshipId);
                }
                return 'Divisioona';
            }

            const breadcrumbs = computed(() => {
                const crumbs = [];
                const routeName = route.name;
                const params = route.params;
                const query = route.query;

                // Always add home
                crumbs.push({
                    key: 'home',
                    label: 'Etusivu',
                    icon: '🏠',
                    to: { name: 'home' },
                    disabled: routeName === 'home'
                });

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
                    const teamName = query.team_name || `Joukkue ${params.teamId}`;
                    
                    // If we have both championship and team, the division breadcrumb is already added above
                    // Just add the team
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
                                    ...(query.team_name ? { team_name: query.team_name } : {})
                                }
                            }
                            : { 
                                name: 'team', 
                                params: { teamId: params.teamId },
                                query: query.team_name ? { team_name: query.team_name } : {}
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
