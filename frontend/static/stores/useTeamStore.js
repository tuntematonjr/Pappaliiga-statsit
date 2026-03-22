(function () {
    const { defineStore } = Pinia;

    const FRESH_MS = 5 * 60 * 1000;

    function now() {
        return Date.now();
    }

    function createSegment() {
        return {
            data: null,
            loading: false,
            error: null,
            fetchedAt: null,
            promise: null
        };
    }

    function cacheKeyFor(teamId, championshipId) {
        const teamPart = String(teamId || '');
        const champPart = championshipId ? String(championshipId) : 'auto';
        return `${teamPart}::${champPart}`;
    }

    function isFresh(segment, championshipId) {
        if (!segment || !segment.fetchedAt || segment.error) {
            return false;
        }
        const fresh = now() - segment.fetchedAt < FRESH_MS;
        if (!fresh) return false;
        if (championshipId) {
            const resolvedChampionship =
                segment?.data?.currentChampionshipId
                || segment?.data?.current_championship_id
                || null;
            if (!resolvedChampionship) {
                return false;
            }
            return String(resolvedChampionship) === String(championshipId);
        }
        return fresh;
    }

    window.useTeamStore = defineStore('team', {
        state: () => ({
            teams: {}
        }),
        getters: {
            getTeamState(state) {
                return teamId => {
                    if (!teamId) return null;
                    return state.teams[teamId] || null;
                };
            },
            getTeamPageSegment(state) {
                return (teamId, championshipId = null) => {
                    if (!teamId) return null;
                    const entry = state.teams[teamId] || null;
                    if (!entry) return null;
                    const key = cacheKeyFor(teamId, championshipId);
                    return entry.pages?.[key] || entry.page || null;
                };
            }
        },
        actions: {
            ensureTeamEntry(teamId) {
                if (!teamId) return null;
                if (!this.teams[teamId]) {
                    this.teams[teamId] = {
                        pages: {},
                        page: createSegment(),
                        selectedChampionship: null
                    };
                }
                return this.teams[teamId];
            },
            async fetchTeamPage(teamId, championshipId, options = {}) {
                if (!teamId) return null;
                const entry = this.ensureTeamEntry(teamId);
                const { force = false } = options;
                const targetChampionship = championshipId || entry.selectedChampionship || null;
                const key = cacheKeyFor(teamId, targetChampionship);
                if (!entry.pages[key]) {
                    entry.pages[key] = createSegment();
                }
                const segment = entry.pages[key];
                entry.page = segment;

                if (segment.loading && segment.promise) {
                    try {
                        await segment.promise;
                    } catch (_error) {
                    }
                }
                if (!force && isFresh(segment, targetChampionship)) {
                    return segment.data;
                }

                segment.loading = true;
                segment.error = null;
                segment.promise = (async () => {
                    try {
                        const data = await window.apiClient.getTeamPage(teamId, targetChampionship);
                        segment.data = data || {};
                        segment.fetchedAt = now();
                        const resolvedChampionship =
                            data?.currentChampionshipId
                            || data?.current_championship_id
                            || targetChampionship
                            || null;
                        entry.selectedChampionship = resolvedChampionship;
                        if (resolvedChampionship) {
                            const resolvedKey = cacheKeyFor(teamId, resolvedChampionship);
                            entry.pages[resolvedKey] = segment;
                        }
                        entry.page = segment;
                        return segment.data;
                    } catch (error) {
                        segment.error = error?.message || 'Joukkuesivun lataus epäonnistui';
                        throw error;
                    } finally {
                        segment.loading = false;
                        segment.promise = null;
                    }
                })();

                return segment.promise;
            }
        }
    });
})();

