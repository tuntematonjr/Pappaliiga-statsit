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
            }
        },
        actions: {
            ensureTeamEntry(teamId) {
                if (!teamId) return null;
                if (!this.teams[teamId]) {
                    this.teams[teamId] = {
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

                if (entry.page.loading && entry.page.promise) {
                    try {
                        await entry.page.promise;
                    } catch (_error) {
                    }
                }
                if (!force && isFresh(entry.page, targetChampionship)) {
                    return entry.page.data;
                }

                entry.page.loading = true;
                entry.page.error = null;
                entry.page.promise = (async () => {
                    try {
                        const data = await window.apiClient.getTeamPage(teamId, targetChampionship);
                        entry.page.data = data || {};
                        entry.page.fetchedAt = now();
                        const resolvedChampionship =
                            data?.currentChampionshipId
                            || data?.current_championship_id
                            || targetChampionship
                            || null;
                        entry.selectedChampionship = resolvedChampionship;
                        return entry.page.data;
                    } catch (error) {
                        entry.page.error = error?.message || 'Joukkuesivun lataus epäonnistui';
                        throw error;
                    } finally {
                        entry.page.loading = false;
                        entry.page.promise = null;
                    }
                })();

                return entry.page.promise;
            }
        }
    });
})();

