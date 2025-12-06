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
            fetchedAt: null
        };
    }

    function isFresh(segment) {
        return !!(segment && segment.fetchedAt && now() - segment.fetchedAt < FRESH_MS && !segment.error);
    }

    function ensureSeasonEntry(entry, championshipId) {
        if (!entry.seasons) {
            entry.seasons = {};
        }
        if (!entry.seasons[championshipId]) {
            entry.seasons[championshipId] = {
                details: createSegment(),
                mapStats: createSegment(),
                matches: createSegment()
            };
        }
        return entry.seasons[championshipId];
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
                        profile: createSegment(),
                        seasonsList: createSegment(),
                        seasons: {}
                    };
                }
                return this.teams[teamId];
            },
            async fetchTeamProfile(teamId, options = {}) {
                if (!teamId) return null;
                const entry = this.ensureTeamEntry(teamId);
                const { force = false } = options;
                if (entry.profile.loading) {
                    return entry.profile.data;
                }
                if (!force && isFresh(entry.profile)) {
                    return entry.profile.data;
                }
                entry.profile.loading = true;
                entry.profile.error = null;
                try {
                    const data = await window.apiClient.getTeamInfo(teamId);
                    entry.profile.data = data;
                    entry.profile.fetchedAt = now();
                    return data;
                } catch (error) {
                    entry.profile.error = error?.message || 'Joukkueen tietojen lataus epäonnistui';
                    throw error;
                } finally {
                    entry.profile.loading = false;
                }
            },
            async fetchTeamSeasons(teamId, options = {}) {
                if (!teamId) return [];
                const entry = this.ensureTeamEntry(teamId);
                const { force = false } = options;
                if (entry.seasonsList.loading) {
                    return entry.seasonsList.data || [];
                }
                if (!force && isFresh(entry.seasonsList)) {
                    return entry.seasonsList.data || [];
                }
                entry.seasonsList.loading = true;
                entry.seasonsList.error = null;
                try {
                    const data = await window.apiClient.getTeamSeasons(teamId);
                    entry.seasonsList.data = Array.isArray(data) ? data : [];
                    entry.seasonsList.fetchedAt = now();
                    return entry.seasonsList.data;
                } catch (error) {
                    entry.seasonsList.error = error?.message || 'Joukkueen kausien lataus epäonnistui';
                    throw error;
                } finally {
                    entry.seasonsList.loading = false;
                }
            },
            async fetchSeasonDetails(teamId, championshipId, options = {}) {
                if (!teamId || !championshipId) return null;
                const entry = this.ensureTeamEntry(teamId);
                const seasonEntry = ensureSeasonEntry(entry, championshipId);
                const { force = false } = options;
                if (seasonEntry.details.loading) {
                    return seasonEntry.details.data;
                }
                if (!force && isFresh(seasonEntry.details)) {
                    return seasonEntry.details.data;
                }
                seasonEntry.details.loading = true;
                seasonEntry.details.error = null;
                try {
                    // API expects (teamId, championshipId)
                    const data = await window.apiClient.getTeamDetails(teamId, championshipId);
                    seasonEntry.details.data = data;
                    seasonEntry.details.fetchedAt = now();
                    return data;
                } catch (error) {
                    seasonEntry.details.error = error?.message || 'Joukkueen kausitietojen lataus epäonnistui';
                    throw error;
                } finally {
                    seasonEntry.details.loading = false;
                }
            },
            async fetchSeasonMapStats(teamId, championshipId, options = {}) {
                if (!teamId || !championshipId) return [];
                const entry = this.ensureTeamEntry(teamId);
                const seasonEntry = ensureSeasonEntry(entry, championshipId);
                const { force = false } = options;
                if (seasonEntry.mapStats.loading) {
                    return seasonEntry.mapStats.data || [];
                }
                if (!force && isFresh(seasonEntry.mapStats)) {
                    return seasonEntry.mapStats.data || [];
                }
                seasonEntry.mapStats.loading = true;
                seasonEntry.mapStats.error = null;
                try {
                    const data = await window.apiClient.getTeamMapStats(teamId, championshipId);
                    seasonEntry.mapStats.data = Array.isArray(data) ? data : [];
                    seasonEntry.mapStats.fetchedAt = now();
                    return seasonEntry.mapStats.data;
                } catch (error) {
                    seasonEntry.mapStats.error = error?.message || 'Karttatilastojen lataus epäonnistui';
                    throw error;
                } finally {
                    seasonEntry.mapStats.loading = false;
                }
            },
            async fetchSeasonMatches(teamId, championshipId, options = {}) {
                if (!teamId) return [];
                const entry = this.ensureTeamEntry(teamId);
                const targetChampionship = championshipId || options.fallbackChampionship || null;
                if (!targetChampionship) {
                    // Without championship fetch general matches list
                    const cacheKey = '__all__';
                    if (!entry.seasons[cacheKey]) {
                        entry.seasons[cacheKey] = {
                            details: createSegment(),
                            mapStats: createSegment(),
                            matches: createSegment()
                        };
                    }
                    const globalEntry = entry.seasons[cacheKey];
                    if (globalEntry.matches.loading) {
                        return globalEntry.matches.data || [];
                    }
                    if (!options.force && isFresh(globalEntry.matches)) {
                        return globalEntry.matches.data || [];
                    }
                    globalEntry.matches.loading = true;
                    globalEntry.matches.error = null;
                    try {
                        // API expects (teamId, seasonId)
                        const data = await window.apiClient.getTeamMatches(teamId, null);
                        globalEntry.matches.data = Array.isArray(data) ? data : [];
                        globalEntry.matches.fetchedAt = now();
                        return globalEntry.matches.data;
                    } catch (error) {
                        globalEntry.matches.error = error?.message || 'Ottelulistan lataus epäonnistui';
                        throw error;
                    } finally {
                        globalEntry.matches.loading = false;
                    }
                }

                const seasonEntry = ensureSeasonEntry(entry, targetChampionship);
                if (seasonEntry.matches.loading) {
                    return seasonEntry.matches.data || [];
                }
                if (!options.force && isFresh(seasonEntry.matches)) {
                    return seasonEntry.matches.data || [];
                }
                seasonEntry.matches.loading = true;
                seasonEntry.matches.error = null;
                try {
                    // API expects (teamId, seasonId)
                    const data = await window.apiClient.getTeamMatches(teamId, targetChampionship);
                    seasonEntry.matches.data = Array.isArray(data) ? data : [];
                    seasonEntry.matches.fetchedAt = now();
                    return seasonEntry.matches.data;
                } catch (error) {
                    seasonEntry.matches.error = error?.message || 'Ottelulistan lataus epäonnistui';
                    throw error;
                } finally {
                    seasonEntry.matches.loading = false;
                }
            }
        }
    });
})();

