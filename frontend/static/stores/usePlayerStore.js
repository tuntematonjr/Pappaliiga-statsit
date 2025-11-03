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

    function ensurePlayerEntry(state, playerId) {
        if (!state[playerId]) {
            state[playerId] = {
                profile: createSegment(),
                seasons: createSegment(),
                maps: {}
            };
        }
        return state[playerId];
    }

    function ensureMapEntry(entry, championshipId) {
        if (!entry.maps) {
            entry.maps = {};
        }
        if (!entry.maps[championshipId]) {
            entry.maps[championshipId] = createSegment();
        }
        return entry.maps[championshipId];
    }

    window.usePlayerStore = defineStore('player', {
        state: () => ({
            players: {}
        }),
        getters: {
            getPlayerState(state) {
                return playerId => {
                    if (!playerId) return null;
                    return state.players[playerId] || null;
                };
            }
        },
        actions: {
            ensureEntry(playerId) {
                return ensurePlayerEntry(this.players, playerId);
            },
            async fetchProfile(playerId, options = {}) {
                if (!playerId) return null;
                const entry = this.ensureEntry(playerId);
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
                    const data = await window.apiClient.getPlayerInfo(playerId);
                    entry.profile.data = data;
                    entry.profile.fetchedAt = now();
                    return data;
                } catch (error) {
                    entry.profile.error = error?.message || 'Pelaajan tietojen lataus epäonnistui';
                    throw error;
                } finally {
                    entry.profile.loading = false;
                }
            },
            async fetchSeasons(playerId, options = {}) {
                if (!playerId) return [];
                const entry = this.ensureEntry(playerId);
                const { force = false } = options;
                if (entry.seasons.loading) {
                    return entry.seasons.data || [];
                }
                if (!force && isFresh(entry.seasons)) {
                    return entry.seasons.data || [];
                }
                entry.seasons.loading = true;
                entry.seasons.error = null;
                try {
                    const data = await window.apiClient.getPlayerSeasonStats(playerId);
                    entry.seasons.data = Array.isArray(data) ? data : [];
                    entry.seasons.fetchedAt = now();
                    return entry.seasons.data;
                } catch (error) {
                    entry.seasons.error = error?.message || 'Pelaajan kausitilastojen lataus epäonnistui';
                    throw error;
                } finally {
                    entry.seasons.loading = false;
                }
            },
            async fetchMapStats(playerId, championshipId, options = {}) {
                if (!playerId || !championshipId) return [];
                const entry = this.ensureEntry(playerId);
                const mapEntry = ensureMapEntry(entry, championshipId);
                const { force = false } = options;
                if (mapEntry.loading) {
                    return mapEntry.data || [];
                }
                if (!force && isFresh(mapEntry)) {
                    return mapEntry.data || [];
                }
                mapEntry.loading = true;
                mapEntry.error = null;
                try {
                    const data = await window.apiClient.getPlayerMapStats(playerId, championshipId);
                    mapEntry.data = Array.isArray(data) ? data : [];
                    mapEntry.fetchedAt = now();
                    return mapEntry.data;
                } catch (error) {
                    mapEntry.error = error?.message || 'Pelaajan karttatilastojen lataus epäonnistui';
                    throw error;
                } finally {
                    mapEntry.loading = false;
                }
            }
        }
    });
})();

