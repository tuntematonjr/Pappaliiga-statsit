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
                elo: createSegment(),
                maps: {},
                progression: {}
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

    function ensureProgressionEntry(entry, championshipId) {
        if (!entry.progression) {
            entry.progression = {};
        }
        if (!entry.progression[championshipId]) {
            entry.progression[championshipId] = createSegment();
        }
        return entry.progression[championshipId];
    }

    function applyBundleToEntry(entry, bundle, championshipId = null) {
        const payload = (bundle && typeof bundle === 'object') ? bundle : {};
        const profile = payload.player || null;
        const seasons = Array.isArray(payload.seasons) ? payload.seasons : [];
        const selectedChampionshipId = String(
            payload.selected_championship_id
            || payload.selectedChampionshipId
            || championshipId
            || ''
        );
        const mapStats = Array.isArray(payload.map_stats || payload.mapStats) ? (payload.map_stats || payload.mapStats) : [];
        const progression = Array.isArray(payload.progression) ? payload.progression : [];
        const eloSummary = payload.elo_summary || payload.eloSummary || null;
        const eloHistory = Array.isArray(payload.elo_history || payload.eloHistory)
            ? (payload.elo_history || payload.eloHistory)
            : [];

        if (profile) {
            entry.profile.data = profile;
            entry.profile.error = null;
            entry.profile.fetchedAt = now();
        }
        entry.seasons.data = seasons;
        entry.seasons.error = null;
        entry.seasons.fetchedAt = now();

        if (selectedChampionshipId) {
            const mapEntry = ensureMapEntry(entry, selectedChampionshipId);
            mapEntry.data = mapStats;
            mapEntry.error = null;
            mapEntry.fetchedAt = now();

            const progressionEntry = ensureProgressionEntry(entry, selectedChampionshipId);
            progressionEntry.data = progression;
            progressionEntry.error = null;
            progressionEntry.fetchedAt = now();
        }

        if (eloSummary || eloHistory.length) {
            entry.elo.data = {
                elo_summary: eloSummary,
                elo_history: eloHistory
            };
            entry.elo.error = null;
            entry.elo.fetchedAt = now();
        }
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
            async fetchBundle(playerId, championshipId = null, options = {}) {
                if (!playerId) return null;
                const entry = this.ensureEntry(playerId);
                const { force = false } = options;
                const bundleKey = championshipId ? String(championshipId) : '__default__';
                entry.bundle = entry.bundle || {};
                const segment = entry.bundle[bundleKey] || createSegment();
                entry.bundle[bundleKey] = segment;

                if (segment.loading) {
                    return segment.data;
                }
                if (!force && isFresh(segment)) {
                    return segment.data;
                }
                segment.loading = true;
                segment.error = null;
                try {
                    const data = await window.apiClient.getPlayerBundle(playerId, championshipId);
                    segment.data = data || {};
                    segment.fetchedAt = now();
                    applyBundleToEntry(entry, segment.data, championshipId);
                    return segment.data;
                } catch (error) {
                    segment.error = error?.message || 'Pelaajan tietojen lataus epäonnistui';
                    throw error;
                } finally {
                    segment.loading = false;
                }
            },
            async fetchProfile(playerId, options = {}) {
                if (!playerId) return null;
                const entry = this.ensureEntry(playerId);
                try {
                    await this.fetchBundle(playerId, null, options);
                    return entry.profile.data;
                } catch (error) {
                    entry.profile.error = error?.message || 'Pelaajan tietojen lataus epäonnistui';
                    throw error;
                }
            },
            async fetchSeasons(playerId, options = {}) {
                if (!playerId) return [];
                const entry = this.ensureEntry(playerId);
                try {
                    await this.fetchBundle(playerId, null, options);
                    return entry.seasons.data;
                } catch (error) {
                    entry.seasons.error = error?.message || 'Pelaajan kausitilastojen lataus epäonnistui';
                    throw error;
                }
            },
            async fetchMapStats(playerId, championshipId, options = {}) {
                if (!playerId || !championshipId) return [];
                try {
                    await this.fetchBundle(playerId, championshipId, options);
                    const entry = this.ensureEntry(playerId);
                    return ensureMapEntry(entry, championshipId).data || [];
                } catch (error) {
                    const entry = this.ensureEntry(playerId);
                    const mapEntry = ensureMapEntry(entry, championshipId);
                    mapEntry.error = error?.message || 'Pelaajan karttatilastojen lataus epäonnistui';
                    throw error;
                }
            },
            async fetchProgression(playerId, championshipId, season, division, options = {}) {
                if (!playerId || !championshipId || season == null || division == null) return [];
                try {
                    await this.fetchBundle(playerId, championshipId, options);
                    const entry = this.ensureEntry(playerId);
                    return ensureProgressionEntry(entry, championshipId).data || [];
                } catch (error) {
                    const entry = this.ensureEntry(playerId);
                    const progressionEntry = ensureProgressionEntry(entry, championshipId);
                    progressionEntry.error = error?.message || 'Pelaajan kehitystrendin lataus epäonnistui';
                    throw error;
                }
            },
            async fetchElo(playerId, options = {}) {
                if (!playerId) return null;
                const entry = this.ensureEntry(playerId);
                entry.elo = entry.elo || createSegment();
                const segment = entry.elo;
                const { force = false, limit = 50 } = options;

                if (segment.loading) {
                    return segment.data;
                }
                if (!force && isFresh(segment)) {
                    return segment.data;
                }

                segment.loading = true;
                segment.error = null;
                try {
                    const data = await window.apiClient.getPlayerElo(playerId, { limit });
                    segment.data = data || {};
                    segment.fetchedAt = now();
                    return segment.data;
                } catch (error) {
                    segment.error = error?.message || 'Pelaajan Elo-tietojen lataus epäonnistui';
                    throw error;
                } finally {
                    segment.loading = false;
                }
            }
        }
    });
})();

