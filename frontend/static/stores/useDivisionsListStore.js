(function () {
    const { defineStore } = Pinia;

    const FRESH_MS = 5 * 60 * 1000; // 5 minutes cache

    function now() {
        return Date.now();
    }

    function isFresh(segment) {
        return !!(segment && segment.fetchedAt && now() - segment.fetchedAt < FRESH_MS && !segment.error);
    }

    function createSegment() {
        return {
            data: null,
            loading: false,
            error: null,
            fetchedAt: null,
            fromCache: false
        };
    }

    /**
     * Normalizes division data using the existing divisionNormalizer utility.
     */
    function normalizeDivision(raw) {
        if (!window.divisionNormalizer || typeof window.divisionNormalizer.normalizeDivision !== 'function') {
            console.warn('[divisionsListStore] divisionNormalizer not available, using raw data');
            return { ok: true, division: raw, warnings: [] };
        }
        return window.divisionNormalizer.normalizeDivision(raw);
    }

    /**
     * Sorts divisions by tier ascending, then by division_id ascending.
     */
    function sortDivisions(divisions) {
        return [...divisions].sort((a, b) => {
            if (a.tier !== b.tier) {
                return a.tier - b.tier;
            }
            return (a.divisionId || a.id) - (b.divisionId || b.id);
        });
    }

    /**
     * Filters divisions by status and search query.
     */
    function filterDivisions(divisions, statusFilter, searchQuery) {
        let filtered = divisions;

        // Filter by status
        if (statusFilter && statusFilter.toLowerCase() !== 'all') {
            filtered = filtered.filter(div => 
                div.status && div.status.toLowerCase() === statusFilter.toLowerCase()
            );
        }

        // Filter by search query
        if (searchQuery && searchQuery.trim()) {
            const query = searchQuery.trim().toLowerCase();
            filtered = filtered.filter(div => {
                const name = (div.name || '').toLowerCase();
                const id = String(div.divisionId || div.id || '');
                return name.includes(query) || id.includes(query);
            });
        }

        return filtered;
    }

    /**
     * Groups divisions by tier.
     */
    function groupByTier(divisions) {
        const groups = new Map();
        for (const division of divisions) {
            const tier = division.tier || 1;
            if (!groups.has(tier)) {
                groups.set(tier, []);
            }
            groups.get(tier).push(division);
        }
        return groups;
    }

    /**
     * Calculates progress percentage.
     */
    function calculateProgress(played, total) {
        if (!total || total === 0) return 0;
        return Math.min(100, Math.round((played / total) * 100));
    }

    window.useDivisionsListStore = defineStore('divisionsList', {
        state: () => ({
            seasonData: {}, // Map of seasonId -> { summary, divisions, etc }
            currentSeasonId: null,
            filters: {
                status: 'all',
                search: '',
                sortBy: 'tier' // tier, name, status
            },
            offlineMode: false,
            lastFetchTimestamp: null
        }),
        getters: {
            currentSeason(state) {
                if (!state.currentSeasonId) return null;
                return state.seasonData[state.currentSeasonId] || null;
            },
            currentSummary(state) {
                const season = this.currentSeason;
                return season?.summary?.data || null;
            },
            currentDivisions(state) {
                const season = this.currentSeason;
                if (!season || !season.divisions || !season.divisions.data) {
                    return [];
                }
                return season.divisions.data;
            },
            filteredDivisions(state) {
                const divisions = this.currentDivisions;
                const filtered = filterDivisions(
                    divisions, 
                    state.filters.status,
                    state.filters.search
                );
                
                // Apply sorting
                if (state.filters.sortBy === 'tier') {
                    return sortDivisions(filtered);
                } else if (state.filters.sortBy === 'name') {
                    return [...filtered].sort((a, b) => 
                        (a.name || '').localeCompare(b.name || '', 'fi')
                    );
                } else if (state.filters.sortBy === 'status') {
                    return [...filtered].sort((a, b) => {
                        const statusOrder = { active: 1, waiting: 2, finished: 3 };
                        const aVal = statusOrder[a.status] || 999;
                        const bVal = statusOrder[b.status] || 999;
                        return aVal - bVal;
                    });
                }
                
                return filtered;
            },
            divisionsByTier(state) {
                const divisions = this.filteredDivisions;
                return groupByTier(divisions);
            },
            isLoading(state) {
                const season = this.currentSeason;
                return season?.summary?.loading || season?.divisions?.loading || false;
            },
            hasError(state) {
                const season = this.currentSeason;
                return !!(season?.summary?.error || season?.divisions?.error);
            },
            errorMessage(state) {
                const season = this.currentSeason;
                return season?.summary?.error || season?.divisions?.error || null;
            }
        },
        actions: {
            ensureSeasonEntry(seasonId) {
                if (!this.seasonData[seasonId]) {
                    this.seasonData[seasonId] = {
                        summary: createSegment(),
                        divisions: createSegment()
                    };
                }
                return this.seasonData[seasonId];
            },
            
            setCurrentSeason(seasonId) {
                this.currentSeasonId = seasonId;
            },

            setFilter(key, value) {
                if (this.filters.hasOwnProperty(key)) {
                    this.filters[key] = value;
                }
            },

            resetFilters() {
                this.filters = {
                    status: 'all',
                    search: '',
                    sortBy: 'tier'
                };
            },

            async fetchSeasonSummary(seasonId, options = {}) {
                const { force = false } = options;
                const season = this.ensureSeasonEntry(seasonId);

                if (season.summary.loading) {
                    return season.summary.data;
                }

                if (!force && isFresh(season.summary)) {
                    return season.summary.data;
                }

                season.summary.loading = true;
                season.summary.error = null;

                try {
                    const result = await window.apiClient.getSeasonSummary(seasonId);
                    const data = result?.data || result;
                    
                    season.summary.data = data;
                    season.summary.fetchedAt = now();
                    season.summary.fromCache = result?.fromCache || false;
                    
                    if (result?.fromCache) {
                        this.offlineMode = true;
                        this.lastFetchTimestamp = result?.meta?.timestamp || season.summary.fetchedAt;
                    } else {
                        this.offlineMode = false;
                    }

                    return data;
                } catch (error) {
                    season.summary.error = error?.message || 'Failed to load season summary';
                    console.error('[divisionsListStore] fetchSeasonSummary error:', error);
                    throw error;
                } finally {
                    season.summary.loading = false;
                }
            },

            async fetchDivisions(seasonId, options = {}) {
                const { force = false } = options;
                const season = this.ensureSeasonEntry(seasonId);

                if (season.divisions.loading) {
                    return season.divisions.data;
                }

                if (!force && isFresh(season.divisions)) {
                    return season.divisions.data;
                }

                season.divisions.loading = true;
                season.divisions.error = null;

                try {
                    const result = await window.apiClient.getDivisions(seasonId);
                    const rawData = result?.data || result;
                    const list = Array.isArray(rawData) ? rawData : [];

                    // Normalize all divisions
                    const normalized = [];
                    const warnings = [];

                    for (const raw of list) {
                        const result = normalizeDivision(raw);
                        if (result.ok) {
                            normalized.push(result.division);
                            if (result.warnings && result.warnings.length) {
                                warnings.push(...result.warnings);
                            }
                        } else {
                            warnings.push(`Skipped invalid division: ${result.error}`);
                        }
                    }

                    if (warnings.length > 0) {
                        console.warn('[divisionsListStore] Normalization warnings:', warnings);
                    }

                    season.divisions.data = normalized;
                    season.divisions.fetchedAt = now();
                    season.divisions.fromCache = result?.fromCache || false;

                    if (result?.fromCache) {
                        this.offlineMode = true;
                        this.lastFetchTimestamp = result?.meta?.timestamp || season.divisions.fetchedAt;
                    } else {
                        this.offlineMode = false;
                    }

                    return normalized;
                } catch (error) {
                    season.divisions.error = error?.message || 'Failed to load divisions';
                    console.error('[divisionsListStore] fetchDivisions error:', error);
                    
                    // Try to use cached data on error
                    if (season.divisions.data && season.divisions.data.length > 0) {
                        console.warn('[divisionsListStore] Using cached divisions due to error');
                        this.offlineMode = true;
                        return season.divisions.data;
                    }
                    
                    throw error;
                } finally {
                    season.divisions.loading = false;
                }
            },

            async loadSeasonData(seasonId, options = {}) {
                this.setCurrentSeason(seasonId);
                
                try {
                    await Promise.all([
                        this.fetchSeasonSummary(seasonId, options),
                        this.fetchDivisions(seasonId, options)
                    ]);
                } catch (error) {
                    console.error('[divisionsListStore] loadSeasonData error:', error);
                    // Don't throw - individual fetch methods handle errors
                }
            },

            clearSeasonData(seasonId) {
                if (seasonId && this.seasonData[seasonId]) {
                    delete this.seasonData[seasonId];
                }
            },

            clearAllData() {
                this.seasonData = {};
                this.currentSeasonId = null;
                this.offlineMode = false;
                this.lastFetchTimestamp = null;
            }
        }
    });
})();
