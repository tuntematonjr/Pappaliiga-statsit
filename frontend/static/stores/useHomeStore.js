(function () {
    const { defineStore } = Pinia;
    const DEV_MODE = typeof window !== 'undefined' && ['localhost', '127.0.0.1'].includes(window.location.hostname);

    function toNumber(value, fallback = 0) {
        if (value === null || value === undefined) {
            return fallback;
        }
        const direct = Number(value);
        if (Number.isFinite(direct)) {
            return direct;
        }
        const parsed = Number(String(value).replace(',', '.'));
        return Number.isFinite(parsed) ? parsed : fallback;
    }

    function pickValue(obj, candidates) {
        if (!obj) return undefined;
        const paths = Array.isArray(candidates) ? candidates : [candidates];
        for (const path of paths) {
            if (!path) continue;
            const segments = String(path).split('.');
            let current = obj;
            let found = true;
            for (const segment of segments) {
                if (current && Object.prototype.hasOwnProperty.call(current, segment)) {
                    current = current[segment];
                } else {
                    found = false;
                    break;
                }
            }
            if (found && current !== undefined) {
                return current;
            }
        }
        return undefined;
    }

    function computePercent(part, total) {
        const played = toNumber(part);
        const max = toNumber(total);
        if (!max || max <= 0) return 0;
        return Math.min(100, Math.round((played / max) * 1000) / 10);
    }

    function defaultProgress() {
        return {
            overall: { played: 0, total: 0, percent: 0 },
            regular: { played: 0, total: 0, percent: 0 },
            playoffs: { played: 0, total: 0, percent: 0 }
        };
    }

    function computeProgress(stats, divisions) {
        const progress = defaultProgress();
        const source = stats || {};

        const overallPlayed =
            pickValue(source, ['progress.overall.played', 'matches_played', 'played_matches']) ??
            divisions.reduce((sum, division) => sum + toNumber(division?.season?.matches_played), 0);
        const overallTotal =
            pickValue(source, ['progress.overall.total', 'matches_total', 'scheduled_matches']) ??
            divisions.reduce((sum, division) => sum + toNumber(division?.season?.matches_total), 0);

        progress.overall = {
            played: toNumber(overallPlayed),
            total: toNumber(overallTotal),
            percent: computePercent(overallPlayed, overallTotal)
        };

        const regularPlayed =
            pickValue(source, ['progress.regular.played', 'progress.runkosarja.played']) ?? overallPlayed;
        const regularTotal =
            pickValue(source, ['progress.regular.total', 'progress.runkosarja.total']) ?? overallTotal;
        progress.regular = {
            played: toNumber(regularPlayed, overallPlayed),
            total: toNumber(regularTotal, overallTotal),
            percent: computePercent(regularPlayed, regularTotal)
        };

        const playoffPlayed =
            pickValue(source, ['progress.playoffs.played', 'progress.playoff.played']) ??
            divisions.reduce((sum, division) => sum + toNumber(division?.playoffs?.matches_played), 0);
        const playoffTotal =
            pickValue(source, ['progress.playoffs.total', 'progress.playoff.total']) ??
            divisions.reduce((sum, division) => sum + toNumber(division?.playoffs?.matches_total), 0);
        progress.playoffs = {
            played: toNumber(playoffPlayed),
            total: toNumber(playoffTotal),
            percent: computePercent(playoffPlayed, playoffTotal)
        };

        return progress;
    }

    function inferTierValue(divisionId) {
        const numeric = toNumber(divisionId, 0);
        if (numeric >= 1 && numeric <= 5) return 1;
        if (numeric >= 6 && numeric <= 10) return 2;
        if (numeric >= 11 && numeric <= 15) return 3;
        if (numeric >= 16 && numeric <= 20) return 4;
        return 5;
    }

    function legacyNormalizeDivision(raw, index) {
        if (!raw || typeof raw !== 'object') {
            return null;
        }
        const fallbackId = raw.division_id ?? raw.id ?? raw.slug ?? index;
        const seasonData = raw.season && typeof raw.season === 'object' ? raw.season : {};
        const playoffsData = raw.playoffs && typeof raw.playoffs === 'object' ? raw.playoffs : {};
        return {
            id: String(fallbackId),
            divisionId: toNumber(fallbackId, index),
            name: raw.name || `Division ${fallbackId}`,
            tier: toNumber(raw.tier, inferTierValue(fallbackId)),
            season: {
                teams: toNumber(seasonData.teams, 0),
                matches_played: toNumber(seasonData.matches_played, 0),
                matches_total: toNumber(seasonData.matches_total, 0),
                status: (seasonData.status || raw.status || 'waiting').toLowerCase(),
                winner: seasonData.winner || null
            },
            playoffs: {
                teams: toNumber(playoffsData.teams, 8),
                matches_played: toNumber(playoffsData.matches_played, 0),
                matches_total: toNumber(playoffsData.matches_total, 7),
                status: (playoffsData.status || 'waiting').toLowerCase(),
                winner: playoffsData.winner || null
            },
            slug: raw.slug || null,
            seasonNumber: raw.season_number ?? raw.seasonNumber ?? null,
            raw
        };
    }

    const normalizerRef = typeof window !== 'undefined' ? window.divisionNormalizer : null;

    function getCacheDebugKeys() {
        if (typeof window === 'undefined' || !window.localStorage) {
            return [];
        }
        const keys = [];
        try {
            for (let i = 0; i < window.localStorage.length; i += 1) {
                const key = window.localStorage.key(i);
                if (key && key.startsWith('pl:cache')) {
                    keys.push(key);
                }
            }
        } catch (error) {
            // ignore cache key issues
        }
        return keys;
    }

    function normalizeDivisionEntry(raw, index) {
        if (normalizerRef && typeof normalizerRef.normalizeDivision === 'function') {
            const result = normalizerRef.normalizeDivision(raw);
            if (result && result.ok && result.division) {
                return { ok: true, division: result.division, warnings: result.warnings || [] };
            }
            return {
                ok: false,
                division: null,
                warnings: result?.warnings || [],
                error: result?.error || 'Normalization failed'
            };
        }
        const fallback = legacyNormalizeDivision(raw, index);
        if (fallback) {
            return { ok: true, division: fallback, warnings: [] };
        }
        return { ok: false, division: null, warnings: ['Normalization failed'] };
    }

    function normalizeSummary(raw) {
        if (!raw || typeof raw !== 'object') {
            return { raw: null, aggregates: {} };
        }
        const aggregates =
            raw.aggregates ||
            raw.stats ||
            raw.summary ||
            raw.overview ||
            raw;
        return { raw, aggregates };
    }

    function defaultSeasonState() {
        return {
            loading: false,
            error: null,
            apiParam: null,
            fetchedAt: null,
            stats: {},
            rawStats: null,
            divisions: [],
            rawDivisions: [],
            divisionsMeta: null,
            progress: defaultProgress(),
            offline: false,
            cacheTimestamp: null,
            validationWarnings: [],
            warningMessage: '',
            usingCache: false
        };
    }

    window.useHomeStore = defineStore('home', {
        state: () => ({
            lifetimeSummary: null,
            summaryLoading: false,
            summaryError: null,
            summaryFetchedAt: null,
            seasonCache: {}
        }),
        getters: {
            hasSummary(state) {
                return Boolean(state.lifetimeSummary && Object.keys(state.lifetimeSummary).length);
            },
            getSeasonState: state => key => state.seasonCache[key] || defaultSeasonState()
        },
        actions: {
            async fetchLifetimeSummary(options = {}) {
                const { force = false } = options;
                if (this.summaryLoading) {
                    return this.lifetimeSummary;
                }

                const isFresh =
                    this.summaryFetchedAt && Date.now() - this.summaryFetchedAt < 10 * 60 * 1000;
                if (!force && this.lifetimeSummary && isFresh) {
                    return this.lifetimeSummary;
                }

                this.summaryLoading = true;
                this.summaryError = null;

                try {
                    let summaryResponse = await window.apiClient.fetchLifetimeSummary();
                    let summary = summaryResponse?.data ?? summaryResponse;
                    if (!summary || typeof summary !== 'object') {
                        const fallbackResponse = await window.apiClient.getStatsOverview();
                        summary = fallbackResponse?.data ?? fallbackResponse;
                    }
                    this.lifetimeSummary = normalizeSummary(summary);
                    this.summaryFetchedAt = Date.now();
                    return this.lifetimeSummary;
                } catch (error) {
                    if (error && error.status === 404) {
                        try {
                            const fallback = await window.apiClient.getStatsOverview();
                            const payload = fallback?.data ?? fallback;
                            this.lifetimeSummary = normalizeSummary(payload);
                            this.summaryFetchedAt = Date.now();
                            return this.lifetimeSummary;
                        } catch (fallbackError) {
                            this.summaryError =
                                fallbackError?.message || 'Yleistilastojen lataus epäonnistui';
                            throw fallbackError;
                        }
                    } else {
                        this.summaryError =
                            error?.message || 'Yleistilastojen lataus epäonnistui';
                        throw error;
                    }
                } finally {
                    this.summaryLoading = false;
                }
            },
            async ensureSummary() {
                if (!this.hasSummary) {
                    try {
                        await this.fetchLifetimeSummary();
                    } catch (error) {
                        // Swallow here; caller may render error state via summaryError
                    }
                }
            },
            async fetchSeason(key, options = {}) {
                if (!key) {
                    return defaultSeasonState();
                }

                const { force = false, apiParam } = options;
                const existing = this.seasonCache[key];

                if (existing && existing.loading) {
                    return existing;
                }

                const isFresh =
                    existing && existing.fetchedAt && Date.now() - existing.fetchedAt < 2 * 60 * 1000;
                if (existing && !force && isFresh && !existing.error) {
                    return existing;
                }

                const identifier = apiParam ?? existing?.apiParam ?? key;

                this.seasonCache[key] = {
                    ...(existing || defaultSeasonState()),
                    loading: true,
                    error: null,
                    apiParam: identifier
                };

                try {
                    if (DEV_MODE) {
                        console.info('[homeStore] fetchSeason start', { key, identifier, force });
                    }
                    const [healthResult, summaryResult, divisionsResult] = await Promise.allSettled([
                        window.apiClient.healthCheck(identifier).catch(() => false),
                        window.apiClient.getSeasonSummary(identifier),
                        window.apiClient.getDivisions(identifier)
                    ]);

                    const healthOk = healthResult.status === 'fulfilled' ? Boolean(healthResult.value) : false;

                    let summaryData = null;
                    let summaryMeta = null;
                    if (summaryResult.status === 'fulfilled') {
                        summaryData = summaryResult.value.data || summaryResult.value;
                        summaryMeta = summaryResult.value.meta || {};
                    } else {
                        summaryData = await window.apiClient
                            .getSeasonStats(identifier)
                            .then(res => res.data || res)
                            .catch(() => ({}));
                    }

                    let divisionsData = [];
                    let divisionsMeta = null;
                    let apiValidationWarnings = [];
                    if (divisionsResult.status === 'fulfilled') {
                        divisionsData = divisionsResult.value.data || [];
                        divisionsMeta = divisionsResult.value.meta || {};
                        apiValidationWarnings = (divisionsResult.value.errors || []).map(
                            error => error?.message || 'Division validation error'
                        );
                    } else {
                        const fallback = await window.apiClient
                            .getDivisionsBySeason(identifier)
                            .then(res => (Array.isArray(res?.data) ? res.data : res))
                            .catch(() => []);
                        divisionsData = Array.isArray(fallback) ? fallback : [];
                    }

                    if (divisionsData.length === 0 && DEV_MODE) {
                        console.warn('[homeStore] Divisions API returned 0 items', {
                            meta: divisionsMeta,
                            cacheKeys: getCacheDebugKeys()
                        });
                    }

                    const normalizationWarnings = [];
                    let normalizedDivisions = divisionsData
                        .map((entry, index) => {
                            const result = normalizeDivisionEntry(entry, index);
                            if (result.ok && result.division) {
                                if (Array.isArray(result.warnings) && result.warnings.length) {
                                    normalizationWarnings.push(...result.warnings);
                                }
                                return result.division;
                            }
                            if (result.error) {
                                normalizationWarnings.push(result.error);
                            }
                            return null;
                        })
                        .filter(Boolean);

                    if (!normalizedDivisions.length && divisionsData.length) {
                        const fallback = divisionsData
                            .map((entry, index) => legacyNormalizeDivision(entry, index))
                            .filter(Boolean);
                        if (fallback.length) {
                            normalizationWarnings.push('Normalized set empty; falling back to legacy mapping.');
                            normalizedDivisions = fallback;
                        }
                    }

                    if (DEV_MODE) {
                        console.info('[homeStore] divisions counts', {
                            raw: divisionsData.length,
                            normalized: normalizedDivisions.length,
                            warnings: [...apiValidationWarnings, ...normalizationWarnings]
                        });
                    }

                    const stats = summaryData?.aggregates || summaryData?.stats || summaryData || {};
                    const progress = computeProgress(stats, normalizedDivisions);
                    const usingCache = Boolean(summaryMeta?.usedCacheDueToError || divisionsMeta?.usedCacheDueToError);
                    const cacheTimestamp = summaryMeta?.cacheTimestamp || divisionsMeta?.cacheTimestamp || null;
                    const offline = !healthOk || usingCache;

                    const payload = {
                        loading: false,
                        error: null,
                        apiParam: identifier,
                        fetchedAt: Date.now(),
                        stats,
                        rawStats: summaryData,
                        rawDivisions: divisionsData,
                        divisionsMeta,
                        divisions: normalizedDivisions,
                        progress,
                        offline,
                        cacheTimestamp,
                        usingCache,
                        validationWarnings: [...apiValidationWarnings, ...normalizationWarnings],
                        warningMessage: normalizedDivisions.length ? '' : 'Division data missing or invalid.'
                    };

                    this.seasonCache[key] = payload;
                    if (DEV_MODE) {
                        console.info('[homeStore] fetchSeason success', {
                            key,
                            divisions: normalizedDivisions.length,
                            offline,
                            cacheTimestamp
                        });
                    }
                    return payload;
                } catch (error) {
                    this.seasonCache[key] = {
                        ...(this.seasonCache[key] || defaultSeasonState()),
                        loading: false,
                        error: error?.message || 'Kausitietojen lataus epäonnistui',
                        fetchedAt: Date.now(),
                        apiParam: identifier
                    };
                    throw error;
                }
            },
            clearSeasonCache(keys) {
                if (!keys) {
                    this.seasonCache = {};
                    return;
                }
                const list = Array.isArray(keys) ? keys : [keys];
                list.forEach(key => {
                    delete this.seasonCache[key];
                });
            }
        }
    });
})();
