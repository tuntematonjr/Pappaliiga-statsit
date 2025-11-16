(function () {
    const { defineStore } = Pinia;
    const DEV_MODE = typeof window !== 'undefined' && ['localhost', '127.0.0.1'].includes(window.location.hostname);
    const DEV_MOCK_DATA = typeof window !== 'undefined' ? window.__PAPPALIIGA_DEV_DIVISIONS__ : null;

    function formatCacheBanner(timestamp) {
        if (!timestamp) {
            return 'Backend routes not found; showing cached data.';
        }
        try {
            const formatted = new Date(timestamp).toLocaleString('fi-FI', {
                hour: '2-digit',
                minute: '2-digit',
                day: 'numeric',
                month: 'numeric',
                year: 'numeric'
            });
            return `Backend routes not found; showing cached data from ${formatted}.`;
        } catch (error) {
            return `Backend routes not found; showing cached data (timestamp: ${timestamp}).`;
        }
    }

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

    function pickNumeric(obj, candidates) {
        if (!obj) return null;
        const raw = pickValue(obj, candidates);
        const numeric = toNumber(raw, null);
        return Number.isFinite(numeric) ? numeric : null;
    }

    const PLAYOFF_SERIES_MATCH_TOTAL = 7;

    function normalizeMatchPair(playedInput, totalInput, options = {}) {
        const { fallbackTotal = null, forceTotal = null } = options;
        const providedTotal = Math.max(0, toNumber(totalInput, 0));
        let total = providedTotal;
        let estimated = false;
        if (Number.isFinite(forceTotal) && forceTotal >= 0) {
            total = forceTotal;
            estimated = true;
        } else {
            const fallbackNumeric = Math.max(0, toNumber(fallbackTotal, 0));
            if (total <= 0 && fallbackNumeric > 0) {
                total = fallbackNumeric;
                estimated = true;
            }
        }
        const playedRaw = Math.max(0, toNumber(playedInput, 0));
        const cap = total > 0 ? total : playedRaw;
        const played = cap > 0 ? Math.min(playedRaw, cap) : playedRaw;
        return {
            played,
            total,
            effectiveTotal: total > 0 ? total : played,
            estimated
        };
    }

    function aggregateDivisionMatches(divisions, blockKey, options = {}) {
        const { estimateTotal, forceTotal } = options;
        if (!Array.isArray(divisions) || !divisions.length) {
            return { played: 0, total: 0, effectiveTotal: 0, estimatedTotal: 0 };
        }
        return divisions.reduce(
            (acc, division) => {
                const block = division?.[blockKey];
                if (!block) {
                    return acc;
                }
                const matches = normalizeMatchPair(
                    block.matches_played ?? block.matchesPlayed,
                    block.matches_total ?? block.matchesTotal,
                    {
                        fallbackTotal:
                            typeof estimateTotal === 'function' ? estimateTotal(block, division) : null,
                        forceTotal: forceTotal
                    }
                );
                acc.played += matches.played;
                acc.total += matches.total;
                acc.effectiveTotal += matches.effectiveTotal;
                acc.estimatedTotal += matches.estimated ? matches.total : 0;
                return acc;
            },
            { played: 0, total: 0, effectiveTotal: 0, estimatedTotal: 0 }
        );
    }

    function computePercent(part, total) {
        const played = toNumber(part);
        const max = toNumber(total);
        if (!max || max <= 0) return 0;
        return Math.min(100, Math.round((played / max) * 1000) / 10);
    }

    function createProgressSection() {
        return { played: 0, total: 0, percent: 0, source: 'unknown' };
    }

    function defaultProgress() {
        return {
            overall: createProgressSection(),
            regular: createProgressSection(),
            playoffs: createProgressSection()
        };
    }

    function computeProgress(stats, divisions) {
        const source = stats || {};
        const regularAggregate = aggregateDivisionMatches(divisions, 'season');
        const playoffAggregate = aggregateDivisionMatches(divisions, 'playoffs', {
            forceTotal: PLAYOFF_SERIES_MATCH_TOTAL
        });
        const forcedPlayoffTotal = divisions.length * PLAYOFF_SERIES_MATCH_TOTAL;
        if (forcedPlayoffTotal > 0) {
            playoffAggregate.total = forcedPlayoffTotal;
            if (playoffAggregate.effectiveTotal < forcedPlayoffTotal) {
                playoffAggregate.effectiveTotal = forcedPlayoffTotal;
            }
            playoffAggregate.estimatedTotal = forcedPlayoffTotal;
        }

        const overallAggregate = {
            played: regularAggregate.played + playoffAggregate.played,
            total: regularAggregate.total + playoffAggregate.total,
            effectiveTotal: regularAggregate.effectiveTotal + playoffAggregate.effectiveTotal,
            estimatedTotal: (regularAggregate.estimatedTotal || 0) + (playoffAggregate.estimatedTotal || 0)
        };

        const statsRegular = {
            played: pickNumeric(source, [
                'progress.regular_matches_played',
                'progress.regular.played',
                'progress.runkosarja.played',
                'progress.regular_played'
            ]),
            total: pickNumeric(source, [
                'progress.regular_matches_total',
                'progress.regular.total',
                'progress.runkosarja.total',
                'progress.regular_total'
            ])
        };
        const statsPlayoffs = {
            played: pickNumeric(source, [
                'progress.playoff_matches_played',
                'progress.playoffs.played',
                'progress.playoff.played'
            ]),
            total: pickNumeric(source, [
                'progress.playoff_matches_total',
                'progress.playoffs.total',
                'progress.playoff.total'
            ])
        };
        if (forcedPlayoffTotal > 0) {
            statsPlayoffs.total = forcedPlayoffTotal;
        }
        const statsOverall = {
            played: pickNumeric(source, [
                'progress.overall_matches_played',
                'progress.overall.played',
                'matches_played',
                'played_matches'
            ]),
            total: pickNumeric(source, [
                'progress.overall_matches_total',
                'progress.overall.total',
                'matches_total',
                'scheduled_matches'
            ])
        };
        const regularTargetTotal = Number.isFinite(statsRegular.total)
            ? statsRegular.total
            : regularAggregate.total;
        const forcedOverallTotal = Math.max(0, regularTargetTotal) + Math.max(0, forcedPlayoffTotal);
        if (forcedOverallTotal > 0) {
            statsOverall.total = forcedOverallTotal;
        }

        function finalizeSection(statsSection, aggregateSection) {
            const hasStatsPlayed = Number.isFinite(statsSection.played);
            const hasStatsTotal = Number.isFinite(statsSection.total);
            const played = hasStatsPlayed ? statsSection.played : aggregateSection.played;
            const rawTotal = hasStatsTotal ? statsSection.total : aggregateSection.total;
            const fallbackTotal =
                rawTotal > 0
                    ? rawTotal
                    : hasStatsPlayed
                        ? played
                        : aggregateSection.effectiveTotal;
            const total = Math.max(0, fallbackTotal);
            const usedEstimate = aggregateSection.estimatedTotal > 0;
            let sourceLabel = 'derived';
            if (hasStatsPlayed && hasStatsTotal) {
                sourceLabel = 'stats';
            } else if (hasStatsPlayed || hasStatsTotal) {
                sourceLabel = 'mixed';
            } else if (usedEstimate) {
                sourceLabel = 'estimated';
            }
            return {
                played,
                total,
                percent: computePercent(played, total),
                source: sourceLabel
            };
        }

        return {
            regular: finalizeSection(statsRegular, regularAggregate),
            playoffs: finalizeSection(statsPlayoffs, playoffAggregate),
            overall: finalizeSection(statsOverall, overallAggregate)
        };
    }

    function inferTierValue(divisionId) {
        const numeric = toNumber(divisionId, 0);
        if (numeric >= 1 && numeric <= 5) return 1;
        if (numeric >= 6 && numeric <= 10) return 2;
        if (numeric >= 11 && numeric <= 15) return 3;
        if (numeric >= 16 && numeric <= 20) return 4;
        return 5;
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
        const warnings = [];
        if (!normalizerRef || typeof normalizerRef.normalizeDivision !== 'function') {
            warnings.push('Division normalizer unavailable.');
            return { ok: false, division: null, warnings };
        }
        try {
            const result = normalizerRef.normalizeDivision(raw);
            if (Array.isArray(result?.warnings) && result.warnings.length) {
                warnings.push(...result.warnings);
            }
            if (result && result.division) {
                return { ok: result.ok !== false, division: result.division, warnings };
            }
            if (result?.error) {
                warnings.push(result.error);
            } else {
                warnings.push('Division normalizer returned no data.');
            }
        } catch (error) {
            warnings.push(error?.message || 'Normalization error');
        }
        warnings.push('Normalization failed');
        return { ok: false, division: null, warnings };
    }

    function normalizeDivisionCollection(source) {
        if (!Array.isArray(source) || !source.length) {
            return { divisions: [], warnings: [] };
        }
        const warnings = [];
        const mapped = source
            .map((entry, index) => {
                const result = normalizeDivisionEntry(entry, index);
                if (Array.isArray(result.warnings) && result.warnings.length) {
                    warnings.push(...result.warnings);
                }
                return result.division;
            })
            .filter(Boolean);
        return { divisions: mapped, warnings };
    }

    function metricsToTotals(metrics) {
        if (!Array.isArray(metrics) || !metrics.length) {
            return null;
        }
        return metrics.reduce((acc, metric) => {
            if (metric && metric.id) {
                acc[metric.id] = Number(metric.value) || 0;
            }
            return acc;
        }, {});
    }

    function normalizeSummary(raw) {
        if (!raw || typeof raw !== 'object') {
            return { raw: null, aggregates: {} };
        }
        const summaryTotals =
            raw.summary_totals ||
            raw.summaryTotals ||
            raw.totals ||
            metricsToTotals(raw.metrics);
        if (summaryTotals) {
            const aggregates = {
                ...raw,
                summary_totals: summaryTotals,
                summaryTotals,
                totals: raw.totals || summaryTotals
            };
            Object.entries(summaryTotals).forEach(([key, value]) => {
                if (aggregates[key] === undefined) {
                    aggregates[key] = value;
                }
            });
            return { raw, aggregates };
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
            usingCache: false,
            bannerMessage: '',
            dataBadge: '',
            health: { ok: true, probableRoute: false, route: null }
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
                    const EndpointMissingError = typeof window !== 'undefined' ? window.ApiEndpointNotFound : null;
                    const [healthResult, summaryResult, divisionsResult] = await Promise.allSettled([
                        window.apiClient.healthCheck(identifier).catch(() => ({ ok: false })),
                        window.apiClient.getSeasonSummary(identifier),
                        window.apiClient.getDivisions(identifier)
                    ]);

                    const healthPayload = healthResult.status === 'fulfilled' ? healthResult.value || { ok: false } : { ok: false };
                    const healthOk = Boolean(healthPayload?.ok);

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
                    const normalizedSummary = normalizeSummary(summaryData);
                    const normalizedStats = normalizedSummary.aggregates || {};
                    const normalizedRaw = normalizedSummary.raw;
                    if (typeof console !== 'undefined' && console.log) {
                        console.log('seasonSummary', identifier, normalizedRaw);
                    }

                    let divisionsData = [];
                    let divisionsMeta = null;
                    let apiValidationWarnings = [];
                    let missingRoutes = false;
                    if (divisionsResult.status === 'fulfilled') {
                        divisionsData = Array.isArray(divisionsResult.value.data)
                            ? divisionsResult.value.data
                            : Array.isArray(divisionsResult.value)
                                ? divisionsResult.value
                                : [];
                        divisionsMeta = divisionsResult.value.meta || {};
                        apiValidationWarnings = (divisionsResult.value.errors || []).map(
                            error => error?.message || 'Division validation error'
                        );
                    } else {
                        const reason = divisionsResult.reason;
                        const isRouteError = EndpointMissingError && reason instanceof EndpointMissingError;
                        if (isRouteError) {
                            missingRoutes = true;
                        } else {
                            const fallback = await window.apiClient
                                .getDivisionsBySeason(identifier)
                                .then(res => (Array.isArray(res?.data) ? res.data : res))
                                .catch(() => []);
                            divisionsData = Array.isArray(fallback) ? fallback : [];
                        }
                    }

                    if (divisionsData.length === 0 && DEV_MODE) {
                        console.warn('[homeStore] Divisions API returned 0 items', {
                            meta: divisionsMeta,
                            cacheKeys: getCacheDebugKeys()
                        });
                    }

                    const { divisions: normalizedDivisionsFromApi, warnings: normalizationWarningsRaw } = normalizeDivisionCollection(divisionsData);
                    const normalizationWarnings = [...apiValidationWarnings, ...normalizationWarningsRaw];
                    const rawCount = Array.isArray(divisionsData) ? divisionsData.length : 0;
                    const normalizedCount = normalizedDivisionsFromApi.length;

                    let finalDivisions = normalizedDivisionsFromApi;
                    let finalRawDivisions = divisionsData;
                    let dataBadge = '';
                    let bannerMessage = '';
                    let usingCache = Boolean(summaryMeta?.usedCacheDueToError || divisionsMeta?.usedCacheDueToError);
                    let cacheTimestamp = summaryMeta?.cacheTimestamp || divisionsMeta?.cacheTimestamp || null;
                    let resolvedPathLabel = divisionsMeta?.resolvedPath || 'unresolved';

                    if (missingRoutes) {
                        if (existing && Array.isArray(existing.divisions) && existing.divisions.length) {
                            finalDivisions = existing.divisions.slice();
                            finalRawDivisions = Array.isArray(existing.rawDivisions)
                                ? existing.rawDivisions.slice()
                                : [];
                            bannerMessage = formatCacheBanner(existing.cacheTimestamp || existing.fetchedAt);
                            cacheTimestamp = existing.cacheTimestamp || existing.fetchedAt || cacheTimestamp;
                            usingCache = true;
                            resolvedPathLabel = 'cache';
                        } else if (DEV_MOCK_DATA && Array.isArray(DEV_MOCK_DATA.divisions) && DEV_MOCK_DATA.divisions.length) {
                            const devResult = normalizeDivisionCollection(DEV_MOCK_DATA.divisions);
                            finalDivisions = devResult.divisions;
                            finalRawDivisions = DEV_MOCK_DATA.divisions;
                            normalizationWarnings.push(...devResult.warnings, 'Using developer mock dataset.');
                            bannerMessage = 'Backend routes not found; showing developer mock data.';
                            dataBadge = 'DEV DATA';
                            cacheTimestamp = DEV_MOCK_DATA.timestamp || cacheTimestamp || Date.now();
                            usingCache = true;
                            resolvedPathLabel = 'dev-mock';
                        } else {
                            throw divisionsResult.reason || new Error('Division routes unavailable');
                        }
                    }

                    const finalCount = finalDivisions.length;
                    const stats = normalizedStats;
                    const progress = computeProgress(stats, finalDivisions);
                    const offline = !healthOk || usingCache || Boolean(bannerMessage);
                    divisionsMeta = { ...(divisionsMeta || {}), resolvedPath: resolvedPathLabel };

                    if (typeof console !== 'undefined' && console.info) {
                        console.info('[homeStore] divisions raw %s → normalized %s → filtered %s (used=%s)', rawCount, normalizedCount, finalCount, resolvedPathLabel);
                    }

                    const payload = {
                        loading: false,
                        error: null,
                        apiParam: identifier,
                        fetchedAt: Date.now(),
                        stats,
                        rawStats: normalizedRaw,
                        rawDivisions: finalRawDivisions,
                        divisionsMeta,
                        divisions: finalDivisions,
                        progress,
                        offline,
                        cacheTimestamp,
                        usingCache,
                        validationWarnings: normalizationWarnings,
                        warningMessage: finalDivisions.length ? '' : 'Division data missing or invalid.',
                        bannerMessage,
                        dataBadge,
                        health: healthPayload
                    };

                    this.seasonCache[key] = payload;
                    if (DEV_MODE) {
                        console.info('[homeStore] fetchSeason success', {
                            key,
                            divisions: finalDivisions.length,
                            offline,
                            cacheTimestamp,
                            resolvedPath: resolvedPathLabel,
                            banner: bannerMessage || null,
                            badge: dataBadge || null
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
