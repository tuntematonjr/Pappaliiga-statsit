(function () {
    const demoDebugEnabled =
        typeof window !== 'undefined' &&
        (window.PL_DEMO_DEBUG === true || window.PL_DEMO_DEBUG === '1' || ['localhost', '127.0.0.1'].includes(window.location.hostname));
    const backgroundRetryDone = new Set();

    function delay(ms) {
        const timeout = Number(ms);
        if (!Number.isFinite(timeout) || timeout <= 0) return Promise.resolve();
        return new Promise(resolve => setTimeout(resolve, timeout));
    }

    function resolveMatchId(match) {
        if (!match || typeof match !== 'object') return null;
        const value = match.matchId ?? match.match_id ?? null;
        if (value === null || value === undefined || value === '') return null;
        return String(value);
    }

    function getFaceitRoomUrl(matchId) {
        if (matchId === null || matchId === undefined || matchId === '') return '';
        return `https://www.faceit.com/cs2/room/${matchId}`;
    }

    function extractAvailableDemoLinks(byMatch = {}, match = null) {
        const matchId = resolveMatchId(match);
        if (!matchId) return [];
        const hit = byMatch?.[matchId] || {};
        return Object.entries(hit)
            .map(([demoIndex, payload]) => ({
                demoIndex: Number(demoIndex),
                exists: !!payload?.exists,
                url: payload?.url || ''
            }))
            .filter(item => item.exists && item.url)
            .sort((a, b) => a.demoIndex - b.demoIndex);
    }

    function buildDemoMatchRequests(matches = []) {
        if (!Array.isArray(matches)) return [];
        const out = [];
        matches.forEach(match => {
            if (!match || !match.played) return;
            const matchId = resolveMatchId(match);
            if (!matchId) return;
            const maps = Array.isArray(match.maps) ? match.maps : [];
            const mapsCount = maps.length;
            const bestOf = Number(match.bestOf ?? match.best_of ?? 0);
            const expectedCount = Math.max(mapsCount, Number.isFinite(bestOf) && bestOf > 0 ? bestOf : 0, 2);
            out.push({ matchId, expectedCount });
        });
        return out;
    }

    function mapDemoItemsToByIndex(items = []) {
        const byIndex = {};
        const sorted = [...(Array.isArray(items) ? items : [])]
            .map(item => ({
                sourceIndex: Number(item?.demo_index ?? item?.demoIndex ?? -1),
                url: String(item?.url || '')
            }))
            .filter(item => item.sourceIndex >= 0 && item.url)
            .sort((a, b) => a.sourceIndex - b.sourceIndex);

        sorted.forEach((item, visualIndex) => {
            byIndex[visualIndex] = {
                exists: true,
                url: item.url,
                sourceIndex: item.sourceIndex
            };
        });
        return byIndex;
    }

    async function fetchDemoLinksForMatch({
        apiClient,
        championshipId,
        matchId,
        expectedCount = null,
        forceRefresh = false,
        persistCache = false
    }) {
        if (!apiClient || typeof apiClient.getMatchDemos !== 'function') {
            return [];
        }
        if (!championshipId || !matchId) {
            return [];
        }

        try {
            const items = await apiClient.getMatchDemos({
                championshipId,
                matchId,
                expectedCount
            }, {
                persistCache,
                forceRefresh
            });
            return Array.isArray(items) ? items : [];
        } catch (_error) {
            return [];
        }
    }

    async function fetchDemoAvailabilityForMatch({
        apiClient,
        championshipId,
        matchId,
        mapsCount,
        existingByIndex = {},
        refreshFalse = false,
        forceRefresh = false,
        persistCache = false,
        onBackgroundResult = null
    }) {
        if (!apiClient) {
            return { ...(existingByIndex || {}) };
        }
        if (!championshipId || !matchId) {
            return { ...(existingByIndex || {}) };
        }

        const targetCount = Math.max(0, Number(mapsCount) || 0);
        if (demoDebugEnabled && typeof console !== 'undefined') {
            console.info('[matchLinks][demos] availability_start', {
                championshipId: String(championshipId),
                matchId: String(matchId),
                mapsCount: targetCount,
                forceRefresh: forceRefresh === true,
                existingKeys: Object.keys(existingByIndex || {}).length
            });
        }

        const items = await fetchDemoLinksForMatch({
            apiClient,
            championshipId,
            matchId,
            expectedCount: targetCount || null,
            forceRefresh,
            persistCache
        });
        let mapped = mapDemoItemsToByIndex(items);

        const shouldRetryForced =
            !Object.keys(mapped).length &&
            targetCount > 0 &&
            forceRefresh !== true;

        if (shouldRetryForced) {
            if (demoDebugEnabled && typeof console !== 'undefined') {
                console.info('[matchLinks][demos] forced_retry_start', {
                    championshipId: String(championshipId),
                    matchId: String(matchId),
                    expectedCount: targetCount
                });
            }
            await delay(120);
            const forcedItems = await fetchDemoLinksForMatch({
                apiClient,
                championshipId,
                matchId,
                expectedCount: targetCount || null,
                forceRefresh: true,
                persistCache: false
            });
            mapped = mapDemoItemsToByIndex(forcedItems);
            if (demoDebugEnabled && typeof console !== 'undefined') {
                console.info('[matchLinks][demos] forced_retry_done', {
                    championshipId: String(championshipId),
                    matchId: String(matchId),
                    count: Object.keys(mapped).length,
                    indices: Object.keys(mapped).map(key => Number(key)).sort((a, b) => a - b)
                });
            }
        }

        if (demoDebugEnabled && typeof console !== 'undefined') {
            console.info('[matchLinks][demos] availability_done', {
                championshipId: String(championshipId),
                matchId: String(matchId),
                count: Object.keys(mapped).length,
                refreshFalse: refreshFalse === true
            });
        }

        const hasMapped = Object.keys(mapped).length > 0;
        const backgroundKey = `${String(championshipId)}:${String(matchId)}:${targetCount}`;
        const shouldScheduleBackgroundRetry =
            !hasMapped &&
            targetCount > 0 &&
            forceRefresh !== true &&
            !backgroundRetryDone.has(backgroundKey);

        if (shouldScheduleBackgroundRetry) {
            backgroundRetryDone.add(backgroundKey);
            (async () => {
                await delay(2500);
                const delayedItems = await fetchDemoLinksForMatch({
                    apiClient,
                    championshipId,
                    matchId,
                    expectedCount: targetCount || null,
                    forceRefresh: true,
                    persistCache: false
                });
                const delayedMapped = mapDemoItemsToByIndex(delayedItems);
                const delayedCount = Object.keys(delayedMapped).length;

                if (demoDebugEnabled && typeof console !== 'undefined') {
                    console.info('[matchLinks][demos] background_retry_done', {
                        championshipId: String(championshipId),
                        matchId: String(matchId),
                        count: delayedCount,
                        indices: Object.keys(delayedMapped).map(key => Number(key)).sort((a, b) => a - b)
                    });
                }

                if (delayedCount > 0 && typeof onBackgroundResult === 'function') {
                    try {
                        onBackgroundResult(delayedMapped);
                    } catch (_error) {
                    }
                }
            })();
        }

        if (hasMapped) {
            return mapped;
        }
        if (refreshFalse) {
            return { ...(existingByIndex || {}) };
        }
        return mapped;
    }

    window.MatchLinksUtils = {
        resolveMatchId,
        getFaceitRoomUrl,
        extractAvailableDemoLinks,
        buildDemoMatchRequests,
        fetchDemoLinksForMatch,
        fetchDemoAvailabilityForMatch
    };
})();
