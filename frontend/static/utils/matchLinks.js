(function () {
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

    function getReplay2DUrl(demoUrl) {
        if (demoUrl === null || demoUrl === undefined || demoUrl === '') return '';
        const normalized = String(demoUrl).trim();
        if (!normalized) return '';
        return `https://replay.pappa.aukko.net/player?demourl=${encodeURIComponent(normalized)}`;
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
        const items = await fetchDemoLinksForMatch({
            apiClient,
            championshipId,
            matchId,
            expectedCount: targetCount || null,
            forceRefresh,
            persistCache
        });
        let mapped = mapDemoItemsToByIndex(items);

        const hasMapped = Object.keys(mapped).length > 0;
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
        getReplay2DUrl,
        extractAvailableDemoLinks,
        buildDemoMatchRequests,
        fetchDemoLinksForMatch,
        fetchDemoAvailabilityForMatch
    };
})();
