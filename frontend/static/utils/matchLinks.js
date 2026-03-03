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

    function buildDemoCandidates(matches = []) {
        if (!Array.isArray(matches)) return [];
        const out = [];
        matches.forEach(match => {
            if (!match || !match.played) return;
            const matchId = resolveMatchId(match);
            if (!matchId) return;
            const maps = Array.isArray(match.maps) ? match.maps : [];
            if (maps.length) {
                maps.forEach((_map, idx) => {
                    out.push({ matchId, demoIndex: idx });
                });
                return;
            }
            const bestOf = Number(match.bestOf ?? match.best_of ?? 0);
            if (Number.isFinite(bestOf) && bestOf > 0) {
                for (let idx = 0; idx < bestOf; idx += 1) {
                    out.push({ matchId, demoIndex: idx });
                }
            }
        });
        return out;
    }

    async function fetchDemoAvailabilityForMatch({
        apiClient,
        championshipId,
        matchId,
        mapsCount,
        existingByIndex = {},
        refreshFalse = false,
        forceRefresh = false,
        persistCache = false
    }) {
        if (!apiClient || typeof apiClient.getMatchDemoExists !== 'function') {
            return { ...(existingByIndex || {}) };
        }
        if (!championshipId || !matchId) {
            return { ...(existingByIndex || {}) };
        }

        const targetCount = Math.max(0, Number(mapsCount) || 0);
        if (!targetCount) return { ...(existingByIndex || {}) };

        const next = { ...(existingByIndex || {}) };
        await Promise.all(Array.from({ length: targetCount }, async (_unused, idx) => {
            const hasExisting = Object.prototype.hasOwnProperty.call(next, idx);
            if (hasExisting) {
                const existing = next[idx] || {};
                const shouldRefreshFalse = refreshFalse === true && existing?.exists === false;
                if (!shouldRefreshFalse) return;
            }
            try {
                const result = await apiClient.getMatchDemoExists({
                    championshipId,
                    matchId,
                    demoIndex: idx
                }, {
                    persistCache,
                    forceRefresh: forceRefresh === true || (refreshFalse === true && hasExisting)
                });
                next[idx] = {
                    exists: !!result?.exists,
                    url: result?.url || ''
                };
            } catch (_error) {
                next[idx] = { exists: false, url: '' };
            }
        }));
        return next;
    }

    async function fetchDemoAvailabilityForCandidates({
        apiClient,
        championshipId,
        candidates,
        existingByMatch = {},
        refreshFalse = false,
        forceRefresh = false,
        persistCache = false
    }) {
        const byMatch = { ...(existingByMatch || {}) };
        if (!apiClient || typeof apiClient.getMatchDemoExists !== 'function') return byMatch;
        if (!championshipId || !Array.isArray(candidates) || !candidates.length) return byMatch;

        await Promise.all(candidates.map(async item => {
            const matchId = String(item?.matchId || '');
            const demoIndex = Number(item?.demoIndex ?? -1);
            if (!matchId || demoIndex < 0) return;
            const existingPayload = byMatch?.[matchId]?.[demoIndex] || null;
            if (existingPayload && !(refreshFalse && existingPayload.exists === false)) {
                return;
            }
            try {
                const result = await apiClient.getMatchDemoExists({
                    championshipId,
                    matchId,
                    demoIndex
                }, {
                    persistCache,
                    forceRefresh: forceRefresh === true || (refreshFalse === true && existingPayload?.exists === false)
                });
                if (!byMatch[matchId]) byMatch[matchId] = {};
                byMatch[matchId][demoIndex] = {
                    exists: !!result?.exists,
                    url: result?.url || ''
                };
            } catch (_error) {
                if (!byMatch[matchId]) byMatch[matchId] = {};
                byMatch[matchId][demoIndex] = { exists: false, url: '' };
            }
        }));

        return byMatch;
    }

    window.MatchLinksUtils = {
        resolveMatchId,
        getFaceitRoomUrl,
        extractAvailableDemoLinks,
        buildDemoCandidates,
        fetchDemoAvailabilityForMatch,
        fetchDemoAvailabilityForCandidates
    };
})();
