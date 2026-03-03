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
            maps.forEach((map, idx) => {
                if (map?.isForfeit) return;
                out.push({ matchId, demoIndex: idx });
            });
        });
        return out;
    }

    async function fetchDemoAvailabilityForMatch({
        apiClient,
        championshipId,
        matchId,
        mapsCount,
        existingByIndex = {},
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
            if (Object.prototype.hasOwnProperty.call(next, idx)) return;
            try {
                const result = await apiClient.getMatchDemoExists({
                    championshipId,
                    matchId,
                    demoIndex: idx
                }, { persistCache });
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
        persistCache = false
    }) {
        const byMatch = {};
        if (!apiClient || typeof apiClient.getMatchDemoExists !== 'function') return byMatch;
        if (!championshipId || !Array.isArray(candidates) || !candidates.length) return byMatch;

        await Promise.all(candidates.map(async item => {
            const matchId = String(item?.matchId || '');
            const demoIndex = Number(item?.demoIndex ?? -1);
            if (!matchId || demoIndex < 0) return;
            try {
                const result = await apiClient.getMatchDemoExists({
                    championshipId,
                    matchId,
                    demoIndex
                }, { persistCache });
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
