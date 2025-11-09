(function () {
    'use strict';

    const TIER_RANGES = [
        { id: 1, label: 'Tier 1 (Div 1-5)', range: 'Div 1-5', min: 1, max: 5 },
        { id: 2, label: 'Tier 2 (Div 6-10)', range: 'Div 6-10', min: 6, max: 10 },
        { id: 3, label: 'Tier 3 (Div 11-15)', range: 'Div 11-15', min: 11, max: 15 },
        { id: 4, label: 'Tier 4 (Div 16-20)', range: 'Div 16-20', min: 16, max: 20 },
        { id: 5, label: 'Tier 5 (Div 21-25)', range: 'Div 21-25', min: 21, max: 25 }
    ];

    const DEFAULT_PLAYOFFS = Object.freeze({
        teams: 8,
        matches_played: 0,
        matches_total: 7,
        status: 'waiting',
        winner: null
    });

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

    function clamp(value, min, max) {
        const numeric = toNumber(value, min);
        if (!Number.isFinite(numeric)) {
            return min;
        }
        return Math.max(min, Math.min(max, numeric));
    }

    function inferTier(divisionId) {
        const numeric = toNumber(divisionId, 0);
        const match = TIER_RANGES.find(entry => numeric >= entry.min && numeric <= entry.max);
        return match ? match.id : 5;
    }

    function clampMatches(block) {
        const matchesTotal = Math.max(0, toNumber(block.matches_total, 0));
        const matchesPlayed = clamp(block.matches_played, 0, matchesTotal || 0);
        return {
            ...block,
            matches_played: matchesPlayed,
            matches_total: matchesTotal
        };
    }

    function normalizeDivision(raw) {
        const warnings = [];
        if (!raw || typeof raw !== 'object') {
            return { ok: false, error: 'Division payload missing', warnings };
        }
        const divisionId = raw.division_id ?? raw.divisionId ?? raw.id ?? raw.slug;
        if (divisionId === undefined || divisionId === null) {
            warnings.push('division_id missing');
            return { ok: false, error: 'division_id missing', warnings };
        }
        const resolvedId = String(divisionId);
        const name = raw.name || `Division ${divisionId}`;
        const tierValue = toNumber(raw.tier, null);
        const tier = Number.isFinite(tierValue) ? tierValue : inferTier(divisionId);
        const seasonRaw = raw.season && typeof raw.season === 'object' ? raw.season : {};
        const playoffsRaw = raw.playoffs && typeof raw.playoffs === 'object' ? raw.playoffs : DEFAULT_PLAYOFFS;

        const seasonStatus = (seasonRaw.status || 'waiting').toLowerCase();
        const playoffsStatus = (playoffsRaw.status || DEFAULT_PLAYOFFS.status).toLowerCase();
        const normalizedSeason = clampMatches({
            teams: toNumber(seasonRaw.teams, 0),
            matches_played: toNumber(seasonRaw.matches_played, 0),
            matches_total: toNumber(seasonRaw.matches_total, 0),
            status: ['waiting', 'active', 'finished'].includes(seasonStatus) ? seasonStatus : 'waiting',
            winner: seasonRaw.winner ? String(seasonRaw.winner) : null
        });

        const normalizedPlayoffs = clampMatches({
            teams: toNumber(playoffsRaw.teams, DEFAULT_PLAYOFFS.teams) || DEFAULT_PLAYOFFS.teams,
            matches_played: toNumber(playoffsRaw.matches_played, DEFAULT_PLAYOFFS.matches_played),
            matches_total: toNumber(playoffsRaw.matches_total, DEFAULT_PLAYOFFS.matches_total) || DEFAULT_PLAYOFFS.matches_total,
            status: ['waiting', 'active', 'finished'].includes(playoffsStatus) ? playoffsStatus : DEFAULT_PLAYOFFS.status,
            winner: playoffsRaw.winner ? String(playoffsRaw.winner) : null
        });

        return {
            ok: true,
            warnings,
            division: {
                id: resolvedId,
                divisionId: toNumber(raw.division_id ?? raw.divisionId, null),
                name: String(name),
                tier,
                tierMeta: TIER_RANGES.find(entry => entry.id === tier) || TIER_RANGES[TIER_RANGES.length - 1],
                season: normalizedSeason,
                playoffs: normalizedPlayoffs,
                slug: raw.slug || null,
                seasonNumber: raw.season_number ?? raw.seasonNumber ?? null,
                raw
            }
        };
    }

    window.divisionNormalizer = Object.freeze({
        normalizeDivision,
        inferTier
    });
})();
