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
        matchesPlayed: 0,
        matchesTotal: 7,
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
        // Handle both camelCase and snake_case input, output camelCase
        const matchesTotal = Math.max(0, toNumber(block.matchesTotal ?? block.matches_total, 0));
        const matchesPlayed = clamp(block.matchesPlayed ?? block.matches_played, 0, matchesTotal || 0);
        return {
            ...block,
            matchesPlayed: matchesPlayed,
            matchesTotal: matchesTotal
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
        
        // Handle both camelCase (API) and snake_case (legacy) field names
        // Output in camelCase for component compatibility
        const normalizedSeason = clampMatches({
            teams: toNumber(seasonRaw.teams, 0),
            matchesPlayed: toNumber(seasonRaw.matches_played ?? seasonRaw.matchesPlayed, 0),
            matchesTotal: toNumber(seasonRaw.matches_total ?? seasonRaw.matchesTotal, 0),
            status: ['waiting', 'active', 'finished'].includes(seasonStatus) ? seasonStatus : 'waiting',
            winner: seasonRaw.winner ?? seasonRaw.winner_team ? String(seasonRaw.winner ?? seasonRaw.winner_team) : null
        });

        const normalizedPlayoffs = clampMatches({
            teams: toNumber(playoffsRaw.teams, DEFAULT_PLAYOFFS.teams) || DEFAULT_PLAYOFFS.teams,
            matchesPlayed: toNumber(playoffsRaw.matches_played ?? playoffsRaw.matchesPlayed, DEFAULT_PLAYOFFS.matches_played),
            matchesTotal: toNumber(playoffsRaw.matches_total ?? playoffsRaw.matchesTotal, DEFAULT_PLAYOFFS.matches_total) || DEFAULT_PLAYOFFS.matches_total,
            status: ['waiting', 'active', 'finished'].includes(playoffsStatus) ? playoffsStatus : DEFAULT_PLAYOFFS.status,
            winner: playoffsRaw.winner ?? playoffsRaw.winner_team ? String(playoffsRaw.winner ?? playoffsRaw.winner_team) : null
        });

        // Normalize best player and MVP team data (handle both camelCase and snake_case)
        const metaData = raw.meta ?? raw.metadata ?? {};
        const bestPlayer = metaData.mvp_player ?? metaData.best_player ?? raw.best_player ?? raw.bestPlayer;
        const mvpTeam = metaData.winner_team ?? metaData.mvp_team ?? raw.mvp_team ?? raw.mvpTeam;
        const winners = Array.isArray(raw.winners) ? raw.winners : [];

        // Calculate overall division status based on season and playoff status
        let overallStatus = raw.status ?? seasonStatus;
        if (overallStatus === 'finished' && playoffsStatus === 'active') {
            overallStatus = 'active'; // Season finished but playoffs ongoing
        }

        return {
            ok: true,
            warnings,
            division: {
                id: resolvedId,
                divisionId: toNumber(raw.division_id ?? raw.divisionId, null),
                name: String(name),
                tier,
                tierMeta: TIER_RANGES.find(entry => entry.id === tier) || TIER_RANGES[TIER_RANGES.length - 1],
                status: overallStatus,
                season: normalizedSeason,
                playoffs: normalizedPlayoffs,
                bestPlayer: bestPlayer ? {
                    name: bestPlayer.name,
                    rating: toNumber(bestPlayer.rating, 0)
                } : null,
                mvpTeam: mvpTeam ? String(mvpTeam) : null,
                winners: winners,
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
