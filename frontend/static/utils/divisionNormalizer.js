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
        teams: 0,
        matchesPlayed: 0,
        matchesTotal: 0,
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

    function cleanDivisionName(rawName) {
        if (!rawName) return '';
        // Remove season suffix like "S11", "S12", etc.
        return String(rawName).replace(/\s+S\d+$/i, '').trim();
    }

    function formatSeasonLabel(value) {
        if (value === null || value === undefined) return '';
        const text = String(value).trim();
        if (!text) return '';
        if (/^kausi\s+/i.test(text)) return text;
        if (/^s\d+/i.test(text)) return text.toUpperCase();
        return `Kausi ${text}`;
    }

    function normalizeDivisionName({ name, divisionNum }) {
        const rawName = cleanDivisionName(name);
        if (rawName && !isLikelyId(rawName)) return rawName;
        const numericDivision = toNumber(divisionNum, null);
        if (numericDivision === 0) return 'Mestaruussarja';
        if (numericDivision != null) return `${numericDivision} Divisioona`;
        return 'Divisioona';
    }

    function buildDivisionBreadcrumbMeta(input = {}) {
        const normalizedName = normalizeDivisionName({
            name: input.name,
            divisionNum: input.divisionNum
        });
        const seasonLabel = formatSeasonLabel(input.season);
        let label = seasonLabel ? `${normalizedName} (${seasonLabel})` : normalizedName;
        if (input.isPlayoffs && !/playoffs?/i.test(label)) {
            label = `${label} (Playoffs)`;
        }
        return {
            name: normalizedName,
            label
        };
    }

    function isLikelyId(value) {
        if (value === null || value === undefined) return false;
        const text = String(value).trim();
        if (!text) return false;
        if (/^\d+$/.test(text)) return true;
        return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(text);
    }

    function slugifyDivisionName(value) {
        if (!value) return '';
        const base = String(value);
        const normalized = typeof base.normalize === 'function' ? base.normalize('NFD') : base;
        return normalized
            .replace(/[\u0300-\u036f]/g, '')
            .replace(/[^a-zA-Z0-9]+/g, '-')
            .replace(/^-+|-+$/g, '')
            .toLowerCase();
    }

    function getDivisionSlug(division) {
        if (!division || typeof division !== 'object') {
            return null;
        }
        const raw = division.raw && typeof division.raw === 'object' ? division.raw : division;
        const rawSlug = raw.slug || division.slug || null;
        const name = cleanDivisionName(raw.name || division.name || '');
        const derived = slugifyDivisionName(name);

        if (rawSlug && !isLikelyId(rawSlug)) {
            return String(rawSlug);
        }
        if (derived) {
            return derived;
        }
        return rawSlug || raw.division_id || raw.divisionId || raw.id || division.id || null;
    }

    function getDivisionHrefId(division) {
        if (!division || typeof division !== 'object') {
            return null;
        }
        return getDivisionSlug(division);
    }

    function getPlayoffsHrefId(division) {
        if (!division || typeof division !== 'object') {
            return null;
        }
        const raw = division.raw && typeof division.raw === 'object' ? division.raw : division;
        const playoffs = raw.playoffs && typeof raw.playoffs === 'object' ? raw.playoffs : division.playoffs || {};
        return (
            playoffs.playoff_championship_id ||
            playoffs.playoffChampionshipId ||
            playoffs.championship_id ||
            playoffs.championshipId ||
            null
        );
    }

    function hasPlayoffsStarted(division) {
        if (!division || typeof division !== 'object') {
            return false;
        }
        const raw = division.raw && typeof division.raw === 'object' ? division.raw : division;
        const playoffs = raw.playoffs && typeof raw.playoffs === 'object' ? raw.playoffs : division.playoffs || {};
        const matchesPlayed = toNumber(playoffs.matches_played ?? playoffs.matchesPlayed, 0);
        const status = String(playoffs.status || '').toLowerCase();
        return matchesPlayed > 0 || status === 'active' || status === 'finished';
    }

    function clampMatches(block) {
        const matchesTotal = Math.max(0, toNumber(block.matches_played_total ?? block.matches_total, 0));
        const rawPlayed = toNumber(block.matches_played, 0);
        const playedCap = matchesTotal > 0 ? matchesTotal : Math.max(rawPlayed, 0);
        const matchesPlayed = clamp(rawPlayed, 0, playedCap);
        return {
            ...block,
            matchesPlayed,
            matchesTotal
        };
    }

    function normalizeDivision(raw) {
        const warnings = [];
        if (!raw || typeof raw !== 'object') {
            return { ok: false, error: 'Division payload missing', warnings };
        }
        const divisionId =
            raw.division_id ??
            raw.divisionId ??
            raw.id ??
            raw.championship_id ??
            raw.championshipId;
        if (divisionId === undefined || divisionId === null) {
            warnings.push('division_id missing');
            return { ok: false, error: 'division_id missing', warnings };
        }
        const resolvedId = String(divisionId);
        const name = raw.name || `Division ${divisionId}`;
        const tierValue = toNumber(raw.tier, null);
        const tier = Number.isFinite(tierValue) ? tierValue : inferTier(divisionId);
        const seasonRaw = raw.season && typeof raw.season === 'object' ? raw.season : {};
        const playoffsRaw =
            raw.playoffs && typeof raw.playoffs === 'object'
                ? raw.playoffs
                : raw.playoff && typeof raw.playoff === 'object'
                    ? raw.playoff
                    : DEFAULT_PLAYOFFS;

        const normalizeStatus = (value, fallback = 'waiting') => {
            const normalized = String(value || '').toLowerCase();
            return ['waiting', 'active', 'finished'].includes(normalized) ? normalized : fallback;
        };

        const seasonStatus = normalizeStatus(seasonRaw.status || seasonRaw.state || raw.status, 'waiting');
        const playoffsStatus = normalizeStatus(playoffsRaw.status || raw.playoff_status, DEFAULT_PLAYOFFS.status);

        const normalizedSeason = clampMatches({
            teams: toNumber(seasonRaw.teams, 0),
            matches_played: toNumber(seasonRaw.matches_played ?? seasonRaw.matchesPlayed, 0),
            matches_total: toNumber(seasonRaw.matches_total ?? seasonRaw.matchesTotal, 0),
            status: seasonStatus,
            winner: seasonRaw.winner_team ? String(seasonRaw.winner_team) : null
        });

        const normalizedPlayoffs = clampMatches({
            teams: toNumber(playoffsRaw.teams, DEFAULT_PLAYOFFS.teams) || DEFAULT_PLAYOFFS.teams,
            matches_played: toNumber(
                playoffsRaw.matches_played ?? playoffsRaw.matchesPlayed,
                DEFAULT_PLAYOFFS.matchesPlayed
            ),
            matches_total:
                toNumber(playoffsRaw.matches_total ?? playoffsRaw.matchesTotal, DEFAULT_PLAYOFFS.matchesTotal) ||
                DEFAULT_PLAYOFFS.matchesTotal,
            status: playoffsStatus,
            winner: playoffsRaw.winner_team ? String(playoffsRaw.winner_team) : null
        });

        const metaData = raw.meta ?? {};
        const bestPlayer = metaData.best_player;
        const mvpTeam = metaData.winner_team;
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
                divisionId: toNumber(raw.division_id, null),
                name: String(name),
                tier,
                tierMeta: TIER_RANGES.find(entry => entry.id === tier) || TIER_RANGES[TIER_RANGES.length - 1],
                status: overallStatus,
                season: normalizedSeason,
                playoffs: normalizedPlayoffs,
                bestPlayer: bestPlayer ? {
                    name: bestPlayer.name,
                    kd: toNumber(bestPlayer.kd, 0)
                } : null,
                mvpTeam: mvpTeam ? String(mvpTeam) : null,
                winners: winners,
                slug: getDivisionSlug(raw),
                seasonNumber: raw.season_number ?? null,
                raw
            }
        };
    }

    window.divisionNormalizer = Object.freeze({
        normalizeDivision,
        inferTier,
        cleanDivisionName,
        formatSeasonLabel,
        normalizeDivisionName,
        buildDivisionBreadcrumbMeta,
        slugifyDivisionName,
        getDivisionSlug,
        getDivisionHrefId,
        getPlayoffsHrefId,
        hasPlayoffsStarted
    });
})();
