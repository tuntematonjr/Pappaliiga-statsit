// Team detail view that renders stats, maps, matches, players and veto aggregates.
// Every DB-backed field is surfaced as a stat, column, chart point or tooltip.

const isDevEnv = typeof window !== 'undefined' && ['localhost', '127.0.0.1'].includes(window.location.hostname);

const PLAYER_COLUMNS = [
    { key: 'nickname', label: 'Pelaaja', sortable: true, colClass: 'col-name' },
    { key: 'mapsPlayed', label: 'Kartat', sortable: true, numeric: true },
    { key: 'roundsPlayed', label: 'R', sortable: true, numeric: true },
    { key: 'rating', label: 'Rating', sortable: true, numeric: true, decimals: 2, colClass: 'col-rating' },
    { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2, colClass: 'col-kd' },
    { key: 'kr', label: 'K/R', sortable: true, numeric: true, decimals: 2 },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1, colClass: 'col-adr' },
    { key: 'hsPct', label: 'HS%', sortable: true, numeric: true, decimals: 1 },
    { key: 'kills', label: 'Kills', sortable: true, numeric: true },
    { key: 'deaths', label: 'Deaths', sortable: true, numeric: true },
    { key: 'assists', label: 'A', sortable: true, numeric: true },
    { key: 'damage', label: 'Dmg', sortable: true, numeric: true },
    { key: 'utilityDamage', label: 'U-Dmg', sortable: true, numeric: true },
    { key: 'mvps', label: 'MVP', sortable: true, numeric: true },
    { key: 'sniperKills', label: 'AWP', sortable: true, numeric: true },
    { key: 'pistolKills', label: 'Pistol', sortable: true, numeric: true },
    { key: 'entryLine', label: 'Entry', sortable: true, numeric: true },
    { key: 'clutch1v1Line', label: '1v1', sortable: true, numeric: true },
    { key: 'clutch1v2Line', label: '1v2', sortable: true, numeric: true },
    { key: 'clutchKills', label: 'Clutch K', sortable: true, numeric: true },
    { key: 'mk2k', label: '2K', sortable: true, numeric: true },
    { key: 'mk3k', label: '3K', sortable: true, numeric: true },
    { key: 'mk4k', label: '4K', sortable: true, numeric: true },
    { key: 'mk5k', label: 'Ace', sortable: true, numeric: true },
    { key: 'enemiesFlashed', label: 'Flashed', sortable: true, numeric: true },
    { key: 'flashSuccessLine', label: 'Flash%', sortable: true, numeric: true }
];

const MAP_COLUMNS = [
    { key: 'mapName', label: 'Kartta', sortable: true, colClass: 'col-name col-map-name', width: '210px' },
    { key: 'played', label: 'Pelattu', sortable: true, numeric: true },
    { key: 'games', label: 'Maps', sortable: true, numeric: true },
    { key: 'wins', label: 'W', sortable: true, numeric: true },
    { key: 'losses', label: 'L', sortable: true, numeric: true },
    { key: 'winrate', label: 'Win%', sortable: true, numeric: true, decimals: 1, colClass: 'col-winrate' },
    { key: 'rating', label: 'Rating', sortable: true, numeric: true, decimals: 2 },
    { key: 'picks', label: 'Picks', sortable: true, numeric: true },
    { key: 'oppPicks', label: 'Opp Picks', sortable: true, numeric: true },
    { key: 'pickRate', label: 'Pick%', sortable: true, numeric: true, decimals: 1 },
    { key: 'ban1', label: 'Ban1', sortable: true, numeric: true },
    { key: 'ban2', label: 'Ban2', sortable: true, numeric: true },
    { key: 'oppBan', label: 'Opp Ban', sortable: true, numeric: true },
    { key: 'totalOwnBan', label: 'Own Bans', sortable: true, numeric: true },
    { key: 'decov', label: 'Dec/OV', sortable: true, numeric: true },
    { key: 'rd', label: 'RD', sortable: true, numeric: true },
    { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2, colClass: 'col-kd' },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1, colClass: 'col-adr' },
    { key: 'ctWr', label: 'CT%', sortable: true, numeric: true, decimals: 1 },
    { key: 'tWr', label: 'T%', sortable: true, numeric: true, decimals: 1 },
    { key: 'damage', label: 'Dmg', sortable: true, numeric: true },
    { key: 'utilityDamage', label: 'U-Dmg', sortable: true, numeric: true },
    { key: 'mvps', label: 'MVP', sortable: true, numeric: true },
    { key: 'kills', label: 'Kills', sortable: true, numeric: true },
    { key: 'deaths', label: 'Deaths', sortable: true, numeric: true }
];

const SCOUT_MAP_COLUMNS = [
    { key: 'mapName', label: 'Map', sortable: true, colClass: 'col-name col-map-name', width: '200px' },
    { key: 'played', label: 'Pelattu', sortable: true, numeric: true, colClass: 'mono-num col-played' },
    { key: 'picks', label: 'Omat pickit', sortable: true, numeric: true, colClass: 'mono-num col-picks' },
    { key: 'oppPicks', label: 'Vastustajan pickit', sortable: true, numeric: true, colClass: 'mono-num col-opp-picks' },
    { key: 'winrate', label: 'Win %', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num col-winrate' },
    { key: 'pickWinRate', label: 'Win % (oma pick)', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num col-winrate-own' },
    { key: 'oppPickWinRate', label: 'Win % (vastustajan pick)', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num col-winrate-opp' },
    { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2, colClass: 'mono-num col-kd' },
    { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1, colClass: 'mono-num col-adr' },
    { key: 'rd', label: 'Eräero', sortable: true, numeric: true, colClass: 'mono-num col-rd' },
    { key: 'ban1', label: '1. banni (oma)', sortable: true, numeric: true, colClass: 'mono-num col-ban1' },
    { key: 'ban2', label: '2. banni (oma)', sortable: true, numeric: true, colClass: 'mono-num col-ban2' },
    { key: 'oppBan', label: 'Vastustajan banni', sortable: true, numeric: true, colClass: 'mono-num col-opp-ban' },
    { key: 'totalOwnBan', label: 'Banneja yhteensä', sortable: true, numeric: true, colClass: 'mono-num col-ban-total' },
    { key: 'decov', label: 'Decider / overflow', sortable: true, numeric: true, colClass: 'mono-num col-decov' }
];

const SCOUT_MAP_GROUPS = [
    { label: '', colSpan: 1, className: 'group-map' },
    { label: 'Pelattu', colSpan: 3, className: 'group-usage group-divider' },
    { label: 'Tulokset', colSpan: 3, className: 'group-results group-divider' },
    { label: 'Suorituskyky', colSpan: 3, className: 'group-performance group-divider' },
    { label: 'Bannit', colSpan: 4, className: 'group-veto group-divider' },
    { label: '', colSpan: 1, className: 'group-series group-divider' }
];

const VETO_COLUMNS = [
    { key: 'mapName', label: 'Kartta', sortable: true },
    { key: 'timesPicked', label: 'Omat pickit', sortable: true, numeric: true },
    { key: 'timesOpponentPicked', label: 'Vast. pickit', sortable: true, numeric: true },
    { key: 'timesBanned', label: 'Bannit', sortable: true, numeric: true },
    { key: 'pickRate', label: 'Pick%', sortable: true, numeric: true, decimals: 1 },
    { key: 'banRate', label: 'Ban%', sortable: true, numeric: true, decimals: 1 },
    { key: 'pickWinRate', label: 'Win pick', sortable: true, numeric: true, decimals: 1 },
    { key: 'deciderWinRate', label: 'Win decider', sortable: true, numeric: true, decimals: 1 }
];

const MATCHES_PAGE_SIZE = 8;

function createSegment() {
    return { data: null, loading: false, error: null, fetchedAt: null };
}

function safeDivide(num, den) {
    if (!den) return 0;
    return num / den;
}

function toNumber(value, fallback = 0) {
    if (value === null || value === undefined) return fallback;
    const numeric = Number(value);
    if (Number.isFinite(numeric)) return numeric;
    const parsed = Number(String(value).replace(',', '.'));
    return Number.isFinite(parsed) ? parsed : fallback;
}

function formatNumber(value, decimals = 0) {
    const numeric = toNumber(value);
    if (!Number.isFinite(numeric)) return '-';
    return decimals > 0 ? numeric.toFixed(decimals) : numeric.toLocaleString('fi-FI');
}

function formatPercent(value, decimals = 1) {
    const numeric = toNumber(value);
    const scaled = Math.abs(numeric) <= 1 ? numeric * 100 : numeric;
    return `${scaled.toFixed(decimals)}%`;
}

function normalizePercent(value) {
    const numeric = toNumber(value);
    if (!Number.isFinite(numeric)) return 0;
    return Math.abs(numeric) <= 1 ? numeric * 100 : numeric;
}

function beautifyMapName(raw) {
    if (!raw) return 'Kartta';
    const value = String(raw).trim();
    const lower = value.toLowerCase();
    if (lower === 'forfeit') return null; // never display forfeit as a map
    const core = lower.startsWith('de_') ? lower.slice(3) : lower;
    const parts = core.split(/[_-]/).filter(Boolean);
    if (!parts.length) return value;
    return parts.map(p => p.charAt(0).toUpperCase() + p.slice(1)).join(' ');
}

function mapKey(name) {
    return String(name || '').trim().toLowerCase();
}

function formatDate(ts) {
    if (!ts) return '';
    const d = new Date(ts * 1000);
    return d.toLocaleDateString('fi-FI', { year: 'numeric', month: 'short', day: 'numeric' });
}

function paginate(items, page, pageSize) {
    if (!Array.isArray(items) || !items.length) {
        return { items: [], total: 0, totalPages: 0, page: 1 };
    }
    const total = items.length;
    const totalPages = Math.max(1, Math.ceil(total / pageSize));
    const safePage = Math.min(Math.max(page, 1), totalPages);
    const start = (safePage - 1) * pageSize;
    return {
        items: items.slice(start, start + pageSize),
        total,
        totalPages,
        page: safePage
    };
}

function normalizeSeasonData(pageData) {
    if (!pageData) return null;
    return pageData.seasonData || pageData.season_data || pageData.season || null;
}

function normalizeMap(entry) {
    if (!entry) return null;
    const prettyName = entry.mapPretty || entry.map_pretty || entry.pretty_name || null;
    const rawName = entry.mapName || entry.map_name || entry.map || prettyName || 'Kartta';
    const lowerRaw = String(rawName || '').toLowerCase();
    if (lowerRaw === 'forfeit' || (prettyName && String(prettyName).toLowerCase() === 'forfeit')) {
        return null; // never include forfeit entries as maps
    }
    const beautified = prettyName || beautifyMapName(rawName) || rawName;
    const playedRaw = toNumber(entry.played ?? entry.matches ?? entry.maps ?? entry.maps_played);
    const games = toNumber(entry.games ?? entry.games_played ?? playedRaw ?? (entry.wins + entry.losses));
    const played = playedRaw || games;
    const wins = toNumber(entry.wins ?? entry.maps_won);
    const losses = toNumber(entry.losses ?? entry.maps_lost ?? Math.max(0, (games || played) - wins));
    const picks = toNumber(entry.picks ?? entry.times_picked ?? entry.timesPicked);
    const oppPicks = toNumber(entry.opp_picks ?? entry.oppPicks);
    const ban1 = toNumber(entry.ban1);
    const ban2 = toNumber(entry.ban2);
    const oppBan = toNumber(entry.opp_ban ?? entry.oppBan);
    const totalOwnBan = toNumber(entry.total_own_ban ?? entry.totalOwnBan ?? (ban1 + ban2));
    const decov = toNumber(entry.decov);
    const kills = toNumber(entry.kills);
    const deaths = toNumber(entry.deaths);
    const kdRaw = toNumber(entry.kd ?? entry.kd_ratio);
    const kd = kdRaw || (deaths ? kills / deaths : kills);
    const adr = toNumber(entry.adr ?? entry.average_damage ?? (entry.damage && games ? entry.damage / (games * 30) : 0));
    let rating = toNumber(entry.rating ?? entry.rating_2 ?? entry.map_rating ?? entry.hltv_rating);
    if (!rating && kd) rating = kd;
    const damage = toNumber(entry.damage);
    const utilityDamage = toNumber(entry.utility_damage ?? entry.utilityDamage);
    const mvps = toNumber(entry.mvps ?? entry.mvp);
    const rd = toNumber(entry.rd ?? entry.round_diff ?? entry.rounds_diff);
    const totalGames = games || played || wins + losses;
    const winrateRaw = toNumber(entry.winrate ?? entry.win_rate ?? entry.winRate ?? entry.wr);
    const winrate = Number.isFinite(winrateRaw) && winrateRaw !== 0
        ? (Math.abs(winrateRaw) <= 1 ? winrateRaw * 100 : winrateRaw)
        : (totalGames ? (wins / totalGames) * 100 : 0);
    const totalPicks = picks + oppPicks;
    const pickRateRaw = toNumber(entry.pick_rate ?? entry.pickRate);
    const pickRate = Number.isFinite(pickRateRaw) && pickRateRaw !== 0
        ? (Math.abs(pickRateRaw) <= 1 ? pickRateRaw * 100 : pickRateRaw)
        : (totalPicks ? (picks / totalPicks) * 100 : (played ? (picks / played) * 100 : 0));
    const ctWr = toNumber(entry.ct_wr ?? entry.ct_wr_pct ?? entry.ctWinrate ?? 0);
    const tWr = toNumber(entry.t_wr ?? entry.t_wr_pct ?? entry.tWinrate ?? 0);

    const identifier = entry.mapId || entry.map_id || rawName;

    return {
        id: identifier,
        mapName: beautified,
        played,
        games,
        wins,
        losses,
        winrate,
        rating,
        picks,
        oppPicks,
        pickRate,
        ban1,
        ban2,
        oppBan,
        totalOwnBan,
        decov,
        rd,
        kd,
        adr,
        ctWr,
        tWr,
        damage,
        utilityDamage,
        mvps,
        kills,
        deaths
    };
}

function normalizePlayer(player, idx = 0) {
    if (!player) return null;
    const mapsPlayed = toNumber(player.mapsPlayed ?? player.maps_played ?? player.maps ?? player.map_count);
    const roundsPlayed = toNumber(player.roundsPlayed ?? player.rounds_played ?? player.rounds ?? 0);
    const kills = toNumber(player.kills);
    const deaths = toNumber(player.deaths);
    const assists = toNumber(player.assists);
    const damage = toNumber(player.damage);
    const utilityDamage = toNumber(player.utilityDamage ?? player.utility_damage);
    const mvps = toNumber(player.mvps);
    const sniperKills = toNumber(player.sniperKills ?? player.sniper_kills);
    const pistolKills = toNumber(player.pistolKills ?? player.pistol_kills);
    const enemiesFlashed = toNumber(player.enemiesFlashed ?? player.enemies_flashed);
    const flashCount = toNumber(player.flashCount ?? player.flash_count);
    const flashSuccesses = toNumber(player.flashSuccesses ?? player.flash_successes);
    const entryCount = toNumber(player.entryCount ?? player.entry_count);
    const entryWins = toNumber(player.entryWins ?? player.entry_wins);
    const cl1v1Attempts = toNumber(player.cl1v1Attempts ?? player.cl_1v1_attempts);
    const cl1v1Wins = toNumber(player.cl1v1Wins ?? player.cl_1v1_wins);
    const cl1v2Attempts = toNumber(player.cl1v2Attempts ?? player.cl_1v2_attempts);
    const cl1v2Wins = toNumber(player.cl1v2Wins ?? player.cl_1v2_wins);
    const mk2k = toNumber(player.mk2k ?? player.mk_2k ?? player['2k']);
    const mk3k = toNumber(player.mk3k ?? player.mk_3k ?? player['3k']);
    const mk4k = toNumber(player.mk4k ?? player.mk_4k ?? player['4k']);
    const mk5k = toNumber(player.mk5k ?? player.mk_5k ?? player['5k']);
    const clutchKills = toNumber(player.clutchKills ?? player.clutch_kills ?? player.clutches ?? player.clutch_wins);
    let rating = toNumber(player.rating ?? player.rating2 ?? player.rating_2 ?? player.hltv_rating);
    const kdRaw = toNumber(player.kd ?? player.kdRatio ?? player.kd_ratio);
    const kd = kdRaw || (deaths ? kills / deaths : kills);
    const adr = toNumber(player.adr ?? player.average_damage ?? (roundsPlayed ? damage / roundsPlayed : 0));
    const kr = toNumber(player.kr ?? player.killsPerRound ?? player.kills_per_round ?? (roundsPlayed ? kills / roundsPlayed : 0));
    const hsPct = toNumber(player.hsPct ?? player.hs_pct ?? player.hs_percent ?? player.headshot_percent);
    const entryWinPct = entryCount ? (entryWins / entryCount) * 100 : 0;
    const clutch1v1Pct = cl1v1Attempts ? (cl1v1Wins / cl1v1Attempts) * 100 : 0;
    const clutch1v2Pct = cl1v2Attempts ? (cl1v2Wins / cl1v2Attempts) * 100 : 0;
    const flashSuccessPct = flashCount ? (flashSuccesses / flashCount) * 100 : 0;
    if (!rating && kd) rating = kd;

    return {
        playerId: player.player_id || player.id || `player-${idx}`,
        nickname: player.nickname || player.name || 'Pelaaja',
        mapsPlayed,
        roundsPlayed,
        rating,
        kd,
        adr,
        kr,
        hsPct,
        kills,
        deaths,
        assists,
        damage,
        utilityDamage,
        mvps,
        sniperKills,
        pistolKills,
        entryCount,
        entryWins,
        entryLine: entryWinPct,
        cl1v1Attempts,
        cl1v1Wins,
        cl1v1Line: clutch1v1Pct,
        cl1v2Attempts,
        cl1v2Wins,
        cl1v2Line: clutch1v2Pct,
        clutchKills,
        mk2k,
        mk3k,
        mk4k,
        mk5k,
        enemiesFlashed,
        flashCount,
        flashSuccesses,
        flashSuccessLine: flashSuccessPct
    };
}

function normalizeMatch(match, teamId = null) {
    if (!match) return null;
    const matchId = match.match_id || match.matchId || match.id;
    const playedFlag = toNumber(match.played);
    const bestOf = toNumber(match.best_of ?? match.bestOf ?? match.bo ?? 0);
    const matchWinnerId = match.winner_team_id || match.winnerTeamId || null;
    const matchIsForfeit = !!(match.is_forfeit ?? match.isForfeit);
    const rawMaps = Array.isArray(match.maps) ? match.maps : [];
    const left = match.left || {
        team_id: match.team1_id,
        team_name: match.team1_name,
        avatar: match.t1_avatar
    };
    const right = match.right || {
        team_id: match.team2_id,
        team_name: match.team2_name,
        avatar: match.t2_avatar
    };
    const meOnLeft = teamId ? String(left?.team_id) === String(teamId) : true;
    const mySide = meOnLeft ? left : right;
    const oppSide = meOnLeft ? right : left;
    const myName = mySide?.team_name || mySide?.team || match.team1_name || match.team2_name || match.team;
    const oppName = oppSide?.team_name
        || oppSide?.team
        || match.opponent_name
        || match.opponent
        || (meOnLeft ? (match.team2_name || match.team2) : (match.team1_name || match.team1))
        || '';
    const maps = [];
    let forfeitedMaps = 0;
    rawMaps.forEach((m, idx) => {
        if (m.is_forfeit) {
            const winnerId = m.winner_team_id || m.winnerTeamId || null;
            if (winnerId && String(winnerId) !== String(teamId)) {
                forfeitedMaps += 1;
            }
        }
        const rawMapName = m.map || m.map_name || m.name || `Map ${idx + 1}`;
        const displayName = beautifyMapName(rawMapName);
        if (!displayName) return; // skip invalid entries
        let scoreFor = toNumber(m.rf ?? m.score_for ?? m.score_team1 ?? 0);
        let scoreAgainst = toNumber(m.ra ?? m.score_against ?? m.score_team2 ?? 0);
        const mapWinnerId = m.winner_team_id || m.winnerTeamId || null;
        if (m.is_forfeit && mapWinnerId && scoreFor === 0 && scoreAgainst === 0) {
            if (String(mapWinnerId) === String(teamId)) {
                scoreFor = 13;
                scoreAgainst = 0;
            } else {
                scoreFor = 0;
                scoreAgainst = 13;
            }
        }
        const leftStats = m.left || {};
        const rightStats = m.right || {};
        const myStats = meOnLeft ? leftStats : rightStats;
        const oppStats = meOnLeft ? rightStats : leftStats;
        maps.push({
            id: `${matchId}-map-${idx}`,
            mapName: displayName,
            roundIndex: toNumber(m.round_index ?? m.roundIndex ?? idx),
            scoreFor,
            scoreAgainst,
            pickTeamId: m.pick_team_id || m.pickTeamId || null,
            isForfeit: !!m.is_forfeit,
            winnerTeamId: mapWinnerId,
            adr: toNumber(myStats.adr),
            kd: toNumber(myStats.kd),
            kills: toNumber(myStats.kills),
            deaths: toNumber(myStats.deaths),
            oppAdr: toNumber(oppStats.adr),
            oppKd: toNumber(oppStats.kd),
            oppKills: toNumber(oppStats.kills),
            oppDeaths: toNumber(oppStats.deaths)
        });
    });
    maps.sort((a, b) => (a.roundIndex || 0) - (b.roundIndex || 0));
    const mapWins = maps.filter(m => m.scoreFor > m.scoreAgainst).length;
    const mapLosses = maps.filter(m => m.scoreFor < m.scoreAgainst).length;
    const mapDraws = maps.length - mapWins - mapLosses;
    const roundsFor = maps.reduce((sum, m) => sum + m.scoreFor, 0);
    const roundsAgainst = maps.reduce((sum, m) => sum + m.scoreAgainst, 0);
    let roundDiff = toNumber(match.round_diff ?? match.roundDiff);
    if (!Number.isFinite(roundDiff)) {
        roundDiff = roundsFor - roundsAgainst;
    }
    // parenthesize fallbacks to avoid ?? precedence issues with ||
    const played = maps.length || toNumber(match.map_count ?? match.played ?? 0);
    const matchRating = maps.length ? safeDivide(maps.reduce((sum, m) => sum + (m.kd || 0), 0), maps.length) : 0;
    const rawTeamScore = match.team_score ?? match.teamScore ?? match.series_wins;
    const rawOppScore = match.opp_score ?? match.oppScore ?? match.series_losses;
    const teamScore = rawTeamScore != null ? toNumber(rawTeamScore) : mapWins;
    const oppScore = rawOppScore != null ? toNumber(rawOppScore) : mapLosses;

    return {
        matchId,
        ts: toNumber(match.ts ?? match.started_at ?? match.start_ts ?? match.date),
        status: match.status || (playedFlag ? 'finished' : 'scheduled'),
        bestOf: bestOf || Math.max(1, maps.length),
        played,
        forfeitedMaps,
        isForfeit: matchIsForfeit,
        winnerTeamId: matchWinnerId,
        teamScore,
        oppScore,
        mapDraws,
        roundsFor,
        roundsAgainst,
        roundDiff,
        matchRating,
        team1Name: match.team1_name || match.team1 || myName,
        team2Name: match.team2_name || match.opponent_name || oppName,
        opponentName: oppName,
        me: mySide,
        opponent: oppSide,
        faceitUrl: match.faceit_url || match.faceitUrl || '',
        maps
    };
}

function normalizeVeto(entry) {
    if (!entry) return null;
    const pretty = beautifyMapName(entry.map_name || entry.mapName || entry.selected_map_name);
    return {
        mapName: pretty || 'Kartta',
        timesPicked: toNumber(entry.times_picked ?? entry.picked),
        timesBanned: toNumber(entry.times_banned ?? entry.banned),
        timesOpponentPicked: toNumber(entry.times_opponent_picked ?? entry.timesOpponentPicked),
        pickRate: toNumber(entry.pick_rate ?? entry.pickRate),
        banRate: toNumber(entry.ban_rate ?? entry.banRate),
        pickWinRate: toNumber(entry.pick_win_rate ?? entry.pickWinRate)
    };
}

function getMatchResult(match) {
    if (!match) return 'pending';
    if (match.isForfeit && match.winnerTeamId) {
        return String(match.winnerTeamId) === String(match.me?.team_id || match.teamId) ? 'win' : 'loss';
    }
    if (match.teamScore > match.oppScore) return 'win';
    if (match.teamScore < match.oppScore) return 'loss';
    if (match.roundDiff && match.roundDiff > 0) return 'win';
    if (match.roundDiff && match.roundDiff < 0) return 'loss';
    return 'draw';
}

window.TeamDetail = {
    name: 'TeamDetail',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; },
        get SortableTable() { return window.SortableTable; },
        get ProgressBar() { return window.ProgressBar; },
        get SplitBar() { return window.SplitBar; },
        get SparklineChart() { return window.SparklineChart; },
        get RadarChart() { return window.RadarChart; }
    },
    props: {
        teamId: { type: [String, Number], required: true },
        championshipId: { type: [String, Number], default: null }
    },
    data() {
        const teamStore = typeof window.useTeamStore === 'function' ? window.useTeamStore() : null;
        return {
            teamStore,
            selectedChampionship: this.championshipId ? String(this.championshipId) : null,
            activeTab: 'overview',
            matchMetric: 'roundDiff',
            mapMetric: 'winrate',
            matchesPage: 1,
            SCOUT_MAP_COLUMNS,
            SCOUT_MAP_GROUPS,
            MAP_COLUMNS,
            PLAYER_COLUMNS,
            VETO_COLUMNS,
            scoutTableKey: 0,
            scoutLogKey: null
        };
    },
    computed: {
        teamEntry() {
            if (!this.teamStore || !this.teamId) return null;
            return this.teamStore.getTeamState(this.teamId);
        },
        pageSegment() {
            return this.teamEntry?.page || createSegment();
        },
        pageData() {
            return this.pageSegment.data || null;
        },
        teamInfo() {
            return this.pageData?.team || this.pageData?.profile || null;
        },
        seasonOptions() {
            const seasons = Array.isArray(this.pageData?.seasons) ? this.pageData.seasons : [];
            const normalized = seasons.map(season => {
                const value = season.championship_id || season.championshipId || season.id;
                return {
                    value: value ? String(value) : null,
                    label: season.name || `Kausi ${season.season} · Div ${season.division_num}`,
                    season: toNumber(season.season),
                    division: season.division_num,
                    isPlayoffs: season.is_playoffs
                };
            }).filter(option => option.value);
            return normalized.sort((a, b) => {
                const seasonDiff = (b.season || 0) - (a.season || 0);
                if (seasonDiff !== 0) return seasonDiff;
                const av = Number(a.value);
                const bv = Number(b.value);
                if (Number.isFinite(av) && Number.isFinite(bv)) return bv - av;
                return 0;
            });
        },
        currentChampionshipId() {
            if (this.pageData?.currentChampionshipId) return String(this.pageData.currentChampionshipId);
            if (this.selectedChampionship) return String(this.selectedChampionship);
            return this.seasonOptions[0]?.value || null;
        },
        seasonData() {
            const data = normalizeSeasonData(this.pageData) || null;
            if (!data) return null;
            const dataChamp = data.championshipId || data.championship_id || null;
            if (this.currentChampionshipId && dataChamp && String(dataChamp) !== String(this.currentChampionshipId)) {
                return null;
            }
            return data;
        },
        teamStats() {
            return this.seasonData?.teamStats || this.seasonData?.stats || this.seasonData?.team_stats || {};
        },
        // Map stats cover all DB fields: played/games/wins/losses/winrate/picks/oppPicks/pickRate/ban1/ban2/oppBan/totalOwnBan/decov/rd/kd/adr/rating/ct/t splits/damage/utilityDamage/mvps/kills/deaths
        mapStats() {
            const maps = Array.isArray(this.seasonData?.mapStats) ? this.seasonData.mapStats : (Array.isArray(this.seasonData?.map_stats) ? this.seasonData.map_stats : []);
            const normalized = maps.map(normalizeMap).filter(Boolean);

            const lookup = {};
            normalized.forEach(m => {
                const key = String(m.id || m.mapName).toLowerCase();
                lookup[key] = m;
            });

            // Fallback aggregation from match maps to avoid missing data
            const matchAgg = {};
            this.matchesList.forEach(match => {
                (match.maps || []).forEach(map => {
                    const name = beautifyMapName(map.mapName);
                    if (!name) return;
                    const key = mapKey(name);
                    const bucket = matchAgg[key] || {
                        id: key,
                        mapName: name,
                        games: 0,
                        wins: 0,
                        losses: 0,
                        picks: 0,
                        oppPicks: 0,
                        decov: 0,
                        rd: 0,
                        adrSum: 0,
                        kdSum: 0
                    };
                    bucket.games += 1;
                    if (map.scoreFor > map.scoreAgainst) bucket.wins += 1;
                    else if (map.scoreFor < map.scoreAgainst) bucket.losses += 1;
                    if (map.pickTeamId) {
                        if (String(map.pickTeamId) === String(this.teamId)) bucket.picks += 1;
                        else bucket.oppPicks += 1;
                    } else {
                        bucket.decov += 1;
                    }
                    bucket.rd += (map.scoreFor - map.scoreAgainst);
                    bucket.adrSum += toNumber(map.adr);
                    bucket.kdSum += toNumber(map.kd);
                    matchAgg[key] = bucket;
                });
            });

            // Build pool only from season data, match maps, and season veto list (season-level pool).
            const poolNames = new Set();
            normalized.forEach(m => poolNames.add(m.mapName));
            this.matchesList.forEach(match => {
                (match.maps || []).forEach(map => {
                    if (map.mapName) poolNames.add(map.mapName);
                });
            });
            const vetoPool = Array.isArray(this.seasonData?.vetoHistory) ? this.seasonData.vetoHistory
                : (Array.isArray(this.seasonData?.veto_history) ? this.seasonData.veto_history : []);
            vetoPool.forEach(entry => {
                const raw = entry.map_name || entry.mapName;
                const pretty = entry.selected_map_name || entry.pretty_name;
                const name = beautifyMapName(pretty || raw);
                if (name) poolNames.add(name);
            });

            const finalMaps = [];
            poolNames.forEach(name => {
                const key = String(name).toLowerCase();
                const existing = lookup[key];
                if (existing) {
                    // Enrich existing entry with aggregated match data if missing
                    const agg = matchAgg[key];
                    if (agg) {
                        const games = existing.games || existing.played || agg.games;
                        const wins = existing.wins || agg.wins;
                        const losses = existing.losses || agg.losses;
                        const winrate = games ? (wins / games) * 100 : existing.winrate;
                        const picks = existing.picks || agg.picks;
                        const oppPicks = existing.oppPicks || agg.oppPicks;
                        const totalPicks = picks + oppPicks;
                        const pickRate = totalPicks ? (picks / totalPicks) * 100 : existing.pickRate;
                        const decov = existing.decov || agg.decov;
                        const rd = existing.rd || agg.rd;
                        const adr = existing.adr || (agg.games ? agg.adrSum / agg.games : 0);
                        const rating = existing.rating || (agg.games ? safeDivide(agg.kdSum, agg.games) : existing.rating);
                        const kd = existing.kd || (agg.games ? safeDivide(agg.kdSum, agg.games) : existing.kd);
                        finalMaps.push({
                            ...existing,
                            games,
                            played: existing.played || games,
                            wins,
                            losses,
                            winrate,
                            picks,
                            oppPicks,
                            pickRate,
                            decov,
                            rd,
                            adr,
                            rating,
                            kd
                        });
                    } else {
                        finalMaps.push(existing);
                    }
                } else {
                    const display = beautifyMapName(name) || name;
                    const agg = matchAgg[key];
                    finalMaps.push({
                        id: name,
                        mapName: display,
                        played: agg?.games || 0,
                        games: agg?.games || 0,
                        wins: agg?.wins || 0,
                        losses: agg?.losses || 0,
                        winrate: agg?.games ? (agg.wins / agg.games) * 100 : 0,
                        rating: agg?.games ? safeDivide(agg.kdSum, agg.games) : 0,
                        picks: agg?.picks || 0,
                        oppPicks: agg?.oppPicks || 0,
                        pickRate: (() => {
                            const total = (agg?.picks || 0) + (agg?.oppPicks || 0);
                            const picks = agg?.picks || 0;
                            return total ? (picks / total) * 100 : 0;
                        })(),
                        ban1: 0,
                        ban2: 0,
                        oppBan: 0,
                        totalOwnBan: 0,
                        decov: agg?.decov || 0,
                        rd: agg?.rd || 0,
                        kd: agg?.games ? safeDivide(agg.kdSum, agg.games) : 0,
                        adr: agg?.games ? agg.adrSum / agg.games : 0,
                        ctWr: 0,
                        tWr: 0,
                        damage: 0,
                        utilityDamage: 0,
                        mvps: 0,
                        kills: 0,
                        deaths: 0
                    });
                }
            });

            return finalMaps.sort((a, b) => (b.played || 0) - (a.played || 0) || String(a.mapName).localeCompare(String(b.mapName)));
        },
        mapTotals() {
            const totals = {
                played: 0,
                games: 0,
                wins: 0,
                losses: 0,
                picks: 0,
                oppPicks: 0,
                ban1: 0,
                ban2: 0,
                oppBan: 0,
                totalOwnBan: 0,
                decov: 0,
                rd: 0,
                kills: 0,
                deaths: 0,
                damage: 0,
                utilityDamage: 0,
                mvps: 0,
                adrWeighted: 0,
                ratingWeighted: 0,
                ctWrWeighted: 0,
                tWrWeighted: 0
            };
            if (!this.mapStats.length) return { ...totals, avgAdr: 0, avgRating: 0, kd: 0, winrate: 0, pickRate: 0, ctWr: 0, tWr: 0 };
            this.mapStats.forEach(map => {
                const games = map.games || map.played || (map.wins + map.losses) || 0;
                totals.played += map.played || games;
                totals.games += games;
                totals.wins += map.wins || 0;
                totals.losses += map.losses || 0;
                totals.picks += map.picks || 0;
                totals.oppPicks += map.oppPicks || 0;
                totals.ban1 += map.ban1 || 0;
                totals.ban2 += map.ban2 || 0;
                totals.oppBan += map.oppBan || 0;
                totals.totalOwnBan += map.totalOwnBan || 0;
                totals.decov += map.decov || 0;
                totals.rd += map.rd || 0;
                totals.kills += map.kills || 0;
                totals.deaths += map.deaths || 0;
                totals.damage += map.damage || 0;
                totals.utilityDamage += map.utilityDamage || 0;
                totals.mvps += map.mvps || 0;
                totals.adrWeighted += (map.adr || 0) * (games || 1);
                totals.ratingWeighted += (map.rating || 0) * (games || 1);
                totals.ctWrWeighted += (map.ctWr || 0) * (games || 1);
                totals.tWrWeighted += (map.tWr || 0) * (games || 1);
            });
            const games = totals.games || totals.played || 1;
            const kd = totals.deaths ? totals.kills / totals.deaths : totals.kills || 0;
            const avgAdr = totals.adrWeighted / games;
            const avgRating = totals.ratingWeighted / games;
            const winrate = games ? (totals.wins / games) * 100 : 0;
            const totalPicks = totals.picks + totals.oppPicks;
            const pickRate = totalPicks ? (totals.picks / totalPicks) * 100 : 0;
            const ctWr = games ? (totals.ctWrWeighted / games) : 0;
            const tWr = games ? (totals.tWrWeighted / games) : 0;
            return { ...totals, kd, avgAdr, avgRating, winrate, pickRate, ctWr, tWr };
        },
        seasonStatCards() {
            const s = this.teamStats || {};
            const wins = toNumber(s.wins ?? s.matches_won ?? s.maps_won ?? 0);
            const losses = toNumber(s.losses ?? s.matches_lost ?? s.maps_lost ?? 0);
            const matches = toNumber(s.matches ?? s.matches_played ?? s.series_played ?? (wins + losses));
            let winRate = matches ? (wins / matches) * 100 : toNumber(s.win_rate ?? s.match_win_rate ?? 0);
            if (winRate <= 1) winRate = winRate * 100;
            const roundsWon = toNumber(s.rounds_won);
            const roundsLost = toNumber(s.rounds_lost);
            const roundsDiff = toNumber(s.rounds_diff ?? s.round_diff ?? (roundsWon - roundsLost));
            const mapsPlayed = toNumber(s.maps_played ?? this.mapTotals.played);
            const mapsWon = toNumber(s.maps_won ?? this.mapTotals.wins);
            let mapWinRate = mapsPlayed ? (mapsWon / Math.max(1, mapsPlayed)) * 100 : this.mapTotals.winrate;
            if (mapWinRate <= 1) mapWinRate = mapWinRate * 100;
            const avgRating = this.players.length
                ? (this.players.reduce((acc, p) => acc + (p.rating || 0), 0) / this.players.length)
                : toNumber(s.rating ?? s.rating_2 ?? s.hltv_rating ?? this.mapTotals.avgRating);
            const prevSeason = (this.pageData?.seasons || []).find(sea => String(sea.championship_id || sea.championshipId) !== String(this.currentChampionshipId));
            let prevWinRate = prevSeason ? toNumber(prevSeason.win_rate) : null;
            if (prevWinRate != null && prevWinRate <= 1) prevWinRate *= 100;
            const winTrend = prevWinRate != null ? (winRate - prevWinRate) : null;
            const totalClutch = this.players.reduce((acc, p) => acc + (p.clutchKills || 0), 0);

            return [
                { key: 'matches', label: 'Ottelut', value: formatNumber(matches), caption: `${formatNumber(wins)} - ${formatNumber(losses)} · ${formatPercent(winRate || 0, 1)}${winTrend != null ? ` (${winTrend >= 0 ? '+' : ''}${formatNumber(winTrend, 1)} vs ed. kausi)` : ''}` },
                { key: 'maps', label: 'Kartat', value: formatNumber(mapsPlayed || this.mapTotals.games), caption: `${formatNumber(this.mapTotals.wins)} - ${formatNumber(this.mapTotals.losses)} · ${formatPercent(mapWinRate || this.mapTotals.winrate, 1)}` },
                { key: 'rounds', label: 'Rundit', value: `${formatNumber(roundsWon)}/${formatNumber(roundsLost)}`, caption: `Ero ${roundsDiff > 0 ? '+' : ''}${formatNumber(roundsDiff)}` },
                { key: 'pickban', label: 'Pick/Ban', value: `${formatNumber(this.mapTotals.picks)}/${formatNumber(this.mapTotals.totalOwnBan)}`, caption: `Vast. pickit ${formatNumber(this.mapTotals.oppPicks)} · Bannit ${formatNumber(this.mapTotals.oppBan)}` },
                { key: 'adr', label: 'ADR', value: formatNumber(this.mapTotals.avgAdr, 1), caption: `Damage ${formatNumber(this.mapTotals.damage)}` },
                { key: 'kd', label: 'K/D', value: formatNumber(this.mapTotals.kd, 2), caption: `${formatNumber(this.mapTotals.kills)} / ${formatNumber(this.mapTotals.deaths)}` },
                { key: 'mvps', label: 'MVP:t', value: formatNumber(this.mapTotals.mvps), caption: 'Karttakohtaiset MVP:t' },
                { key: 'rating', label: 'Rating', value: avgRating ? formatNumber(avgRating, 2) : '-', caption: `Karttojen rating-keskiarvo ${formatNumber(this.mapTotals.avgRating, 2)}` },
                { key: 'clutch', label: 'Clutch', value: formatNumber(totalClutch), caption: 'Joukkueen clutch-killat (summa)' }
            ];
        },
        seasonSnapshotStats() {
            const s = this.teamStats || {};
            const playedMatches = this.matchesList.filter(m => m.played);
            const upcomingMatches = this.matchesList.length - playedMatches.length;
            const matchWins = playedMatches.filter(m => getMatchResult(m) === 'win').length;
            const matchLosses = playedMatches.filter(m => getMatchResult(m) === 'loss').length;
            const matches = playedMatches.length;
            const winRate = matches ? (matchWins / matches) * 100 : normalizePercent(s.win_rate ?? s.match_win_rate ?? this.mapTotals.winrate ?? 0);
            const avgRating = this.players.length
                ? (this.players.reduce((acc, p) => acc + (p.rating || 0), 0) / this.players.length)
                : toNumber(s.rating ?? s.rating_2 ?? s.hltv_rating ?? this.mapTotals.avgRating);
            const mapsPlayed = playedMatches.reduce((acc, m) => {
                if (!m.maps?.length && m.isForfeit && m.winnerTeamId) {
                    return acc + Math.max(1, m.bestOf || 2);
                }
                return acc + (m.maps?.length || 0);
            }, 0);
            const mapWins = playedMatches.reduce((acc, m) => {
                if (!m.maps?.length && m.isForfeit && m.winnerTeamId) {
                    return acc + (String(m.winnerTeamId) === String(this.teamId) ? Math.max(1, m.bestOf || 2) : 0);
                }
                return acc + (m.maps || []).filter(mp => {
                    if (mp.isForfeit && mp.winnerTeamId) {
                        return String(mp.winnerTeamId) === String(this.teamId);
                    }
                    return mp.scoreFor > mp.scoreAgainst;
                }).length;
            }, 0);
            const mapLosses = playedMatches.reduce((acc, m) => {
                if (!m.maps?.length && m.isForfeit && m.winnerTeamId) {
                    return acc + (String(m.winnerTeamId) !== String(this.teamId) ? Math.max(1, m.bestOf || 2) : 0);
                }
                return acc + (m.maps || []).filter(mp => {
                    if (mp.isForfeit && mp.winnerTeamId) {
                        return String(mp.winnerTeamId) !== String(this.teamId);
                    }
                    return mp.scoreFor < mp.scoreAgainst;
                }).length;
            }, 0);
            const activePlayers = this.players.filter(p => (p.mapsPlayed || 0) > 0).length;
            const forfeitedMaps = playedMatches.reduce((acc, m) => {
                const forfeitsFromMaps = m.forfeitedMaps || 0;
                const shouldFallback = !m.maps?.length
                    && forfeitsFromMaps === 0
                    && m.isForfeit
                    && m.winnerTeamId
                    && String(m.winnerTeamId) !== String(this.teamId);
                const fallbackCount = shouldFallback ? Math.max(1, m.bestOf || 2) : 0;
                return acc + forfeitsFromMaps + fallbackCount;
            }, 0);
            const roundsWon = playedMatches.reduce((acc, m) => acc + (m.roundsFor || 0), 0);
            const roundsLost = playedMatches.reduce((acc, m) => acc + (m.roundsAgainst || 0), 0);
            const roundsDiff = roundsWon - roundsLost;

            const hasMapData = (this.mapTotals.games || this.mapTotals.played || this.mapTotals.wins || this.mapTotals.losses);
            const hasRating = (this.players.length && Number.isFinite(avgRating)) || (hasMapData && avgRating > 0);
            const hasWinRate = matches > 0;
            const missingTip = 'Data not available for this season';

            return [
                {
                    key: 'activePlayers',
                    label: 'Aktiiviset pelaajat',
                    value: formatNumber(activePlayers),
                    sub: 'Vähintään 1 kartta',
                    tone: 'stat-neutral'
                },
                {
                    key: 'matches',
                    label: 'Ottelut',
                    value: formatNumber(matches),
                    sub: '',
                    tone: 'stat-neutral'
                },
                {
                    key: 'maps',
                    label: 'Pelatut kartat',
                    value: formatNumber(mapsPlayed),
                    sub: `${formatNumber(mapWins)}W – ${formatNumber(mapLosses)}L`,
                    tone: 'stat-neutral'
                },
                {
                    key: 'forfeits',
                    label: 'Luovutetut kartat',
                    value: formatNumber(forfeitedMaps),
                    sub: 'Kausi yhteensä',
                    tone: 'stat-neutral'
                },
                {
                    key: 'winrate',
                    label: 'Voittoprosentti',
                    value: hasWinRate ? formatPercent(winRate, 1) : '—',
                    sub: '',
                    tone: 'stat-neutral',
                    missing: !hasWinRate,
                    tooltip: !hasWinRate ? missingTip : ''
                },
                {
                    key: 'upcoming',
                    label: 'Tulevat ottelut',
                    value: formatNumber(upcomingMatches),
                    sub: 'Tulevat',
                    tone: 'stat-neutral'
                },
                {
                    key: 'rounds',
                    label: 'Eräero',
                    value: formatNumber(roundsDiff),
                    sub: `${formatNumber(roundsWon)}–${formatNumber(roundsLost)}`,
                    tone: 'stat-neutral'
                },
                {
                    key: 'rating',
                    label: 'Keskiarvo rating',
                    value: hasRating ? formatNumber(avgRating, 2) : '—',
                    sub: 'Joukkueen keskiarvo',
                    tone: 'stat-neutral',
                    missing: !hasRating,
                    tooltip: !hasRating ? missingTip : ''
                }
            ];
        },
        seasonDetailStats() {
            return [];
        },
        scoutMapRows() {
            const fallbackAgg = new Map();
            const statsAgg = new Map();
            const banAgg = new Map();
            const decovAgg = new Map();
            const hasMapStats = this.mapStats.length > 0;
            const hasMatches = this.matchesList.length > 0;
            const hasVeto = this.vetoByMatch.length > 0;

            const ensureFallback = (rawName) => {
                const name = beautifyMapName(rawName) || rawName;
                if (!name) return null;
                const key = mapKey(name);
                if (!fallbackAgg.has(key)) {
                    fallbackAgg.set(key, {
                        mapName: name,
                        games: 0,
                        wins: 0,
                        losses: 0,
                        adrSum: 0,
                        kdSum: 0
                    });
                }
                return fallbackAgg.get(key);
            };

            this.vetoByMatch.forEach(entry => {
                const steps = Array.isArray(entry.steps) ? entry.steps : [];
                let teamBanCount = 0;
                steps.forEach(step => {
                    if (!step.mapName) return;
                    if (step.action !== 'pick' && step.action !== 'ban') return;
                    ensureFallback(step.mapName);
                });
                steps.forEach(step => {
                    if (!step.mapName) return;
                    const key = mapKey(step.mapName);
                    if (step.action === 'ban') {
                        const bans = banAgg.get(key) || { ban1: 0, ban2: 0, oppBan: 0 };
                        if (step.actor === 'team') {
                            teamBanCount += 1;
                            if (teamBanCount === 1) bans.ban1 += 1;
                            else if (teamBanCount === 2) bans.ban2 += 1;
                            else bans.ban2 += 1;
                        } else if (step.actor === 'opponent') {
                            bans.oppBan += 1;
                        }
                        banAgg.set(key, bans);
                    }
                    if (step.action === 'decider' || step.action === 'overflow') {
                        const counts = decovAgg.get(key) || { decider: 0, overflow: 0 };
                        if (step.action === 'decider') counts.decider += 1;
                        if (step.action === 'overflow') counts.overflow += 1;
                        decovAgg.set(key, counts);
                    }
                });
            });

            this.vetoHistory.forEach(entry => {
                if (!entry.mapName) return;
                ensureFallback(entry.mapName);
            });

            this.matchesList.forEach(match => {
                (match.maps || []).forEach(map => {
                    const bucket = ensureFallback(map.mapName);
                    if (!bucket) return;
                    bucket.games += 1;
                    if (map.isForfeit && map.winnerTeamId) {
                        if (String(map.winnerTeamId) === String(this.teamId)) bucket.wins += 1;
                        else bucket.losses += 1;
                    } else if (map.scoreFor > map.scoreAgainst) {
                        bucket.wins += 1;
                    } else if (map.scoreFor < map.scoreAgainst) {
                        bucket.losses += 1;
                    }
                    bucket.adrSum += toNumber(map.adr);
                    bucket.kdSum += toNumber(map.kd);
                    const key = mapKey(bucket.mapName);
                    const agg = statsAgg.get(key) || {
                        played: 0,
                        wins: 0,
                        losses: 0,
                        rd: 0,
                        adrSum: 0,
                        kdSum: 0,
                        picks: 0,
                        oppPicks: 0,
                        pickWins: 0,
                        oppPickWins: 0
                    };
                    agg.played += 1;
                    agg.rd += (map.scoreFor - map.scoreAgainst);
                    agg.adrSum += toNumber(map.adr);
                    agg.kdSum += toNumber(map.kd);
                    if (map.isForfeit && map.winnerTeamId) {
                        if (String(map.winnerTeamId) === String(this.teamId)) agg.wins += 1;
                        else agg.losses += 1;
                    } else if (map.scoreFor > map.scoreAgainst) {
                        agg.wins += 1;
                    } else if (map.scoreFor < map.scoreAgainst) {
                        agg.losses += 1;
                    }
                    if (map.pickTeamId) {
                        if (String(map.pickTeamId) === String(this.teamId)) {
                            agg.picks += 1;
                            if (map.isForfeit && map.winnerTeamId) {
                                if (String(map.winnerTeamId) === String(this.teamId)) agg.pickWins += 1;
                            } else if (map.scoreFor > map.scoreAgainst) {
                                agg.pickWins += 1;
                            }
                        } else {
                            agg.oppPicks += 1;
                            if (map.isForfeit && map.winnerTeamId) {
                                if (String(map.winnerTeamId) === String(this.teamId)) agg.oppPickWins += 1;
                            } else if (map.scoreFor > map.scoreAgainst) {
                                agg.oppPickWins += 1;
                            }
                        }
                    } else {
                        const counts = decovAgg.get(key) || { decider: 0, overflow: 0 };
                        if (Number(match.bestOf) === 3) counts.decider += 1;
                        else if (Number(match.bestOf) === 2) counts.overflow += 1;
                        decovAgg.set(key, counts);
                    }
                    statsAgg.set(key, agg);
                });
            });

            const fallbackMaps = Array.from(fallbackAgg.values()).map(entry => {
                const games = entry.games || 0;
                return {
                    mapName: entry.mapName,
                    games,
                    played: games,
                    wins: entry.wins,
                    losses: entry.losses,
                    winrate: games ? (entry.wins / games) * 100 : 0,
                    adr: games ? entry.adrSum / games : 0,
                    rating: games ? safeDivide(entry.kdSum, games) : 0,
                    picks: 0,
                    oppPicks: 0,
                    totalOwnBan: 0,
                    oppBan: 0
                };
            });

            const baseMaps = this.mapStats.length
                ? [...this.mapStats]
                : (fallbackMaps.length ? fallbackMaps : this.vetoTrendMapPool.map(map => ({
                    mapName: map.mapName,
                    games: 0,
                    played: 0,
                    wins: 0,
                    losses: 0,
                    winrate: 0,
                    adr: 0,
                    rating: 0,
                    picks: 0,
                    oppPicks: 0,
                    totalOwnBan: 0,
                    oppBan: 0
                })));
            if (this.mapStats.length && fallbackMaps.length) {
                const baseKeys = new Set(baseMaps.map(map => mapKey(map.mapName)));
                fallbackMaps.forEach(map => {
                    if (!baseKeys.has(mapKey(map.mapName))) baseMaps.push(map);
                });
            }

            return baseMaps.map(map => {
                const key = mapKey(map.mapName);
                const agg = statsAgg.get(key) || {
                    played: 0,
                    wins: 0,
                    losses: 0,
                    rd: 0,
                    adrSum: 0,
                    kdSum: 0,
                    picks: 0,
                    oppPicks: 0,
                    pickWins: 0,
                    oppPickWins: 0
                };
                const played = hasMatches ? agg.played : (map.games || map.played || 0);
                const wins = hasMatches ? agg.wins : (map.wins || 0);
                const losses = hasMatches ? agg.losses : (map.losses || 0);
                const winrate = played ? (wins / played) * 100 : 0;
                const adr = hasMatches ? (played ? safeDivide(agg.adrSum, played) : 0) : toNumber(map.adr);
                const bans = banAgg.get(key) || { ban1: 0, ban2: 0, oppBan: 0 };
                const decov = decovAgg.get(key) || { decider: 0, overflow: 0 };
                const picksOwn = hasMatches ? agg.picks : (map.picks || 0);
                const picksOpp = hasMatches ? agg.oppPicks : (map.oppPicks || 0);
                const ban1 = hasVeto ? bans.ban1 : (map.ban1 || 0);
                const ban2 = hasVeto ? bans.ban2 : (map.ban2 || 0);
                const oppBan = hasVeto ? bans.oppBan : (map.oppBan || 0);
                const totalOwnBan = hasVeto ? (bans.ban1 + bans.ban2) : (map.totalOwnBan || 0);

                return {
                    ...map,
                    played,
                    picks: picksOwn,
                    oppPicks: picksOpp,
                    winrate,
                    wins,
                    losses,
                    pickWinRate: picksOwn ? (agg.pickWins / picksOwn) * 100 : null,
                    oppPickWinRate: picksOpp ? (agg.oppPickWins / picksOpp) * 100 : null,
                    pickWins: agg.pickWins,
                    oppPickWins: agg.oppPickWins,
                    kd: hasMatches ? (played ? safeDivide(agg.kdSum, played) : 0) : toNumber(map.kd),
                    adr,
                    rd: hasMatches ? agg.rd : toNumber(map.rd),
                    ban1,
                    ban2,
                    oppBan,
                    totalOwnBan,
                    decov: decov.decider + decov.overflow,
                    deciderCount: decov.decider,
                    overflowCount: decov.overflow
                };
            });
        },
        scoutMapDefaultSort() {
            return { column: 'played', order: 'desc', numeric: true };
        },
        vetoTrendMatches() {
            return this.matchesList || [];
        },
        vetoMatchMeta() {
            const meta = {};
            this.vetoByMatch.forEach(entry => {
                const match = entry.match || {};
                const matchId = entry.matchId || match.matchId;
                if (!matchId) return;
                const format = entry.format || this.detectSeriesFormat(match.bestOf, entry.steps || []);
                const decider = entry.steps?.find(step => step.action === 'decider')?.mapName || null;
                const overflow = entry.steps?.find(step => step.action === 'overflow')?.mapName || null;
                meta[matchId] = {
                    seriesType: format === 'bo3' ? 'BO3' : 'BO2',
                    decider,
                    overflow
                };
            });
            return meta;
        },
        vetoTrendColumns() {
            return this.vetoTrendMatches.map((match, idx) => {
                const dateLabel = formatDate(match.ts);
                const opponent = match.opponentName || match.team2Name || match.opponent?.team_name || 'Vastustaja';
                const meta = this.vetoMatchMeta[match.matchId] || {};
                const badgeTitle = meta.decider
                    ? `Decider: ${meta.decider}`
                    : meta.overflow
                        ? `Overflow: ${meta.overflow}`
                        : '';
                const label = `#${idx + 1}`;
                return {
                    id: match.matchId,
                    label,
                    title: [
                        label,
                        dateLabel,
                        opponent ? `vs ${opponent}` : '',
                        meta.seriesType ? meta.seriesType : '',
                        badgeTitle
                    ].filter(Boolean).join(' · ')
                };
            });
        },
        vetoTrendMapPool() {
            const pool = new Map();
            const pickLookup = {};
            const playedLookup = {};
            this.mapStats.forEach(row => {
                pickLookup[mapKey(row.mapName)] = normalizePercent(row.pickRate) || 0;
                playedLookup[mapKey(row.mapName)] = toNumber(row.games || row.played || 0);
                if (row.mapName) {
                    pool.set(mapKey(row.mapName), { mapName: row.mapName });
                }
            });
            this.matchesList.forEach(match => {
                (match.maps || []).forEach(map => {
                    const name = beautifyMapName(map.mapName);
                    if (!name) return;
                    const key = mapKey(name);
                    playedLookup[key] = (playedLookup[key] || 0) + 1;
                    pool.set(mapKey(name), { mapName: name });
                });
            });
            this.vetoByMatch.forEach(entry => {
                entry.steps.forEach(step => {
                    if (!step.mapName) return;
                    const key = mapKey(step.mapName);
                    if (!pool.has(key)) pool.set(key, { mapName: step.mapName });
                });
            });
            return Array.from(pool.values())
                .map(row => ({
                    ...row,
                    pickRate: pickLookup[mapKey(row.mapName)] || 0,
                    played: playedLookup[mapKey(row.mapName)] || 0
                }))
                .sort((a, b) => (b.played || 0) - (a.played || 0) || a.mapName.localeCompare(b.mapName));
        },
        vetoTrendRows() {
            if (!this.vetoTrendMatches.length) return [];
            const byMatch = {};
            this.vetoByMatch.forEach(entry => {
                const actions = {};
                let teamBanCount = 0;
                entry.steps.forEach(step => {
                    if (!step.mapName) return;
                    const key = mapKey(step.mapName);
                    const bucket = actions[key] || { pick: null, ban: null };
                    if (step.action === 'pick') {
                        bucket.pick = { actor: step.actor, teamName: step.teamName };
                    }
                    if (step.action === 'ban') {
                        let order = null;
                        if (step.actor === 'team') {
                            teamBanCount += 1;
                            order = teamBanCount;
                        }
                        bucket.ban = { actor: step.actor, teamName: step.teamName, order };
                    }
                    actions[key] = bucket;
                });
                byMatch[entry.matchId] = { entry, actions };
            });

            return this.vetoTrendMapPool.map(map => {
                const key = mapKey(map.mapName);
                const cells = this.vetoTrendMatches.map(match => {
                    const veto = byMatch[match.matchId];
                    const actionPick = veto?.actions?.[key]?.pick || null;
                    const actionBan = veto?.actions?.[key]?.ban || null;
                    const actionInfo = actionPick || actionBan;
                    const opponent = match.opponentName || match.team2Name || match.opponent?.team_name || 'Vastustaja';
                    const dateLabel = formatDate(match.ts);
                    const resultKey = getMatchResult(match);
                    const resultLabel = resultKey === 'win' ? 'W' : resultKey === 'loss' ? 'L' : resultKey === 'draw' ? 'D' : 'Pending';
                    const meta = this.vetoMatchMeta[match.matchId] || {};
                    let className = 'veto-heatmap__cell--none';
                    let actionLabel = 'Not present';
                    let phaseLabel = '';
                    let byLabel = '';

                    if (actionPick) {
                        actionLabel = 'Pick';
                        if (actionPick.actor === 'team') {
                            className = 'veto-heatmap__cell--team-pick';
                            byLabel = this.teamInfo?.teamName || 'Team';
                        } else if (actionPick.actor === 'opponent') {
                            className = 'veto-heatmap__cell--opp-pick';
                            byLabel = opponent;
                        }
                    } else if (actionBan) {
                        actionLabel = 'Ban';
                        if (actionBan.actor === 'team') {
                            if (actionBan.order === 1) {
                                className = 'veto-heatmap__cell--opp-ban';
                                phaseLabel = 'Team first ban';
                            } else if (actionBan.order === 2) {
                                className = 'veto-heatmap__cell--team-ban1';
                                phaseLabel = 'Team second ban';
                            } else {
                                className = 'veto-heatmap__cell--team-ban1';
                                phaseLabel = 'Team ban';
                            }
                            byLabel = this.teamInfo?.teamName || 'Team';
                        } else if (actionBan.actor === 'opponent') {
                            className = 'veto-heatmap__cell--team-ban2';
                            phaseLabel = 'Opponent ban';
                            byLabel = opponent;
                        }
                    }

                    const title = [
                        map.mapName,
                        `Match ${match.matchId}`,
                        opponent ? `vs ${opponent}` : '',
                        dateLabel,
                        meta.seriesType ? meta.seriesType : '',
                        `Result ${resultLabel}`,
                        actionLabel,
                        phaseLabel,
                        byLabel ? `By ${byLabel}` : '',
                        meta.decider ? `Decider: ${meta.decider}` : '',
                        meta.overflow ? `Overflow: ${meta.overflow}` : ''
                    ].filter(Boolean).join(' · ');

                    return { className, title, hasAction: !!actionInfo };
                });

                return {
                    mapName: map.mapName,
                    rowLabel: map.mapName,
                    cells
                };
            });
        },
        vetoLegendEntries() {
            const buildCell = (className, label) => ({ className, label });
            return [
                buildCell('veto-heatmap__cell--team-pick', 'Team pick'),
                buildCell('veto-heatmap__cell--opp-pick', 'Opp pick'),
                buildCell('veto-heatmap__cell--opp-ban', 'Team ban (1st)'),
                buildCell('veto-heatmap__cell--team-ban1', 'Team ban (2nd)'),
                buildCell('veto-heatmap__cell--team-ban2', 'Opp ban'),
                buildCell('veto-heatmap__cell--none', 'Not present')
            ];
        },
        mapComparisonCards() {
            return [...this.mapStats].sort((a, b) => (b.played || 0) - (a.played || 0) || (b.winrate || 0) - (a.winrate || 0)).slice(0, 6);
        },
        mapWinrateSeries() {
            const maxWin = Math.max(...this.mapStats.map(m => toNumber(m.winrate || 0)), 1);
            return this.mapStats.map(map => {
                const winrate = toNumber(map.winrate || 0);
                return {
                    label: map.mapName,
                    winrate,
                    adr: toNumber(map.adr || 0),
                    rating: toNumber(map.rating || map.kd || 0),
                    height: Math.max(10, (winrate / maxWin) * 100),
                    games: map.games || map.played || 0,
                    picks: map.picks || 0
                };
            });
        },
        mapPerformanceSeries() {
            const metric = this.mapMetric;
            const series = this.mapWinrateSeries.map(item => {
                const value = metric === 'adr' ? item.adr : metric === 'rating' ? item.rating : item.winrate;
                return { ...item, value };
            });
            const maxValue = Math.max(...series.map(i => i.value || 0), 1);
            return series.map(item => ({
                ...item,
                height: Math.max(10, (item.value / maxValue) * 100)
            }));
        },
        hasMapPerformanceData() {
            return this.mapStats.some(map => (map.games || map.played || map.wins || map.losses || map.picks || map.oppPicks));
        },
        mapWinLossStack() {
            return this.mapStats.filter(map => (map.games || map.played || map.wins || map.losses)).map(map => ({
                label: map.mapName,
                wins: map.wins,
                losses: map.losses
            }));
        },
        mapDefaultSort() {
            return { column: 'played', order: 'desc', numeric: true };
        },
        mapMaxPlayed() {
            return Math.max(...this.mapStats.map(m => m.played || 0), 1);
        },
        mapMaxPicks() {
            return Math.max(...this.mapStats.map(m => (m.picks || 0) + (m.oppPicks || 0)), 1);
        },
        mapMaxBans() {
            return Math.max(...this.mapStats.map(m => (m.totalOwnBan || 0) + (m.oppBan || 0)), 1);
        },
        mapRadarMetrics() {
            const withGames = this.mapStats.filter(map => map.games || map.played);
            if (!withGames.length) return [];
            const top = [...withGames].sort((a, b) => (b.winrate || 0) - (a.winrate || 0)).slice(0, 7);
            return top.map(map => ({
                label: map.mapName,
                value: (() => {
                    const wr = toNumber(map.winrate || 0);
                    return Math.abs(wr) <= 1 ? wr * 100 : wr;
                })(),
                max: 100
            }));
        },
        // Match history uses every field: status/best_of/played/opponent info/avatars/maps scores/picks/forfeit/ADR/KD plus Faceit URL
        matchesList() {
            const matches = this.seasonData?.matchHistory || this.seasonData?.match_history || this.seasonData?.matches || [];
            const normalized = Array.isArray(matches) ? matches.map(m => normalizeMatch(m, this.teamId)).filter(Boolean) : [];
            return normalized.sort((a, b) => {
                const at = a.ts ?? 0;
                const bt = b.ts ?? 0;
                if (!at && bt) return 1; // missing dates go to bottom
                if (at && !bt) return -1;
                return bt - at; // newest first for tables
            });
        },
        paginatedMatches() {
            const total = this.matchesList.length;
            return {
                items: this.matchesList,
                total,
                totalPages: 1,
                page: 1
            };
        },
        matchesPerformanceSeries() {
            const sorted = [...this.matchesList].sort((a, b) => (a.ts || 0) - (b.ts || 0));
            return sorted.map(match => {
                const value = this.matchMetric === 'adr'
                    ? safeDivide(match.maps.reduce((sum, m) => sum + (m.adr || 0), 0), Math.max(match.maps.length, 1))
                    : this.matchMetric === 'rating'
                        ? match.matchRating || 0
                    : (match.roundDiff ?? 0);
                return {
                    label: formatDate(match.ts) || match.matchId,
                    value,
                    opponent: match.opponentName || match.team2Name || match.opponent?.team_name,
                    result: getMatchResult(match),
                    tooltip: `${match.teamScore}-${match.oppScore} vs ${match.opponentName || match.team2Name || match.opponent?.team_name}`
                };
            });
        },
        matchTrendPoints() {
            if (!this.matchesPerformanceSeries.length) return [];
            const maxAbs = Math.max(...this.matchesPerformanceSeries.map(p => Math.abs(p.value)), 1);
            return this.matchesPerformanceSeries.map(p => p.value / maxAbs);
        },
        // Player stats table uses every DB field: maps/rounds/kills/deaths/assists/mvps/sniper_kills/utility_damage/enemies_flashed/flash_count/flash_successes/entry_count/entry_wins/clutch fields/pistol_kills/adr/kr/kd/rating/hs_pct/damage/multi-kills
        players() {
            const players = this.seasonData?.playerStats || this.seasonData?.player_stats || this.seasonData?.players || this.seasonData?.roster || [];
            return Array.isArray(players) ? players.map((p, idx) => normalizePlayer(p, idx)).filter(Boolean) : [];
        },
        playerDefaultSort() {
            return { column: 'rating', order: 'desc', numeric: true };
        },
        playersByRating() {
            return [...this.players].sort((a, b) => (b.rating || 0) - (a.rating || 0));
        },
        playerStackedKda() {
            return this.players.map(p => ({
                label: p.nickname,
                kills: p.kills,
                deaths: p.deaths,
                assists: p.assists
            }));
        },
        playerMaxKdaTotal() {
            return Math.max(...this.playerStackedKda.map(p => (p.kills + p.deaths + p.assists) || 0), 1);
        },
        // Veto aggregates: times_banned, times_picked, times_opponent_picked, pick/ban rate, pick_win_rate (derived)
        vetoAggregatesData() {
            const raw = this.seasonData?.vetoAggregates || this.seasonData?.veto_aggregates || [];
            return Array.isArray(raw) ? raw.map(normalizeVeto).filter(Boolean) : [];
        },
        derivedVetoCounts() {
            const counts = {};
            this.vetoByMatch.forEach(entry => {
                entry.steps.forEach(step => {
                    if (step.action !== 'pick' && step.action !== 'ban') return;
                    if (step.actor === 'system') return;
                    const key = mapKey(step.mapName);
                    if (!key) return;
                    const bucket = counts[key] || { mapName: step.mapName, timesPicked: 0, timesOpponentPicked: 0, timesBanned: 0 };
                    if (step.action === 'pick') {
                        if (step.actor === 'team') bucket.timesPicked += 1;
                        if (step.actor === 'opponent') bucket.timesOpponentPicked += 1;
                    }
                    if (step.action === 'ban' && step.actor === 'team') {
                        bucket.timesBanned += 1;
                    }
                    counts[key] = bucket;
                });
            });
            return counts;
        },
        enhancedVetoAggregates() {
            const pickOutcomes = {};
            this.matchesList.forEach(match => {
                match.maps.forEach(map => {
                    const name = beautifyMapName(map.mapName);
                    const key = mapKey(name);
                    if (!key) return;
                    if (map.pickTeamId && String(map.pickTeamId) === String(this.teamId)) {
                        pickOutcomes[key] = pickOutcomes[key] || { mapName: name, wins: 0, total: 0 };
                        pickOutcomes[key].total += 1;
                        if (map.scoreFor > map.scoreAgainst) pickOutcomes[key].wins += 1;
                    }
                });
            });

            const deciderOutcomes = {};
            this.matchesList.forEach(match => {
                match.maps.forEach(map => {
                    if (map.pickTeamId) return;
                    const name = beautifyMapName(map.mapName);
                    const key = mapKey(name);
                    if (!key) return;
                    deciderOutcomes[key] = deciderOutcomes[key] || { mapName: name, wins: 0, total: 0 };
                    deciderOutcomes[key].total += 1;
                    if (map.scoreFor > map.scoreAgainst) deciderOutcomes[key].wins += 1;
                });
            });

            const aggregates = this.vetoAggregatesData.map(row => {
                const key = mapKey(row.mapName);
                const derived = this.derivedVetoCounts[key] || {};
                const pickData = pickOutcomes[key] || { wins: 0, total: 0 };
                const deciderData = deciderOutcomes[key] || { wins: 0, total: 0 };
                const timesPicked = derived.timesPicked || row.timesPicked || 0;
                const timesOpponentPicked = derived.timesOpponentPicked || row.timesOpponentPicked || 0;
                const timesBanned = derived.timesBanned || row.timesBanned || 0;
                const totalActions = timesPicked + timesOpponentPicked + timesBanned;
                const pickRate = totalActions ? (timesPicked / totalActions) * 100 : (row.pickRate || 0);
                const banRate = totalActions ? (timesBanned / totalActions) * 100 : (row.banRate || 0);
                const derivedWinRate = pickData.total ? (pickData.wins / pickData.total) * 100 : row.pickWinRate || 0;
                const deciderWinRate = deciderData.total ? (deciderData.wins / deciderData.total) * 100 : 0;
                return {
                    ...row,
                    mapName: row.mapName,
                    timesPicked,
                    timesOpponentPicked,
                    timesBanned,
                    pickRate,
                    banRate,
                    pickWinRate: derivedWinRate,
                    deciderWinRate
                };
            });

            Object.entries(this.derivedVetoCounts).forEach(([key, derived]) => {
                const existing = aggregates.find(row => mapKey(row.mapName) === key);
                if (existing) return;
                const totalActions = derived.timesPicked + derived.timesOpponentPicked + derived.timesBanned;
                aggregates.push({
                    mapName: derived.mapName,
                    timesPicked: derived.timesPicked,
                    timesOpponentPicked: derived.timesOpponentPicked,
                    timesBanned: derived.timesBanned,
                    pickRate: totalActions ? (derived.timesPicked / totalActions) * 100 : 0,
                    banRate: totalActions ? (derived.timesBanned / totalActions) * 100 : 0,
                    pickWinRate: 0,
                    deciderWinRate: 0
                });
            });

            const maxPick = Math.max(...aggregates.map(e => e.timesPicked || 0), 0);
            const maxBan = Math.max(...aggregates.map(e => e.timesBanned || 0), 0);
            return aggregates.map(e => ({
                ...e,
                isTopPick: e.timesPicked === maxPick && maxPick > 0,
                isTopBan: e.timesBanned === maxBan && maxBan > 0
            }));
        },
        vetoDefaultSort() {
            return { column: 'timesPicked', order: 'desc', numeric: true };
        },
        // Veto history: match_id/map_name/status/selected_by_team_id/_name/round_num/order -> rendered as BO2/BO3 step timeline
        vetoHistory() {
            const raw = this.seasonData?.vetoHistory || this.seasonData?.veto_history || [];
            return Array.isArray(raw) ? raw.map(entry => ({
                matchId: entry.match_id || entry.matchId,
                mapName: beautifyMapName(entry.map_name || entry.mapName) || 'Kartta',
                status: (entry.status || '').toLowerCase(),
                selectedByTeamId: entry.selected_by_team_id || entry.selectedByTeamId,
                selectedByTeamName: entry.selected_by_team_name || entry.selectedByTeamName,
                roundNum: toNumber(entry.round_num ?? entry.roundNum ?? entry.order),
                order: toNumber(entry.order ?? entry.order_in_match ?? entry.round_num ?? entry.roundNum)
            })) : [];
        },
        vetoByMatch() {
            if (!this.vetoHistory.length) return [];
            const matchMap = {};
            this.matchesList.forEach(m => { matchMap[m.matchId] = m; });
            const grouped = {};
            this.vetoHistory.forEach(step => {
                const bucket = grouped[step.matchId] || [];
                bucket.push(step);
                grouped[step.matchId] = bucket;
            });
            return Object.entries(grouped).map(([matchId, steps]) => {
                const match = matchMap[matchId] || {};
                const format = this.detectSeriesFormat(match.bestOf, steps);
                const sortedSteps = [...steps].sort((a, b) => (a.order || a.roundNum || 0) - (b.order || b.roundNum || 0));
                const decorated = this.decorateVetoSteps(sortedSteps, format, match);
                return { matchId, match, format, steps: decorated };
            });
        },
        vetoSummaryLookup() {
            const lookup = {};
            this.vetoByMatch.forEach(entry => {
                lookup[entry.matchId] = entry.steps.map(s => `${s.step}. ${s.label}: ${s.mapName}`).join(' • ');
            });
            return lookup;
        },
        loading() {
            return this.pageSegment.loading;
        },
        loadError() {
            return this.pageSegment.error;
        }
    },
    watch: {
        teamId: {
            immediate: true,
            handler() {
                this.selectedChampionship = this.championshipId ? String(this.championshipId) : null;
                this.bootstrap();
            }
        },
        championshipId(newVal) {
            if (newVal) {
                this.selectedChampionship = String(newVal);
                this.fetchSeason(String(newVal), { force: true });
            }
        },
        matchesList() {
            this.matchesPage = 1;
        },
        seasonData() {
            if (!isDevEnv) return;
            const key = `${this.currentChampionshipId || 'none'}-${this.matchesList.length}-${this.vetoHistory.length}`;
            if (this.scoutLogKey === key) return;
            this.scoutLogKey = key;
            const sampleRow = this.scoutMapRows[0] || null;
            console.debug('[MapPerformance] season debug', {
                seasonId: this.currentChampionshipId,
                matches: this.matchesList.length,
                vetoEvents: this.vetoHistory.length,
                sampleRow
            });
        }
    },
    methods: {
        async bootstrap() {
            if (!this.teamStore || !this.teamId) return;
            try {
                const data = await this.teamStore.fetchTeamPage(this.teamId, this.selectedChampionship);
                if (data?.currentChampionshipId) {
                    this.selectedChampionship = String(data.currentChampionshipId);
                    this.updateRoute(this.selectedChampionship);
                }
            } catch (err) {
                console.error('TeamDetail bootstrap failed', err);
            }
        },
        async fetchSeason(championshipId, options = {}) {
            if (!this.teamStore || !this.teamId || !championshipId) return;
            try {
                await this.teamStore.fetchTeamPage(this.teamId, championshipId, options);
            } catch (err) {
                console.error('TeamDetail season fetch failed', err);
            }
        },
        selectChampionship(championshipId) {
            if (!championshipId || championshipId === this.currentChampionshipId) return;
            this.selectedChampionship = championshipId;
            this.fetchSeason(championshipId);
            this.updateRoute(championshipId);
        },
        updateRoute(championshipId) {
            if (!this.$router || !this.$route) return;
            const params = { ...(this.$route.params || {}), teamId: this.teamId };
            const query = { ...(this.$route.query || {}) };
            if (championshipId) {
                query.championship = championshipId;
            } else {
                delete query.championship;
            }
            this.$router.replace({
                name: this.$route.name || 'team',
                params,
                query
            }).catch(() => {});
        },
        changeMatchesPage(page) {
            const total = this.paginatedMatches.totalPages || 1;
            const next = Math.min(Math.max(page, 1), total);
            this.matchesPage = next;
        },
        selectTab(tab) {
            this.activeTab = tab;
            if (tab === 'matches') {
                this.matchesPage = 1;
            }
        },
        resetScoutSort() {
            this.scoutTableKey += 1;
        },
        formatWinLoss(wins, losses) {
            return `${formatNumber(wins)}–${formatNumber(losses)}`;
        },
        winHeatStyle(value) {
            const pct = Math.min(100, Math.max(0, normalizePercent(value)));
            const hue = (pct / 100) * 120;
            const color = `hsla(${hue.toFixed(1)}, 70%, 45%, 0.35)`;
            return {
                background: `linear-gradient(90deg, ${color}, transparent)`
            };
        },
        detectSeriesFormat(bestOf, steps = []) {
            if (Number(bestOf) === 2) return 'bo2';
            if (Number(bestOf) === 3) return 'bo3';
            const statuses = (steps || []).map(s => (s.status || s.action || '').toLowerCase());
            if (statuses.some(st => st.includes('overflow'))) return 'bo2';
            if (statuses.some(st => st.includes('decider'))) return 'bo3';
            const stepCount = Array.isArray(steps) ? steps.length : 0;
            if (stepCount === 6) return 'bo2';
            return stepCount >= 6 ? 'bo3' : 'bo2';
        },
        decorateVetoSteps(steps, format, match = {}) {
            const expected = format === 'bo3'
                ? ['ban', 'ban', 'pick', 'pick', 'ban', 'ban']
                : ['ban', 'ban', 'ban', 'ban', 'pick', 'pick'];
            const teamName = this.teamInfo?.teamName || 'Oma joukkue';
            const opponentName = match?.opponentName || match?.team2Name || 'Vastustaja';

            const normalized = (steps || []).map((step, idx) => {
                const raw = (step.status || step.action || '').toLowerCase();
                let action = null;
                if (raw.includes('ban') || raw === 'drop' || raw === 'banned') action = 'ban';
                else if (raw.includes('pick')) action = 'pick';
                else if (raw.includes('decider')) action = 'decider';
                else if (raw.includes('overflow')) action = 'overflow';
                if (!action && expected[idx]) action = expected[idx];
                if (!action) action = 'ban';
                const actor = step.selectedByTeamId
                    ? (String(step.selectedByTeamId) === String(this.teamId) ? 'team' : 'opponent')
                    : 'system';
                const mapName = beautifyMapName(step.mapName) || 'Kartta';
                return {
                    ...step,
                    action,
                    actor,
                    step: idx + 1,
                    label: this.actionLabel(action),
                    mapName,
                    teamName: step.selectedByTeamName || (actor === 'team' ? teamName : opponentName)
                };
            });

            const usedNames = new Set(normalized.map(s => s.mapName).filter(Boolean));
            const hasDecider = normalized.some(s => s.action === 'decider');
            const hasOverflow = normalized.some(s => s.action === 'overflow');
            if (format === 'bo3' && !hasDecider) {
                const deciderName = this.resolveDeciderMap(match, usedNames);
                normalized.push({
                    action: 'decider',
                    actor: 'system',
                    step: normalized.length + 1,
                    label: 'Decider',
                    mapName: deciderName,
                    teamName: 'Decider'
                });
                usedNames.add(deciderName);
            }
            if (format === 'bo2' && !hasOverflow) {
                const overflowName = this.resolveOverflowMap(usedNames);
                normalized.push({
                    action: 'overflow',
                    actor: 'system',
                    step: normalized.length + 1,
                    label: 'Overflow',
                    mapName: overflowName,
                    teamName: 'Overflow'
                });
            }
            return normalized.map((step, idx) => ({ ...step, step: idx + 1 }));
        },
        resolveDeciderMap(match, usedNames = new Set()) {
            const maps = Array.isArray(match?.maps) ? match.maps : [];
            const leftover = maps.find(m => !m.pickTeamId && !usedNames.has(m.mapName)) || maps.find(m => !usedNames.has(m.mapName));
            if (leftover?.mapName) return leftover.mapName;
            const fromPool = this.mapStats.find(m => !usedNames.has(m.mapName));
            return fromPool?.mapName || 'Decider';
        },
        resolveOverflowMap(usedNames = new Set()) {
            const pool = new Set();
            this.mapStats.forEach(m => { if (m.mapName) pool.add(m.mapName); });
            this.vetoAggregatesData.forEach(v => { if (v.mapName) pool.add(v.mapName); });
            const candidate = Array.from(pool).find(name => !usedNames.has(name));
            return candidate || 'Overflow';
        },
        actionLabel(action) {
            if (action === 'pick') return 'Pick';
            if (action === 'ban') return 'Ban';
            if (action === 'decider') return 'Decider';
            return 'Overflow';
        },
        formatPercent,
        formatNumber,
        formatDate,
        getMatchResult,
        teamLogo() {
            return this.teamInfo?.logo || this.teamInfo?.avatar || this.teamInfo?.image || '';
        }
    },
    template: `
        <div class="team-detail">
            <loading-spinner v-if="loading && !teamInfo" message="Joukkuetta ladataan..."></loading-spinner>
            <error-message v-else-if="loadError && !teamInfo" :message="loadError" @retry="bootstrap"></error-message>
            <div v-else>
                <header class="team-hero">
                    <div class="team-hero__logo" v-if="teamLogo()">
                        <img :src="teamLogo()" :alt="teamInfo?.teamName || 'Joukkue'" />
                    </div>
                    <div class="team-hero__content">
                        <h1 class="team-hero__title title-accent titleUnderlinePage">
                            {{ teamInfo?.displayName || teamInfo?.teamName || 'Joukkue' }}
                        </h1>
                        <div class="team-hero__season" v-if="seasonOptions.length">
                            <span class="pill">Kausi {{ seasonOptions[0]?.season }}</span>
                            <span class="pill">Div {{ seasonOptions[0]?.division }}</span>
                            <span class="pill pill--accent" v-if="seasonOptions[0]?.isPlayoffs">Playoffs</span>
                            <a v-if="teamInfo?.faceit_url || teamInfo?.faceitUrl" class="pill pill--link" :href="teamInfo?.faceit_url || teamInfo?.faceitUrl" target="_blank" rel="noopener">Faceit</a>
                        </div>
                    </div>
                    <div v-if="seasonOptions.length" class="team-season-selector">
                        <label class="season-select-label" for="season-select">Valitse kausi</label>
                        <select
                            id="season-select"
                            class="season-select"
                            :value="currentChampionshipId"
                            @change="selectChampionship($event.target.value)"
                        >
                            <option v-for="season in seasonOptions" :key="season.value" :value="season.value">
                                {{ season.label }}
                            </option>
                        </select>
                    </div>
                </header>

                <nav class="team-tabs" role="tablist">
                    <button
                        v-for="tab in ['overview', 'maps', 'matches', 'players', 'veto']"
                        :key="tab"
                        type="button"
                        class="team-tab"
                        :class="{ 'team-tab--active': activeTab === tab }"
                        @click="selectTab(tab)"
                        role="tab"
                    >
                        {{ { overview: 'Yleiskuva', maps: 'Kartat', matches: 'Ottelut', players: 'Pelaajat', veto: 'Veto/Nosto' }[tab] }}
                    </button>
                </nav>

                <section v-if="activeTab === 'overview'" class="team-section scout-view">
                    <div class="scout-panel scout-snapshot">
                        <div class="section-heading">
                            <div>
                            <h2 class="section-title titleUnderline">Kauden yleiskuva</h2>
                            <span class="section-sub">Valittu kausi · ydintilastot</span>
                            </div>
                        </div>
                        <div class="scout-snapshot-row">
                            <div v-for="stat in seasonSnapshotStats" :key="stat.key" class="scout-snapshot-item">
                                <div class="snapshot-label">{{ stat.label }}</div>
                                <div class="snapshot-value mono-num" :class="stat.tone" :title="stat.tooltip || ''">{{ stat.value }}</div>
                                <div class="snapshot-sub">{{ stat.sub }}</div>
                            </div>
                        </div>
                        <div v-if="seasonDetailStats.length" class="scout-snapshot-extra">
                            <div v-for="stat in seasonDetailStats" :key="stat.key" class="snapshot-mini">
                                <div class="snapshot-mini__label">{{ stat.label }}</div>
                                <div class="snapshot-mini__value mono-num">{{ stat.value }}</div>
                                <div class="snapshot-mini__sub">{{ stat.sub }}</div>
                            </div>
                        </div>
                    </div>

                    <div class="scout-panel scout-veto">
                        <div class="section-heading">
                            <div>
                        <h3 class="section-title titleUnderline">Veto-historia</h3>
                        <span class="section-sub">Pick/Ban otteluittain (uusin → vanhin)</span>
                            </div>
                        </div>
                        <div class="veto-heatmap-legend veto-heatmap-legend--detailed">
                            <div v-for="entry in vetoLegendEntries" :key="entry.label" class="veto-legend-item">
                                <div class="veto-heatmap__cell veto-legend-cell" :class="entry.className">
                                </div>
                                <span class="veto-legend-label">{{ entry.label }}</span>
                            </div>
                        </div>
                        <div v-if="vetoTrendRows.length" class="veto-heatmap">
                            <div class="veto-heatmap__header">
                                <div class="veto-heatmap__corner">Map</div>
                                <div class="veto-heatmap__cols">
                                <div
                                    v-for="(col, idx) in vetoTrendColumns"
                                    :key="col.id"
                                    class="veto-heatmap__col"
                                    :class="{ 'veto-heatmap__col--divider': (idx + 1) % 4 === 0 }"
                                    :title="col.title"
                                >
                                    <span class="veto-heatmap__col-label">{{ col.label }}</span>
                                </div>
                            </div>
                        </div>
                            <div v-for="row in vetoTrendRows" :key="row.mapName" class="veto-heatmap__row">
                                <div class="veto-heatmap__row-label">{{ row.rowLabel }}</div>
                                <div class="veto-heatmap__cells">
                                    <div
                                        v-for="(cell, idx) in row.cells"
                                        :key="row.mapName + '-' + idx"
                                        class="veto-heatmap__cell"
                                        :class="[cell.className, ((idx + 1) % 4 === 0) ? 'veto-heatmap__cell--divider' : '']"
                                        :title="cell.title"
                                    >
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div v-else class="empty-state-container compact">
                            <div class="empty-state-card">
                                <div class="empty-state-icon">🗳️</div>
                                <p class="empty-state-description">Ei vetoa kaudella.</p>
                            </div>
                        </div>
                    </div>

                    <div class="scout-panel scout-table">
                        <div class="section-heading">
                            <div>
                        <h3 class="section-title titleUnderline">Karttakohtainen suorituskyky (kausi)</h3>
                        <span class="section-sub">Oletusjärjestys: Pelattu (laskeva)</span>
                            </div>
                            <button type="button" class="btn-reset-sort" @click="resetScoutSort">Reset sort</button>
                        </div>
                        <div class="table-wrapper">
                            <sortable-table
                                :key="scoutTableKey"
                                :columns="SCOUT_MAP_COLUMNS"
                                :header-groups="SCOUT_MAP_GROUPS"
                                :data="scoutMapRows"
                                :default-sort="scoutMapDefaultSort"
                                :sticky-header="true"
                                :compact="true"
                            >
                                <template #cell-mapName="{ row }">
                                    <div class="map-name">
                                        <span class="map-name-text">{{ row.mapName }}</span>
                                    </div>
                                </template>
                                <template #cell-played="{ row }">
                                    <span class="mono-num" :class="row.played <= 2 ? 'mono-muted' : ''">{{ row.played }}</span>
                                </template>
                                <template #cell-picks="{ row }">
                                    <span class="mono-num">{{ row.picks }}</span>
                                </template>
                                <template #cell-oppPicks="{ row }">
                                    <span class="mono-num">{{ row.oppPicks }}</span>
                                </template>
                                <template #cell-winrate="{ row }">
                                    <div v-if="row.played > 0" class="scout-cell mono-num" :style="winHeatStyle(row.winrate)">
                                        {{ formatPercent(row.winrate || 0, 1) }} ({{ formatWinLoss(row.wins || 0, row.losses || 0) }})
                                    </div>
                                    <span v-else class="cell-muted mono-num" title="Not played this season">—</span>
                                </template>
                                <template #cell-pickWinRate="{ row }">
                                    <div v-if="row.picks > 0" class="scout-cell mono-num" :style="winHeatStyle(row.pickWinRate)">
                                        {{ formatPercent(row.pickWinRate || 0, 1) }} ({{ formatWinLoss(row.pickWins || 0, Math.max(0, (row.picks || 0) - (row.pickWins || 0))) }})
                                    </div>
                                    <span v-else class="cell-muted mono-num" title="Not played this season">—</span>
                                </template>
                                <template #cell-oppPickWinRate="{ row }">
                                    <div v-if="row.oppPicks > 0" class="scout-cell mono-num" :style="winHeatStyle(row.oppPickWinRate)">
                                        {{ formatPercent(row.oppPickWinRate || 0, 1) }} ({{ formatWinLoss(row.oppPickWins || 0, Math.max(0, (row.oppPicks || 0) - (row.oppPickWins || 0))) }})
                                    </div>
                                    <span v-else class="cell-muted mono-num" title="Not played this season">—</span>
                                </template>
                                <template #cell-kd="{ row }">
                                    <span v-if="row.played > 0" class="mono-num">{{ formatNumber(row.kd, 2) }}</span>
                                    <span v-else class="cell-muted mono-num">—</span>
                                </template>
                                <template #cell-adr="{ row }">
                                    <span v-if="row.played > 0" class="mono-num">{{ formatNumber(row.adr, 1) }}</span>
                                    <span v-else class="cell-muted mono-num">—</span>
                                </template>
                                <template #cell-rd="{ row }">
                                    <span v-if="row.played > 0" class="mono-num">{{ formatNumber(row.rd, 0) }}</span>
                                    <span v-else class="cell-muted mono-num">—</span>
                                </template>
                            </sortable-table>
                        </div>
                    </div>

                </section>

                <section v-if="activeTab === 'maps'" class="team-section">
                    <div class="section-heading">
                        <h2 class="section-title titleUnderline">Kartat - Yksityiskohtainen analyysi</h2>
                        <span class="section-sub">Kaikki karttakohtaiset kentät (peli-, pick-, ban-, damage-, MVP-, decider/overflow)</span>
                    </div>
                    <div v-if="mapStats.length" class="table-wrapper">
                        <sortable-table
                            :columns="MAP_COLUMNS"
                            :data="mapStats"
                            :default-sort="mapDefaultSort"
                            :colorize-columns="['winrate','kd','adr']"
                            :sticky-header="true"
                            :compact="true"
                        >
                            <template #cell-mapName="{ row }">
                                <div class="map-name">
                                    <span class="map-name-text">{{ row.mapName }}</span>
                                </div>
                            </template>
                            <template #cell-played="{ row }">
                                <div class="cell-with-bar">
                                    <span>{{ row.played }}</span>
                                    <div class="inline-bar inline-bar--thin">
                                        <div class="inline-bar__fill" :style="{ width: ((row.played / mapMaxPlayed) * 100).toFixed(1) + '%' }"></div>
                                    </div>
                                </div>
                            </template>
                            <template #cell-winrate="{ row }">
                                <div class="cell-with-bar">
                                    <span>{{ formatPercent(row.winrate, 1) }}</span>
                                    <div class="inline-bar inline-bar--thin">
                                        <div class="inline-bar__fill inline-bar__fill--accent" :style="{ width: Math.min(100, row.winrate) + '%' }"></div>
                                    </div>
                                </div>
                            </template>
                            <template #cell-picks="{ row }">
                                <div class="cell-with-bar">
                                    <span>{{ row.picks }}</span>
                                    <div class="inline-bar inline-bar--thin">
                                        <div class="inline-bar__fill" :style="{ width: ((row.picks + row.oppPicks) ? ((row.picks + row.oppPicks) / mapMaxPicks * 100) : 0) + '%' }"></div>
                                    </div>
                                </div>
                            </template>
                            <template #cell-oppPicks="{ row }">
                                <span>{{ row.oppPicks }}</span>
                            </template>
                            <template #cell-ban1="{ row }">
                                <div class="micro-stack micro-stack--row">
                                    <span class="micro-chip">1st {{ row.ban1 }}</span>
                                    <span class="micro-chip">2nd {{ row.ban2 }}</span>
                                </div>
                            </template>
                            <template #cell-oppBan="{ row }">
                                <div class="cell-with-bar">
                                    <span>{{ row.oppBan }}</span>
                                    <div class="inline-bar inline-bar--thin">
                                        <div class="inline-bar__fill inline-bar__fill--danger" :style="{ width: ((row.totalOwnBan + row.oppBan) ? ((row.totalOwnBan + row.oppBan) / mapMaxBans * 100) : 0) + '%' }"></div>
                                    </div>
                                </div>
                            </template>
                            <template #cell-totalOwnBan="{ row }">
                                <span>{{ row.totalOwnBan }}</span>
                            </template>
                            <template #cell-rating="{ row }">
                                <span class="stat-strong">{{ formatNumber(row.rating, 2) }}</span>
                            </template>
                            <template #cell-kd="{ row }">
                                <span class="stat-strong">{{ formatNumber(row.kd, 2) }}</span>
                            </template>
                            <template #cell-adr="{ row }">
                                <div class="cell-with-bar">
                                    <span class="stat-strong">{{ formatNumber(row.adr, 1) }}</span>
                                    <div class="inline-bar inline-bar--thin">
                                        <div class="inline-bar__fill" :style="{ width: Math.min(100, row.adr / 120 * 100) + '%' }"></div>
                                    </div>
                                </div>
                            </template>
                            <template #cell-ctWr="{ row }">
                                <span>{{ formatPercent(row.ctWr || 0, 1) }}</span>
                            </template>
                            <template #cell-tWr="{ row }">
                                <span>{{ formatPercent(row.tWr || 0, 1) }}</span>
                            </template>
                            <template #cell-damage="{ row }">
                                <span>{{ formatNumber(row.damage) }}</span>
                            </template>
                            <template #cell-utilityDamage="{ row }">
                                <span>{{ formatNumber(row.utilityDamage) }}</span>
                            </template>
                        </sortable-table>
                    </div>
                    <div v-else class="empty-state-container">
                        <div class="empty-state-card">
                            <div class="empty-state-icon">📊</div>
                            <h3 class="empty-state-title">Ei karttatietoja</h3>
                            <p class="empty-state-description">Tälle kaudelle ei ole karttastatistiikkaa saatavilla.</p>
                        </div>
                    </div>
                    <div class="glass-card" v-if="mapWinLossStack.length">
                        <div class="section-heading">
                            <h3>Karttakohtainen W/L jakauma</h3>
                            <span class="section-sub">Stacked bar - voitetut vs hävityt kartat</span>
                        </div>
                        <div class="stacked-bars">
                            <div v-for="row in mapWinLossStack" :key="row.label" class="stacked-bars__row">
                                <span class="stacked-bars__label">{{ row.label }}</span>
                                <split-bar :wins="row.wins" :losses="row.losses" height="18px" :showLabels="true" :showPercent="true" :showShimmer="false" />
                            </div>
                        </div>
                    </div>
                    <div class="glass-card" v-if="mapRadarMetrics.length">
                        <div class="section-heading">
                            <h3>Kartta strength radar</h3>
                            <span class="section-sub">Win% per kartta</span>
                        </div>
                        <radar-chart :metrics="mapRadarMetrics" :radius="80" />
                    </div>
                </section>

                <section v-if="activeTab === 'matches'" class="team-section">
                    <div class="section-heading">
                        <h2 class="section-title titleUnderline">Ottelut ({{ paginatedMatches.total }} yhteensä)</h2>
                    </div>
                    <div class="glass-card">
                        <div class="section-heading">
                            <h3>Suoritus ajan yli</h3>
                            <div class="toggle-group">
                                <button class="pill" :class="{ 'pill--active': matchMetric === 'roundDiff' }" @click="matchMetric = 'roundDiff'">Eräero</button>
                                <button class="pill" :class="{ 'pill--active': matchMetric === 'adr' }" @click="matchMetric = 'adr'">ADR</button>
                                <button class="pill" :class="{ 'pill--active': matchMetric === 'rating' }" @click="matchMetric = 'rating'">Rating</button>
                            </div>
                        </div>
                        <div v-if="matchesPerformanceSeries.length" class="trend-wrapper">
                            <sparkline-chart :points="matchTrendPoints" height="80" width="320" />
                            <div class="trend-legend">
                                <div v-for="point in matchesPerformanceSeries" :key="point.label" class="trend-pill" :class="'trend-pill--' + point.result">
                                    <span class="pill-label">{{ point.label }}</span>
                                    <span class="pill-value">{{ point.value.toFixed(1) }}</span>
                                    <span class="pill-meta">{{ point.opponent }}</span>
                                </div>
                            </div>
                        </div>
                        <div v-else class="empty-state-container compact">
                            <div class="empty-state-card">
                                <div class="empty-state-icon">📈</div>
                                <p class="empty-state-description">Ei tarpeeksi otteluita trendille.</p>
                            </div>
                        </div>
                    </div>
                    <div v-if="paginatedMatches.items.length" class="table-wrapper">
                        <table class="data-table">
                            <thead>
                                <tr>
                                    <th></th>
                                    <th>Pvm</th>
                                    <th>Vastustaja</th>
                                    <th>BO</th>
                                    <th>Score</th>
                                    <th>Eräero</th>
                                    <th>Tila</th>
                                    <th>Maps</th>
                                    <th>Linkki</th>
                                </tr>
                            </thead>
                            <tbody>
                                <tr v-for="match in paginatedMatches.items" :key="match.matchId">
                                    <td>
                                        <span :class="['result-dot', 'result-dot--' + getMatchResult(match)]"></span>
                                    </td>
                                    <td>{{ formatDate(match.ts) }}</td>
                                    <td :title="vetoSummaryLookup[match.matchId] || ''">{{ match.opponentName || match.team2Name || match.opponent?.team_name || 'Vastustaja' }}</td>
                                    <td>BO{{ match.bestOf }}</td>
                                    <td>{{ match.teamScore }} - {{ match.oppScore }}</td>
                                    <td :class="match.roundDiff >= 0 ? 'stat-positive' : 'stat-negative'">{{ match.roundDiff }}</td>
                                    <td>{{ match.status }}</td>
                                    <td>
                                        <div class="micro-stack" v-if="match.maps && match.maps.length">
                                            <span v-for="map in match.maps" :key="map.id" class="micro-chip">{{ map.mapName }} {{ map.scoreFor }}-{{ map.scoreAgainst }}</span>
                                        </div>
                                        <span v-else class="cell-muted">Ei karttoja</span>
                                    </td>
                                    <td>
                                        <a v-if="match.faceitUrl" :href="match.faceitUrl" target="_blank" rel="noopener" class="chip chip--link">FACEIT</a>
                                        <span v-else class="cell-muted">-</span>
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                        <div class="table-pagination" v-if="paginatedMatches.totalPages > 1">
                            <button type="button" class="pill" :disabled="paginatedMatches.page === 1" @click="changeMatchesPage(paginatedMatches.page - 1)">Edellinen</button>
                            <span class="pagination-meta">Sivu {{ paginatedMatches.page }} / {{ paginatedMatches.totalPages }}</span>
                            <button type="button" class="pill" :disabled="paginatedMatches.page === paginatedMatches.totalPages" @click="changeMatchesPage(paginatedMatches.page + 1)">Seuraava</button>
                        </div>
                    </div>
                    <div v-else class="empty-state-container">
                        <div class="empty-state-card">
                            <div class="empty-state-icon">🎮</div>
                            <h3 class="empty-state-title">Ei otteluita</h3>
                            <p class="empty-state-description">Tälle kaudelle ei ole otteluhistoriaa saatavilla.</p>
                        </div>
                    </div>
                </section>

                <section v-if="activeTab === 'players'" class="team-section">
                    <h2 class="section-title titleUnderline">Pelaajat</h2>
                    <div v-if="players.length" class="table-wrapper">
                        <sortable-table
                            :columns="PLAYER_COLUMNS"
                            :data="players"
                            :default-sort="playerDefaultSort"
                            :colorize-columns="['rating','kd','adr','kr']"
                            :sticky-header="true"
                            :compact="true"
                        >
                            <template #cell-nickname="{ row }">
                                <div class="player-cell">
                                    <div class="avatar-placeholder">{{ row.nickname ? row.nickname.slice(0, 2).toUpperCase() : 'PL' }}</div>
                                    <div>
                                        <div class="player-name">{{ row.nickname }}</div>
                                        <div class="player-sub">Maps {{ row.mapsPlayed }} · Rnds {{ row.roundsPlayed }}</div>
                                    </div>
                                </div>
                            </template>
                            <template #cell-rating="{ row }">
                                <div class="cell-with-bar">
                                    <span class="stat-strong">{{ formatNumber(row.rating, 2) }}</span>
                                    <div class="inline-bar inline-bar--thin">
                                        <div class="inline-bar__fill inline-bar__fill--accent" :style="{ width: Math.min(100, row.rating / 2 * 100) + '%' }"></div>
                                    </div>
                                </div>
                            </template>
                            <template #cell-kd="{ row }">
                                <span class="stat-strong">{{ formatNumber(row.kd, 2) }}</span>
                            </template>
                            <template #cell-adr="{ row }">
                                <span class="stat-strong">{{ formatNumber(row.adr, 1) }}</span>
                            </template>
                            <template #cell-hsPct="{ row }">
                                <span>{{ formatPercent(row.hsPct || 0, 1) }}</span>
                            </template>
                            <template #cell-entryLine="{ row }">
                                <span>{{ row.entryWins }}/{{ row.entryCount }} ({{ formatPercent(row.entryLine || 0, 1) }})</span>
                            </template>
                            <template #cell-clutch1v1Line="{ row }">
                                <span>{{ row.cl1v1Wins }}/{{ row.cl1v1Attempts }} ({{ formatPercent(row.cl1v1Line || 0, 1) }})</span>
                            </template>
                            <template #cell-clutch1v2Line="{ row }">
                                <span>{{ row.cl1v2Wins }}/{{ row.cl1v2Attempts }} ({{ formatPercent(row.cl1v2Line || 0, 1) }})</span>
                            </template>
                            <template #cell-flashSuccessLine="{ row }">
                                <span>{{ row.flashSuccesses }}/{{ row.flashCount }} ({{ formatPercent(row.flashSuccessLine || 0, 1) }})</span>
                            </template>
                        </sortable-table>
                    </div>
                    <div v-else class="empty-state-container">
                        <div class="empty-state-card">
                            <div class="empty-state-icon">👤</div>
                            <h3 class="empty-state-title">Ei pelaajatietoja</h3>
                            <p class="empty-state-description">Tälle kaudelle ei ole pelaajatietoja saatavilla.</p>
                        </div>
                    </div>
                    <div class="players-charts" v-if="playersByRating.length">
                        <div class="glass-card">
                            <div class="section-heading">
                                <h3>Pelaajien rating</h3>
                            </div>
                            <div class="bar-chart bar-chart--horizontal">
                                <div v-for="p in playersByRating" :key="p.playerId" class="bar-chart__row">
                                    <span class="bar-chart__label">{{ p.nickname }}</span>
                                    <div class="bar-chart__track">
                                        <div class="bar-chart__track-fill" :style="{ width: Math.min(100, p.rating / 2 * 100) + '%' }"></div>
                                    </div>
                                    <span class="bar-chart__value">{{ formatNumber(p.rating, 2) }}</span>
                                </div>
                            </div>
                        </div>
                        <div class="glass-card" v-if="playerStackedKda.length">
                            <div class="section-heading">
                                <h3>K/D/A per pelaaja</h3>
                            </div>
                            <div class="stacked-bars">
                                <div v-for="row in playerStackedKda" :key="row.label" class="stacked-bars__row">
                                    <span class="stacked-bars__label">{{ row.label }}</span>
                                    <div class="stacked-bars__bar">
                                        <span class="stacked-seg stacked-seg--kills" :style="{ width: ((row.kills / playerMaxKdaTotal) * 100) + '%' }">K {{ row.kills }}</span>
                                        <span class="stacked-seg stacked-seg--deaths" :style="{ width: ((row.deaths / playerMaxKdaTotal) * 100) + '%' }">D {{ row.deaths }}</span>
                                        <span class="stacked-seg stacked-seg--assists" :style="{ width: ((row.assists / playerMaxKdaTotal) * 100) + '%' }">A {{ row.assists }}</span>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </section>

                <section v-if="activeTab === 'veto'" class="team-section">
                    <h2 class="section-title titleUnderline">Ban/Nosto Tilastot</h2>
                    <div v-if="enhancedVetoAggregates.length" class="table-wrapper">
                        <sortable-table
                            :columns="VETO_COLUMNS"
                            :data="enhancedVetoAggregates"
                            :default-sort="vetoDefaultSort"
                            :sticky-header="true"
                            :compact="true"
                        >
                            <template #cell-mapName="{ row }">
                                <span :class="[{ 'stat-strong': row.isTopPick || row.isTopBan }]">{{ row.mapName }}</span>
                                <span v-if="row.isTopPick" class="chip chip--accent">Top pick</span>
                                <span v-if="row.isTopBan" class="chip chip--warn">Top ban</span>
                            </template>
                            <template #cell-timesPicked="{ row }">
                                <div class="cell-with-bar">
                                    <span>{{ row.timesPicked }}</span>
                                    <div class="inline-bar inline-bar--thin">
                                        <div class="inline-bar__fill inline-bar__fill--accent" :style="{ width: ((row.timesPicked + row.timesOpponentPicked + row.timesBanned) ? (row.timesPicked / (row.timesPicked + row.timesOpponentPicked + row.timesBanned) * 100) : 0) + '%' }"></div>
                                    </div>
                                </div>
                            </template>
                            <template #cell-timesOpponentPicked="{ row }">
                                <span>{{ row.timesOpponentPicked }}</span>
                            </template>
                            <template #cell-timesBanned="{ row }">
                                <div class="cell-with-bar">
                                    <span>{{ row.timesBanned }}</span>
                                    <div class="inline-bar inline-bar--thin">
                                        <div class="inline-bar__fill inline-bar__fill--danger" :style="{ width: ((row.timesPicked + row.timesOpponentPicked + row.timesBanned) ? (row.timesBanned / (row.timesPicked + row.timesOpponentPicked + row.timesBanned) * 100) : 0) + '%' }"></div>
                                    </div>
                                </div>
                            </template>
                            <template #cell-pickRate="{ row }">
                                <span>{{ formatPercent(row.pickRate, 1) }}</span>
                            </template>
                            <template #cell-banRate="{ row }">
                                <span>{{ formatPercent(row.banRate, 1) }}</span>
                            </template>
                            <template #cell-pickWinRate="{ row }">
                                <span>{{ formatPercent(row.pickWinRate, 1) }}</span>
                            </template>
                            <template #cell-deciderWinRate="{ row }">
                                <span>{{ formatPercent(row.deciderWinRate, 1) }}</span>
                            </template>
                        </sortable-table>
                    </div>
                    <div v-else class="empty-state-container">
                        <div class="empty-state-card">
                            <div class="empty-state-icon">🗳️</div>
                            <h3 class="empty-state-title">Ei ban/nosto historiaa</h3>
                            <p class="empty-state-description">Tälle kaudelle ei ole ban/nosto historiatietoja saatavilla.</p>
                        </div>
                    </div>
                    <div class="glass-card" v-if="enhancedVetoAggregates.length">
                        <div class="section-heading">
                            <h3>Picks vs bans per kartta</h3>
                        </div>
                        <div class="stacked-bars">
                            <div v-for="row in enhancedVetoAggregates" :key="row.mapName" class="stacked-bars__row">
                                <span class="stacked-bars__label">{{ row.mapName }}</span>
                                <div class="stacked-bars__bar">
                                    <span class="stacked-seg stacked-seg--picks" :class="{ 'stacked-seg--highlight': row.isTopPick }" :style="{ width: ((row.timesPicked + row.timesOpponentPicked + row.timesBanned) ? (row.timesPicked / (row.timesPicked + row.timesOpponentPicked + row.timesBanned) * 100) : 0) + '%' }">Pick {{ row.timesPicked }}</span>
                                    <span class="stacked-seg stacked-seg--picks-opp" :style="{ width: ((row.timesPicked + row.timesOpponentPicked + row.timesBanned) ? (row.timesOpponentPicked / (row.timesPicked + row.timesOpponentPicked + row.timesBanned) * 100) : 0) + '%' }">Vast {{ row.timesOpponentPicked }}</span>
                                    <span class="stacked-seg stacked-seg--bans" :class="{ 'stacked-seg--highlight': row.isTopBan }" :style="{ width: ((row.timesPicked + row.timesOpponentPicked + row.timesBanned) ? (row.timesBanned / (row.timesPicked + row.timesOpponentPicked + row.timesBanned) * 100) : 0) + '%' }">Ban {{ row.timesBanned }}</span>
                                </div>
                            </div>
                        </div>
                    </div>
                    <div class="glass-card" v-if="vetoByMatch.length">
                        <div class="section-heading">
                            <h3>BO2/BO3 veto-polku</h3>
                            <span class="section-sub">Jokainen askel, joukkue ja decider/overflow korostettu</span>
                        </div>
                        <div class="veto-timeline" v-for="v in vetoByMatch" :key="v.matchId">
                            <div class="veto-timeline__header">
                                <span class="pill">Match {{ v.matchId }}</span>
                                <span class="pill">Format {{ v.format.toUpperCase() }}</span>
                                <span class="pill" v-if="v.match?.opponentName || v.match?.team2Name">vs {{ v.match?.opponentName || v.match?.team2Name }}</span>
                            </div>
                            <div class="veto-steps">
                                <div v-for="step in v.steps" :key="step.step + step.mapName" class="veto-step" :class="'veto-step--' + step.action">
                                    <div class="veto-step__order">#{{ step.step }}</div>
                                    <div class="veto-step__title">{{ step.label }}</div>
                                    <div class="veto-step__map">{{ step.mapName }}</div>
                                    <div class="veto-step__actor">{{ step.teamName || 'Järjestelmä' }}</div>
                                </div>
                            </div>
                        </div>
                    </div>
                </section>
            </div>
        </div>
    `
};
