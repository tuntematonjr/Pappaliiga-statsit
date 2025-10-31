// TeamDetail - detailed per-team view for Division page
window.TeamDetail = {
    name: 'TeamDetail',
    props: {
        championshipId: { type: String, required: false },
        teamId: { type: String, required: true }
    },
    components: {
        get SortableTable() { return window.SortableTable; },
        get LoadingSpinner() { return window.LoadingSpinner; },
        get SplitBar() { return window.SplitBar; }
    },
    data() {
        return {
            team: null,
            seasonOptions: [],
            selectedChampionship: this.championshipId || null,
            players: [],
            loading: false,
            error: null,
            defaultAvatar: '/static/pappaliiga-logo-white-bg.png',
            mode: 'basic', // 'basic' | 'advanced'
            matchFilter: 'all',
            expandedMatchId: null
        };
    },
    computed: {
        backRoute() {
            const slug = this.$route && this.$route.query && this.$route.query.divisionSlug ? this.$route.query.divisionSlug : (this.$parent && this.$parent.division ? this.$parent.division.slug : '');
            const query = {};
            if (this.selectedChampionship) query.championship = this.selectedChampionship;
            if (this.$route && this.$route.query && this.$route.query.season) query.season = this.$route.query.season;
            return { name: 'division', params: { slug: slug }, query };
        },
        activeChampionshipId() {
            if (this.selectedChampionship) return this.selectedChampionship;
            if (this.championshipId) return this.championshipId;
            if (this.$route && this.$route.query && this.$route.query.championship) return this.$route.query.championship;
            return null;
        },
        headerStats() {
            if (!this.team) return {};
            return {
                maps: this.team.maps || 0,
                rounds_diff: this.team.rounds_diff || 0,
                win_rate: this.team.win_rate || 0,
                kd: this.team.kd || 0,
                adr: this.team.adr || 0
            };
        },
        basicColumns() {
                return [
                { key: 'nickname', label: 'Pelaajanimi', sortable: true, numeric: false, colClass: 'col-name', align: 'left' },
                { key: 'maps', label: 'Maps', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'rounds', label: 'Er\u00e4t', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2, colClass: 'col-numeric', align: 'center' },
                { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1, colClass: 'col-numeric', align: 'center' },
                { key: 'kr', label: 'KR', sortable: true, numeric: true, decimals: 2, colClass: 'col-numeric', align: 'center' },
                { key: 'damage', label: 'Vahinko', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'kills', label: 'Kills', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'deaths', label: 'Deaths', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'assists', label: 'Assists', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'hs_pct', label: 'HS%', sortable: true, numeric: true, decimals: 1, colClass: 'col-numeric', align: 'center' },
                { key: 'two_k', label: '2K', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'three_k', label: '3K', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'four_k', label: '4K', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'ace', label: 'ACE', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'mvps', label: 'MVPs', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' }
            ];
        },
        advancedColumns() {
            // Advanced mode should only include advanced-specific metrics (no duplication of basic columns)
            return [
                { key: 'nickname', label: 'Nickname', sortable: true, numeric: false, colClass: 'col-name', align: 'left' },
                { key: 'clutch_kills', label: 'Clutch-tilanteet', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'onevone_wr', label: '1v1 WR', sortable: true, numeric: false, align: 'center' },
                { key: 'onev2_wr', label: '1v2 WR', sortable: true, numeric: false, align: 'center' },
                { key: 'entry_wr', label: 'Entry-voittoprosentti', sortable: true, numeric: false, align: 'center' },
                { key: 'util_dmg', label: 'Util-vahinko', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'udpr', label: 'UDPR', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'survival_pct', label: 'Surv %', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'rating', label: 'Rating', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'flash_succ', label: 'Flash Succ', sortable: false, numeric: false, align: 'center' },
                { key: 'flashed', label: 'Flashed', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'enem_flash', label: 'Enem/Flash', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'pistol_kills', label: 'Pistol', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'sniper_kills', label: 'Sniper', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' }
            ];
        }
        ,
        mapsColumns() {
            return [
                { key: 'map', label: 'Kartta', sortable: true, numeric: false, colClass: 'col-name', align: 'left', width: '200px' },
                { key: 'played', label: 'Pelattu', sortable: true, numeric: true, width: '90px', colClass: 'col-numeric', align: 'center' },
                { key: 'picks', label: 'Omat pickit', sortable: true, numeric: true, width: '120px', colClass: 'col-numeric', align: 'center' },
                { key: 'opp_picks', label: 'Vast. pickit', sortable: true, numeric: true, width: '120px', colClass: 'col-numeric', align: 'center' },
                { key: 'wr_pct', label: 'WR %', sortable: true, numeric: true, decimals: 1, width: '110px', colClass: 'col-numeric', align: 'center' },
                { key: 'wr_own_pick', label: 'WR oma %', sortable: true, numeric: true, decimals: 1, width: '120px', colClass: 'col-numeric', align: 'center' },
                { key: 'wr_opp_pick', label: 'WR vast. %', sortable: true, numeric: true, decimals: 1, width: '120px', colClass: 'col-numeric', align: 'center' },
                { key: 'kd', label: 'KD', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center', decimals: 2 },
                { key: 'adr', label: 'ADR', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center', decimals: 1 },
                { key: 'rd', label: '+RD', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'ban1', label: '1. ban', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'ban2', label: '2. ban', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'opp_ban', label: 'Vast. ban', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'total_own_ban', label: 'Omat banit', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' },
                { key: 'dec_overflow', label: 'Dec/Overflow', sortable: true, numeric: true, colClass: 'col-numeric', align: 'center' }
            ];
        },
        mapsData() {
            // Use team.map_stats from API; return empty if not present
            if (!this.team || !this.team.map_stats || !this.team.map_stats.length) {
                return [];
            }
            return this.team.map_stats.map((m, idx) => {
                const wins = Number(m.win || m.wins || 0);
                const totalGames = typeof m.games !== 'undefined' ? Number(m.games) : Number(m.played || 0);
                const played = totalGames || Number(m.played || 0);
                const baseLoss = typeof m.loss !== 'undefined' ? Number(m.loss) : null;
                const losses = baseLoss !== null ? baseLoss : Math.max(played - wins, 0);
                const winsOwn = Number(m.wins_own || m.win_own || 0);
                const gamesOwn = Number(m.games_own || m.game_own || 0);
                const winsOpp = Number(m.wins_opp || m.win_opp || 0);
                const gamesOpp = Number(m.games_opp || m.game_opp || 0);
                const winRate = (wins + losses) ? (wins / (wins + losses)) * 100 : 0;
                const ownRate = gamesOwn ? (winsOwn / gamesOwn) * 100 : 0;
                const oppRate = gamesOpp ? (winsOpp / gamesOpp) * 100 : 0;
                const mapName = m.name || (typeof m.map_pretty === 'string' ? m.map_pretty : null);
                const decOverflow = typeof m.dec_overflow !== 'undefined' ? m.dec_overflow : (m.decov || 0);
                const logo = m.logo || m.image || m.image_sm || m.image_lg || null;
                return {
                    id: m.map || idx,
                    map: mapName || m.map || 'Unknown',
                    map_code: m.map || null,
                    logo,
                    played: Number(played || 0),
                    picks: Number(m.picks || 0),
                    opp_picks: Number(m.opp_picks || 0),
                    win: wins,
                    loss: losses,
                    wr_pct: Number(winRate.toFixed(1)),
                    wins_own: winsOwn,
                    games_own: gamesOwn,
                    wr_own_pick: Number(ownRate.toFixed(1)),
                    wins_opp: winsOpp,
                    games_opp: gamesOpp,
                    wr_opp_pick: Number(oppRate.toFixed(1)),
                    kd: Number(m.kd || 0),
                    adr: Number(m.adr || 0),
                    rd: Number(m.rd || 0),
                    ban1: Number(m.ban1 || 0),
                    ban2: Number(m.ban2 || 0),
                    opp_ban: Number(m.opp_ban || 0),
                    total_own_ban: Number(m.total_own_ban || 0),
                    dec_overflow: Number(decOverflow || 0)
                };
            });
        },
        matchesData() {
            // Use team.matches from API; return empty if not present
            if (!this.team || !Array.isArray(this.team.matches) || !this.team.matches.length) {
                return [];
            }
            const coerceNumber = (value) => {
                if (value === null || value === undefined || value === '') return null;
                const num = Number(value);
                return Number.isNaN(num) ? null : num;
            };
            const ownTeamId = this.teamId ? String(this.teamId) : null;
            return this.team.matches.map((m, idx) => {
                const matchId = m.match_id || m.id || idx;
                const opponentId = m.opponent_id || m.opponentId || null;
                const rawMaps = Array.isArray(m.maps) ? m.maps : [];
                const originalDate = m.date || m.scheduled || '';
                const originalTime = m.time || m.scheduled_time || '';
                const dateInfo = this.resolveMatchDateTime({
                    timestamp: m.timestamp,
                    date: originalDate,
                    time: originalTime,
                    timezone: m.timezone || m.tz || null
                });
                const maps = rawMaps.map((mp, mapIdx) => {
                    const teamStats = mp.team_stats || {};
                    const opponentStats = mp.opponent_stats || {};
                    const pickTeamId = mp.picked_by_team_id != null ? String(mp.picked_by_team_id) : null;
                    const isDecider = Boolean(mp.is_decider);
                    let pickLabel = null;
                    if (isDecider) {
                        pickLabel = 'Decider';
                    } else if (pickTeamId) {
                        if (ownTeamId && pickTeamId === ownTeamId) {
                            pickLabel = 'Own pick';
                        } else if (opponentId && pickTeamId === String(opponentId)) {
                            pickLabel = 'Opp. pick';
                        } else if (mp.picked_by_name) {
                            pickLabel = `${mp.picked_by_name} pick`;
                        } else {
                            pickLabel = 'Pick';
                        }
                    }
                    const rawCode = mp.map_code || mp.map_catalog_id || mp.map || mp.map_id || null;
                    const mapName = mp.map_name || mp.map_pretty || mp.map || rawCode || `Map ${mapIdx + 1}`;
                    const key = `${matchId}-${mapIdx}-${rawCode || mp.map_name || ''}`;
                    const candidateImage = mp.map_image || mp.map_image_lg || mp.map_image_sm || mp.image || mp.image_url || mp.image_sm || mp.image_lg || mp.thumbnail || mp.thumbnail_url || mp.logo || mp.map_logo || null;
                    const teamScoreRaw = coerceNumber(mp.team_score);
                    const opponentScoreRaw = coerceNumber(mp.opponent_score);
                    const hasTeamScore = Number.isFinite(teamScoreRaw);
                    const hasOpponentScore = Number.isFinite(opponentScoreRaw);
                    const playedByScore = hasTeamScore && hasOpponentScore && ((teamScoreRaw || 0) + (opponentScoreRaw || 0) > 0);
                    const explicitPlayed = typeof mp.played === 'boolean' ? mp.played : null;
                    const isForfeit = Boolean(mp.is_forfeit);
                    const played = (explicitPlayed != null ? explicitPlayed : playedByScore) || isForfeit;
                    const mapImage = candidateImage || null;
                    return {
                        key,
                        number: mp.map_number != null ? mp.map_number : mapIdx + 1,
                        code: rawCode || null,
                        name: mapName,
                        image: mapImage,
                        team_score: hasTeamScore ? teamScoreRaw : null,
                        opponent_score: hasOpponentScore ? opponentScoreRaw : null,
                        is_forfeit: isForfeit,
                        pick_label: pickLabel,
                        team_stats: {
                            adr: coerceNumber((teamStats && teamStats.adr) != null ? teamStats.adr : teamStats.ADR) ?? 0,
                            kd: coerceNumber((teamStats && teamStats.kd) != null ? teamStats.kd : teamStats.KD) ?? 0,
                            damage: coerceNumber((teamStats && teamStats.damage) != null ? teamStats.damage : teamStats.DMG) ?? 0,
                            kills: coerceNumber(teamStats.kills) ?? 0,
                            deaths: coerceNumber(teamStats.deaths) ?? 0
                        },
                        opponent_stats: {
                            adr: coerceNumber((opponentStats && opponentStats.adr) != null ? opponentStats.adr : opponentStats.ADR) ?? 0,
                            kd: coerceNumber((opponentStats && opponentStats.kd) != null ? opponentStats.kd : opponentStats.KD) ?? 0,
                            damage: coerceNumber((opponentStats && opponentStats.damage) != null ? opponentStats.damage : opponentStats.DMG) ?? 0,
                            kills: coerceNumber(opponentStats.kills) ?? 0,
                            deaths: coerceNumber(opponentStats.deaths) ?? 0
                        },
                        is_decider: isDecider,
                        played,
                        played_by_score: playedByScore,
                        winner: teamScoreRaw > opponentScoreRaw ? 'team' : (opponentScoreRaw > teamScoreRaw ? 'opponent' : 'tie')
                    };
                });
                const mapLines = maps.map((mapObj, mapIdx) => {
                    const num = mapObj.number != null ? mapObj.number : (mapIdx + 1);
                    const mapName = mapObj.name || mapObj.code || `Map ${num}`;
                    const teamScore = Number.isFinite(mapObj.team_score) ? Number(mapObj.team_score) : 0;
                    const opponentScore = Number.isFinite(mapObj.opponent_score) ? Number(mapObj.opponent_score) : 0;
                    const playedByScore = Number.isFinite(mapObj.team_score) && Number.isFinite(mapObj.opponent_score) && (teamScore + opponentScore > 0);
                    const played = typeof mapObj.played === 'boolean'
                        ? mapObj.played
                        : (playedByScore || Boolean(mapObj.is_forfeit));
                    let scoreText = '-';
                    if (playedByScore) {
                        scoreText = `${teamScore}-${opponentScore}`;
                    } else if (mapObj.is_forfeit) {
                        scoreText = 'Forfeit';
                    } else if (mapObj.played) {
                        scoreText = `${teamScore}-${opponentScore}`;
                    } else {
                        scoreText = 'Not played';
                    }
                    const metaParts = [];
                    if (mapObj.pick_label) metaParts.push(mapObj.pick_label);
                    if (mapObj.is_forfeit && scoreText !== 'Forfeit') metaParts.push('Forfeit');
                    if (!playedByScore && !mapObj.is_forfeit && mapObj.is_decider) metaParts.push('Decider');
                    const resultClass = playedByScore
                        ? (teamScore > opponentScore ? 'win' : (teamScore < opponentScore ? 'loss' : 'tie'))
                        : 'not-played';
                    return {
                        key: mapObj.key || `${matchId}-map-${mapIdx}`,
                        title: `Map ${num}: ${mapName}`,
                        score: scoreText,
                        meta: metaParts.filter(Boolean).join(' | '),
                        resultClass,
                        played,
                        image: mapObj.image || null
                    };
                });
                const totals = this.computeMatchTotals({ maps });
                const mapSummary = mapLines.length
                    ? mapLines.map(line => {
                        const scoreSegment = line.score && line.score !== '-' ? ` ${line.score}` : '';
                        const metaSegment = line.meta ? ` (${line.meta})` : '';
                        return `${line.title}${scoreSegment}${metaSegment}`.trim();
                    }).join(' | ')
                    : (m.map || m.map_name || '');
                const teamScore = coerceNumber(m.team_score);
                const opponentScore = coerceNumber(m.opponent_score);
                const scoreLabel = (teamScore != null && opponentScore != null)
                    ? `${teamScore}-${opponentScore}`
                    : (m.score || '');
                return {
                    id: matchId,
                    date: dateInfo.localDate || originalDate || '',
                    time: dateInfo.localTime || originalTime || '',
                    localDate: dateInfo.localDate || originalDate || '',
                    localTime: dateInfo.localTime || originalTime || '',
                    originalDate,
                    originalTime,
                    opponent: m.opponent || m.opponent_name || '',
                    opponent_logo: m.opponent_logo || '',
                    opponent_id: opponentId,
                    competition: m.competition || m.champ || '',
                    timestamp: dateInfo.timestamp,
                    rawTimestamp: dateInfo.rawTimestamp,
                    isoDateTime: dateInfo.isoDateTime,
                    map: mapSummary,
                    score: scoreLabel,
                    status: this.normalizeMatchStatus(m.status || (m.scheduled ? 'upcoming' : 'finished')),
                    faceit_url: m.faceit_url || '',
                    team_score: teamScore,
                    opponent_score: opponentScore,
                    maps,
                    totals,
                    mapLines,
                    best_of: coerceNumber(m.best_of),
                    team_logo: m.team_logo || (this.team && this.team.logo) || '',
                    team_name: m.team_name || (this.team && this.team.name) || '',
                };
            });
        },
        hasMatches() {
            return Array.isArray(this.matchesData) && this.matchesData.length > 0;
        },
        upcomingMatches() {
            if (!this.hasMatches) return [];
            return this.matchesData.filter(match => this.isUpcomingMatch(match));
        },
        playedMatches() {
            if (!this.hasMatches) return [];
            return this.matchesData.filter(match => !this.isUpcomingMatch(match));
        },
        filteredMatches() {
            if (!this.hasMatches) return [];
            let subset = this.matchesData;
            if (this.matchFilter === 'upcoming') {
                subset = this.upcomingMatches;
            } else if (this.matchFilter === 'played') {
                subset = this.playedMatches;
            }
            return this.sortMatches(subset, this.matchFilter);
        }
    },
    methods: {
        goBack() {
            const slug = this.$route && this.$route.query && this.$route.query.divisionSlug ? this.$route.query.divisionSlug : (this.$parent && this.$parent.division ? this.$parent.division.slug : null);
            if (slug) {
                this.$router.push({ name: 'division', params: { slug }, query: { championship: this.selectedChampionship } });
            } else if (window.history && window.history.length > 1) {
                this.$router.back();
            } else {
                // fallback to root
                this.$router.push({ name: 'home' });
            }
        },
        avatarUrl(src) {
            if (!src) return this.defaultAvatar;
            try {
                return window.apiClient && window.apiClient.proxyAvatar ? window.apiClient.proxyAvatar(src) : src;
            } catch (e) {
                return src;
            }
        },
        async loadSeasons() {
            // Try API if available
            try {
                if (window.apiClient && typeof window.apiClient.getTeamSeasons === 'function') {
                    const seasons = await window.apiClient.getTeamSeasons(this.teamId);
                    this.seasonOptions = (seasons || []).map(s => ({ championship_id: s.championship_id, label: s.label || (`Season ${s.season} - Div ${s.division_num}`) }));
                    if (!this.selectedChampionship && this.seasonOptions.length) {
                        this.selectedChampionship = this.seasonOptions[0].championship_id;
                    }
                    return;
                }
            } catch (e) {
                // ignore and fallback
            }
            // Fallback: use incoming prop or route query
            if (this.championshipId) {
                this.seasonOptions = [{ championship_id: this.championshipId, label: `Selected` }];
                if (!this.selectedChampionship) this.selectedChampionship = this.championshipId;
            } else if (this.$route && this.$route.query && this.$route.query.championship) {
                this.seasonOptions = [{ championship_id: this.$route.query.championship, label: `Selected` }];
                if (!this.selectedChampionship) this.selectedChampionship = this.$route.query.championship;
            }
        },
        onSeasonChange() {
            this.matchFilter = 'all';
            this.expandedMatchId = null;
            this.loadTeam();
        },
        async handleTeamChange() {
            this.team = null;
            this.players = [];
            await this.loadSeasons();
            if (this.selectedChampionship && !this.seasonOptions.some(opt => opt.championship_id === this.selectedChampionship)) {
                this.selectedChampionship = null;
            }
            if (!this.selectedChampionship && this.seasonOptions.length) {
                this.selectedChampionship = this.seasonOptions[0].championship_id;
            }
            this.matchFilter = 'all';
            this.expandedMatchId = null;
            await this.loadTeam();
        },
        async handleChampionshipChange(newChampionshipId) {
            this.team = null;
            this.players = [];
            await this.loadSeasons();
            if (newChampionshipId) {
                this.selectedChampionship = newChampionshipId;
            } else if (!this.selectedChampionship && this.seasonOptions.length) {
                this.selectedChampionship = this.seasonOptions[0].championship_id;
            }
            this.matchFilter = 'all';
            this.expandedMatchId = null;
            await this.loadTeam();
        },
        setMatchFilter(filter) {
            if (this.matchFilter === filter) return;
            this.matchFilter = filter;
            this.expandedMatchId = null;
        },
        toggleMatchDetails(matchId) {
            if (!matchId) {
                this.expandedMatchId = null;
                return;
            }
            this.expandedMatchId = (this.expandedMatchId === matchId) ? null : matchId;
        },
        isMatchExpanded(matchId) {
            if (!matchId) return false;
            return this.expandedMatchId === matchId;
        },
        normalizeMatchStatus(raw) {
            const value = (raw || '').toString().toLowerCase();
            if (!value) return 'finished';
            if (value.includes('cancel')) return 'cancelled';
            if (value.includes('live') || value === 'ongoing') return 'live';
            if (value.includes('upcoming') || value === 'scheduled' || value === 'pending' || value === 'open') return 'upcoming';
            if (value.includes('forfeit')) return 'finished';
            if (value.includes('finish') || value === 'done' || value === 'completed') return 'finished';
            return value;
        },
        isUpcomingMatch(match) {
            const status = (match && match.status) ? match.status.toString().toLowerCase() : '';
            return status === 'upcoming' || status === 'live';
        },
        statusLabel(status) {
            const normalized = (status || '').toString().toLowerCase();
            if (normalized === 'upcoming') return 'Tulossa';
            if (normalized === 'live') return 'K\u00e4ynniss\u00e4';
            if (normalized === 'cancelled') return 'Peruttu';
            return 'Pelattu';
        },
        statusChipClass(status) {
            const normalized = (status || '').toString().toLowerCase();
            if (normalized === 'upcoming') return 'muted';
            if (normalized === 'live') return 'accent';
            if (normalized === 'cancelled') return 'muted';
            return 'success';
        },
        matchSortKey(match) {
            if (!match) return 0;
            const ts = typeof match.timestamp === 'number' ? match.timestamp : Number(match.timestamp);
            if (!Number.isNaN(ts) && ts > 0) return ts;
            const rawTs = typeof match.rawTimestamp === 'number' ? match.rawTimestamp : Number(match.rawTimestamp);
            if (!Number.isNaN(rawTs) && rawTs > 0) return rawTs;
            const isoSource = match && match.isoDateTime ? String(match.isoDateTime) : '';
            if (isoSource) {
                const parsedIso = Date.parse(isoSource);
                if (!Number.isNaN(parsedIso)) return parsedIso;
            }
            const originalDate = match && match.originalDate ? String(match.originalDate) : '';
            const originalTime = match && match.originalTime ? String(match.originalTime) : '';
            if (originalDate) {
                const isoCandidate = `${originalDate}T${originalTime || '00:00'}`;
                const parsed = Date.parse(isoCandidate);
                if (!Number.isNaN(parsed)) return parsed;
                const fallback = Date.parse(`${originalDate} ${originalTime}`);
                if (!Number.isNaN(fallback)) return fallback;
            }
            return 0;
        },
        resolveMatchDateTime(source = {}) {
            const timestampValue = source.timestamp;
            let rawTimestamp = null;
            if (timestampValue !== null && timestampValue !== undefined && timestampValue !== '') {
                const numeric = Number(timestampValue);
                if (!Number.isNaN(numeric) && numeric > 0) {
                    rawTimestamp = numeric > 1e12 ? numeric : numeric * 1000;
                }
            }
            let dateObj = rawTimestamp ? new Date(rawTimestamp) : null;
            const dateStr = (source.date || '').trim();
            const timeStr = (source.time || '').trim();
            const candidateStrings = [];
            if ((!dateObj || Number.isNaN(dateObj.getTime())) && dateStr) {
                if (timeStr) {
                    candidateStrings.push(`${dateStr}T${timeStr}Z`);
                    candidateStrings.push(`${dateStr}T${timeStr}`);
                    candidateStrings.push(`${dateStr} ${timeStr}`);
                } else {
                    candidateStrings.push(`${dateStr}T00:00Z`);
                    candidateStrings.push(dateStr);
                }
                for (const candidate of candidateStrings) {
                    const parsed = Date.parse(candidate);
                    if (!Number.isNaN(parsed)) {
                        dateObj = new Date(parsed);
                        if (!rawTimestamp) rawTimestamp = parsed;
                        break;
                    }
                }
            }
            if (!dateObj || Number.isNaN(dateObj.getTime())) {
                return {
                    localDate: dateStr,
                    localTime: timeStr,
                    timestamp: null,
                    rawTimestamp,
                    isoDateTime: null,
                    originalDate: dateStr,
                    originalTime: timeStr
                };
            }
            const localDate = dateObj.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
            const localTime = dateObj.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' });
            return {
                localDate,
                localTime,
                timestamp: dateObj.getTime(),
                rawTimestamp: rawTimestamp != null ? rawTimestamp : dateObj.getTime(),
                isoDateTime: dateObj.toISOString(),
                originalDate: dateStr,
                originalTime: timeStr
            };
        },
        sortMatches(matches, filter) {
            if (!Array.isArray(matches)) return [];
            const asc = filter === 'upcoming';
            return [...matches].sort((a, b) => {
                const aKey = this.matchSortKey(a);
                const bKey = this.matchSortKey(b);
                if (aKey === bKey) {
                    if (a.id === b.id) return 0;
                    return asc ? (a.id > b.id ? 1 : -1) : (a.id > b.id ? -1 : 1);
                }
                return asc ? aKey - bKey : bKey - aKey;
            });
        },
        matchCardStatusClass(status) {
            const normalized = (status || '').toString().toLowerCase();
            if (normalized === 'live') return 'is-live';
            if (normalized === 'upcoming') return 'is-upcoming';
            if (normalized === 'cancelled') return 'is-cancelled';
            return 'is-finished';
        },
        matchHasScore(match) {
            if (!match) return false;
            return match.team_score != null && match.opponent_score != null;
        },
        // Helper: parse strings like '6-9' or '38/70' to numbers
        parseDashStats(s) {
            if (!s || typeof s !== 'string') return { a: 0, b: 0, total: 0, pct: 0 };
            const parts = s.split('-');
            if (parts.length === 2) {
                const a = Number(parts[0]) || 0;
                const b = Number(parts[1]) || 0;
                const total = a + b;
                const pct = total ? Math.round((a / total) * 100) : 0;
                return { a, b, total, pct };
            }
            // fallback: try slash format
            const parts2 = s.split('/');
            if (parts2.length === 2) {
                const a = Number(parts2[0]) || 0;
                const b = Number(parts2[1]) || 0;
                const pct = b ? Math.round((a / b) * 100) : 0;
                return { a, b, total: b, pct };
            }
            return { a: 0, b: 0, total: 0, pct: 0 };
        },
        flashToSplit(s) {
            // Accept '38/70' or '38/199' style; returns wins and losses
            if (!s || typeof s !== 'string') return { wins: 0, losses: 0 };
            const parts = s.split('/');
            if (parts.length === 2) {
                const wins = Number(parts[0]) || 0;
                const total = Number(parts[1]) || 0;
                const losses = Math.max(0, total - wins);
                return { wins, losses };
            }
            return { wins: 0, losses: 0 };
        },
        formatSplitLabel(value, suffix) {
            const num = Number(value || 0);
            return `${num} ${suffix}`;
        },
        dashSplit(value, winSuffix = 'W', lossSuffix = 'L') {
            const stats = this.parseDashStats(value);
            const wins = Number(stats.a || 0);
            const losses = Number(stats.b || 0);
            return {
                wins,
                losses,
                leftText: this.formatSplitLabel(wins, winSuffix),
                rightText: this.formatSplitLabel(losses, lossSuffix)
            };
        },
        flashSplit(value, winSuffix = 'Onn.', lossSuffix = 'Epa.') {
            const stats = this.flashToSplit(value);
            const wins = Number(stats.wins || 0);
            const losses = Number(stats.losses || 0);
            return {
                wins,
                losses,
                leftText: this.formatSplitLabel(wins, winSuffix),
                rightText: this.formatSplitLabel(losses, lossSuffix)
            };
        },
        formatNumber(value, decimals = 0) {
            const num = Number(value);
            if (Number.isNaN(num)) {
                return (0).toFixed(decimals);
            }
            return num.toFixed(decimals);
        },
        formatMapScore(value, map) {
            if (!map) return '\u2013';
            if (value === null || value === undefined || value === '') {
                return map.is_forfeit ? 'FF' : '\u2013';
            }
            const num = Number(value);
            if (Number.isNaN(num)) {
                return map.is_forfeit ? 'FF' : '\u2013';
            }
            const shouldShowNumeric = Boolean(map.played) || Boolean(map.played_by_score) || Boolean(map.is_forfeit);
            if (!shouldShowNumeric) return '\u2013';
            return this.formatNumber(num, 0);
        },
        formatPercent(value, decimals = 1) {
            const num = Number(value);
            if (Number.isNaN(num)) {
                return (0).toFixed(decimals) + '%';
            }
            return num.toFixed(decimals) + '%';
        },
        splitFromNumbers(wins, losses, winSuffix = 'W', lossSuffix = 'L') {
            const winNum = Number(wins || 0);
            const lossNum = Number(losses || 0);
            return {
                wins: winNum,
                losses: lossNum,
                leftText: this.formatSplitLabel(winNum, winSuffix),
                rightText: this.formatSplitLabel(lossNum, lossSuffix)
            };
        },
        splitFromCounts(wins, total, winSuffix = 'W', lossSuffix = 'L') {
            const winNum = Number(wins || 0);
            const totalNum = Number(total || 0);
            const lossNum = Math.max(totalNum - winNum, 0);
            return this.splitFromNumbers(winNum, lossNum, winSuffix, lossSuffix);
        },
        computeMatchTotals(match) {
            if (!match || !Array.isArray(match.maps) || !match.maps.length) return null;
            let mapsWon = 0;
            let mapsLost = 0;
            let roundsFor = 0;
            let roundsAgainst = 0;
            let kills = 0;
            let deaths = 0;
            let counted = 0;
            match.maps.forEach(map => {
                const playedFlag = typeof map.played === 'boolean' ? map.played : true;
                const includeMap = playedFlag || Boolean(map.is_forfeit);
                if (!includeMap) return;
                counted += 1;
                const teamScore = Number(map.team_score || 0);
                const oppScore = Number(map.opponent_score || 0);
                roundsFor += teamScore;
                roundsAgainst += oppScore;
                if (teamScore > oppScore) mapsWon += 1;
                else if (oppScore > teamScore) mapsLost += 1;
                kills += Number((map.team_stats && map.team_stats.kills) || 0);
                deaths += Number((map.team_stats && map.team_stats.deaths) || 0);
            });
            if (!counted) return null;
            const roundDiff = roundsFor - roundsAgainst;
            const kd = deaths ? (kills / deaths) : kills;
            return {
                mapsWon,
                mapsLost,
                mapsPlayed: mapsWon + mapsLost,
                roundsFor,
                roundsAgainst,
                roundDiff,
                kd
            };
        },
        formatSigned(value) {
            const num = Number(value || 0);
            if (Number.isNaN(num) || num === 0) return '0';
            return num > 0 ? `+${num}` : `${num}`;
        },
        async loadTeam() {
            this.loading = true; this.error = null;
            try {
                // Use selected championship if available, fallback to prop
                const championshipId = this.activeChampionshipId;
                // Fetch team details from API
                let payload = null;
                if (window.apiClient && typeof window.apiClient.getTeamDetails === 'function') {
                    payload = await window.apiClient.getTeamDetails(championshipId, this.teamId);
                } else {
                    throw new Error('API client not available');
                }
                if (!payload) {
                    this.team = null;
                    this.players = [];
                    this.error = 'Team not found';
                } else {
                    this.team = payload;
                    this.team.map_stats = Array.isArray(payload.map_stats) ? payload.map_stats : [];
                    this.team.matches = Array.isArray(payload.matches) ? payload.matches : [];
                    // Normalize players array
                    this.players = (payload.players || []).map((p, idx) => ({
                        id: p.player_id || p.id || idx,
                        nickname: p.nickname || p.name || p.nick || 'Unknown',
                        maps: Number(p.maps || 0),
                        rounds: Number(p.rounds || 0),
                        kd: Number(p.kd || 0),
                        adr: Number(p.adr || 0),
                        kr: Number(p.kr || 0),
                        damage: Number(p.damage || 0),
                        kills: Number(p.kills || 0),
                        deaths: Number(p.deaths || 0),
                        assists: Number(p.assists || 0),
                        hs_pct: Number(p.hs_pct || 0),
                        two_k: Number(p.two_k || 0),
                        three_k: Number(p.three_k || 0),
                        four_k: Number(p.four_k || 0),
                        ace: Number(p.ace || 0),
                        mvps: Number(p.mvps || 0),
                        clutch_kills: Number(p.clutch_kills || 0),
                        onevone_wr: p.onevone_wr || '',
                        onev2_wr: p.onev2_wr || '',
                        entry_wr: p.entry_wr || '',
                        util_dmg: Number(p.util_dmg || 0),
                        udpr: Number(p.udpr || 0),
                        survival_pct: Number(p.survival_pct || 0),
                        rating: Number(p.rating || 0),
                        flash_succ: p.flash_succ || '',
                        enem_flash: p.enem_flash || '',
                        flashed: Number(p.flashed || 0),
                        pistol_kills: Number(p.pistol_kills || 0),
                        sniper_kills: Number(p.sniper_kills || 0),
                        avatar: p.avatar || this.defaultAvatar
                    }));
                }
                this.expandedMatchId = null;
            } catch (err) {
                this.error = err && err.message ? err.message : 'Failed to load team data';
            } finally {
                this.loading = false;
            }
        },
        toggleMode(m) { this.mode = m; }
    },
    mounted() {
        this.loadSeasons().then(() => {
            // if selectedChampionship was not set by loadSeasons, use prop or route
            if (!this.selectedChampionship) this.selectedChampionship = this.championshipId || (this.$route && this.$route.query && this.$route.query.championship) || null;
            this.loadTeam();
        });
    },
    watch: {
        teamId(newVal, oldVal) {
            if (newVal && newVal !== oldVal) {
                this.handleTeamChange();
            }
        },
        championshipId(newVal, oldVal) {
            if (newVal !== oldVal) {
                this.handleChampionshipChange(newVal);
            }
        }
    },
    template: `
        <div class="team-detail card">
            <div style="margin-bottom:10px; display:flex; align-items:center; gap:12px;">
                <button class="chip" @click.prevent="goBack">Takaisin divisioonaan</button>
                <div v-if="seasonOptions && seasonOptions.length">
                    <label style="color:var(--muted); margin-right:8px; font-size:0.9rem;">Season:</label>
                    <select v-model="selectedChampionship" @change="onSeasonChange" style="padding:6px 10px; border-radius:8px; background:var(--card); color:var(--text); border:1px solid var(--border);">
                        <option v-for="s in seasonOptions" :key="s.championship_id" :value="s.championship_id">{{ s.label }}</option>
                    </select>
                </div>
            </div>
            <div class="team-detail-header">
                <div class="team-meta">
                    <img v-if="team" :src="avatarUrl(team.logo)" class="logo-large" alt="" />
                    <div>
                        <h3 v-if="team">{{ team.name }}</h3>
                        <p v-if="team" style="margin:2px 0 0;color:var(--muted);font-size:0.9rem;">
                            {{ team.championship_name || 'Championship' }} \u00b7 Season {{ team.season || '?' }} \u00b7 Div {{ team.division_num || '?' }}
                        </p>
                        <div v-if="team" class="team-stat-chips" style="display:flex;flex-wrap:wrap;gap:6px;margin-top:6px;">
                            <span class="chip muted">Matches: {{ team.matches_won || 0 }}-{{ team.matches_lost || 0 }}</span>
                            <span class="chip muted">Maps: {{ team.map_wins || 0 }}-{{ team.map_losses || 0 }}</span>
                            <span class="chip">Map WR: {{ formatPercent(team.win_rate) }}</span>
                            <span class="chip">Match WR: {{ formatPercent(team.match_win_rate) }}</span>
                            <span class="chip">KD: {{ formatNumber(team.kd, 2) }}</span>
                            <span class="chip">ADR: {{ formatNumber(team.adr, 1) }}</span>
                            <span class="chip muted">Rounds \u00b1: {{ formatNumber(team.rounds_diff || 0, 0) }}</span>
                        </div>
                    </div>
                </div>
            </div>
            <loading-spinner v-if="loading" message="Loading team..."></loading-spinner>
            <div v-else-if="error" class="error">{{ error }}</div>
            <div v-else>
                <div v-if="players && players.length" class="team-section">
                    <h3 class="section-title">Pelaajatilastot</h3>
                    <div class="team-detail-controls">
                        <div class="toggle-group" role="tablist" aria-label="Stats mode">
                            <button :class="['chip', mode==='basic' ? 'active' : '']" @click.prevent="toggleMode('basic')">Basic</button>
                            <button :class="['chip', mode==='advanced' ? 'active' : '']" @click.prevent="toggleMode('advanced')">Advanced</button>
                        </div>
                    </div>
                    <transition name="mode-fade" mode="out-in">
                        <sortable-table :key="mode" :columns="mode==='basic' ? basicColumns : advancedColumns" :data="players" :compact="true" :defaultSort="(mode==='basic' ? { column: 'kd', order: 'desc' } : { column: 'rating', order: 'desc' })" :colorize-columns="mode==='basic' ? ['kd','adr'] : ['udpr','rating','survival_pct']" aria-label="Team players table">
                        <template v-slot:cell-nickname="{ row }">
                            <div style="display:flex;align-items:center;gap:8px;">
                                <img :src="avatarUrl(row.avatar)" class="logo" style="width:24px;height:24px;object-fit:cover;border-radius:4px;" />
                                <span :title="row.nickname">{{ row.nickname }}</span>
                            </div>
                        </template>
                        <!-- Advanced-specific rendering slots -->
                        <template v-slot:cell-onevone_wr="{ row }">
                            <div class="split-wrapper">
                                <split-bar
                                    v-bind="dashSplit(row.onevone_wr, 'W', 'L')"
                                    height="32px"
                                    :show-labels="true"
                                    :show-percent="true"
                                ></split-bar>
                            </div>
                        </template>
                        <template v-slot:cell-entry_wr="{ row }">
                            <div class="split-wrapper">
                                <split-bar
                                    v-bind="dashSplit(row.entry_wr, 'W', 'L')"
                                    height="32px"
                                    :show-labels="true"
                                    :show-percent="true"
                                ></split-bar>
                            </div>
                        </template>
                        <template v-slot:cell-onev2_wr="{ row }">
                            <div class="split-wrapper">
                                <split-bar
                                    v-bind="dashSplit(row.onev2_wr, 'W', 'L')"
                                    height="32px"
                                    :show-labels="true"
                                    :show-percent="true"
                                ></split-bar>
                            </div>
                        </template>
                        <template v-slot:cell-flash_succ="{ row }">
                            <div class="split-wrapper">
                                <split-bar
                                    v-bind="flashSplit(row.flash_succ, 'Onn.', 'Epa.')"
                                    height="32px"
                                    :show-labels="true"
                                    :show-percent="true"
                                ></split-bar>
                            </div>
                        </template>
                        <template v-slot:cell-enem_flash="{ row }">
                            <div style="text-align:center;">
                                <span>
                                    {{ (function(){
                                        const v = parseDashStats(row.enem_flash);
                                        const num = Number(v.a || 0);
                                        const den = Number(v.b || 0);
                                        if (!den) return '-';
                                        return (num / den).toFixed(2);
                                    })() }}
                                </span>
                            </div>
                        </template>
                        <template v-slot:cell-util_dmg="{ row }">
                            <div style="text-align:right">{{ row.util_dmg }}</div>
                        </template>
                        <template v-slot:cell-udpr="{ row }">
                            <div style="text-align:center">{{ row.udpr }}</div>
                        </template>
                    </sortable-table>
                    </transition>
                </div>
                <!-- Maps stats table -->
                <div class="team-section">
                    <h3 class="section-title">Kartta tilastot</h3>
                    <div v-if="mapsData && mapsData.length" class="map-stats-table team-map-table">
                        <sortable-table :columns="mapsColumns" :data="mapsData" :compact="true" :defaultSort="{ column: 'played', order: 'desc' }" :colorize-columns="['kd','adr']">
                            <template v-slot:cell-wr_own_pick="{ row }">
                                <div class="split-wrapper map-split">
                                    <split-bar
                                        v-bind="splitFromCounts(row.wins_own, row.games_own, 'W', 'L')"
                                        height="32px"
                                        :show-labels="true"
                                        :show-percent="true"
                                    ></split-bar>
                                </div>
                            </template>
                            <template v-slot:cell-map="{ row }">
                                <div v-if="row.mapLines && row.mapLines.length" class="match-map-lines">
                                    <div v-for="line in row.mapLines" :key="line.key" class="map-line">
                                        <div class="map-line-main">
                                            <div class="map-line-left">
                                                <div v-if="line.image" class="map-line-thumb">
                                                    <img :src="line.image" :alt="line.title" loading="lazy" />
                                                </div>
                                                <span class="map-line-title">{{ line.title }}</span>
                                            </div>
                                            <span class="map-line-score" :class="line.resultClass">{{ line.score }}</span>
                                        </div>
                                        <div v-if="line.meta" class="map-line-meta">{{ line.meta }}</div>
                                    </div>
                                </div>
                                <div v-else>{{ row.map || '-' }}</div>
                            </template>
                            <template v-slot:cell-wr_opp_pick="{ row }">
                                <div class="split-wrapper map-split">
                                    <split-bar
                                        v-bind="splitFromCounts(row.wins_opp, row.games_opp, 'W', 'L')"
                                        height="32px"
                                        :show-labels="true"
                                        :show-percent="true"
                                    ></split-bar>
                                </div>
                            </template>
                            <template v-slot:cell-wr_pct="{ row }">
                                <div class="split-wrapper map-split">
                                    <split-bar
                                        v-bind="splitFromNumbers(row.win, row.loss, 'W', 'L')"
                                        height="32px"
                                        :show-labels="true"
                                    :show-percent="true"
                                ></split-bar>
                            </div>
                        </template>
                    </sortable-table>
                </div>
                <p v-else class="no-data">Ei karttatilastoja saatavilla</p>
                </div>
                <!-- Matches section (upcoming & past) -->
                <div class="team-section">
                    <h3 class="section-title">Ottelut</h3>
                    <div v-if="hasMatches">
                        <div class="team-matches-toolbar">
                            <div class="toggle-group" role="tablist" aria-label="Match filter">
                                <button :class="['chip', matchFilter==='all' ? 'active' : '']" @click.prevent="setMatchFilter('all')">Kaikki ({{ matchesData.length }})</button>
                                <button :class="['chip', matchFilter==='upcoming' ? 'active' : '']" @click.prevent="setMatchFilter('upcoming')">Tulossa ({{ upcomingMatches.length }})</button>
                                <button :class="['chip', matchFilter==='played' ? 'active' : '']" @click.prevent="setMatchFilter('played')">Pelatut ({{ playedMatches.length }})</button>
                            </div>
                        </div>
                        <div v-if="filteredMatches.length" class="match-card-list">
                            <article
                                v-for="match in filteredMatches"
                                :key="match.id"
                                :class="['match-card', matchCardStatusClass(match.status), isMatchExpanded(match.id) ? 'is-expanded' : '']"
                            >
                                <header class="match-card-header">
                                    <div class="match-card-meta">
                                        <span class="match-date">{{ match.date || '--' }}</span>
                                        <span v-if="match.time" class="match-time">klo {{ match.time }}</span>
                                        <span v-if="match.competition" class="match-competition">{{ match.competition }}</span>
                                    </div>
                                    <div class="match-card-actions">
                                        <span :class="['chip', statusChipClass(match.status)]">{{ statusLabel(match.status) }}</span>
                                        <a v-if="match.faceit_url" class="chip link faceit-link" :href="match.faceit_url" target="_blank" rel="noopener">Open on FACEIT</a>
                                        <button class="chip link" @click.prevent="toggleMatchDetails(match.id)">
                                            {{ isMatchExpanded(match.id) ? 'Sulje' : 'Näytä tiedot' }}
                                        </button>
                                    </div>
                                </header>
                                <div class="match-card-body">
                                    <div class="match-scoreboard">
                                        <div class="match-side">
                                            <img :src="avatarUrl(match.team_logo || (team && team.logo))" alt="" class="match-side-logo" />
                                            <div class="match-side-meta">
                                                <div class="match-side-name">{{ match.team_name || (team && team.name) || 'Team' }}</div>
                                            </div>
                                        </div>
                                        <div class="match-score-center">
                                            <div class="match-score-value">
                                                <template v-if="matchHasScore(match)">
                                                    <span class="score">{{ formatNumber(match.team_score, 0) }}</span>
                                                    <span class="separator">-</span>
                                                    <span class="score">{{ formatNumber(match.opponent_score, 0) }}</span>
                                                </template>
                                                <template v-else>
                                                    <span class="score">{{ isUpcomingMatch(match) ? 'vs' : '-' }}</span>
                                                </template>
                                            </div>
                                            <div class="match-score-sub" v-if="match.best_of">
                                                <span class="chip muted">BO{{ match.best_of }}</span>
                                            </div>
                                        </div>
                                        <div class="match-side opponent">
                                            <img :src="avatarUrl(match.opponent_logo)" alt="" class="match-side-logo" />
                                            <div class="match-side-meta">
                                                <div class="match-side-name">{{ match.opponent || 'Opponent' }}</div>
                                            </div>
                                        </div>
                                    </div>
                                    <div v-if="match.mapLines && match.mapLines.length && !isMatchExpanded(match.id)" class="match-map-lines">
                                        <div v-for="line in match.mapLines" :key="line.key" class="map-line">
                                            <div class="map-line-main">
                                                <div class="map-line-left">
                                                    <div v-if="line.image" class="map-line-thumb">
                                                        <img :src="line.image" :alt="line.title" loading="lazy" />
                                                    </div>
                                                    <span class="map-line-title">{{ line.title }}</span>
                                                </div>
                                                <span class="map-line-score" :class="line.resultClass">{{ line.score }}</span>
                                            </div>
                                            <div v-if="line.meta" class="map-line-meta">{{ line.meta }}</div>
                                        </div>
                                    </div>
                                </div>
                                <transition name="fade">
                                    <div v-if="isMatchExpanded(match.id)" class="match-card-details">
                                        <div class="match-detail-meta">
                                            <span class="chip muted">{{ statusLabel(match.status) }}</span>
                                            <span v-if="match.best_of" class="chip muted">BO{{ match.best_of }}</span>
                                            <span v-if="match.totals" class="chip muted">Rounds {{ match.totals.roundsFor }}-{{ match.totals.roundsAgainst }}</span>
                                            <span v-if="match.totals" class="chip muted">RD {{ formatSigned(match.totals.roundDiff) }}</span>
                                            <span v-if="match.totals" class="chip muted">K/D {{ formatNumber(match.totals.kd, 2) }}</span>
                                            <a v-if="match.faceit_url" class="chip link faceit-link" :href="match.faceit_url" target="_blank" rel="noopener">Open on FACEIT</a>
                                        </div>
                                        <div v-if="match.maps && match.maps.length" class="match-map-list">
                                            <div v-for="map in match.maps" :key="map.key || map.number" class="match-map-card">
                                                <div class="map-card-header">
                                                    <span class="map-index" v-if="map.number != null">Map {{ map.number }}</span>
                                                </div>
                                                <div class="map-card-name">
                                                    {{ map.name }}
                                                </div>
                                                <div class="map-card-meta">
                                                    <span v-if="map.pick_label" class="tag">{{ map.pick_label }}</span>
                                                    <span v-if="map.is_forfeit" class="tag warning">Forfeit</span>
                                                    <span v-if="!map.played" class="tag muted">Not played</span>
                                                    <span v-if="map.is_decider && map.played && !map.is_forfeit" class="tag muted">Decider</span>
                                                </div>
                                                <div class="map-card-body">
                                                    <div :class="['map-card-side', map.winner === 'team' ? 'winner' : (map.winner === 'opponent' ? 'loser' : 'tie')]">
                                                        <div class="map-side-name">{{ match.team_name || (team && team.name) || 'Team' }}</div>
                                                        <div class="map-rounds">Rounds {{ formatMapScore(map.team_score, map) }}</div>
                                                        <div class="map-metrics">
                                                            <div class="map-metric">
                                                                <span class="label">ADR</span>
                                                                <span class="value">{{ formatNumber(map.team_stats.adr, 1) }}</span>
                                                            </div>
                                                            <div class="map-metric">
                                                                <span class="label">K/D</span>
                                                                <span class="value">{{ formatNumber(map.team_stats.kd, 2) }}</span>
                                                            </div>
                                                            <div class="map-metric">
                                                                <span class="label">DMG</span>
                                                                <span class="value">{{ formatNumber(map.team_stats.damage, 0) }}</span>
                                                            </div>
                                                        </div>
                                                    </div>
                                                    <div class="map-card-thumb" :class="{ 'fallback': !map.image }">
                                                        <template v-if="map.image">
                                                            <img :src="map.image" :alt="map.name" loading="lazy" />
                                                        </template>
                                                        <template v-else>
                                                            <div class="map-thumb-placeholder">{{ map.name }}</div>
                                                        </template>
                                                    </div>
                                                    <div :class="['map-card-side', 'opponent', map.winner === 'opponent' ? 'winner' : (map.winner === 'team' ? 'loser' : 'tie')]">
                                                        <div class="map-side-name">{{ match.opponent || 'Opponent' }}</div>
                                                        <div class="map-rounds">Rounds {{ formatMapScore(map.opponent_score, map) }}</div>
                                                        <div class="map-metrics">
                                                            <div class="map-metric">
                                                                <span class="label">ADR</span>
                                                                <span class="value">{{ formatNumber(map.opponent_stats.adr, 1) }}</span>
                                                            </div>
                                                            <div class="map-metric">
                                                                <span class="label">K/D</span>
                                                                <span class="value">{{ formatNumber(map.opponent_stats.kd, 2) }}</span>
                                                            </div>
                                                            <div class="map-metric">
                                                                <span class="label">DMG</span>
                                                                <span class="value">{{ formatNumber(map.opponent_stats.damage, 0) }}</span>
                                                            </div>
                                                        </div>
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                        <p v-else class="no-data">Ei karttadataa tälle ottelulle</p>
                                    </div>
                                </transition>
                            </article>
                        </div>
                        <p v-else class="no-data">Ei otteluita tälle valinnalle</p>
                    </div>
                    <p v-else class="no-data">Ei otteluita saatavilla</p>
                </div>
                <div v-if="!players || !players.length" class="team-section">
                    <p class="no-data">Ei pelaajadataa saatavilla</p>
                </div>
            </div>
        </div>
    `
};
