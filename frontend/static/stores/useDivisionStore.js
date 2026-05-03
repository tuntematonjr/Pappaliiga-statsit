(function () {
    const { defineStore } = Pinia;

    const FRESH_MS = 5 * 60 * 1000;
    const DEFAULT_TEAM_LOGO = window.PAPPALIIGA_DEFAULT_LOGO;

    function now() {
        return Date.now();
    }

    function isFresh(entry) {
        return !!(entry && entry.fetchedAt && now() - entry.fetchedAt < FRESH_MS && !entry.error);
    }

    function createSegment() {
        return {
            data: null,
            loading: false,
            error: null,
            fetchedAt: null
        };
    }

    function createEntry() {
        return {
            details: createSegment(),
            standings: createSegment(),
            maps: createSegment(),
            highlights: createSegment(),
            rawMatches: createSegment()
        };
    }

    function ensureAvatar(url) {
        if (!url) return DEFAULT_TEAM_LOGO;
        try {
            const resolved = window.apiClient.proxyAvatar(url);
            return resolved || DEFAULT_TEAM_LOGO;
        } catch (error) {
            return DEFAULT_TEAM_LOGO;
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

    function percent(part, total) {
        const played = toNumber(part);
        const max = toNumber(total);
        if (!max) return 0;
        return Math.round((played / max) * 1000) / 10;
    }

    function deriveStandingsFromMatches(matches, teams = [], championshipId = null) {
        if (!Array.isArray(matches) || !matches.length) {
            return [];
        }
        const filtered = matches.filter(match => {
            if (!championshipId) return true;
            const matchChampId = match.championship_id || match.championshipId;
            return String(matchChampId || '') === String(championshipId);
        });
        if (!filtered.length) return [];

        const teamMeta = (teams || []).reduce((acc, team) => {
            if (!team?.team_id) return acc;
            acc[String(team.team_id)] = team;
            return acc;
        }, {});

        const table = new Map();
        function ensureTeam(id, fallbackName = 'Joukkue') {
            const key = String(id);
            if (!table.has(key)) {
                const meta = teamMeta[key] || {};
                table.set(key, {
                    id: key,
                    team_id: key,
                    name: meta.display_name || meta.team_name || fallbackName,
                    logo: ensureAvatar(meta.avatar || meta.logo || meta.team_logo),
                    wins: 0,
                    losses: 0,
                    matchesPlayed: 0,
                    matches_played: 0,
                    matches: 0,
                    roundDiff: toNumber(meta.rounds_diff ?? (meta.rounds_won ?? 0) - (meta.rounds_lost ?? 0)),
                    roundsWon: toNumber(meta.rounds_won ?? 0),
                    roundsLost: toNumber(meta.rounds_lost ?? 0),
                    kd: toNumber(meta.kd ?? 0),
                    adr: toNumber(meta.adr ?? 0),
                    points: toNumber(meta.points ?? 0),
                    raw: meta
                });
            }
            return table.get(key);
        }

        filtered.forEach(match => {
            const team1 = match.team1_id || match.team1Id;
            const team2 = match.team2_id || match.team2Id;
            if (!team1 || !team2) return;
            const t1Score = toNumber(match.team1_score ?? match.team1Score ?? match.score_team1 ?? 0);
            const t2Score = toNumber(match.team2_score ?? match.team2Score ?? match.score_team2 ?? 0);

            const entry1 = ensureTeam(team1, match.team1_name || match.team1Name || 'Joukkue 1');
            const entry2 = ensureTeam(team2, match.team2_name || match.team2Name || 'Joukkue 2');

            entry1.matchesPlayed += 1;
            entry2.matchesPlayed += 1;
            entry1.matches_played += 1;
            entry2.matches_played += 1;
            entry1.matches += 1;
            entry2.matches += 1;
            entry1.wins += t1Score;
            entry1.losses += t2Score;
            entry2.wins += t2Score;
            entry2.losses += t1Score;
        });

        const standings = Array.from(table.values()).map(row => ({
            ...row,
            winRate: percent(row.wins, row.wins + row.losses),
            winPct: percent(row.wins, row.wins + row.losses)
        }));

        standings.sort((a, b) => {
            if (b.wins !== a.wins) return b.wins - a.wins;
            if (b.roundDiff !== a.roundDiff) return b.roundDiff - a.roundDiff;
            if (b.winRate !== a.winRate) return b.winRate - a.winRate;
            return (a.name || '').localeCompare(b.name || '', 'fi');
        });

        return standings.map((row, idx) => ({
            ...row,
            rank: idx + 1
        }));
    }

    function deriveHighlights(details, standings) {
        if (!details || !Array.isArray(standings) || !standings.length) {
            return [];
        }
        const topTeam = standings[0];
        const clutchTeam = [...standings].sort((a, b) => b.roundDiff - a.roundDiff)[0];
        const kdAce = [...standings].sort((a, b) => b.kd - a.kd)[0];

        const highlights = [];
        if (topTeam) {
            highlights.push({
                id: 'top-team',
                title: 'Kärkipaikka',
                description: `${topTeam.name} johtaa sarjaa`,
                metric: `${topTeam.wins}-${topTeam.losses}`,
                tooltip: `Voitto-% ${topTeam.winPct} | Round diff ${topTeam.roundDiff >= 0 ? '+' : ''}${topTeam.roundDiff}`,
                team: topTeam
            });
        }
        if (clutchTeam && clutchTeam.id !== topTeam?.id) {
            highlights.push({
                id: 'round-diff',
                title: 'Roundi-ylivoima',
                description: `${clutchTeam.name} dominoi erissä`,
                metric: `${clutchTeam.roundDiff >= 0 ? '+' : ''}${clutchTeam.roundDiff}`,
                tooltip: `${clutchTeam.roundsWon} voitettua erää`,
                team: clutchTeam
            });
        }
        if (kdAce && kdAce.id !== topTeam?.id && kdAce.id !== clutchTeam?.id) {
            highlights.push({
                id: 'kd-ace',
                title: 'KD-kunkku',
                description: `${kdAce.name} pitää tapposuhteen kurissa`,
                metric: kdAce.kd.toFixed(2),
                tooltip: `ADR ${kdAce.adr}`,
                team: kdAce
            });
        }
        return highlights;
    }

    window.useDivisionStore = defineStore('division', {
        state: () => ({
            divisions: {}
        }),
        getters: {
            getDivisionState(state) {
                return id => {
                    if (!id) {
                        return createEntry();
                    }
                    return state.divisions[id] || createEntry();
                };
            }
        },
        actions: {
            ensureEntry(id) {
                if (!id) return createEntry();
                if (!this.divisions[id]) {
                    this.divisions[id] = createEntry();
                }
                return this.divisions[id];
            },
            async fetchDivisionDetails(id, options = {}) {
                if (!id) return null;
                const { force = false } = options;
                const entry = this.ensureEntry(id);
                if (entry.details.loading) {
                    return entry.details.data;
                }
                if (!force && isFresh(entry.details)) {
                    return entry.details.data;
                }
                entry.details.loading = true;
                entry.details.error = null;
                try {
                    const data = await window.apiClient.getDivisionById(id);
                    entry.details.data = data;
                    entry.details.fetchedAt = now();
                    entry.highlights.data = [];
                    entry.highlights.fetchedAt = now();
                    if (Array.isArray(data?.map_stats) && data.map_stats.length) {
                        entry.maps.data = data.map_stats;
                        entry.maps.fetchedAt = now();
                    }
                    return data;
                } catch (error) {
                    entry.details.error = error?.message || 'Divisioonan lataus epäonnistui';
                    throw error;
                } finally {
                    entry.details.loading = false;
                }
            },
            async fetchDivisionBundle(id, options = {}) {
                if (!id || !window.apiClient || typeof window.apiClient.getDivisionPage !== 'function') {
                    return this.fetchDivisionDetails(id, options);
                }
                const { force = false } = options;
                const entry = this.ensureEntry(id);
                if (!force && isFresh(entry.details) && isFresh(entry.rawMatches)) {
                    return entry.details.data;
                }
                entry.details.loading = true;
                entry.rawMatches.loading = true;
                entry.details.error = null;
                entry.rawMatches.error = null;
                try {
                    const bundle = await window.apiClient.getDivisionPage(id);
                    const data = bundle.details || {};
                    if (bundle.bracket != null) {
                        data.bracket = bundle.bracket;
                    }
                    entry.details.data = data;
                    entry.details.fetchedAt = now();
                    entry.details.loading = false;
                    entry.highlights.data = [];
                    entry.highlights.fetchedAt = now();
                    if (Array.isArray(data?.map_stats) && data.map_stats.length) {
                        entry.maps.data = data.map_stats;
                        entry.maps.fetchedAt = now();
                    }
                    const matches = Array.isArray(bundle.matches) ? bundle.matches : [];
                    entry.rawMatches.data = matches;
                    entry.rawMatches.fetchedAt = now();
                    entry.rawMatches.loading = false;
                    return data;
                } catch (error) {
                    entry.details.error = error?.message || 'Divisioonan lataus epäonnistui';
                    entry.rawMatches.error = error?.message || 'Ottelijoiden lataus epäonnistui';
                    throw error;
                } finally {
                    entry.details.loading = false;
                    entry.rawMatches.loading = false;
                }
            },
            async fetchDivisionStandings(id, options = {}) {
                if (!id) return [];
                const { force = false } = options;
                const entry = this.ensureEntry(id);
                if (entry.standings.loading) {
                    return entry.standings.data || [];
                }
                if (!force && isFresh(entry.standings)) {
                    return entry.standings.data || [];
                }
                entry.standings.loading = true;
                entry.standings.error = null;
                try {
                    let standings = [];
                    // Use pre-fetched raw matches from bundle if available, else fetch separately
                    let matches;
                    if (isFresh(entry.rawMatches) && Array.isArray(entry.rawMatches.data)) {
                        matches = entry.rawMatches.data;
                    } else if (typeof window.apiClient.getDivisionMatches === 'function') {
                        matches = await window.apiClient.getDivisionMatches(id);
                        entry.rawMatches.data = Array.isArray(matches) ? matches : [];
                        entry.rawMatches.fetchedAt = now();
                    }
                    const scopedMatches = Array.isArray(matches)
                        ? matches.filter(match => String(match?.championship_id ?? match?.championshipId ?? '') === String(id))
                        : [];
                    if (scopedMatches.length) {
                        const details = entry.details.data || (await this.fetchDivisionDetails(id));
                        standings = deriveStandingsFromMatches(scopedMatches, details?.teams || [], id);
                    }
                    entry.standings.data = standings;
                    entry.standings.fetchedAt = now();
                    return standings;
                } catch (error) {
                    entry.standings.error = error?.message || 'Sarjataulukon lataus epäonnistui';
                    throw error;
                } finally {
                    entry.standings.loading = false;
                }
            },
            async fetchDivisionMaps(id, options = {}) {
                if (!id) return [];
                const { force = false } = options;
                const entry = this.ensureEntry(id);
                if (entry.maps.loading) {
                    return entry.maps.data || [];
                }
                if (!force && isFresh(entry.maps)) {
                    return entry.maps.data || [];
                }
                entry.maps.loading = true;
                entry.maps.error = null;
                try {
                    const maps = await window.apiClient.getDivisionMapStats(id);
                    entry.maps.data = Array.isArray(maps) ? maps : [];
                    entry.maps.fetchedAt = now();
                    return entry.maps.data;
                } catch (error) {
                    entry.maps.error = error?.message || 'Karttatilastojen lataus epäonnistui';
                    throw error;
                } finally {
                    entry.maps.loading = false;
                }
            },
            async fetchDivisionHighlights(id, options = {}) {
                if (!id) return [];
                const { force = false } = options;
                const entry = this.ensureEntry(id);
                if (entry.highlights.loading) {
                    return entry.highlights.data || [];
                }
                if (!force && isFresh(entry.highlights)) {
                    return entry.highlights.data || [];
                }
                entry.highlights.loading = true;
                entry.highlights.error = null;
                try {
                    const details = entry.details.data || (await this.fetchDivisionDetails(id));
                    const standings = entry.standings.data || [];
                    const highlights = deriveHighlights(details, standings);
                    entry.highlights.data = highlights;
                    entry.highlights.fetchedAt = now();
                    return highlights;
                } catch (error) {
                    entry.highlights.error = error?.message || 'Nostoja ei voitu hakea';
                    throw error;
                } finally {
                    entry.highlights.loading = false;
                }
            }
        }
    });
})();
