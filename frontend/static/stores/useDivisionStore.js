(function () {
    const { defineStore } = Pinia;

    const FRESH_MS = 5 * 60 * 1000;

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
            highlights: createSegment()
        };
    }

    function ensureAvatar(url) {
        if (!url) return null;
        try {
            return window.apiClient.proxyAvatar(url);
        } catch (error) {
            return url;
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

    function deriveStandings(details) {
        if (!details || !Array.isArray(details.teams)) {
            return [];
        }
        const standings = details.teams
            .filter(team => team && !details.excluded_team_ids?.includes?.(team.team_id))
            .map(team => {
                const wins = toNumber(team.wins ?? team.matches_won ?? team.maps_won ?? 0);
                const losses = toNumber(team.losses ?? team.matches_lost ?? team.maps_lost ?? 0);
                const matchesPlayed = toNumber(team.matches_played ?? wins + losses);
                const roundDiff = toNumber(team.rounds_diff ?? (team.rounds_won ?? 0) - (team.rounds_lost ?? 0));
                const mapWinRate = toNumber(team.win_rate ?? team.map_win_rate ?? team.match_win_rate);
                const kd = toNumber(team.kd ?? team.rating ?? 0);
                return {
                    id: team.team_id,
                    team_id: team.team_id,
                    name: team.display_name || team.team_name || team.name || 'Joukkue',
                    logo: ensureAvatar(team.avatar || team.logo || team.team_logo),
                    wins,
                    losses,
                    matchesPlayed,
                    winRate: mapWinRate,
                    roundDiff,
                    roundsWon: toNumber(team.rounds_won ?? 0),
                    roundsLost: toNumber(team.rounds_lost ?? 0),
                    kd: toNumber(team.kd ?? 0),
                    adr: toNumber(team.adr ?? 0),
                    points: toNumber(team.points ?? wins * 3),
                    raw: team
                };
            });

        standings.sort((a, b) => {
            if (b.wins !== a.wins) {
                return b.wins - a.wins;
            }
            if (b.roundDiff !== a.roundDiff) {
                return b.roundDiff - a.roundDiff;
            }
            if (b.winRate !== a.winRate) {
                return b.winRate - a.winRate;
            }
            return (a.name || '').localeCompare(b.name || '', 'fi');
        });

        return standings.map((row, index) => ({
            ...row,
            rank: index + 1,
            winPct: percent(row.wins, row.matchesPlayed)
        }));
    }

    function deriveHighlights(details, standings) {
        if (!details) {
            return [];
        }
        const teams = standings && standings.length ? standings : deriveStandings(details);
        if (!teams.length) {
            return [];
        }
        const topTeam = teams[0];
        const clutchTeam = [...teams].sort((a, b) => b.roundDiff - a.roundDiff)[0];
        const kdAce = [...teams].sort((a, b) => b.kd - a.kd)[0];

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
                    entry.standings.data = deriveStandings(data);
                    entry.standings.fetchedAt = now();
                    entry.highlights.data = deriveHighlights(data, entry.standings.data);
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
                    let standings = null;
                    try {
                        if (typeof window.apiClient.getDivisionStandings === 'function') {
                            standings = await window.apiClient.getDivisionStandings(id);
                        }
                    } catch (error) {
                        if (!error || error.status !== 404) {
                            console.warn('Division standings endpoint failed, using fallback', error);
                        }
                    }
                    if (!Array.isArray(standings) || !standings.length) {
                        const details = entry.details.data || (await this.fetchDivisionDetails(id));
                        standings = deriveStandings(details);
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
                    let highlights = null;
                    try {
                        if (typeof window.apiClient.getDivisionHighlights === 'function') {
                            highlights = await window.apiClient.getDivisionHighlights(id);
                        }
                    } catch (error) {
                        if (!error || error.status !== 404) {
                            console.warn('Division highlights endpoint failed, using fallback', error);
                        }
                    }
                    if (!Array.isArray(highlights) || !highlights.length) {
                        const details = entry.details.data || (await this.fetchDivisionDetails(id));
                        const standings = entry.standings.data || deriveStandings(details);
                        highlights = deriveHighlights(details, standings);
                    }
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
