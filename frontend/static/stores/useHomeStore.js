(function () {
    const { defineStore } = Pinia;

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

    function pickValue(obj, candidates) {
        if (!obj) return undefined;
        const paths = Array.isArray(candidates) ? candidates : [candidates];
        for (const path of paths) {
            if (!path) continue;
            const segments = String(path).split('.');
            let current = obj;
            let found = true;
            for (const segment of segments) {
                if (current && Object.prototype.hasOwnProperty.call(current, segment)) {
                    current = current[segment];
                } else {
                    found = false;
                    break;
                }
            }
            if (found && current !== undefined) {
                return current;
            }
        }
        return undefined;
    }

    function computePercent(part, total) {
        const played = toNumber(part);
        const max = toNumber(total);
        if (!max || max <= 0) return 0;
        return Math.min(100, Math.round((played / max) * 1000) / 10);
    }

    function defaultProgress() {
        return {
            overall: { played: 0, total: 0, percent: 0 },
            regular: { played: 0, total: 0, percent: 0 },
            playoffs: { played: 0, total: 0, percent: 0 }
        };
    }

    function computeProgress(stats, divisions) {
        const progress = defaultProgress();
        const source = stats || {};

        const overallPlayed =
            pickValue(source, ['progress.overall.played', 'matches_played', 'played_matches']) ??
            divisions.reduce((sum, division) => sum + toNumber(division?.matchesPlayed ?? division?.played_matches), 0);
        const overallTotal =
            pickValue(source, ['progress.overall.total', 'matches_total', 'scheduled_matches']) ??
            divisions.reduce((sum, division) => sum + toNumber(division?.matchesTotal ?? division?.total_matches), 0);

        progress.overall = {
            played: toNumber(overallPlayed),
            total: toNumber(overallTotal),
            percent: computePercent(overallPlayed, overallTotal)
        };

        const regularPlayed = pickValue(source, ['progress.regular.played', 'progress.runkosarja.played']);
        const regularTotal = pickValue(source, ['progress.regular.total', 'progress.runkosarja.total']);
        const playoffPlayed = pickValue(source, ['progress.playoffs.played', 'progress.playoff.played']);
        const playoffTotal = pickValue(source, ['progress.playoffs.total', 'progress.playoff.total']);

        if (regularPlayed !== undefined || regularTotal !== undefined) {
            progress.regular = {
                played: toNumber(regularPlayed, overallPlayed),
                total: toNumber(regularTotal, overallTotal),
                percent: computePercent(regularPlayed ?? overallPlayed, regularTotal ?? overallTotal)
            };
        }

        if (playoffPlayed !== undefined || playoffTotal !== undefined) {
            progress.playoffs = {
                played: toNumber(playoffPlayed),
                total: toNumber(playoffTotal),
                percent: computePercent(playoffPlayed, playoffTotal)
            };
        }

        return progress;
    }

    const DEFAULT_TEAM_LOGO = window.PAPPALIIGA_DEFAULT_LOGO;

    function ensureAvatar(url) {
        if (!url) return DEFAULT_TEAM_LOGO;
        try {
            const resolved = window.apiClient.proxyAvatar(url);
            return resolved || DEFAULT_TEAM_LOGO;
        } catch (error) {
            return DEFAULT_TEAM_LOGO;
        }
    }

    function normalizeDivision(raw, index) {
        if (!raw || typeof raw !== 'object' || raw.is_playoff_secondary) {
            return null;
        }

        const fallbackKey = `division-${index}`;
        const identifier = raw.championship_id ?? raw.id ?? raw.slug ?? raw.code ?? raw.uid;
        const key = identifier != null ? String(identifier) : fallbackKey;
        const name =
            raw.name ||
            raw.display_name ||
            raw.title ||
            (raw.division != null ? `Division ${raw.division}` : `Division ${index + 1}`);
        const phase = raw.phase || (raw.is_playoff ? 'Playoffs' : null);
        const subtitle = raw.subtitle || raw.tagline || phase || null;

        const matchesPlayed = toNumber(raw.played_matches ?? raw.matches_played ?? raw.matches ?? 0);
        const matchesTotal = toNumber(raw.total_matches ?? raw.scheduled_matches ?? raw.schedule_count ?? 0);
        const progressPercent = matchesTotal > 0 ? Math.min(100, Math.round((matchesPlayed / matchesTotal) * 100)) : 0;

        const wins = toNumber(raw.win_count ?? raw.wins ?? raw.matches_won ?? 0);
        const losses = toNumber(raw.loss_count ?? raw.losses ?? raw.matches_lost ?? 0);
        const draws = toNumber(raw.draws ?? raw.ties ?? raw.matches_drawn ?? 0);
        const roundDiff = toNumber(raw.rounds_diff ?? raw.round_diff ?? raw.rounds_delta ?? 0);

        const teams = Array.isArray(raw.teams) ? raw.teams : [];
        const teamCount = teams.length || toNumber(raw.team_count ?? raw.teams_count ?? 0);

        const topTeam = teams
            .slice()
            .sort(
                (a, b) =>
                    toNumber(b.points ?? b.wins ?? b.rating ?? b.score ?? 0) -
                    toNumber(a.points ?? a.wins ?? a.rating ?? a.score ?? 0)
            )[0];

        const topTeamInfo = topTeam
            ? {
                  id: topTeam.team_id ?? topTeam.id ?? topTeam.slug ?? null,
                  name: topTeam.display_name || topTeam.team_name || topTeam.name || '',
                  logo: ensureAvatar(topTeam.logo || topTeam.avatar || topTeam.team_logo || topTeam.image),
                  record: {
                      wins: toNumber(topTeam.wins ?? topTeam.win_count ?? topTeam.matches_won ?? 0),
                      losses: toNumber(topTeam.losses ?? topTeam.loss_count ?? topTeam.matches_lost ?? 0)
                  },
                  rating: toNumber(topTeam.rating ?? topTeam.rating_2 ?? topTeam.hltv_rating ?? 0),
                  points: toNumber(topTeam.points ?? topTeam.score ?? 0)
              }
            : null;

        const routeParam = raw.championship_id ?? raw.id ?? raw.slug ?? raw.code ?? key;

        return {
            key,
            name,
            subtitle,
            phase,
            status: raw.status || raw.state || null,
            matchesPlayed,
            matchesTotal,
            progressPercent,
            wins,
            losses,
            draws,
            roundDiff,
            teamCount,
            topTeam: topTeamInfo,
            badge: ensureAvatar(raw.badge || raw.image || raw.logo),
            route: routeParam
                ? { name: 'division', params: { championshipId: String(routeParam) } }
                : null,
            raw
        };
    }

    function normalizeSummary(raw) {
        if (!raw || typeof raw !== 'object') {
            return { raw: null, aggregates: {} };
        }
        const aggregates =
            raw.aggregates ||
            raw.stats ||
            raw.summary ||
            raw.overview ||
            raw;
        return { raw, aggregates };
    }

    function defaultSeasonState() {
        return {
            loading: false,
            error: null,
            apiParam: null,
            fetchedAt: null,
            stats: {},
            rawStats: null,
            divisions: [],
            progress: defaultProgress()
        };
    }

    window.useHomeStore = defineStore('home', {
        state: () => ({
            lifetimeSummary: null,
            summaryLoading: false,
            summaryError: null,
            summaryFetchedAt: null,
            seasonCache: {}
        }),
        getters: {
            hasSummary(state) {
                return Boolean(state.lifetimeSummary && Object.keys(state.lifetimeSummary).length);
            },
            getSeasonState: state => key => state.seasonCache[key] || defaultSeasonState()
        },
        actions: {
            async fetchLifetimeSummary(options = {}) {
                const { force = false } = options;
                if (this.summaryLoading) {
                    return this.lifetimeSummary;
                }

                const isFresh =
                    this.summaryFetchedAt && Date.now() - this.summaryFetchedAt < 10 * 60 * 1000;
                if (!force && this.lifetimeSummary && isFresh) {
                    return this.lifetimeSummary;
                }

                this.summaryLoading = true;
                this.summaryError = null;

                try {
                    let summary = await window.apiClient.getHome();
                    if (!summary || typeof summary !== 'object') {
                        summary = await window.apiClient.getStatsOverview();
                    }
                    this.lifetimeSummary = normalizeSummary(summary);
                    this.summaryFetchedAt = Date.now();
                    return this.lifetimeSummary;
                } catch (error) {
                    if (error && error.status === 404) {
                        try {
                            const fallback = await window.apiClient.getStatsOverview();
                            this.lifetimeSummary = normalizeSummary(fallback);
                            this.summaryFetchedAt = Date.now();
                            return this.lifetimeSummary;
                        } catch (fallbackError) {
                            this.summaryError =
                                fallbackError?.message || 'Yleistilastojen lataus epäonnistui';
                            throw fallbackError;
                        }
                    } else {
                        this.summaryError =
                            error?.message || 'Yleistilastojen lataus epäonnistui';
                        throw error;
                    }
                } finally {
                    this.summaryLoading = false;
                }
            },
            async ensureSummary() {
                if (!this.hasSummary) {
                    try {
                        await this.fetchLifetimeSummary();
                    } catch (error) {
                        // Swallow here; caller may render error state via summaryError
                    }
                }
            },
            async fetchSeason(key, options = {}) {
                if (!key) {
                    return defaultSeasonState();
                }

                const { force = false, apiParam } = options;
                const existing = this.seasonCache[key];

                if (existing && existing.loading) {
                    return existing;
                }

                const isFresh =
                    existing && existing.fetchedAt && Date.now() - existing.fetchedAt < 2 * 60 * 1000;
                if (existing && !force && isFresh && !existing.error) {
                    return existing;
                }

                const identifier = apiParam ?? existing?.apiParam ?? key;

                this.seasonCache[key] = {
                    ...(existing || defaultSeasonState()),
                    loading: true,
                    error: null,
                    apiParam: identifier
                };

                try {
                    const [seasonStats, divisions] = await Promise.all([
                        window.apiClient.getSeasonStats(identifier),
                        window.apiClient.getDivisionsBySeason(identifier)
                    ]);

                    const normalizedDivisions = Array.isArray(divisions)
                        ? divisions.map((entry, index) => normalizeDivision(entry, index)).filter(Boolean)
                        : [];

                    const stats = seasonStats?.aggregates || seasonStats?.stats || seasonStats || {};
                    const progress = computeProgress(seasonStats, normalizedDivisions);

                    const payload = {
                        loading: false,
                        error: null,
                        apiParam: identifier,
                        fetchedAt: Date.now(),
                        stats,
                        rawStats: seasonStats,
                        divisions: normalizedDivisions,
                        progress
                    };

                    this.seasonCache[key] = payload;
                    return payload;
                } catch (error) {
                    this.seasonCache[key] = {
                        ...(this.seasonCache[key] || defaultSeasonState()),
                        loading: false,
                        error: error?.message || 'Kausitietojen lataus epäonnistui',
                        fetchedAt: Date.now(),
                        apiParam: identifier
                    };
                    throw error;
                }
            },
            clearSeasonCache(keys) {
                if (!keys) {
                    this.seasonCache = {};
                    return;
                }
                const list = Array.isArray(keys) ? keys : [keys];
                list.forEach(key => {
                    delete this.seasonCache[key];
                });
            }
        }
    });
})();
