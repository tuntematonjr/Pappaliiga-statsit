/**
 * The backend contract for these metrics is shared by:
 *   - StatsSummaryResponse (/api/stats/summary/season/:id) for the selected season row
 *   - StatsSummaryResponse (/api/stats/summary/all) for the global row
 * Cards resolve their values via the following field priorities:
 *   1. Divisioonia → summary_totals.divisions → divisions → total_divisions
 *   2. Joukkueita → summary_totals.teams → teams → total_teams
 *   3. Pelaajia → summary_totals.players → players → total_players
 *   4. Otteluja → summary_totals.matches (played) → matches_played → total_matches
 *   5. Karttoja → summary_totals.maps → maps → maps_played_total
 *   6. Kierroksia → summary_totals.rounds → rounds → total_rounds
 *   7. Tappoja → summary_totals.kills → kills → total_kills
 *   8. Kuolemia → summary_totals.deaths → deaths → total_deaths
 */
const SUMMARY_METRIC_SCHEMA = [
    {
        id: 'divisions',
        label: 'Divisioonia',
        digits: 0,
        key: [
            'summary_totals.divisions',
            'summaryTotals.divisions',
            'totals.divisions',
            'divisions',
            'total_divisions',
            'totalDivisions'
        ]
    },
    {
        id: 'teams',
        label: 'Joukkueita',
        digits: 0,
        key: [
            'summary_totals.teams',
            'summaryTotals.teams',
            'totals.teams',
            'teams',
            'total_teams',
            'team_count',
            'totalTeams'
        ]
    },
    {
        id: 'players',
        label: 'Pelaajia',
        digits: 0,
        key: [
            'summary_totals.players',
            'summaryTotals.players',
            'totals.players',
            'players',
            'total_players',
            'player_count',
            'totalPlayers'
        ]
    },
    {
        id: 'matches',
        label: 'Otteluja',
        digits: 0,
        key: [
            'summary_totals.matches',
            'summaryTotals.matches',
            'totals.matches',
            'matches',
            'matches_played_total',
            'matches_played',
            'matches_total',
            'total_matches',
            'matchesTotal',
            'matchesPlayedTotal'
        ]
    },
    {
        id: 'maps',
        label: 'Karttoja',
        digits: 0,
        key: [
            'summary_totals.maps',
            'summaryTotals.maps',
            'totals.maps',
            'maps',
            'maps_played_total',
            'total_maps_played',
            'mapsPlayedTotal',
            'totalMapsPlayed'
        ]
    },
    {
        id: 'rounds',
        label: 'Kierroksia',
        digits: 0,
        key: [
            'summary_totals.rounds',
            'summaryTotals.rounds',
            'totals.rounds',
            'rounds',
            'rounds_played_total',
            'rounds_played',
            'total_rounds',
            'roundsPlayedTotal',
            'totalRounds'
        ]
    },
    {
        id: 'kills',
        label: 'Tappoja',
        digits: 0,
        key: [
            'summary_totals.kills',
            'summaryTotals.kills',
            'totals.kills',
            'kills',
            'kills_total',
            'total_kills',
            'killsTotal',
            'totalKills'
        ]
    },
    {
        id: 'deaths',
        label: 'Kuolemia',
        digits: 0,
        key: [
            'summary_totals.deaths',
            'summaryTotals.deaths',
            'totals.deaths',
            'deaths',
            'deaths_total',
            'total_deaths',
            'deathsTotal',
            'totalDeaths'
        ]
    }
];

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

function formatMetric(value, schema) {
    if (value === undefined || value === null) {
        return '0';
    }
    let numeric = Number(value);
    if (!Number.isFinite(numeric)) {
        numeric = Number(String(value).replace(',', '.'));
    }
    if (!Number.isFinite(numeric)) {
        return value;
    }

    if (schema?.percent) {
        if (Math.abs(numeric) <= 1) {
            numeric = numeric * 100;
        }
        const decimals = schema?.digits ?? 1;
        return `${numeric.toFixed(decimals)} %`;
    }

    const decimals = schema?.digits ?? (numeric >= 100 ? 0 : 1);
    return new Intl.NumberFormat('fi-FI', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    }).format(numeric);
}

function buildMetricCards(source, schema, context) {
    if (!source || !schema) {
        return [];
    }
    return schema.map(definition => {
        const rawValue =
            typeof definition.getter === 'function'
                ? definition.getter(source, context)
                : pickValue(source, definition.key);
        return {
            key: definition.id,
            label: definition.label,
            value: formatMetric(rawValue, definition)
        };
    });
}

const DEFAULT_DIVISION_STATUS_LABELS = Object.freeze({
    'ei-alkanut': 'Ei alkanut',
    'runkosarja-kaynnissa': 'Runkosarja käynnissä',
    'playoffit-kaynnissa': 'Playoffit käynnissä',
    'taputeltu-loppuun': 'Taputeltu loppuun'
});

const DIVISION_STATUS_META =
    (typeof window !== 'undefined' && window.PAPPALIIGA_DIVISION_STATUS_META) || {};
const DIVISION_STATUS_ORDER = Array.isArray(
    typeof window !== 'undefined' && window.PAPPALIIGA_DIVISION_STATUS_ORDER
)
    ? window.PAPPALIIGA_DIVISION_STATUS_ORDER
    : ['ei-alkanut', 'runkosarja-kaynnissa', 'playoffit-kaynnissa', 'taputeltu-loppuun'];

function sortSeasonsDescending(seasons = []) {
    if (!Array.isArray(seasons)) {
        return [];
    }
    return [...seasons].sort((a, b) => {
        const aId = Number.isFinite(a?.id) ? a.id : Number.NEGATIVE_INFINITY;
        const bId = Number.isFinite(b?.id) ? b.id : Number.NEGATIVE_INFINITY;
        if (aId !== bId) {
            return bId - aId;
        }
        const aNum = Number.isFinite(a?.seasonNumber) ? a.seasonNumber : Number.NEGATIVE_INFINITY;
        const bNum = Number.isFinite(b?.seasonNumber) ? b.seasonNumber : Number.NEGATIVE_INFINITY;
        if (aNum !== bNum) {
            return bNum - aNum;
        }
        const aLabel = a?.label || '';
        const bLabel = b?.label || '';
        return aLabel.localeCompare(bLabel, 'fi');
    });
}

function emptySeasonState() {
    return {
        loading: false,
        error: null,
        stats: {},
        progress: {
            overall: { played: 0, total: 0, percent: 0 },
            regular: { played: 0, total: 0, percent: 0 },
            playoffs: { played: 0, total: 0, percent: 0 }
        },
        divisions: []
    };
}

const HOME_PARTNER_CALLOUTS = Object.freeze([
    {
        id: 'armafi',
        name: 'Armafinland',
        description: 'Yhteisö on avoin kaikille pelaajille ja ryhmille, jotka haluavat kokeilla taktista pelaamista myös Arma-sarjan peleissä. Pelaamme Arma 3 ja Arma Reforger, sekä järjestämme kansainvälisiä TvT-tehtäviä, joissa painotetaan realismia, joukkuepeliä ja yhteistoimintaa. Pelien ulkopuolella meno on rentoa ja mutkatonta, mutta pelissä otetaan tehtävät tosissaan. ',
        primaryLabel: 'Liity AFI Discord',
        primaryHref: 'https://www.armafinland.fi/discord',
        secondaryLabel: 'Lue lisää',
        secondaryHref: 'https://armafinland.fi',
        logo: 'https://armafinland.fi/logot/images/armafin-logo-400px.png'
    },
    {
        id: 'sosso-bot',
        name: 'Sössö The PappaCS bot',
        description: 'Sössö on Discord-botti, joka tuo Pappaliiga-statistiikkaa suoraan Discord-palvelimillesi. Saat reaaliaikaisia tilastoja, ottelutuloksia ja pelaajatietoja kätevästi chatissa.',
        primaryLabel: 'Lue lisää ja kutsu Sössö bot palvelimellesi',
        primaryHref: 'https://cultti.github.io/Sosso-Bot/',
        logo: '/static/sosso-bot-logo.png'
    },
    {
        id: 'mobbi-cs',
        name: 'Mobbi CS',
        description: 'Pappaliigan tilastot: menneet ja tulevat kaudet, joukkueiden ja pelaajien kausikohtaiset statistiikat yhdessä paikassa. Puhdasta raakaa dataa ilman clutteria. Helposti filtteröitävissä olevat pelaaja statsit.',
        primaryLabel: 'Katso dataa Mobbi CS:ssä',
        primaryHref: 'https://cs.mobbi.dev/',
        logo: 'https://cs.mobbi.dev/assets/img/og_image.png'
    },
    {
        id: 'pappaliiga',
        name: 'Pappaliiga',
        description: 'Pappaliigan tarkoituksena on tarjota varttuneemmalle väelle mahdollisuus kilpapelaamiseen; tosissaan ja `ei niin tosissaan`. ',
        primaryLabel: 'Liity Pappaliiga Discord',
        primaryHref: 'https://discord.gg/pappaliiga',
        secondaryLabel: 'Lue lisää',
        secondaryHref: 'https://pappaliiga.fi',
        logo: 'https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png'
    }
]);

window.HomeView = {
    name: 'HomeView',
    components: {
        get LoadingSpinner() {
            return window.LoadingSpinner;
        },
        get ErrorMessage() {
            return window.ErrorMessage;
        },
        get HeroBanner() {
            return window.HeroBanner;
        },
        get StatPanel() {
            return window.StatPanel;
        },
        get SeasonToggle() {
            return window.SeasonToggle;
        },
        get ProgressBar() {
            return window.ProgressBar;
        },
        get DivisionCardList() {
            return window.DivisionCardList;
        },
        get CircularProgress() {
            return window.CircularProgress;
        },
        get SummaryStatCard() {
            return window.SummaryStatCard;
        }
    },
    data() {
        const seasonsStore = typeof window.useSeasonsStore === 'function' ? window.useSeasonsStore() : null;
        const homeStore = typeof window.useHomeStore === 'function' ? window.useHomeStore() : null;
        return {
            seasonsStore,
            homeStore,
            divisionFilter: 'all',
            divisionSearch: '',
            seasonTeamCount: null,
            seasonTeamCountKey: null,
            globalTeamCount: null,
            seasonLoadPromises: {}
        };
    },
    computed: {
        heroTitle() {
            return 'AFI - Unofficial Pappaliiga CS Statsit';
        },
        partnerCallouts() {
            return HOME_PARTNER_CALLOUTS;
        },
        summaryLoading() {
            return this.homeStore?.summaryLoading ?? false;
        },
        summaryError() {
            return this.homeStore?.summaryError ?? null;
        },
        globalSummaryMetrics() {
            const aggregates = this.homeStore?.lifetimeSummary?.aggregates || {};
            const metrics = buildMetricCards(aggregates, SUMMARY_METRIC_SCHEMA);
            if (Number.isFinite(this.globalTeamCount) && this.globalTeamCount > 0) {
                const target = metrics.find(metric => metric.key === 'teams');
                if (target) {
                    target.value = formatMetric(this.globalTeamCount, { digits: 0 });
                }
            }
            return metrics;
        },

       seasonsLoading() {
            return this.seasonsStore?.loading ?? false;
        },
        seasonsError() {
            return this.seasonsStore?.error ?? null;
        },
        sortedSeasons() {
            const source =
                (Array.isArray(this.seasonsStore?.sortedSeasons) && this.seasonsStore.sortedSeasons.length
                    ? this.seasonsStore.sortedSeasons
                    : this.seasonsStore?.seasons) || [];
            return sortSeasonsDescending(source);
        },
        seasonSelectGroups() {
            if (!this.sortedSeasons.length) {
                return [];
            }
            return [
                {
                    id: 'all',
                    label: 'Kaikki kaudet',
                    options: this.sortedSeasons
                }
            ];
        },
        selectedSeasonKey() {
            return this.seasonsStore?.selectedSeasonKey ?? null;
        },
        selectedSeason() {
            if (!this.selectedSeasonKey || !this.seasonsStore) {
                return null;
            }
            return this.seasonsStore.getSeasonByKey(this.selectedSeasonKey);
        },
        currentSeason() {
            if (!this.seasonsStore) return null;
            return (
                this.seasonsStore.currentSeason ||
                this.seasonsStore.seasons?.find(season => season?.isActive) ||
                this.seasonsStore.newestSeason ||
                this.sortedSeasons?.[0] ||
                null
            );
        },
        currentSeasonLabel() {
            if (!this.currentSeason) return 'Tulevat ottelut';
            return this.currentSeason.label || this.currentSeason.shortLabel || `Kausi ${this.currentSeason.seasonNumber ?? this.currentSeason.id ?? ''}`.trim();
        },
        seasonState() {
            if (!this.selectedSeasonKey || !this.homeStore) {
                return emptySeasonState();
            }
            const getter = this.homeStore.getSeasonState;
            if (typeof getter === 'function') {
                return getter(this.selectedSeasonKey) || emptySeasonState();
            }
            return this.homeStore.seasonCache?.[this.selectedSeasonKey] || emptySeasonState();
        },
        seasonLoading() {
            return this.seasonState.loading;
        },
        seasonError() {
            return this.seasonState.error;
        },
        seasonSummaryMetrics() {
            const stats = this.seasonState.stats || {};
            const metrics = buildMetricCards(stats, SUMMARY_METRIC_SCHEMA);
            if (Number.isFinite(this.seasonTeamCount) && this.seasonTeamCount > 0) {
                const target = metrics.find(metric => metric.key === 'teams');
                if (target) {
                    target.value = formatMetric(this.seasonTeamCount, { digits: 0 });
                }
            }
            return metrics;
        },
        seasonDivisions() {
            return Array.isArray(this.seasonState.divisions) ? this.seasonState.divisions : [];
        },
        seasonTitle() {
            const season = this.selectedSeason;
            if (!season) {
                return 'Valitse kausi';
            }
            return season.label || `Kausi ${season.seasonNumber ?? ''}`.trim();
        },
        seasonSummaryHeading() {
            const season = this.selectedSeason;
            if (!season) {
                return 'Kausikohtaiset luvut';
            }
            if (season.seasonNumber) {
                return `Season ${season.seasonNumber} Yleiskatsaus`;
            }
            return `${season.label || 'Kausi'} Yleiskatsaus`;
        },
        seasonSummaryMeta() {
            if (!this.selectedSeason) {
                return 'Valitse kausi nähdäksesi kausikohtaiset luvut.';
            }
            const stats = this.seasonState.stats || {};
            const teamSource = Number.isFinite(this.seasonTeamCount) && this.seasonTeamCount > 0
                ? this.seasonTeamCount
                : pickValue(stats, ['aggregates.total_teams', 'team_count', 'teams']);
            const teams = formatMetric(teamSource, { digits: 0 });
            const players = formatMetric(
                pickValue(stats, ['aggregates.total_players', 'player_count', 'players']),
                { digits: 0 }
            );
            return `Teams: ${teams} · Players: ${players}`;
        },
        seasonProgressSummary() {
            const statsPercent = toNumber(
                pickValue(this.seasonState?.stats, ['progress.finished_percent', 'finished_percent']),
                null
            );
            const overall = this.seasonState?.progress?.overall || {};
            const played = toNumber(overall.played, 0);
            const total = toNumber(overall.total, 0);
            const computedPercent = Number.isFinite(overall.percent)
                ? Math.round(overall.percent)
                : total > 0
                    ? Math.min(100, Math.round((played / total) * 100))
                    : 0;
            return {
                played,
                total,
                percent:
                    statsPercent != null && statsPercent > 0
                        ? Math.round(statsPercent)
                        : computedPercent
            };
        },
        seasonProgressLabel() {
            const { played, total, percent } = this.seasonProgressSummary;
            if (!total) {
                return played > 0 ? `Matches played: ${played}` : '';
            }
            return `Matches: ${played}/${total} · ${percent}%`;
        },
        hasSeasonProgress() {
            const summary = this.seasonProgressSummary;
            return summary.total > 0 || summary.played > 0;
        },
        circularProgressData() {
            const progress = this.seasonState?.progress || {};
            const sections = [
                { key: 'regular', progressKey: 'regular', label: 'Runkosarja', color: 'regular' },
                { key: 'playoff', progressKey: 'playoffs', label: 'Playoffit', color: 'playoff' },
                { key: 'overall', progressKey: 'overall', label: 'Kausi yhteensä', color: 'overall' }
            ];
            const payload = {};
            sections.forEach(section => {
                const block = progress[section.progressKey] || {};
                const played = toNumber(block.played, 0);
                const total = toNumber(block.total, 0);
                const percent = Number.isFinite(block.percent)
                    ? Math.round(block.percent)
                    : total > 0
                        ? Math.min(100, Math.round((played / total) * 100))
                        : 0;
                payload[section.key] = {
                    played,
                    total,
                    label: section.label,
                    sublabel:
                        total > 0
                            ? `${played} / ${total} Ottelut`
                            : played > 0
                                ? `${played} ottelua`
                                : 'Ei otteluita',
                    color: section.color,
                    percent,
                    source: block.source || 'unknown'
                };
            });

            return payload;
        },
        hasCircularProgressData() {
            const payload = this.circularProgressData;
            const keys = ['regular', 'playoff', 'overall'];
            return keys.some(key => {
                const block = payload[key];
                if (!block) return false;
                return block.total > 0 || block.played > 0;
            });
        },
        divisionCount() {
            const stats = this.seasonState?.stats || {};
            const summaryCount = toNumber(
                pickValue(stats, [
                    'progress.divisions_total',
                    'aggregates.divisions_total',
                    'divisions_total'
                ]),
                0
            );
            if (summaryCount > 0) {
                return summaryCount;
            }
            return this.seasonDivisions.length;
        },
        divisionProgressPercent() {
            const percent = this.seasonState?.progress?.overall?.percent ?? 0;
            return Number.isFinite(percent) ? percent : 0;
        },
        divisionOfflineMessage() {
            if (this.seasonState.bannerMessage) {
                return this.seasonState.bannerMessage;
            }
            if (!this.seasonState.offline) return '';
            const timestamp = this.seasonState.cacheTimestamp;
            let formatted = 'unknown time';
            if (timestamp) {
                try {
                    formatted = new Date(timestamp).toLocaleString('fi-FI', {
                        hour: '2-digit',
                        minute: '2-digit',
                        day: 'numeric',
                        month: 'numeric',
                        year: 'numeric'
                    });
                } catch (error) {
                    formatted = new Date(timestamp).toISOString();
                }
            }
            return `Offline: showing cached data (${formatted}). Some values may be outdated.`;
        },
        divisionDataBadge() {
            return this.seasonState.dataBadge || '';
        },
        divisionWarningMessage() {
            const warnings = this.seasonState.validationWarnings || [];
            if (!warnings.length) return '';
            return 'Some divisions could not be loaded (validation error).';
        },
        divisionHeaderMeta() {
            if (!this.selectedSeason) return '';
            const percent = this.divisionProgressPercent.toFixed(0);
            return `${this.divisionCount} divisioonaa · ${percent}% Ottelut`;
        },
        divisionEmptyMessage() {
            if (!this.selectedSeasonKey) {
                return 'Valitse kausi tarkasteltavaksi.';
            }
            if (this.divisionFilter !== 'all' || this.divisionSearch.trim().length > 0) {
                return 'Ei divisioonia valituilla suodattimilla.';
            }
            return 'Tälle kaudelle ei löytynyt divisioonia.';
        },
        divisionFilterOptions() {
            const options = [{ id: 'all', label: 'Kaikki', icon: null }];
            DIVISION_STATUS_ORDER.forEach(state => {
                const meta = DIVISION_STATUS_META[state];
                options.push({
                    id: state,
                    label: meta?.label || DEFAULT_DIVISION_STATUS_LABELS[state] || state,
                    icon: meta?.icon || null
                });
            });
            return options;
        }
    },
    async mounted() {
        await this.bootstrap();
    },
    watch: {
        '$route.params.seasonId': {
            handler(newValue, oldValue) {
                if (newValue === oldValue) return;
                if (!this.sortedSeasons.length || !this.seasonsStore) return;
                const season = this.syncSeasonFromRouteParam(newValue, { fallbackToNewest: true, replaceRoute: true });
                if (season) {
                    if (season.key !== this.selectedSeasonKey) {
                        this.seasonsStore.selectSeason(season.key);
                    }
                    this.loadSeason(season.key, { apiParam: season.apiParam });
                }
            }
        }
    },
    methods: {
        async bootstrap() {
            const tasks = [];
            if (this.homeStore) {
                tasks.push(
                    this.homeStore
                        .ensureSummary()
                        .catch(error => {
                            console.warn('[HomeView] ensureSummary failed', error);
                        })
                );
            }
            if (this.seasonsStore) {
                tasks.push(
                    this.seasonsStore
                        .fetchSeasons()
                        .then(() => {
                            const season = this.initializeSeasonSelection();
                            if (season) {
                                this.loadSeason(season.key, { apiParam: season.apiParam });
                            }
                        })
                        .catch(error => {
                            console.error('[HomeView] fetchSeasons failed', error);
                        })
                );
            }
            await Promise.allSettled(tasks);
            if (!this.selectedSeasonKey && this.sortedSeasons.length) {
                const season = this.initializeSeasonSelection();
                if (season) {
                    this.loadSeason(season.key, { apiParam: season.apiParam });
                }
            }
        },
        initializeSeasonSelection() {
            if (!this.sortedSeasons.length || !this.seasonsStore) {
                return null;
            }
            const routeSeason = this.$route?.params?.seasonId;
            if (routeSeason != null) {
                const resolved = this.syncSeasonFromRouteParam(routeSeason, {
                    fallbackToNewest: true,
                    replaceRoute: true
                });
                if (resolved) {
                    if (resolved.key !== this.selectedSeasonKey) {
                        this.seasonsStore.selectSeason(resolved.key);
                    }
                    return resolved;
                }
            }

            const existing = this.findSeasonRecord(this.selectedSeasonKey);
            if (existing) {
                this.syncRouteWithSelectedSeason({ replace: true });
                return existing;
            }

            const fallback = this.sortedSeasons[0];
            if (fallback) {
                this.seasonsStore.selectSeason(fallback.key);
                this.syncRouteWithSelectedSeason({ replace: true });
                return fallback;
            }
            return null;
        },
        matchSeasonByParam(value) {
            if (value === undefined || value === null) {
                return null;
            }
            const target = String(value);
            const numeric = Number(value);
            return (
                this.sortedSeasons.find(season => {
                    if (!season) return false;
                    if (String(season.key) === target) return true;
                    if (season.apiParam != null && String(season.apiParam) === target) return true;
                    if (Number.isFinite(numeric)) {
                        if (Number.isFinite(season.id) && season.id === numeric) return true;
                        if (Number.isFinite(season.seasonNumber) && season.seasonNumber === numeric) return true;
                    }
                    return false;
                }) || null
            );
        },
        findSeasonRecord(identifier) {
            if (identifier && typeof identifier === 'object') {
                return identifier;
            }
            return this.matchSeasonByParam(identifier);
        },
        syncSeasonFromRouteParam(param, options = {}) {
            const matched = this.matchSeasonByParam(param);
            let targetSeason = matched;
            if (!targetSeason && options.fallbackToNewest) {
                targetSeason = this.sortedSeasons[0] || null;
            }
            if (!matched && targetSeason && options.replaceRoute) {
                this.syncRouteWithSelectedSeason({ replace: true });
            }
            return targetSeason;
        },
        syncRouteWithSelectedSeason(options = {}) {
            if (!this.$router || !this.selectedSeason) return;
            const season = this.selectedSeason;
            const targetId = season.id ?? season.seasonNumber ?? season.key;
            const normalized = targetId != null ? String(targetId) : null;
            const current = this.$route?.params?.seasonId ?? null;
            if (normalized === (current != null ? String(current) : null)) return;
            const method = options.replace ? 'replace' : 'push';
            const nextRoute = normalized
                ? { name: 'home-season', params: { seasonId: normalized }, hash: this.$route?.hash || undefined }
                : { name: 'home', hash: this.$route?.hash || undefined };
            this.$router[method](nextRoute).catch(() => {});
        },
        handleSeasonSelect(value) {
            const season = this.findSeasonRecord(value);
            if (!season) {
                return;
            }

            // If same season selected, just scroll to it (user wants to see it again)
            if (season.key === this.selectedSeasonKey) {
                this.scrollToSeasonSummary();
                return;
            }

            // Update store selection (this triggers data load in bootstrap if needed)
            this.seasonsStore?.selectSeason(season.key);

            // Load season data immediately without waiting for watchers
            this.loadSeason(season.key, { apiParam: season?.apiParam });
            this.syncRouteWithSelectedSeason({ replace: true });

            // Scroll to season summary section
            this.scrollToSeasonSummary();
        },
        async loadSeason(key, options = {}) {
            if (!key || !this.homeStore) {
                return;
            }
            const loadKey = String(key);
            const inFlight = this.seasonLoadPromises?.[loadKey];
            if (inFlight) {
                return inFlight;
            }

            const season = this.seasonsStore?.getSeasonByKey(key);
            const apiParam = options.apiParam ?? season?.apiParam ?? key;
            const request = (async () => {
                try {
                    const payload = await this.homeStore.fetchSeason(key, {
                        apiParam,
                        force: options.force === true
                    });
                    return payload;
                } catch (error) {
                    console.error('Season fetch failed', error);
                    return null;
                } finally {
                    const current = this.seasonLoadPromises?.[loadKey];
                    if (current === request) {
                        delete this.seasonLoadPromises[loadKey];
                    }
                }
            })();

            this.seasonLoadPromises = {
                ...(this.seasonLoadPromises || {}),
                [loadKey]: request
            };
            return request;
        },
        async loadSeasonTeamCount(seasonId, divisions) {
            if (!seasonId || typeof window === 'undefined' || !window.apiClient?.getSeasonTeamCount) {
                this.seasonTeamCount = null;
                this.seasonTeamCountKey = seasonId ? String(seasonId) : null;
                return;
            }
            const key = String(seasonId);
            if (this.seasonTeamCountKey === key && Number.isFinite(this.seasonTeamCount) && this.seasonTeamCount > 0) {
                return;
            }
            try {
                const divisionsArg = Array.isArray(divisions) && divisions.length ? divisions : undefined;
                const count = await window.apiClient.getSeasonTeamCount(key, { divisions: divisionsArg });
                this.seasonTeamCount = Number.isFinite(count) ? count : null;
                this.seasonTeamCountKey = key;
            } catch (error) {
                console.warn('[HomeView] Season team count fetch failed', error);
            }
        },
        async loadGlobalTeamCount() {
            if (typeof window === 'undefined' || !window.apiClient?.getLifetimeUniqueTeamCount) {
                return;
            }
            if (Number.isFinite(this.globalTeamCount) && this.globalTeamCount > 0) {
                return;
            }
            try {
                const seasons = Array.isArray(this.seasonsStore?.seasons) && this.seasonsStore.seasons.length
                    ? this.seasonsStore.seasons
                    : undefined;
                const count = await window.apiClient.getLifetimeUniqueTeamCount({ seasons });
                this.globalTeamCount = Number.isFinite(count) ? count : null;
            } catch (error) {
                console.warn('[HomeView] Lifetime team count fetch failed', error);
            }
        },
        retrySeasons() {
            if (!this.seasonsStore) return;
            this.seasonsStore
                .fetchSeasons({ force: true })
                .then(() => {
                    this.initializeSeasonSelection();
                })
                .catch(error => {
                    console.error('Season list refresh failed', error);
                });
        },
        retrySummary() {
            if (!this.homeStore) return;
            this.homeStore.fetchLifetimeSummary({ force: true }).catch(error => {
                console.error('Summary refresh failed', error);
            });
        },
        retrySeason() {
            if (!this.selectedSeasonKey) return;
            const season = this.selectedSeason;
            this.loadSeason(this.selectedSeasonKey, {
                apiParam: season?.apiParam,
                force: true
            });
            this.seasonTeamCount = null;
            this.seasonTeamCountKey = null;
        },
        getMetricIcon(key) {
            const icons = {
                divisions: '🏆',
                teams: '👥',
                players: '👤',
                matches: '⚔️',
                maps: '🗺️',
                rounds: '🔄',
                kills: '💀',
                deaths: '☠️'
            };
            return icons[key] || '📊';
        },
        scrollToSeasonSummary() {
            this.$nextTick(() => {
                const target = this.$refs.seasonControls;
                if (!target) return;

                try {
                    const rect = target.getBoundingClientRect();
                    const scrollY = window.scrollY || window.pageYOffset || document.documentElement.scrollTop || 0;
                    const vh = window.innerHeight || document.documentElement.clientHeight || 0;

                    // Check if target is already well-positioned in viewport (with generous threshold)
                    // Target is visible if it's within top 30% of viewport
                    const visibleThreshold = vh * 0.3;
                    const isWellPositioned = rect.top >= 0 && rect.top <= visibleThreshold;

                    if (isWellPositioned) return;

                    // Scroll to position target near top of viewport with some padding
                    const targetY = scrollY + rect.top - 80; // 80px padding from top
                    window.scrollTo({
                        top: Math.max(0, targetY),
                        behavior: 'smooth'
                    });
                } catch (error) {
                    console.warn('[HomeView] scrollToSeasonSummary failed', error);
                }
            });
        },
        setDivisionFilter(filter) {
            this.divisionFilter = filter;
        },
        resetDivisionFilters() {
            this.divisionFilter = 'all';
            this.divisionSearch = '';
        }
    },
    template: `
        <div class="home-view">
            <hero-banner
                :title="heroTitle"
                align="center"
            ></hero-banner>

            <section class="home-partners" aria-label="Kumppanikuvaukset">
                <article
                    v-for="(callout, idx) in partnerCallouts"
                    :key="callout.id"
                    class="partner-callout"
                >
                    <header class="partner-callout__header">
                        <div
                            class="logo-wrap logo-card partner-callout__logo-wrap"
                            :class="{
                                'logo-card--armafinland': callout.id === 'armafi',
                                'logo-card--sosso-bot': callout.id === 'sosso-bot',
                                'logo-card--mobbi-cs': callout.id === 'mobbi-cs',
                                'logo-card--pappaliiga': callout.id === 'pappaliiga'
                            }"
                        >
                            <img
                                class="partner-callout__logo"
                                :src="callout.logo"
                                :alt="callout.name + ' logo'"
                                loading="lazy"
                            >
                        </div>
                        <div class="partner-callout__titles">
                            <span class="partner-callout__eyebrow">{{ callout.eyebrow }}</span>
                            <h2 class="title-accent titleUnderlineCard">{{ callout.name }}</h2>
                        </div>
                    </header>
                    <p class="partner-callout__body">{{ callout.description }}</p>
                    <footer
                        class="partner-callout__footer"
                        :class="{ 'partner-callout__footer--single': !(callout.secondaryHref && callout.secondaryLabel) }"
                    >
                        <a
                            :href="callout.primaryHref"
                            class="btn-primary partner-callout__action partner-callout__action--primary"
                            target="_blank"
                            rel="noopener"
                        >
                            {{ callout.primaryLabel }}
                        </a>
                        <a
                            v-if="callout.secondaryHref && callout.secondaryLabel"
                            :href="callout.secondaryHref"
                            class="btn-secondary partner-callout__action partner-callout__action--secondary"
                            target="_blank"
                            rel="noopener"
                        >
                            {{ callout.secondaryLabel }}
                        </a>
                    </footer>
                </article>
            </section>

            <section class="stats-section stats-section--global" aria-labelledby="global-summary-heading">
                <header class="section-heading section-heading--centered">
                    <h2
                        id="global-summary-heading"
                        class="title-accent titleUnderlineSection"
                    >
                        Kaikki kaudet yhteensä
                    </h2>
                </header>
                <loading-spinner
                    v-if="summaryLoading"
                    message="Kokonaisstatistiikkaa ladataan..."
                ></loading-spinner>
                <error-message
                    v-else-if="summaryError"
                    :message="summaryError"
                    @retry="retrySummary"
                ></error-message>
                <div
                    v-else
                    class="summary-card-grid summary-card-grid--lifetime"
                    role="list"
                >
                    <summary-stat-card
                        v-for="metric in globalSummaryMetrics"
                        :key="'global-' + metric.key"
                        :icon="getMetricIcon(metric.key)"
                        :label="metric.label"
                        :value="metric.value"
                    ></summary-stat-card>
                </div>
            </section>

            <section
                class="season-explorer glass-card"
                ref="seasonControls"
                aria-labelledby="season-explorer-heading"
            >
                <header class="season-explorer__intro section-heading section-heading--centered">
                    <div>
                        <h2
                            id="season-explorer-heading"
                            class="title-accent titleUnderlineSection"
                        >
                            Kausiselain
                        </h2>
                    </div>
                </header>

                <div class="season-explorer__section season-explorer__selection">
                    <div class="season-explorer__selector">
                        <season-toggle
                            :seasons="sortedSeasons"
                            :model-value="selectedSeasonKey"
                            :loading="seasonsLoading"
                            :error="seasonsError"
                            :show-heading="false"
                            :flat="true"
                            @update:modelValue="handleSeasonSelect"
                            @retry="retrySeasons"
                        ></season-toggle>
                    </div>

                    <div
                        class="season-explorer__summary"
                        aria-live="polite"
                        aria-atomic="true"
                    >
                        <div
                            v-if="seasonLoading"
                            class="season-skeleton"
                            role="status"
                        >
                            <div class="season-skeleton__header"></div>
                            <div class="season-skeleton__grid">
                                <div v-for="n in 5" :key="'skeleton-block-' + n" class="season-skeleton__card"></div>
                            </div>
                        </div>

                        <error-message
                            v-else-if="seasonError"
                            :message="seasonError"
                            @retry="retrySeason"
                        ></error-message>

                        <template v-else-if="selectedSeasonKey">
                            <div class="season-explorer__summary-header">
                                <div>
                                    <h3
                                        class="title-accent titleUnderlineSection title-duration-slow"
                                        id="season-summary-heading"
                                        aria-live="polite"
                                        aria-atomic="true"
                                    >
                                        {{ seasonSummaryHeading }}
                                    </h3>
                                </div>
                            </div>
                            <div class="summary-card-grid" role="list">
                                <summary-stat-card
                                    v-for="metric in seasonSummaryMetrics"
                                    :key="'season-' + metric.key"
                                    :icon="getMetricIcon(metric.key)"
                                    :label="metric.label"
                                    :value="metric.value"
                                ></summary-stat-card>
                            </div>
                        </template>
                        <div
                            v-else
                            class="season-empty-state"
                            role="status"
                            aria-live="polite"
                        >
                            <p>Valitse kausi yläpuolisesta selectorista tai odota, että kausitiedot latautuvat.</p>
                            <button class="btn-primary" type="button" @click="retrySeason">Yritä uudelleen</button>
                        </div>
                    </div>
                </div>

                <div
                    class="season-explorer__section season-explorer__control-bar"
                    :class="{ 'season-explorer__control-bar--disabled': !selectedSeasonKey }"
                >
                    <div class="control-bar__left">
                        <div class="control-bar__filters">
                            <button
                                v-for="option in divisionFilterOptions"
                                :key="option.id"
                                type="button"
                                class="season-filter-chip"
                                :class="{ 'season-filter-chip--active': divisionFilter === option.id }"
                                :data-status="option.id !== 'all' ? option.id : null"
                                :aria-pressed="divisionFilter === option.id"
                                @click="setDivisionFilter(option.id)"
                            >
                                <span
                                    v-if="option.icon"
                                    class="season-filter-chip__icon"
                                    aria-hidden="true"
                                >
                                    <svg
                                        :viewBox="option.icon.viewBox"
                                        role="presentation"
                                        focusable="false"
                                    >
                                        <path
                                            v-for="(path, idx) in option.icon.paths"
                                            :key="idx"
                                            :d="path.d"
                                            fill="currentColor"
                                        ></path>
                                    </svg>
                                </span>
                                <span class="season-filter-chip__label">{{ option.label }}</span>
                            </button>
                            <button
                                type="button"
                                class="season-filter-reset"
                                :disabled="divisionFilter === 'all' && !divisionSearch"
                                @click="resetDivisionFilters"
                            >
                                Nollaa
                            </button>
                        </div>
                        <div class="control-bar__search">
                            <input
                                type="search"
                                class="control-bar__input"
                                placeholder="Hae divisioonaa..."
                                :value="divisionSearch"
                                @input="divisionSearch = $event.target.value"
                                aria-label="Hae divisioonaa"
                            >
                        </div>
                    </div>
                    <div
                        v-if="hasCircularProgressData"
                        class="control-bar__right"
                    >
                        <div class="progress-circle-card">
                            <circular-progress
                                :value="circularProgressData.regular.played"
                                :max="circularProgressData.regular.total"
                                :label="circularProgressData.regular.label"
                                :sublabel="circularProgressData.regular.sublabel"
                                :color="circularProgressData.regular.color"
                                :size="140"
                                :stroke-width="12"
                                :animation-delay="0"
                            ></circular-progress>
                        </div>
                        <div class="progress-circle-card">
                            <circular-progress
                                :value="circularProgressData.playoff.played"
                                :max="circularProgressData.playoff.total"
                                :label="circularProgressData.playoff.label"
                                :sublabel="circularProgressData.playoff.sublabel"
                                :color="circularProgressData.playoff.color"
                                :size="140"
                                :stroke-width="12"
                                :animation-delay="0.15"
                            ></circular-progress>
                        </div>
                        <div class="progress-circle-card">
                            <circular-progress
                                :value="circularProgressData.overall.played"
                                :max="circularProgressData.overall.total"
                                :label="circularProgressData.overall.label"
                                :sublabel="circularProgressData.overall.sublabel"
                                :color="circularProgressData.overall.color"
                                :size="140"
                                :stroke-width="12"
                                :animation-delay="0.3"
                            ></circular-progress>
                        </div>
                    </div>
                </div>

                <div class="season-explorer__section season-explorer__divisions">
                    <division-card-list
                        class="season-explorer__divisions-list"
                        :divisions="seasonDivisions"
                        :season-label="seasonTitle"
                        :season-options="seasonSelectGroups"
                        :season-loading="seasonsLoading"
                        :selected-season="selectedSeasonKey"
                        :offline-message="divisionOfflineMessage"
                        :data-badge="divisionDataBadge"
                        :warning-message="divisionWarningMessage"
                        :is-loading="seasonLoading"
                        :empty-message="divisionEmptyMessage"
                        :filter-state="divisionFilter"
                        :search-query="divisionSearch"
                        :show-season-picker="false"
                        :show-controls="false"
                        :group-by-division="true"
                        :collapsible-groups="true"
                        @change-season="handleSeasonSelect"
                        @change-filter="setDivisionFilter"
                        @change-search="divisionSearch = $event"
                        @reset-filters="resetDivisionFilters"
                    ></division-card-list>
                </div>
            </section>
        </div>
    `
};
