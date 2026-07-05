const DEFAULT_ELO_CONFIG = Object.freeze({
    initial_elo: 1000.0,
    base_k_factor: 24.0,
    min_elo_delta: -45.0,
    max_elo_delta: 45.0,
    outcome_weights: {
        win_bonus: 0.15,
        loss_penalty: -0.15,
        draw_bonus: 0.0,
    },
    stat_weights: {
        kd: 0.28,
        kr: 0.22,
        adr: 0.20,
        mvps_per_map: 0.08,
        entry_success_rate: 0.07,
        clutch_success_rate: 0.07,
        utility_per_round: 0.04,
        flash_success_rate: 0.04,
    },
    stat_baselines: {
        kd: 1.0,
        kr: 0.70,
        adr: 80.0,
        mvps_per_map: 0.8,
        entry_success_rate: 0.55,
        clutch_success_rate: 0.30,
        utility_per_round: 8.0,
        flash_success_rate: 0.50,
    },
    dynamic_division_elo: {
        fallback_multiplier: 1.0,
        sensitivity: 0.14,
        rank_reference_division: 10.0,
        rank_step: 0.03,
        rank_blend: 0.55,
        min_multiplier: 0.72,
        max_multiplier: 1.45,
        min_samples_per_division: 20.0,
        shrink_to_mean_samples: 50.0,
    },
    dynamic_k_factor: {
        start_multiplier: 2.6,
        stabilize_after_maps: 10.0,
        post_stabilize_multiplier: 1.0,
        min_multiplier: 0.58,
        decay_rate: 0.06,
    },
    initial_elo_bootstrap: {
        rank_reference_division: 10.0,
        rank_step_points: 16.0,
        min_initial_elo: 850.0,
        max_initial_elo: 1150.0,
    },
    formulas: {
        stat_score: 'sum(weight_i * centered(metric_i, baseline_i))',
        centered: 'centered = clamp((metric / baseline) - 1, -1.5, 1.5)',
        k_dynamic: 'K_dynamic(maps) = BASE_K_FACTOR * phase(first_10_maps_high, then_stabilize_to_min)',
        division_multiplier: 'blend(dynamic_season_gap_multiplier, rank_prior_multiplier, rank_blend)',
        new_player_initial_elo: 'elo_before = avg(existing_elos_in_same_season_division) or clamp(1000 + (rank_reference_division-division_num)*rank_step_points)',
        elo_delta: 'delta = clamp(K_dynamic * division_multiplier * (stat_score + outcome_score), MIN_ELO_DELTA, MAX_ELO_DELTA)',
        elo_update: 'elo_after = max(0, elo_before + delta)',
    },
});

const METRIC_LABELS = Object.freeze({
    kd: 'K/D',
    kr: 'Kills per round (K/R)',
    adr: 'Average damage per round (ADR)',
    mvps_per_map: 'MVP:t per kartta',
    entry_success_rate: 'Entry onnistumisprosentti',
    clutch_success_rate: 'Clutch onnistumisprosentti',
    utility_per_round: 'Utility damage per round',
    flash_success_rate: 'Flash onnistumisprosentti',
});

window.LeaderboardView = {
    name: 'LeaderboardView',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; },
        get SortableTable() { return window.SortableTable; }
    },
    data() {
        return {
            loading: true,
            error: null,
            seasons: [],
            selectedSeasonId: '',
            players: [],
            eloConfig: {},
            leaderboardCache: {},
            search: '',
            loadToken: 0
        };
    },
    computed: {
        tableColumns() {
            return [
                { key: 'rank', label: '#', sortable: true, numeric: true, width: '70px', align: 'center' },
                { key: 'nickname', label: 'Pelaaja', sortable: true, width: 'minmax(220px, 1.8fr)', align: 'left', colClass: 'col-name' },
                { key: 'last_team_name', label: 'Joukkue', sortable: true, width: 'minmax(180px, 1.4fr)', align: 'left' },
                { key: 'current_elo', label: 'Elo', sortable: true, numeric: true, width: '110px', align: 'center' },
                { key: 'last_division_multiplier', label: 'Div kerroin', sortable: true, numeric: true, width: '120px', align: 'center' },
                { key: 'last_elo_delta', label: 'Viimeisin', sortable: true, numeric: true, width: '110px', align: 'center' },
                { key: 'matches_processed', label: 'Ottelut', sortable: true, numeric: true, width: '100px', align: 'center' },
                { key: 'last_division_num', label: 'Div', sortable: true, numeric: true, width: '90px', align: 'center' },
                { key: 'last_season', label: 'Kausi', sortable: true, numeric: true, width: '90px', align: 'center' },
            ];
        },
        selectedSeasonLabel() {
            if (!this.selectedSeasonId) return 'Kaikki kaudet';
            const row = this.seasons.find(season => String(this.getSeasonId(season)) === String(this.selectedSeasonId));
            if (!row) return `Kausi ${this.selectedSeasonId}`;
            return this.getSeasonLabel(row);
        },
        filteredPlayers() {
            const needle = String(this.search || '').trim().toLowerCase();
            const base = Array.isArray(this.players) ? this.players : [];
            if (!needle) return base;
            return base.filter(player => String(this.getPlayerName(player)).toLowerCase().includes(needle));
        },
        tableRows() {
            return this.filteredPlayers.map((player, index) => ({
                ...player,
                rank: index + 1,
            }));
        },
        eloFormulas() {
            const formulas = this.effectiveEloConfig?.formulas || {};
            return [
                { key: 'stat_score', label: 'Tilastopisteet', value: formulas.stat_score || '-' },
                { key: 'centered', label: 'Normalisoitu metriikka', value: formulas.centered || '-' },
                { key: 'k_dynamic', label: 'Dynaaminen K', value: formulas.k_dynamic || '-' },
                { key: 'division_multiplier', label: 'Divisioonakerroin', value: formulas.division_multiplier || '-' },
                { key: 'new_player_initial_elo', label: 'Uuden pelaajan base Elo', value: formulas.new_player_initial_elo || '-' },
                { key: 'elo_delta', label: 'Elo delta', value: formulas.elo_delta || '-' },
                { key: 'elo_update', label: 'Elo update', value: formulas.elo_update || '-' },
            ];
        },
        effectiveEloConfig() {
            const incoming = (this.eloConfig && typeof this.eloConfig === 'object') ? this.eloConfig : {};
            const hasIncomingValues = Object.keys(incoming).length > 0;
            if (!hasIncomingValues) return DEFAULT_ELO_CONFIG;

            return {
                ...DEFAULT_ELO_CONFIG,
                ...incoming,
                outcome_weights: {
                    ...DEFAULT_ELO_CONFIG.outcome_weights,
                    ...(incoming.outcome_weights || {}),
                },
                stat_weights: {
                    ...DEFAULT_ELO_CONFIG.stat_weights,
                    ...(incoming.stat_weights || {}),
                },
                stat_baselines: {
                    ...DEFAULT_ELO_CONFIG.stat_baselines,
                    ...(incoming.stat_baselines || {}),
                },
                dynamic_division_elo: {
                    ...DEFAULT_ELO_CONFIG.dynamic_division_elo,
                    ...(incoming.dynamic_division_elo || {}),
                },
                dynamic_k_factor: {
                    ...DEFAULT_ELO_CONFIG.dynamic_k_factor,
                    ...(incoming.dynamic_k_factor || {}),
                },
                initial_elo_bootstrap: {
                    ...DEFAULT_ELO_CONFIG.initial_elo_bootstrap,
                    ...(incoming.initial_elo_bootstrap || {}),
                },
                formulas: {
                    ...DEFAULT_ELO_CONFIG.formulas,
                    ...(incoming.formulas || {}),
                },
            };
        },
        eloConfigRows() {
            const cfg = this.effectiveEloConfig || DEFAULT_ELO_CONFIG;
            const outcome = cfg.outcome_weights || {};
            const dynDiv = cfg.dynamic_division_elo || {};
            const dynK = cfg.dynamic_k_factor || {};
            const initialBootstrap = cfg.initial_elo_bootstrap || {};
            const statWeights = cfg.stat_weights || {};
            const statBaselines = cfg.stat_baselines || {};
            return [
                { key: 'initial_elo', label: 'Aloitus-Elo', value: cfg.initial_elo ?? '-' },
                { key: 'base_k_factor', label: 'Perus K-kerroin', value: cfg.base_k_factor ?? '-' },
                { key: 'min_elo_delta', label: 'Ottelun min Elo-muutos', value: cfg.min_elo_delta ?? '-' },
                { key: 'max_elo_delta', label: 'Ottelun max Elo-muutos', value: cfg.max_elo_delta ?? '-' },
                { key: 'win_bonus', label: 'Lopputulos: voittobonus', value: outcome.win_bonus ?? '-' },
                { key: 'loss_penalty', label: 'Lopputulos: tappiopenalty', value: outcome.loss_penalty ?? '-' },
                { key: 'draw_bonus', label: 'Lopputulos: tasapelibonus', value: outcome.draw_bonus ?? '-' },
                { key: 'k_start', label: 'Dynaaminen K: aloituskerroin', value: dynK.start_multiplier ?? '-' },
                { key: 'k_stabilize_after_maps', label: 'Dynaaminen K: stabiloituu kartan jälkeen', value: dynK.stabilize_after_maps ?? '-' },
                { key: 'k_post_stabilize_multiplier', label: 'Dynaaminen K: stabiloinnin lähtökerroin', value: dynK.post_stabilize_multiplier ?? '-' },
                { key: 'k_min', label: 'Dynaaminen K: minimikerroin', value: dynK.min_multiplier ?? '-' },
                { key: 'k_decay', label: 'Dynaaminen K: hiipumisnopeus', value: dynK.decay_rate ?? '-' },
                { key: 'initial_bootstrap_ref_div', label: 'Uusi pelaaja base Elo: rank-vertailudivisioona', value: initialBootstrap.rank_reference_division ?? '-' },
                { key: 'initial_bootstrap_step', label: 'Uusi pelaaja base Elo: piste-ero per divisioona', value: initialBootstrap.rank_step_points ?? '-' },
                { key: 'initial_bootstrap_min', label: 'Uusi pelaaja base Elo: min', value: initialBootstrap.min_initial_elo ?? '-' },
                { key: 'initial_bootstrap_max', label: 'Uusi pelaaja base Elo: max', value: initialBootstrap.max_initial_elo ?? '-' },
                { key: 'div_fallback', label: 'Divisioona: fallback-kerroin', value: dynDiv.fallback_multiplier ?? '-' },
                { key: 'div_sensitivity', label: 'Divisioona: herkkyys', value: dynDiv.sensitivity ?? '-' },
                { key: 'div_rank_ref', label: 'Divisioona: rank-vertailudivisioona', value: dynDiv.rank_reference_division ?? '-' },
                { key: 'div_rank_step', label: 'Divisioona: rank-step per divisioona', value: dynDiv.rank_step ?? '-' },
                { key: 'div_rank_blend', label: 'Divisioona: rank-blend', value: dynDiv.rank_blend ?? '-' },
                { key: 'div_min', label: 'Divisioona: min kerroin', value: dynDiv.min_multiplier ?? '-' },
                { key: 'div_max', label: 'Divisioona: max kerroin', value: dynDiv.max_multiplier ?? '-' },
                { key: 'div_min_samples', label: 'Divisioona: min samplet', value: dynDiv.min_samples_per_division ?? '-' },
                { key: 'div_shrink', label: 'Divisioona: shrink to mean samplet', value: dynDiv.shrink_to_mean_samples ?? '-' },
                ...Object.keys(statWeights).sort().map((key) => ({
                    key: `stat_weight_${key}`,
                    label: `Paino: ${METRIC_LABELS[key] || key}`,
                    value: statWeights[key],
                })),
                ...Object.keys(statBaselines).sort().map((key) => ({
                    key: `stat_baseline_${key}`,
                    label: `Vertailutaso: ${METRIC_LABELS[key] || key}`,
                    value: statBaselines[key],
                })),
            ];
        }
    },
    async mounted() {
        await this.loadAll();
    },
    watch: {
        selectedSeasonId() {
            this.loadLeaderboard();
        }
    },
    methods: {
        getLeaderboardCacheKey(seasonId = '') {
            return seasonId ? `season:${seasonId}` : 'all';
        },
        applyLeaderboardBundle(bundle) {
            this.players = this.sortPlayers(bundle?.players);
            this.eloConfig = bundle?.elo_config || this.eloConfig || {};
        },
        async loadAll() {
            const requestToken = ++this.loadToken;
            this.loading = true;
            this.error = null;
            try {
                const bundle = await window.apiClient.getSeasonsElo({ limit: 5000 });
                if (requestToken !== this.loadToken) return;
                const rawSeasons = Array.isArray(bundle.seasons) ? [...bundle.seasons] : [];
                this.seasons = rawSeasons.sort((a, b) => Number(this.getSeasonId(b) || 0) - Number(this.getSeasonId(a) || 0));
                this.leaderboardCache = {
                    ...this.leaderboardCache,
                    [this.getLeaderboardCacheKey('')]: {
                        players: Array.isArray(bundle.players) ? [...bundle.players] : [],
                        elo_config: bundle.elo_config || {},
                    },
                };
                this.applyLeaderboardBundle(bundle);
            } catch (error) {
                if (requestToken !== this.loadToken) return;
                this.error = error?.message || 'Elo-listan lataus epäonnistui';
            } finally {
                if (requestToken === this.loadToken) {
                    this.loading = false;
                }
            }
        },
        async loadLeaderboard() {
            const requestToken = ++this.loadToken;
            this.loading = true;
            this.error = null;
            try {
                const cacheKey = this.getLeaderboardCacheKey(this.selectedSeasonId);
                const cachedBundle = this.leaderboardCache[cacheKey];
                if (cachedBundle) {
                    if (requestToken !== this.loadToken) return;
                    this.applyLeaderboardBundle(cachedBundle);
                    return;
                }

                const params = {
                    limit: 5000,
                    includeSeasons: false,
                    includeConfig: false,
                };
                if (this.selectedSeasonId) {
                    params.season = this.selectedSeasonId;
                }
                const bundle = await window.apiClient.getSeasonsElo(params);
                if (requestToken !== this.loadToken) return;
                this.leaderboardCache = {
                    ...this.leaderboardCache,
                    [cacheKey]: {
                        players: Array.isArray(bundle.players) ? [...bundle.players] : [],
                        elo_config: bundle.elo_config || {},
                    },
                };
                this.applyLeaderboardBundle(bundle);
            } catch (error) {
                if (requestToken !== this.loadToken) return;
                this.error = error?.message || 'Elo-listan lataus epäonnistui';
            } finally {
                if (requestToken === this.loadToken) {
                    this.loading = false;
                }
            }
        },
        sortPlayers(players) {
            const rows = Array.isArray(players) ? [...players] : [];
            return rows.sort((a, b) => {
                const eloDiff = Number(b.current_elo || 1000) - Number(a.current_elo || 1000);
                if (eloDiff !== 0) return eloDiff;
                const matchesDiff = Number(b.matches_processed || 0) - Number(a.matches_processed || 0);
                if (matchesDiff !== 0) return matchesDiff;
                return this.getPlayerName(a).localeCompare(this.getPlayerName(b), 'fi');
            });
        },
        getPlayerName(player) {
            return player?.nickname || player?.name || 'Tuntematon pelaaja';
        },
        getPlayerId(player) {
            return player?.player_id || player?.playerId || null;
        },
        getPlayerRoute(player) {
            const playerId = this.getPlayerId(player);
            if (!playerId) return { name: 'elo' };
            const championshipId = player?.last_championship_id || player?.lastChampionshipId || null;
            if (championshipId) {
                return { name: 'player-detail', params: { championshipId: String(championshipId), playerId: String(playerId) } };
            }
            return { name: 'player', params: { playerId: String(playerId) } };
        },
        getSeasonId(season) {
            return season?.id ?? season?.season ?? season?.season_id ?? season?.seasonId ?? null;
        },
        getSeasonLabel(season) {
            return season?.label || season?.name || `Kausi ${this.getSeasonId(season)}`;
        },
        formatElo(value) {
            return Number(value || 1000).toFixed(0);
        },
        formatDelta(value) {
            const numeric = Number(value || 0);
            const prefix = numeric > 0 ? '+' : '';
            return `${prefix}${numeric.toFixed(1)}`;
        },
        formatDivision(value) {
            const numeric = Number(value || 0);
            if (!Number.isFinite(numeric)) return '-';
            if (numeric === 0) return 'M';
            return numeric > 0 ? `D${numeric}` : '-';
        },
        formatTeam(value) {
            const text = String(value || '').trim();
            return text || '-';
        },
        formatMultiplier(value) {
            const numeric = Number(value || 1);
            return Number.isFinite(numeric) ? numeric.toFixed(3) : '1.000';
        },
        csvSafe(value) {
            const text = String(value ?? '');
            const escaped = text.replace(/"/g, '""');
            return `"${escaped}"`;
        },
        buildExportFileName() {
            const seasonPart = this.selectedSeasonId ? `season-${this.selectedSeasonId}` : 'all-seasons';
            const stamp = new Date().toISOString().slice(0, 10);
            return `elo-leaderboard-${seasonPart}-${stamp}.csv`;
        },
        exportLeaderboardCsv() {
            const rows = Array.isArray(this.tableRows) ? this.tableRows : [];
            if (!rows.length) {
                return;
            }

            const headers = [
                'Rank',
                'Player',
                'Team',
                'Current Elo',
                'Division Multiplier',
                'Last Elo Delta',
                'Matches',
                'Division',
                'Season',
                'Player ID'
            ];

            const csvRows = rows.map(row => [
                row.rank,
                this.getPlayerName(row),
                this.formatTeam(row.last_team_name),
                this.formatElo(row.current_elo),
                this.formatMultiplier(row.last_division_multiplier),
                this.formatDelta(row.last_elo_delta),
                Number(row.matches_processed || 0),
                this.formatDivision(row.last_division_num),
                row.last_season || '-',
                this.getPlayerId(row) || ''
            ]);

            const csvContent = [
                headers.map(value => this.csvSafe(value)).join(','),
                ...csvRows.map(values => values.map(value => this.csvSafe(value)).join(','))
            ].join('\n');

            const blob = new Blob(['\uFEFF', csvContent], { type: 'text/csv;charset=utf-8;' });
            const url = URL.createObjectURL(blob);
            const anchor = document.createElement('a');
            anchor.href = url;
            anchor.download = this.buildExportFileName();
            document.body.appendChild(anchor);
            anchor.click();
            document.body.removeChild(anchor);
            URL.revokeObjectURL(url);
        },
        deltaClass(value) {
            const numeric = Number(value || 0);
            if (numeric > 0) return 'is-pos';
            if (numeric < 0) return 'is-neg';
            return 'is-neutral';
        }
    },
    template: `
        <section class="players-view">
            <header class="teams-view__header">
                <h1 class="title-accent titleUnderlinePage">Pelaaja Elo</h1>
                <p class="teams-view__meta">{{ filteredPlayers.length }} / {{ players.length }} pelaajaa · {{ selectedSeasonLabel }} · Nykyhetken Elo</p>
                <div class="teams-view__filters">
                    <select v-model="selectedSeasonId" class="teams-view__season">
                        <option value="">Kaikki kaudet</option>
                        <option
                            v-for="season in seasons"
                            :key="getSeasonId(season)"
                            :value="String(getSeasonId(season))"
                        >
                            {{ getSeasonLabel(season) }}
                        </option>
                    </select>
                    <input
                        v-model="search"
                        type="search"
                        class="teams-view__search"
                        placeholder="Hae pelaajaa..."
                        autocomplete="off"
                    />
                    <button
                        type="button"
                        class="btn-secondary"
                        :disabled="loading || !tableRows.length"
                        @click="exportLeaderboardCsv"
                    >
                        Export CSV
                    </button>
                </div>
            </header>

            <section class="elo-warning-banner" aria-live="polite">
                <strong>HUOM: TÄMÄ ELO-MALLI ON TÄYSIN KEKSITTY JA KOKEELLINEN.</strong>
                <span>
                    Tämä ei tässä vaiheessa todista pelaajan todellista tasoa varmasti. Arvot ovat vain suuntaa antava debug-malli.
                </span>
            </section>

            <section class="elo-explainer glass-card">
                <h3 class="title-accent titleUnderlineCard">Miten Elo lasketaan</h3>
                <p class="elo-explainer__intro">
                    Alla on ensin ihmiskielinen selite mallista, ja sen jälkeen tekninen kaava- sekä config-osio suoraan backendin Elo-configista.
                </p>
                <div class="elo-explainer__grid">
                    <article class="elo-explainer__card">
                        <h4>Miten Elo muodostuu</h4>
                        <div class="elo-explainer__rows">
                            <div class="elo-explainer__row">
                                <span>Aloitusarvo</span>
                                <code>Uusi pelaaja saa base Elon saman kauden + divisioonan Elo-keskiarvosta.</code>
                            </div>
                            <div class="elo-explainer__row">
                                <span>Tyhjä divisioona</span>
                                <code>Jos kausi+divisioona on tyhjä, base Elo bootstrappaa rank-pohjaisella 1000-ankkurilla.</code>
                            </div>
                            <div class="elo-explainer__row">
                                <span>Ottelun muutos</span>
                                <code>Elo-delta tulee stat-scoresta + lopputuloksesta, ja siihen vaikuttavat dynaaminen K sekä divisioonakerroin.</code>
                            </div>
                            <div class="elo-explainer__row">
                                <span>Ensimmäiset 10 karttaa</span>
                                <code>K-kerroin on alussa korkea, jotta parhaat ja heikoimmat erottuvat nopeasti. Sen jälkeen muutos stabiloituu.</code>
                            </div>
                        </div>
                    </article>
                    <article class="elo-explainer__card">
                        <h4>Miksi tämä malli on olemassa</h4>
                        <div class="elo-explainer__rows">
                            <div class="elo-explainer__row">
                                <span>Tarkoitus</span>
                                <code>Malli antaa nopean, vertailukelpoisen signaalin pelaajan suorituskyvystä pitkällä aikavälillä.</code>
                            </div>
                            <div class="elo-explainer__row">
                                <span>Ei virallinen rank</span>
                                <code>Tämä ei ole virallinen tai absoluuttinen tasoluokitus, vaan tilastopohjainen arvio.</code>
                            </div>
                            <div class="elo-explainer__row">
                                <span>All-time Elo</span>
                                <code>Elo kertyy historiasta jatkuvasti. Kausifiltteri Elo-listalla rajaa vain ketkä näytetään.</code>
                            </div>
                        </div>
                    </article>
                    <article class="elo-explainer__card">
                        <h4>Mistä data tulee</h4>
                        <div class="elo-explainer__rows">
                            <div class="elo-explainer__row">
                                <span>Pohjadata</span>
                                <code>Laskenta käyttää ottelu-, kartta- ja pelaajatilastoja backendin tietokannasta.</code>
                            </div>
                            <div class="elo-explainer__row">
                                <span>Rajaukset</span>
                                <code>Forfeit-kartat ja ban-flagilla merkityt ottelut suodatetaan Elo-laskennasta pois.</code>
                            </div>
                            <div class="elo-explainer__row">
                                <span>Läpinäkyvyys</span>
                                <code>Alla olevat kaavat ja muuttujat tulevat endpointin elo_config-payloadista ilman frontend-kovakoodausta.</code>
                            </div>
                        </div>
                    </article>
                </div>
                <div class="elo-explainer__grid">
                    <article class="elo-explainer__card">
                        <h4>Kaavat</h4>
                        <div class="elo-explainer__rows">
                            <div v-for="formula in eloFormulas" :key="formula.key" class="elo-explainer__row">
                                <span>{{ formula.label }}</span>
                                <code>{{ formula.value }}</code>
                            </div>
                        </div>
                    </article>
                    <article class="elo-explainer__card">
                        <h4>Config-muuttujat</h4>
                        <div class="elo-explainer__rows elo-explainer__rows--dense">
                            <div v-for="item in eloConfigRows" :key="item.key" class="elo-explainer__row">
                                <span>{{ item.label }}</span>
                                <strong>{{ item.value }}</strong>
                            </div>
                        </div>
                    </article>
                </div>
            </section>

            <loading-spinner
                v-if="loading"
                message="Elo-listaa ladataan..."
            ></loading-spinner>
            <error-message
                v-else-if="error"
                :message="error"
                @retry="loadLeaderboard"
            ></error-message>
            <div v-else class="scout-table">
                <sortable-table
                    :columns="tableColumns"
                    :data="tableRows"
                    :default-sort="{ column: 'current_elo', order: 'desc', numeric: true }"
                    :sticky-header="true"
                    :compact="true"
                    :mobile-column-limit="4"
                >
                    <template #cell-nickname="slotProps">
                        <router-link :to="getPlayerRoute(slotProps.row)" class="table-link-primary">
                            {{ getPlayerName(slotProps.row) }}
                        </router-link>
                    </template>
                    <template #cell-last_team_name="slotProps">
                        {{ formatTeam(slotProps.value) }}
                    </template>
                    <template #cell-current_elo="slotProps">
                        <strong>{{ formatElo(slotProps.value) }}</strong>
                    </template>
                    <template #cell-last_division_multiplier="slotProps">
                        {{ formatMultiplier(slotProps.value) }}
                    </template>
                    <template #cell-last_elo_delta="slotProps">
                        <span :class="deltaClass(slotProps.value)">{{ formatDelta(slotProps.value) }}</span>
                    </template>
                    <template #cell-matches_processed="slotProps">
                        {{ Number(slotProps.value || 0) }}
                    </template>
                    <template #cell-last_division_num="slotProps">
                        {{ formatDivision(slotProps.value) }}
                    </template>
                    <template #cell-last_season="slotProps">
                        {{ slotProps.value || '-' }}
                    </template>
                </sortable-table>
            </div>
        </section>
    `
};