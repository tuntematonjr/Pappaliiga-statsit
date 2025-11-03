const PLAYER_KPI_SCHEMA = [
    { key: 'rating', label: 'Rating', decimals: 2, max: 2 },
    { key: 'adr', label: 'ADR', decimals: 1, max: 120 },
    { key: 'kd', label: 'K/D', decimals: 2, max: 2 },
    { key: 'entry', label: 'Entry %', decimals: 1, max: 100, percent: true },
    { key: 'clutch', label: 'Clutch %', decimals: 1, max: 100, percent: true },
    { key: 'util', label: 'Util / R', decimals: 2, max: 1 }
];

function toNumber(value, fallback = 0) {
    if (value === null || value === undefined) return fallback;
    const numeric = Number(value);
    if (Number.isFinite(numeric)) return numeric;
    const parsed = Number(String(value).replace(',', '.'));
    return Number.isFinite(parsed) ? parsed : fallback;
}

function formatValue(value, options = {}) {
    if (value === undefined || value === null) return '–';
    const numeric = toNumber(value);
    const decimals = options.decimals ?? (numeric >= 100 ? 0 : 1);
    if (options.percent) {
        return `${numeric.toFixed(decimals)} %`;
    }
    return numeric.toFixed(decimals);
}

function buildKpis(season) {
    if (!season) return [];
    const rating = toNumber(season.rating ?? season.rating_2 ?? season.hltv_rating);
    const adr = toNumber(season.adr ?? season.average_damage);
    const kd = toNumber(season.kd ?? season.kd_ratio);
    const entrySource = season.entry_success ?? season.entry_percent ?? season.entry_rate;
    let entry = toNumber(entrySource ?? 0);
    if (!entry) {
        const wins = toNumber(season.opening_duels_won ?? 0);
        const played = Math.max(1, toNumber(season.opening_duels_played ?? 0));
        entry = (wins / played) * 100;
    }
    const clutch = toNumber(season.clutch_percent ?? season.clutch_rate ?? season.clutch_success ?? 0);
    const utilSource = season.utility_per_round ?? season.utility;
    const util = toNumber(utilSource ?? 0);

    const base = { rating, adr, kd, entry, clutch, util };
    return PLAYER_KPI_SCHEMA.map(def => ({
        key: def.key,
        label: def.label,
        value: base[def.key] ?? 0,
        display: formatValue(base[def.key], def),
        max: def.max,
        percent: def.percent,
        decimals: def.decimals
    }));
}

function buildLineSeries(seasons) {
    if (!Array.isArray(seasons)) return [];
    const sorted = [...seasons].sort((a, b) => toNumber(a.season) - toNumber(b.season));
    return sorted.map(item => ({
        label: `S${item.season} · D${item.division_num}`,
        rating: toNumber(item.rating ?? item.rating_2 ?? item.hltv_rating),
        kd: toNumber(item.kd),
        adr: toNumber(item.adr)
    }));
}

function buildSparklinePoints(series, key = 'rating') {
    return series.map(item => {
        const value = Number(item[key]);
        if (!Number.isFinite(value)) return 0;
        if (key === 'rating') {
            const normalized = Math.max(0.4, Math.min(2.0, value));
            return (normalized - 1) / 1; // roughly -0.6..1.0 -> -1..1
        }
        return value;
    }).map(value => Math.max(-1, Math.min(1, value - 1)));
}

function buildRadarMetrics(kpis) {
    return kpis.map(kpi => ({
        label: kpi.label,
        value: kpi.value,
        max: kpi.max
    }));
}

function buildCompareMetrics(baseKpis, compareKpis) {
    const compareMap = new Map(compareKpis.map(item => [item.key, item]));
    return baseKpis.map(item => ({
        key: item.key,
        label: item.label,
        base: item.value,
        compare: compareMap.get(item.key)?.value ?? null,
        decimals: item.decimals,
        percent: item.percent,
        format: value => formatValue(value, { decimals: item.decimals, percent: item.percent })
    }));
}

function createSegment() {
    return {
        data: null,
        loading: false,
        error: null
    };
}

function buildSeasonOption(season) {
    const championshipId = String(season.championship_id || season.championshipId || season.id || '');
    return {
        value: championshipId,
        season: season.season,
        division: season.division_num,
        team: season.team_name || season.team || null,
        label: `S${season.season} · D${season.division_num}`,
        isPlayoffs: Boolean(season.is_playoff || (season.phase && String(season.phase).toLowerCase().includes('playoff')))
    };
}

window.PlayerView = {
    name: 'PlayerView',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; },
        get StatPanel() { return window.StatPanel; },
        get SparklineChart() { return window.SparklineChart; },
        get RadarChart() { return window.RadarChart; },
        get MapsStats() { return window.MapsStats; },
        get PlayerCompareModal() { return window.PlayerCompareModal; },
        get CopyLink() { return window.CopyLink; }
    },
    data() {
        const playerStore = typeof window.usePlayerStore === 'function' ? window.usePlayerStore() : null;
        return {
            playerStore,
            selectedSeasonId: null,
            seasonMode: 'regular',
            compareVisible: false,
            compareLoading: false,
            compareError: null,
            comparePlayer: null,
            compareMetrics: [],
            compareTargetId: ''
        };
    },
    computed: {
        playerId() {
            return this.$route.params?.playerId || null;
        },
        playerState() {
            if (!this.playerStore || !this.playerId) return null;
            return this.playerStore.getPlayerState(this.playerId) || null;
        },
        profileSegment() {
            return this.playerState?.profile || createSegment();
        },
        seasonsSegment() {
            return this.playerState?.seasons || createSegment();
        },
        mapStatsSegment() {
            if (!this.selectedSeasonId) return createSegment();
            return this.playerState?.maps?.[this.selectedSeasonId] || createSegment();
        },
        profile() {
            return this.profileSegment.data || null;
        },
        allSeasons() {
            return Array.isArray(this.seasonsSegment.data) ? this.seasonsSegment.data.map(buildSeasonOption) : [];
        },
        filteredSeasons() {
            return this.allSeasons.filter(option => option.isPlayoffs === (this.seasonMode === 'playoff'));
        },
        currentSeasonOption() {
            if (!this.selectedSeasonId) return null;
            return this.allSeasons.find(option => option.value === this.selectedSeasonId) || null;
        },
        selectedSeasonStats() {
            if (!this.selectedSeasonId) return null;
            const raw = (this.seasonsSegment.data || []).find(item => String(item.championship_id) === String(this.selectedSeasonId));
            return raw || null;
        },
        kpiMetrics() {
            return buildKpis(this.selectedSeasonStats);
        },
        lineSeries() {
            return buildLineSeries(this.seasonsSegment.data || []);
        },
        sparklinePoints() {
            return buildSparklinePoints(this.lineSeries, 'rating');
        },
        radarMetrics() {
            return buildRadarMetrics(this.kpiMetrics);
        },
        mapStats() {
            return Array.isArray(this.mapStatsSegment.data) ? this.mapStatsSegment.data : [];
        },
        heroTeam() {
            return this.selectedSeasonStats?.team_name || this.selectedSeasonStats?.team || null;
        },
        loading() {
            return this.profileSegment.loading || this.seasonsSegment.loading;
        },
        loadError() {
            return this.profileSegment.error || this.seasonsSegment.error;
        },
        compareMetricsReady() {
            return Array.isArray(this.compareMetrics) && this.compareMetrics.length > 0;
        }
    },
    watch: {
        playerId: {
            immediate: true,
            handler() {
                this.bootstrap();
            }
        },
        filteredSeasons(newOptions) {
            if (!Array.isArray(newOptions) || !newOptions.length) {
                this.selectedSeasonId = null;
                return;
            }
            if (!this.selectedSeasonId || !newOptions.some(option => option.value === this.selectedSeasonId)) {
                this.selectedSeasonId = newOptions[0].value;
                this.loadMapStats();
            }
        },
        selectedSeasonId(newVal, oldVal) {
            if (newVal && newVal !== oldVal) {
                this.loadMapStats();
                this.comparePlayer = null;
                this.compareMetrics = [];
            }
        }
    },
    methods: {
        async bootstrap() {
            if (!this.playerStore || !this.playerId) return;
            this.compareVisible = false;
            this.compareMetrics = [];
            this.comparePlayer = null;
            try {
                await Promise.allSettled([
                    this.playerStore.fetchProfile(this.playerId, { force: true }),
                    this.playerStore.fetchSeasons(this.playerId, { force: true })
                ]);
                const defaults = this.filteredSeasons;
                if (defaults.length && !this.selectedSeasonId) {
                    this.selectedSeasonId = defaults[0].value;
                }
                if (this.selectedSeasonId) {
                    await this.loadMapStats();
                }
            } catch (error) {
                console.error('Player bootstrap failed', error);
            }
        },
        async loadMapStats() {
            if (!this.playerStore || !this.playerId || !this.selectedSeasonId) return;
            try {
                await this.playerStore.fetchMapStats(this.playerId, this.selectedSeasonId, { force: true });
            } catch (error) {
                console.error('Player map stats failed', error);
            }
        },
        handleSeasonModeChange(mode) {
            if (mode === this.seasonMode) return;
            this.seasonMode = mode;
            const options = this.filteredSeasons;
            if (options.length) {
                this.selectedSeasonId = options[0].value;
            } else {
                this.selectedSeasonId = null;
            }
        },
        handleCompareOpen() {
            this.compareVisible = true;
            this.compareError = null;
            this.comparePlayer = null;
            this.compareMetrics = [];
        },
        handleCompareClose() {
            this.compareVisible = false;
        },
        async handleCompareSubmit(candidateId) {
            if (!candidateId || !this.playerStore) return;
            this.compareLoading = true;
            this.compareError = null;
            this.comparePlayer = null;
            this.compareMetrics = [];
            try {
                const [profile, seasons] = await Promise.all([
                    this.playerStore.fetchProfile(candidateId, { force: true }),
                    this.playerStore.fetchSeasons(candidateId, { force: true })
                ]);
                const seasonMatch = (seasons || []).find(item => String(item.championship_id) === this.selectedSeasonId) || seasons?.[0];
                const compareKpis = buildKpis(seasonMatch);
                this.comparePlayer = profile;
                this.compareMetrics = buildCompareMetrics(this.kpiMetrics, compareKpis);
            } catch (error) {
                console.error('Compare player failed', error);
                this.compareError = error?.message || 'Vertailtavaa pelaajaa ei löytynyt';
            } finally {
                this.compareLoading = false;
            }
        }
    },
    template: `
        <div class="player-view">
            <loading-spinner v-if="loading && !profile" message="Pelaajaa ladataan..."></loading-spinner>
            <error-message v-else-if="loadError && !profile" :message="loadError" @retry="bootstrap"></error-message>
            <template v-else>
                <header class="player-hero glass-card">
                    <div class="player-hero__identity">
                        <div class="player-hero__avatar">
                            <img v-if="profile?.avatar" :src="profile.avatar" :alt="profile.nickname" loading="lazy" />
                            <span v-else>{{ (profile?.nickname || '?').charAt(0).toUpperCase() }}</span>
                        </div>
                        <div class="player-hero__meta">
                            <h1>{{ profile?.nickname || 'Pelaaja' }}</h1>
                            <p v-if="heroTeam" class="player-hero__team">{{ heroTeam }}</p>
                            <div class="player-hero__actions">
                                <a v-if="profile?.faceit_url" :href="profile.faceit_url" target="_blank" rel="noopener" class="btn-primary">Faceit</a>
                                <button type="button" class="btn-link" @click="handleCompareOpen">Vertaa pelaajaa</button>
                                <copy-link label="Jaa pelaaja"></copy-link>
                            </div>
                        </div>
                    </div>
                </header>

                <section class="player-controls">
                    <div class="player-mode-toggle" role="tablist" aria-label="Kausityyppi">
                        <button
                            type="button"
                            class="player-mode-toggle__btn"
                            :class="{ 'player-mode-toggle__btn--active': seasonMode === 'regular' }"
                            @click="handleSeasonModeChange('regular')"
                        >
                            Runkosarja
                        </button>
                        <button
                            type="button"
                            class="player-mode-toggle__btn"
                            :class="{ 'player-mode-toggle__btn--active': seasonMode === 'playoff' }"
                            @click="handleSeasonModeChange('playoff')"
                        >
                            Playoffs
                        </button>
                    </div>

                    <div class="player-season-pills" v-if="filteredSeasons.length">
                        <button
                            v-for="season in filteredSeasons"
                            :key="season.value"
                            type="button"
                            class="player-season-pill"
                            :class="{ 'player-season-pill--active': season.value === selectedSeasonId }"
                            @click="selectedSeasonId = season.value"
                        >
                            {{ season.label }}
                        </button>
                    </div>
                    <p v-else class="player-empty">Ei kausia valitulle tilalle.</p>
                </section>

                <section class="player-kpis">
                    <stat-panel :items="kpiMetrics.map(kpi => ({ key: kpi.key, label: kpi.label, value: kpi.display }))" :columns="3"></stat-panel>
                </section>

                <section class="player-charts">
                    <article class="player-chart glass-card">
                        <h3>Rating trendi</h3>
                        <sparkline-chart
                            v-if="sparklinePoints.length"
                            :points="sparklinePoints"
                            :width="200"
                            :height="80"
                        ></sparkline-chart>
                        <p v-else class="player-empty">Riittävästi kausia ei löytynyt.</p>
                    </article>
                    <article class="player-chart glass-card">
                        <h3>Pelityylin profiili</h3>
                        <radar-chart
                            v-if="radarMetrics.length"
                            :metrics="radarMetrics"
                        ></radar-chart>
                        <p v-else class="player-empty">Ei riittäviä mittareita.</p>
                    </article>
                </section>

                <section class="player-maps">
                    <maps-stats
                        v-if="selectedSeasonId"
                        title="Karttakohtainen suoritus"
                        :map-stats="mapStats"
                        :loading="mapStatsSegment.loading"
                        :error="mapStatsSegment.error"
                        :columns="null"
                    ></maps-stats>
                </section>

                <player-compare-modal
                    :visible="compareVisible"
                    :base-player="profile"
                    :compare-player="comparePlayer"
                    :metrics="compareMetrics"
                    :loading="compareLoading"
                    :error="compareError"
                    @close="handleCompareClose"
                    @submit="handleCompareSubmit"
                ></player-compare-modal>
            </template>
        </div>
    `
};
