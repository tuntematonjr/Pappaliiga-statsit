// MapStatsTable Component - Displays per-map stats in sortable table
function formatMapPercent(value) {
    const numeric = Number(value);
    const resolved = Number.isFinite(numeric) ? numeric : 0;
    return resolved.toFixed(1);
}

window.MapStatsTable = {
    name: 'MapStatsTable',
    components: {
        get SortableTable() { return window.SortableTable; }
    },
    props: {
        mapStats: {
            type: Array,
            default: () => []
        },
        columnsConfig: {
            type: Array,
            default: null
        },
        colorizeColumnsConfig: {
            type: Array,
            default: null
        },
        stickyHeader: {
            type: Boolean,
            default: true
        }
    },
    data() {
        return {
            defaultColumns: [
                { key: 'map_name', label: 'Kartta', sortable: true, align: 'left', colClass: 'col-name col-map-name', width: '210px', mobilePinned: true, mobilePriority: 1 },
                { key: 'maps_played', label: 'Pelattu', sortable: true, numeric: true, align: 'right', width: '88px', mobileLabel: 'Pel.', mobilePriority: 2 },
                { key: 'banned', label: 'Bannit', sortable: true, numeric: true, align: 'right', width: '88px', mobilePriority: 5 },
                { key: 'rounds_played', label: 'Rundeja', sortable: true, numeric: true, align: 'right', width: '94px', mobileLabel: 'Erät', mobileHidden: true },
                { key: 'r_per_map', label: 'R/Map', sortable: true, numeric: true, align: 'right', decimals: 0, width: '88px', mobileHidden: true },
                { key: 'kills', label: 'Killed', sortable: true, numeric: true, align: 'right', width: '88px', mobileHidden: true },
                { key: 'deaths', label: 'Deaths', sortable: true, numeric: true, align: 'right', width: '88px', mobileHidden: true },
                { key: 'assists', label: 'Assists', sortable: true, numeric: true, align: 'right', width: '88px', mobileHidden: true },
                { key: 'adr', label: 'ADR', sortable: true, numeric: true, align: 'right', decimals: 1, width: '90px', mobilePriority: 3 },
                { key: 'kr', label: 'K/R', sortable: true, numeric: true, align: 'right', decimals: 2, width: '78px', mobilePriority: 4 },
                { key: 'udpr', label: 'UDPR', sortable: true, numeric: true, align: 'right', decimals: 2, width: '94px', mobileHidden: true },
                { key: 'enemy_flash', label: 'Enemy/Flash', sortable: true, numeric: true, align: 'right', decimals: 2, width: '108px', mobileHidden: true },
                { key: 'mk_2k', label: '2K', sortable: true, numeric: true, align: 'right', width: '68px', mobileHidden: true },
                { key: 'mk_3k', label: '3K', sortable: true, numeric: true, align: 'right', width: '68px', mobileHidden: true },
                { key: 'mk_4k', label: '4K', sortable: true, numeric: true, align: 'right', width: '68px', mobileHidden: true },
                { key: 'mk_5k', label: '5K', sortable: true, numeric: true, align: 'right', width: '68px', mobileHidden: true },
                { key: 'pistol_kills', label: 'Pistol Kills', sortable: true, numeric: true, align: 'right', width: '104px', mobileHidden: true },
                { key: 'sniper_kills', label: 'Sniper Kills', sortable: true, numeric: true, align: 'right', width: '104px', mobileHidden: true }
            ],
            defaultColorizeColumns: [],
            defaultSort: { column: 'maps_played', order: 'desc', numeric: true },
            mapImageLookup: {},
            catalogLoaded: false,
            catalogLoading: false
        };
    },
    computed: {
        resolvedColumns() {
            if (Array.isArray(this.columnsConfig) && this.columnsConfig.length > 0) {
                return this.columnsConfig;
            }
            return this.defaultColumns;
        },
        colorizeColumns() {
            if (Array.isArray(this.colorizeColumnsConfig) && this.colorizeColumnsConfig.length > 0) {
                return this.colorizeColumnsConfig;
            }
            return this.defaultColorizeColumns;
        },
        rows() {
            if (!Array.isArray(this.mapStats) || !this.mapStats.length) {
                return [];
            }
            const mapped = this.mapStats.map((entry, idx) => {
                const curr = entry?.curr || entry || {};
                const mapsPlayed = Number(curr.maps_played ?? entry.maps_played ?? 0);
                const roundsPlayed = Number(curr.rounds_played ?? curr.rounds ?? 0);
                const wins = Number(curr.maps_won ?? curr.wins ?? 0);
                const losses = Number(curr.maps_lost ?? curr.losses ?? 0);
                const kills = Number(curr.kills ?? 0);
                const deaths = Number(curr.deaths ?? 0);
                const kd = Number.isFinite(curr.kd) ? Number(curr.kd) : (deaths > 0 ? kills / deaths : kills || 0);
                const adr = Number(curr.adr ?? 0);
                const kr = Number(curr.kr ?? 0);
                const udpr = Number(curr.udpr ?? 0);
                const enemyFlash = Number(curr.enemy_flash ?? curr.enemyFlash ?? 0);
                const sniperKills = Number(curr.sniper_kills ?? 0);
                const assists = Number(curr.assists ?? 0);
                const mk2k = Number(curr.mk_2k ?? 0);
                const mk3k = Number(curr.mk_3k ?? 0);
                const mk4k = Number(curr.mk_4k ?? 0);
                const mk5k = Number(curr.mk_5k ?? 0);
                const pistolKills = Number(curr.pistol_kills ?? 0);
                const name = entry.pretty_name || curr.pretty_name || entry.map_name || curr.name || 'Kartta';
                const mapId = entry.map_name || curr.map_name || entry.mapId || name;

                return {
                    id: mapId || `map-${idx}`,
                    map_name: name,
                    logo: this.resolveMapImage(entry),
                    maps_played: mapsPlayed,
                    banned: Number(curr.banned ?? 0),
                    rounds_played: roundsPlayed,
                    r_per_map: mapsPlayed > 0 ? Number((roundsPlayed / mapsPlayed).toFixed(2)) : 0,
                    adr,
                    kr,
                    udpr,
                    kills,
                    deaths,
                    enemy_flash: enemyFlash,
                    sniper_kills: sniperKills,
                    assists,
                    mk_2k: mk2k,
                    mk_3k: mk3k,
                    mk_4k: mk4k,
                    mk_5k: mk5k,
                    pistol_kills: pistolKills
                };
            });

            return mapped.sort((a, b) => {
                const plays = (b.maps_played || 0) - (a.maps_played || 0);
                if (plays !== 0) return plays;
                return String(a.map_name || '').localeCompare(String(b.map_name || ''));
            });
        }
    },
    methods: {
        normalizePercent(rawValue, wins = null, losses = null, allowFallback = false) {
            let value = rawValue;
            if (value == null && allowFallback && (wins != null || losses != null)) {
                const w = Number(wins ?? 0);
                const l = Number(losses ?? 0);
                const total = w + l;
                value = total > 0 ? (w / total) * 100 : null;
            }
            if (value == null) {
                return allowFallback ? 0 : (wins || losses) ? (wins / Math.max(1, wins + losses)) * 100 : 0;
            }
            let normalized = Number(value);
            if (!Number.isFinite(normalized)) {
                normalized = 0;
            }
            if (Math.abs(normalized) <= 1) {
                normalized *= 100;
            }
            return Number(normalized.toFixed(1));
        },
        mapKey(name) {
            return window.MapImageUtils ? window.MapImageUtils.mapKey(name) : null;
        },
        extractMapImage(entry) {
            return window.MapImageUtils ? window.MapImageUtils.extractMapImage(entry) : null;
        },
        resolveMapImage(entry) {
            return window.MapImageUtils
                ? window.MapImageUtils.resolveMapImage(entry, { mapImageLookup: this.mapImageLookup, apiClient: window.apiClient })
                : null;
        },
        buildMapImageLookup(stats, existing = {}) {
            return window.MapImageUtils ? window.MapImageUtils.buildMapImageLookup(stats, existing) : { ...(existing || {}) };
        },
        shouldFetchCatalog(stats) {
            if (this.catalogLoaded || this.catalogLoading) return false;
            return window.MapImageUtils ? window.MapImageUtils.shouldFetchCatalog(stats) : false;
        },
        async ensureMapCatalog() {
            if (this.catalogLoaded || this.catalogLoading || !window.apiClient || typeof window.apiClient.getMapsCatalog !== 'function') {
                return;
            }
            this.catalogLoading = true;
            try {
                const catalog = await window.apiClient.getMapsCatalog();
                if (Array.isArray(catalog) && catalog.length) {
                    const lookup = { ...this.mapImageLookup };
                    catalog.forEach(item => {
                        const key = this.mapKey(item?.map_id || item?.pretty_name || item?.map_name || item?.name);
                        const img = item?.image_sm || item?.image_lg || item?.image;
                        if (key && img && !lookup[key]) {
                            lookup[key] = img;
                        }
                    });
                    this.mapImageLookup = lookup;
                }
                this.catalogLoaded = true;
            } catch (error) {
                console.warn('[MapStatsTable] map catalog fetch failed', error);
                this.catalogLoaded = true;
            } finally {
                this.catalogLoading = false;
            }
        }
    },
    watch: {
        mapStats: {
            immediate: true,
            handler(newStats) {
                this.mapImageLookup = this.buildMapImageLookup(newStats, this.mapImageLookup);
                if (this.shouldFetchCatalog(newStats)) {
                    this.ensureMapCatalog();
                }
            }
        }
    },
    template: `
        <div class="map-stats-table">
            <div v-if="rows.length" class="map-sortable">
                <sortable-table
                    :columns="resolvedColumns"
                    :data="rows"
                    :defaultSort="defaultSort"
                    :colorizeColumns="colorizeColumns"
                    :mobile-column-limit="5"
                    :sticky-header="stickyHeader"
                    :compact="true"
                >
                    <template #cell-map_name="{ row }">
                        <div class="map-name">
                            <img v-if="row.logo" :src="row.logo" class="map-logo" alt="" />
                            <span class="map-name-text">{{ row.map_name }}</span>
                        </div>
                    </template>
                </sortable-table>
            </div>
            <p v-else class="map-stats-table__empty">Ei karttatilastoja saatavilla</p>
        </div>
    `
};
