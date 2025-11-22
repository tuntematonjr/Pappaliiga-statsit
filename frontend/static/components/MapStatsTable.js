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
                { key: 'map_name', label: 'Kartta', sortable: true, align: 'left', colClass: 'col-name col-map-name', width: '220px' },
                { key: 'maps_played', label: 'Pelattu', sortable: true, numeric: true, align: 'right', width: '90px' },
                { key: 'banned', label: 'Bannit', sortable: true, numeric: true, align: 'right', width: '90px' },
                { key: 'rounds_played', label: 'Rundeja', sortable: true, numeric: true, align: 'right', width: '90px' },
                { key: 'r_per_map', label: 'R/Map', sortable: true, numeric: true, align: 'right', decimals: 2, width: '90px' },
                { key: 'kills', label: 'Killed', sortable: true, numeric: true, align: 'right', width: '90px' },
                { key: 'deaths', label: 'Deaths', sortable: true, numeric: true, align: 'right', width: '90px' },
                { key: 'assists', label: 'Assists', sortable: true, numeric: true, align: 'right', width: '90px' },
                { key: 'adr', label: 'ADR', sortable: true, numeric: true, align: 'right', decimals: 1, width: '90px' },
                { key: 'kr', label: 'K/R', sortable: true, numeric: true, align: 'right', decimals: 2, width: '80px' },
                { key: 'udpr', label: 'UDPR', sortable: true, numeric: true, align: 'right', decimals: 2, width: '90px' },
                { key: 'enemy_flash', label: 'Enemy/Flash', sortable: true, numeric: true, align: 'right', decimals: 2, width: '110px' },
                { key: 'k2', label: '2K', sortable: true, numeric: true, align: 'right', width: '70px' },
                { key: 'k3', label: '3K', sortable: true, numeric: true, align: 'right', width: '70px' },
                { key: 'k4', label: '4K', sortable: true, numeric: true, align: 'right', width: '70px' },
                { key: 'ace', label: 'Ace', sortable: true, numeric: true, align: 'right', width: '70px' },
                { key: 'pistol_kills', label: 'Pistol Kills', sortable: true, numeric: true, align: 'right', width: '110px' },
                { key: 'sniper_kills', label: 'Sniper Kills', sortable: true, numeric: true, align: 'right', width: '110px' }
            ],
            defaultColorizeColumns: ['kd', 'adr', 'kr', 'udpr'],
            defaultSort: { column: 'maps_played', order: 'desc', numeric: true }
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
                const k2 = Number(curr.k2 ?? curr.two_k ?? 0);
                const k3 = Number(curr.k3 ?? curr.three_k ?? 0);
                const k4 = Number(curr.k4 ?? curr.four_k ?? 0);
                const ace = Number(curr.ace ?? curr.five_k ?? 0);
                const pistolKills = Number(curr.pistol_kills ?? 0);

                return {
                    id: entry.map_name || `map-${idx}`,
                    map_name: entry.map_name || curr.name || 'Kartta',
                    logo: curr.logo || curr.image || entry.image || entry.thumbnail || null,
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
                    k2,
                    k3,
                    k4,
                    ace,
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
