// MapStatsTable Component - Displays per-map stats with deltas
window.MapStatsTable = {
    name: 'MapStatsTable',
    components: {
        DeltaIndicator: window.DeltaIndicator,
        SortableTable: window.SortableTable
    },
    template: `
        <div class="map-stats-table">
            <h3>Divisioonan Kartta Tilastot</h3>
            <div v-if="loading" class="loading">Loading map stats...</div>
            <div v-else-if="error" class="error">{{ error }}</div>
            <div v-else>
                <sortable-table 
                    v-if="rows.length > 0"
                    :columns="columns"
                    :data="rows"
                    :defaultSort="{ column: 'maps_played', order: 'desc', numeric: true }"
                    :colorizeColumns="colorizeColumns"
                    :sort-ready="tableSortReady"
                    class="map-sortable"
                    :compact="true"
                >
                    <!-- Map cell slot -->
                    <template v-slot:cell-map_name="{ row }">
                        <!-- Use the same wrapper class as the SortableTable fallback (.map-name)
                             so the same CSS rules apply whether a slot is provided or not. -->
                        <div class="map-name">
                            <img v-if="row.logo" :src="row.logo" class="map-logo" alt=""/>
                            <span class="map-name-text">{{ row.map_name }}</span>
                        </div>
                    </template>

                    <!-- Numeric columns use delta-indicator if available -->
                    <template v-slot:cell-maps_played="{ row }">
                        <delta-indicator :value="row.maps_played"></delta-indicator>
                    </template>
                    <template v-slot:cell-banned="{ row }">
                        <span class="map-stat-banned">{{ row.banned }}</span>
                    </template>
                    <template v-slot:cell-rounds_played="{ row }">
                        <delta-indicator :value="row.rounds_played"></delta-indicator>
                    </template>
                    <template v-slot:cell-rounds_per_map="{ row }">
                        <delta-indicator :value="row.rounds_per_map"></delta-indicator>
                    </template>
                    <template v-slot:cell-kills="{ row }">
                        <delta-indicator :value="row.kills"></delta-indicator>
                    </template>
                    <template v-slot:cell-assists="{ row }">
                        <delta-indicator :value="row.assists"></delta-indicator>
                    </template>
                    <template v-slot:cell-deaths="{ row }">
                        <delta-indicator :value="row.deaths"></delta-indicator>
                    </template>
                    <template v-slot:cell-k2="{ row }">
                        <delta-indicator :value="row.k2"></delta-indicator>
                    </template>
                    <template v-slot:cell-k3="{ row }">
                        <delta-indicator :value="row.k3"></delta-indicator>
                    </template>
                    <template v-slot:cell-k4="{ row }">
                        <delta-indicator :value="row.k4"></delta-indicator>
                    </template>
                    <template v-slot:cell-ace="{ row }">
                        <delta-indicator :value="row.ace"></delta-indicator>
                    </template>
                    <template v-slot:cell-adr="{ row }">
                        <delta-indicator :value="row.adr" format="decimal" :decimals="1"></delta-indicator>
                    </template>
                    <template v-slot:cell-kr="{ row }">
                        <delta-indicator :value="row.kr" format="decimal" :decimals="2"></delta-indicator>
                    </template>
                    <template v-slot:cell-udpr="{ row }">
                        <delta-indicator :value="row.udpr" format="decimal" :decimals="2"></delta-indicator>
                    </template>
                    <template v-slot:cell-enemy_flash="{ row }">
                        <delta-indicator :value="row.enemy_flash" format="decimal" :decimals="2"></delta-indicator>
                    </template>
                    <template v-slot:cell-pistol_kills="{ row }">
                        <delta-indicator :value="row.pistol_kills"></delta-indicator>
                    </template>
                    <template v-slot:cell-sniper_kills="{ row }">
                        <delta-indicator :value="row.sniper_kills"></delta-indicator>
                    </template>
                </sortable-table>

                <p v-if="rows.length === 0" class="no-data">Ei karttatilastoja saatavilla</p>
            </div>
        </div>
    `,
    props: {
        mapStats: {
            type: Array,
            required: true
        },
        loading: {
            type: Boolean,
            default: false
        },
        error: {
            type: String,
            default: null
        }
    },
    data() {
        return {
            // columns definition for SortableTable
                columns: [
                // Provide explicit widths for key columns to lock column positions
                { key: 'map_name', label: 'Kartta', sortable: true, align: 'left', colClass: 'col-name col-map-name' },
                { key: 'maps_played', label: 'Pelattu', sortable: true, numeric: true },
                { key: 'banned', label: 'Banned', sortable: true, numeric: true },
                { key: 'rounds_played', label: 'Rundeja', sortable: true, numeric: true },
                { key: 'rounds_per_map', label: 'R/Map', sortable: true, numeric: true, format: v => Number(v).toFixed(2) },
                { key: 'kills', label: 'Killed', sortable: true, numeric: true },
                { key: 'assists', label: 'Assists', sortable: true, numeric: true },
                { key: 'deaths', label: 'Deaths', sortable: true, numeric: true },
                { key: 'k2', label: '2K', sortable: true, numeric: true },
                { key: 'k3', label: '3K', sortable: true, numeric: true },
                { key: 'k4', label: '4K', sortable: true, numeric: true },
                { key: 'ace', label: 'ACE', sortable: true, numeric: true },
                { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1 },
                { key: 'kr', label: 'K/R', sortable: true, numeric: true, decimals: 2 },
                { key: 'udpr', label: 'UDPR', sortable: true, numeric: true, decimals: 2 },
                { key: 'enemy_flash', label: 'Enemy/Flash', sortable: true, numeric: true, decimals: 2 },
                { key: 'pistol_kills', label: 'Pistol Kills', sortable: true, numeric: true },
                { key: 'sniper_kills', label: 'Sniper Kills', sortable: true, numeric: true }
            ],
            // which columns to colorize (continuous scales)
            colorizeColumns: ['adr','kr','udpr','rounds_per_map']
        };
    },
    computed: {
        // Flatten mapStats into rows expected by SortableTable
        rows() {
            if (!this.mapStats || this.mapStats.length === 0) return [];
            const mapped = this.mapStats.map((m, idx) => {
                const curr = (m && m.curr) ? m.curr : {};
                const maps_played = Number(curr.maps_played || 0);
                const rounds_source = curr.rounds_played ?? curr.rounds ?? (curr.rounds_per_map ? curr.rounds_per_map * maps_played : null);
                const rounds_played = Number(rounds_source != null ? rounds_source : 0);
                const rounds_per_map = maps_played ? (rounds_played / maps_played) : 0;
                return {
                    // Provide stable key for Vue v-for to prevent DOM reuse issues
                    id: m.map_name || `map-${idx}`,
                    map_name: m.map_name || '',
                    logo: curr.logo || null,
                    maps_played: maps_played,
                    banned: Number(curr.banned || 0),
                    rounds_played: rounds_played,
                    rounds_per_map: Number(rounds_per_map.toFixed(2)),
                    kills: Number(curr.kills || 0),
                    assists: Number(curr.assists || 0),
                    deaths: Number(curr.deaths || 0),
                    k2: Number(curr.k2 || 0),
                    k3: Number(curr.k3 || 0),
                    k4: Number(curr.k4 || 0),
                    ace: Number(curr.ace || 0),
                    adr: Number(curr.adr || 0),
                    kr: Number(curr.kr || 0),
                    udpr: Number(curr.udpr || 0),
                    enemy_flash: Number(curr.enemy_flash || 0),
                    pistol_kills: Number(curr.pistol_kills || 0),
                    sniper_kills: Number(curr.sniper_kills || 0)
                };
            });

            return mapped.sort((a, b) => {
                const diff = Number(b.maps_played || 0) - Number(a.maps_played || 0);
                if (diff !== 0) return diff;
                const roundsDiff = Number(b.rounds_played || 0) - Number(a.rounds_played || 0);
                if (roundsDiff !== 0) return roundsDiff;
                return String(a.map_name || '').localeCompare(String(b.map_name || ''));
            });
        },
        tableSortReady() {
            const rawCount = Array.isArray(this.mapStats) ? this.mapStats.filter(Boolean).length : 0;
            const rowCount = this.rows.length;
            const ready = !this.loading && rowCount > 1 && rawCount > 0 && rowCount === rawCount;
            console.debug('[MapStatsTable] tableSortReady', { rawCount, rowCount, loading: this.loading, ready });
            return ready;
        }
    }
};
