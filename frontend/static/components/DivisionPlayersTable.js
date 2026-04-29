(function () {
    const ALL_COLUMNS = [
        { key: '_rank',         label: '#',        group: null,       sortable: false, numeric: true,  align: 'right', width: '42px' },
        { key: 'nickname',      label: 'Pelaaja',  group: 'core',     sortable: true,  numeric: false, align: 'left',  width: '150px' },
        { key: 'team_name',     label: 'Joukkue',  group: 'core',     sortable: true,  numeric: false, align: 'left',  width: '130px' },
        { key: 'maps_played',   label: 'Kartat',   group: 'core',     sortable: true,  numeric: true,  width: '70px' },
        { key: 'kills',         label: 'Kills',    group: 'core',     sortable: true,  numeric: true,  width: '70px' },
        { key: 'assists',       label: 'Assists',  group: 'extended', sortable: true,  numeric: true,  width: '78px' },
        { key: 'deaths',        label: 'Deaths',   group: 'core',     sortable: true,  numeric: true,  width: '74px' },
        { key: 'kd',            label: 'K/D',      group: 'core',     sortable: true,  numeric: true,  decimals: 2, width: '72px' },
        { key: 'adr',           label: 'ADR',      group: 'core',     sortable: true,  numeric: true,  decimals: 1, width: '72px' },
        { key: 'kr',            label: 'K/R',      group: 'core',     sortable: true,  numeric: true,  decimals: 2, width: '72px' },
        { key: 'hs_pct',        label: 'HS%',      group: 'core',     sortable: true,  numeric: true,  decimals: 1, suffix: '%', width: '72px' },
        { key: 'mvps',          label: 'MVPs',     group: 'extended', sortable: true,  numeric: true,  width: '68px' },
        { key: 'utility_damage',label: 'Util DMG', group: 'extended', sortable: true,  numeric: true,  width: '88px' },
        { key: 'clutch_kills',  label: 'Clutch K', group: 'extended', sortable: true,  numeric: true,  width: '84px' },
        { key: 'sniper_kills',  label: 'Sniper K', group: 'extended', sortable: true,  numeric: true,  width: '84px' },
        { key: 'pistol_kills',  label: 'Pistol K', group: 'extended', sortable: true,  numeric: true,  width: '84px' },
        { key: 'damage',        label: 'Damage',   group: 'special',  sortable: true,  numeric: true,  width: '80px' },
        { key: 'rounds_played', label: 'Rounds',   group: 'special',  sortable: true,  numeric: true,  width: '78px' },
        { key: 'knife_kills',   label: 'Knife K',  group: 'special',  sortable: true,  numeric: true,  width: '76px' },
        { key: 'zeus_kills',    label: 'Zeus K',   group: 'special',  sortable: true,  numeric: true,  width: '74px' },
        { key: '_flash_pct',    label: 'Flash%',   group: 'special',  sortable: true,  numeric: true,  decimals: 1, suffix: '%', width: '74px' },
        { key: '_enemy_flash',  label: 'E/Flash',  group: 'special',  sortable: true,  numeric: true,  decimals: 2, width: '82px' },
        { key: '_cl1v1_pct',    label: '1v1 W%',   group: 'special',  sortable: true,  numeric: true,  decimals: 1, suffix: '%', width: '78px' },
        { key: '_cl1v2_pct',    label: '1v2 W%',   group: 'special',  sortable: true,  numeric: true,  decimals: 1, suffix: '%', width: '78px' },
    ];

    const FIXED_COLUMN_KEYS = ['_rank', 'nickname', 'team_name'];
    const DEFAULT_VISIBLE_STAT_KEYS = [
        'maps_played',
        'kills',
        'assists',
        'deaths',
        'kd',
        'adr',
        'kr',
        'hs_pct',
        'mvps',
        'utility_damage',
        'clutch_kills',
        'sniper_kills',
        'pistol_kills',
    ];

    function derivePlayer(p) {
        return {
            ...p,
            _flash_pct:   p.flash_count      ? (p.flash_successes / p.flash_count)      * 100 : 0,
            _enemy_flash: p.flash_count      ? p.enemies_flashed  / p.flash_count             : 0,
            _cl1v1_pct:   p.cl_1v1_attempts  ? (p.cl_1v1_wins     / p.cl_1v1_attempts) * 100 : 0,
            _cl1v2_pct:   p.cl_1v2_attempts  ? (p.cl_1v2_wins     / p.cl_1v2_attempts) * 100 : 0,
        };
    }

    window.DivisionPlayersTable = {
        name: 'DivisionPlayersTable',
        props: {
            players: { type: Array, default: () => [] },
        },
        data() {
            return {
                selectedPlayerIds: new Set(),
                playerDropdownOpen: false,
                columnDropdownOpen: false,
                dropdownSearch: '',
                columnSearch: '',
                sortKey: 'kd',
                sortDir: 'desc',
                selectedColumnKeys: new Set(DEFAULT_VISIBLE_STAT_KEYS),
                _outsideClickHandler: null,
            };
        },
        computed: {
            teamList() {
                const seen = new Map();
                for (const p of (this.players || [])) {
                    const id = String(p.team_id || '');
                    if (id && !seen.has(id)) {
                        seen.set(id, p.team_name || id);
                    }
                }
                return Array.from(seen.entries())
                    .map(([id, name]) => ({ id, name }))
                    .sort((a, b) => a.name.localeCompare(b.name, 'fi'));
            },
            playersByTeam() {
                const map = {};
                for (const p of (this.players || [])) {
                    const tid = String(p.team_id || '');
                    if (!map[tid]) map[tid] = [];
                    map[tid].push(p);
                }
                return map;
            },
            selectedCount() {
                return this.selectedPlayerIds.size;
            },
            totalCount() {
                return (this.players || []).length;
            },
            playerDropdownSummary() {
                const sel = this.selectedCount;
                const total = this.totalCount;
                if (sel === total) return `Kaikki ${total} pelaajaa`;
                return `${sel} / ${total} pelaajaa`;
            },
            allTeamsState() {
                if (this.selectedCount === 0) return 'none';
                if (this.selectedCount === this.totalCount) return 'all';
                return 'partial';
            },
            dropdownPlayers() {
                const needle = this.dropdownSearch.trim().toLowerCase();
                const list = (this.players || []).slice().sort((a, b) => {
                    const ta = (a.team_name || '').localeCompare(b.team_name || '', 'fi');
                    if (ta !== 0) return ta;
                    return (a.nickname || '').localeCompare(b.nickname || '', 'fi');
                });
                if (!needle) return list;
                return list.filter(p => (p.nickname || '').toLowerCase().includes(needle));
            },
            statColumns() {
                return ALL_COLUMNS.filter(col => !FIXED_COLUMN_KEYS.includes(col.key));
            },
            totalStatCount() {
                return this.statColumns.length;
            },
            selectedStatCount() {
                return this.selectedColumnKeys.size;
            },
            columnDropdownSummary() {
                const selected = this.selectedStatCount;
                const total = this.totalStatCount;
                if (selected === total) return `Kaikki ${total} statia`;
                return `${selected} / ${total} statia`;
            },
            dropdownColumns() {
                const needle = this.columnSearch.trim().toLowerCase();
                if (!needle) return this.statColumns;
                return this.statColumns.filter(col => String(col.label || '').toLowerCase().includes(needle));
            },
            sortedFilteredPlayers() {
                const active = (this.players || [])
                    .filter(p => this.selectedPlayerIds.has(String(p.player_id)))
                    .map(derivePlayer);

                const key = this.sortKey;
                const dir = this.sortDir;

                return active.slice().sort((a, b) => {
                    const av = a[key];
                    const bv = b[key];
                    if (av == null && bv == null) return 0;
                    if (av == null) return 1;
                    if (bv == null) return -1;
                    let cmp;
                    if (typeof av === 'number' && typeof bv === 'number') {
                        cmp = av - bv;
                    } else {
                        cmp = String(av).localeCompare(String(bv), 'fi');
                    }
                    return dir === 'desc' ? -cmp : cmp;
                });
            },
            visibleColumns() {
                return ALL_COLUMNS.filter(col => FIXED_COLUMN_KEYS.includes(col.key) || this.selectedColumnKeys.has(col.key));
            },
        },
        watch: {
            players: {
                immediate: false,
                handler() {
                    this.selectedPlayerIds = new Set();
                },
            },
        },
        methods: {
            teamSelectionState(teamId) {
                const players = this.playersByTeam[teamId] || [];
                if (!players.length) return 'none';
                const selected = players.filter(p => this.selectedPlayerIds.has(String(p.player_id))).length;
                if (selected === 0) return 'none';
                if (selected === players.length) return 'all';
                return 'partial';
            },
            teamStateIcon(state) {
                if (state === 'all') return '✓';
                if (state === 'partial') return '◑';
                return '';
            },
            toggleTeam(teamId) {
                const state = this.teamSelectionState(teamId);
                const players = this.playersByTeam[teamId] || [];
                const newSet = new Set(this.selectedPlayerIds);
                if (state === 'all' || state === 'partial') {
                    players.forEach(p => newSet.delete(String(p.player_id)));
                } else {
                    players.forEach(p => newSet.add(String(p.player_id)));
                }
                this.selectedPlayerIds = newSet;
            },
            toggleAllPlayers() {
                if (this.selectedCount === this.totalCount) {
                    this.selectedPlayerIds = new Set();
                } else {
                    this.selectedPlayerIds = new Set((this.players || []).map(p => String(p.player_id)));
                }
            },
            togglePlayer(playerId) {
                const id = String(playerId);
                const newSet = new Set(this.selectedPlayerIds);
                if (newSet.has(id)) {
                    newSet.delete(id);
                } else {
                    newSet.add(id);
                }
                this.selectedPlayerIds = newSet;
            },
            ensureSortKeyVisible(columnKeys) {
                if (FIXED_COLUMN_KEYS.includes(this.sortKey) || columnKeys.has(this.sortKey)) {
                    return;
                }
                const fallback = DEFAULT_VISIBLE_STAT_KEYS.find(key => columnKeys.has(key))
                    || this.statColumns.find(col => columnKeys.has(col.key))?.key
                    || 'nickname';
                this.sortKey = fallback;
                this.sortDir = fallback === 'nickname' ? 'asc' : 'desc';
            },
            toggleColumn(columnKey) {
                const key = String(columnKey);
                const newSet = new Set(this.selectedColumnKeys);
                if (newSet.has(key)) {
                    newSet.delete(key);
                } else {
                    newSet.add(key);
                }
                this.ensureSortKeyVisible(newSet);
                this.selectedColumnKeys = newSet;
            },
            toggleAllColumns() {
                const newSet = this.selectedStatCount === this.totalStatCount
                    ? new Set()
                    : new Set(this.statColumns.map(col => col.key));
                this.ensureSortKeyVisible(newSet);
                this.selectedColumnKeys = newSet;
            },
            handleSort(col) {
                if (!col.sortable) return;
                if (this.sortKey === col.key) {
                    this.sortDir = this.sortDir === 'desc' ? 'asc' : 'desc';
                } else {
                    this.sortKey = col.key;
                    this.sortDir = col.numeric !== false ? 'desc' : 'asc';
                }
            },
            sortIndicator(col) {
                if (!col.sortable || this.sortKey !== col.key) return '';
                return this.sortDir === 'desc' ? '▼' : '▲';
            },
            formatCell(player, col) {
                const val = player[col.key];
                if (val == null || (typeof val === 'number' && !Number.isFinite(val))) return '–';
                if (col.decimals != null) {
                    const formatted = Number(val).toFixed(col.decimals);
                    return col.suffix ? formatted + col.suffix : formatted;
                }
                return col.suffix ? String(val) + col.suffix : String(val);
            },
            playerRoute(player) {
                const playerId = player?.player_id || null;
                if (!playerId) return null;
                const championshipId =
                    this.$root?.$route?.params?.championshipId ||
                    this.$root?.$route?.query?.championship ||
                    null;
                return {
                    name: 'player',
                    params: { playerId: String(playerId) },
                    query: championshipId ? { championship: String(championshipId) } : {},
                };
            },
            openDropdown() {
                this.playerDropdownOpen = true;
                this.columnDropdownOpen = false;
                this.$nextTick(() => {
                    const input = this.$el?.querySelector('.dp-dropdown__search');
                    if (input) input.focus();
                });
            },
            closeDropdown() {
                this.playerDropdownOpen = false;
                this.dropdownSearch = '';
            },
            openColumnDropdown() {
                this.columnDropdownOpen = true;
                this.playerDropdownOpen = false;
                this.$nextTick(() => {
                    const input = this.$el?.querySelector('.dp-columns-dropdown .dp-dropdown__search');
                    if (input) input.focus();
                });
            },
            closeColumnDropdown() {
                this.columnDropdownOpen = false;
                this.columnSearch = '';
            },
            handleOutsideClick(e) {
                const playerContainer = this.$el?.querySelector('.dp-players-dropdown');
                const columnContainer = this.$el?.querySelector('.dp-columns-dropdown');
                if (playerContainer && !playerContainer.contains(e.target)) {
                    this.closeDropdown();
                }
                if (columnContainer && !columnContainer.contains(e.target)) {
                    this.closeColumnDropdown();
                }
            },
        },
        mounted() {
            this._outsideClickHandler = this.handleOutsideClick.bind(this);
            document.addEventListener('click', this._outsideClickHandler, { passive: true });
        },
        beforeUnmount() {
            if (this._outsideClickHandler) {
                document.removeEventListener('click', this._outsideClickHandler);
            }
        },
        template: `
            <div class="div-players-table">
                <!-- Team toggle chips -->
                <div class="div-players-teams">
                    <button
                        type="button"
                        class="div-players-team-chip"
                        :class="'is-' + allTeamsState"
                        @click="toggleAllPlayers"
                        title="Valitse tai poista kaikki pelaajat"
                    >
                        <span v-if="allTeamsState !== 'none'" class="team-chip-icon">{{ teamStateIcon(allTeamsState) }}</span>
                        Kaikki
                    </button>
                    <button
                        v-for="team in teamList"
                        :key="team.id"
                        type="button"
                        class="div-players-team-chip"
                        :class="'is-' + teamSelectionState(team.id)"
                        :title="team.name"
                        @click="toggleTeam(team.id)"
                    >
                        <span v-if="teamSelectionState(team.id) !== 'none'" class="team-chip-icon">{{ teamStateIcon(teamSelectionState(team.id)) }}</span>
                        {{ team.name }}
                    </button>
                </div>

                <!-- Controls row: player dropdown + column group toggles -->
                <div class="div-players-controls">
                    <div class="dp-dropdown dp-players-dropdown">
                        <button
                            type="button"
                            class="dp-dropdown__trigger chip"
                            @click.stop="playerDropdownOpen ? closeDropdown() : openDropdown()"
                            :aria-expanded="playerDropdownOpen"
                        >
                            {{ playerDropdownSummary }}
                            <span class="dp-dropdown__arrow">{{ playerDropdownOpen ? '▲' : '▾' }}</span>
                        </button>
                        <div v-if="playerDropdownOpen" class="dp-dropdown__panel glass-card" @click.stop>
                            <div class="dp-dropdown__header">
                                <input
                                    v-model="dropdownSearch"
                                    type="text"
                                    class="dp-dropdown__search"
                                    placeholder="Hae pelaajaa..."
                                    autocomplete="off"
                                />
                                <button type="button" class="dp-dropdown__select-all" @click="toggleAllPlayers">
                                    {{ selectedCount === totalCount ? 'Poista kaikki' : 'Valitse kaikki' }}
                                </button>
                            </div>
                            <div class="dp-dropdown__list">
                                <button
                                    v-for="player in dropdownPlayers"
                                    :key="player.player_id"
                                    type="button"
                                    class="dp-dropdown__item"
                                    :class="{ 'is-selected': selectedPlayerIds.has(String(player.player_id)) }"
                                    @click="togglePlayer(player.player_id)"
                                    :aria-pressed="selectedPlayerIds.has(String(player.player_id))"
                                    :title="(selectedPlayerIds.has(String(player.player_id)) ? 'Poista valinta: ' : 'Valitse: ') + player.nickname"
                                >
                                    <span class="dp-dropdown__check">{{ selectedPlayerIds.has(String(player.player_id)) ? '✓' : '' }}</span>
                                    <span class="dp-dropdown__name">{{ player.nickname }}</span>
                                    <span class="dp-dropdown__team">{{ player.team_name }}</span>
                                </button>
                                <p v-if="dropdownPlayers.length === 0" class="dp-dropdown__empty">Ei tuloksia</p>
                            </div>
                        </div>
                    </div>

                    <div class="dp-dropdown dp-columns-dropdown">
                        <button
                            type="button"
                            class="dp-dropdown__trigger chip"
                            @click.stop="columnDropdownOpen ? closeColumnDropdown() : openColumnDropdown()"
                            :aria-expanded="columnDropdownOpen"
                        >
                            {{ columnDropdownSummary }}
                            <span class="dp-dropdown__arrow">{{ columnDropdownOpen ? '▲' : '▾' }}</span>
                        </button>
                        <div v-if="columnDropdownOpen" class="dp-dropdown__panel dp-dropdown__panel--right glass-card" @click.stop>
                            <div class="dp-dropdown__header">
                                <input
                                    v-model="columnSearch"
                                    type="text"
                                    class="dp-dropdown__search"
                                    placeholder="Hae statia..."
                                    autocomplete="off"
                                />
                                <button type="button" class="dp-dropdown__select-all" @click="toggleAllColumns">
                                    {{ selectedStatCount === totalStatCount ? 'Poista kaikki' : 'Valitse kaikki' }}
                                </button>
                            </div>
                            <div class="dp-dropdown__list">
                                <button
                                    v-for="column in dropdownColumns"
                                    :key="column.key"
                                    type="button"
                                    class="dp-dropdown__item"
                                    :class="{ 'is-selected': selectedColumnKeys.has(column.key) }"
                                    @click="toggleColumn(column.key)"
                                    :aria-pressed="selectedColumnKeys.has(column.key)"
                                    :title="(selectedColumnKeys.has(column.key) ? 'Piilota: ' : 'Näytä: ') + column.label"
                                >
                                    <span class="dp-dropdown__check">{{ selectedColumnKeys.has(column.key) ? '✓' : '' }}</span>
                                    <span class="dp-dropdown__name">{{ column.label }}</span>
                                    <span class="dp-dropdown__team">{{ column.group === 'core' ? 'Ydin' : (column.group === 'extended' ? 'Lisä' : 'Erik.') }}</span>
                                </button>
                                <p v-if="dropdownColumns.length === 0" class="dp-dropdown__empty">Ei tuloksia</p>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Table -->
                <p v-if="sortedFilteredPlayers.length === 0" class="division-section__empty">
                    Ei pelaajia valittu.
                </p>
                <div v-else class="table-container">
                    <div class="table-wrapper">
                        <table class="table-sortable sticky-header">
                            <colgroup>
                                <col
                                    v-for="col in visibleColumns"
                                    :key="col.key"
                                    :style="col.width ? 'width:' + col.width : undefined"
                                />
                            </colgroup>
                            <thead>
                                <tr>
                                    <th
                                        v-for="col in visibleColumns"
                                        :key="col.key"
                                        :class="{ sortable: col.sortable, active: sortKey === col.key }"
                                        :style="{ textAlign: col.align || (col.numeric ? 'right' : 'left') }"
                                        @click="handleSort(col)"
                                        :title="col.sortable ? 'Lajittele: ' + col.label : col.label"
                                    >
                                        <span class="th-content">
                                            {{ col.label }}
                                            <span v-if="sortIndicator(col)" class="sort-indicator">{{ sortIndicator(col) }}</span>
                                        </span>
                                    </th>
                                </tr>
                            </thead>
                            <tbody>
                                <tr v-for="(player, idx) in sortedFilteredPlayers" :key="player.player_id">
                                    <td
                                        v-for="col in visibleColumns"
                                        :key="col.key"
                                        :style="{ textAlign: col.align || (col.numeric ? 'right' : 'left') }"
                                    >
                                        <template v-if="col.key === '_rank'">{{ idx + 1 }}</template>
                                        <template v-else-if="col.key === 'nickname'">
                                            <router-link
                                                v-if="playerRoute(player)"
                                                :to="playerRoute(player)"
                                                class="player-link"
                                            >{{ player.nickname }}</router-link>
                                            <span v-else>{{ player.nickname }}</span>
                                        </template>
                                        <template v-else>{{ formatCell(player, col) }}</template>
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `,
    };
})();
