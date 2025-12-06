// SortableTable Component - Table with sorting functionality
window.SortableTable = {
    name: 'SortableTable',
    props: {
        columns: {
            type: Array,
            required: true,
            // Format: [{ key: 'name', label: 'Name', sortable: true, numeric: false, align: 'left' }]
        },
        data: {
            type: Array,
            required: true
        },
        defaultSort: {
            type: Object,
            default: null
            // Format: { column: 'name', order: 'asc' }
        },
        colorizeColumns: {
            type: Array,
            default: () => []
            // Format: ['column1', 'column2'] - columns to apply gradient colorization
        },
        stickyHeader: {
            type: Boolean,
            default: false
        },
        compact: {
            type: Boolean,
            default: false
        },
        sortReady: {
            type: Boolean,
            default: true
        },
        highlightRowId: {
            type: [String, Number],
            default: null
        },
        highlightRowClass: {
            type: String,
            default: 'table-row--highlighted'
        }
    },
    setup(props) {
        // Import composables
        const dataSource = Vue.computed(() => props.data);
        const { sortData, currentSort, toggleSort, applyDefaultSort, sortedData } = window.useTableSort(dataSource, null);
        const { colorizeColumn, getCellStyle } = window.useCellColorization();

        const hasAppliedDefault = Vue.ref(false);

        const runSortPipeline = (rows) => {
            if (!Array.isArray(rows)) {
                console.debug('[SortableTable] runSortPipeline: rows not array, resetting');
                sortedData.value = [];
                return;
            }

            if (!rows.length) {
                console.debug('[SortableTable] runSortPipeline: rows empty, waiting for data');
                sortedData.value = [];
                return;
            }

            if (!hasAppliedDefault.value && props.defaultSort) {
                console.debug('[SortableTable] runSortPipeline: applying default sort', props.defaultSort);
                sortedData.value = applyDefaultSort(
                    [...rows],
                    props.defaultSort.column,
                    props.defaultSort.numeric || false,
                    props.defaultSort.order || 'asc'
                );
                hasAppliedDefault.value = true;
                return;
            }

            if (hasAppliedDefault.value || (currentSort.value && currentSort.value.column)) {
                console.debug('[SortableTable] runSortPipeline: reapplying existing sort', currentSort.value);
                sortedData.value = sortData([...rows]);
                return;
            }

            console.debug('[SortableTable] runSortPipeline: no sort applied, using incoming order');
            sortedData.value = [...rows];
        };

        Vue.watch(dataSource, (newVal) => {
            console.debug('[SortableTable] dataSource change', {
                rows: Array.isArray(newVal) ? newVal.length : null,
                defaultSort: props.defaultSort,
                hasAppliedDefault: hasAppliedDefault.value,
                currentSort: currentSort.value
            });
            if (!Array.isArray(newVal)) {
                console.debug('[SortableTable] value is not array; resetting');
                hasAppliedDefault.value = false;
                sortedData.value = [];
                return;
            }

            if (!newVal.length) {
                console.debug('[SortableTable] array empty; waiting for data');
                sortedData.value = [];
                return;
            }

            if (!props.sortReady) {
                console.debug('[SortableTable] sortReady false; skipping sorting pass');
                sortedData.value = [...newVal];
                return;
            }

            runSortPipeline(newVal);
        }, { immediate: true, deep: true });

        Vue.watch(() => props.sortReady, (ready) => {
            console.debug('[SortableTable] sortReady change detected', { ready, hasAppliedDefault: hasAppliedDefault.value });
            if (!ready) return;

            const currentRows = Array.isArray(props.data) ? props.data : [];
            if (!currentRows.length) return;

            runSortPipeline(currentRows);
        });

        return {
            sortData,
            currentSort,
            toggleSort,
            colorizeColumn,
            getCellStyle,
            sortedData,
            hasAppliedDefault
        };
    },
    computed: {
        sortedRows() {
            if (this.sortedData && this.sortedData.length) {
                return this.sortedData;
            }
            return Array.isArray(this.data) ? this.data : [];
        },
        tableClass() {
            const classes = ['table-sortable'];
            if (this.stickyHeader) classes.push('sticky-header');
            if (this.compact) classes.push('compact');
            return classes.join(' ');
        },
        // Pre-calculate colorization data for specified columns
        colorizedColumns() {
            const result = {};
            this.colorizeColumns.forEach(colKey => {
                const values = this.data.map(row => row[colKey]).filter(v => v != null);
                if (values.length > 0) {
                    result[colKey] = this.colorizeColumn(values);
                }
            });
            return result;
        }
    },
    methods: {
        handleSort(column) {
            if (column.sortable !== false) {
                console.debug('[SortableTable] manual column sort', { column: column.key, numeric: column.numeric });
                // Call sortData with current props data to avoid stale internal references
                this.sortData([...this.data], column.key, column.numeric);
                this.hasAppliedDefault = true;
            }
        },
        getSortIndicator(columnKey) {
            // currentSort may be undefined initially; guard against it
            if (!this.currentSort || !this.currentSort.column) return '';
            if (this.currentSort.column === columnKey) {
                return this.currentSort.order === 'asc' ? '▲' : '▼';
            }
            return '';
        },
        getHeaderClass(column) {
            const classes = [];
            if (column.sortable !== false) classes.push('sortable');
            // Guard: currentSort may be undefined while sorting initialises
            if (this.currentSort && this.currentSort.column === column.key) classes.push('active');
            return classes.join(' ');
        },
        getCellClass(column, row) {
            const classes = [];
            
            // Apply colorization if column is in colorizeColumns list
            if (this.colorizeColumns.includes(column.key) && this.colorizedColumns[column.key]) {
                const value = row[column.key];
                const colorData = this.colorizedColumns[column.key];
                const quartile = this.getQuartile(value, colorData);
                
                if (quartile === 4) classes.push('cell-excellent');
                else if (quartile === 3) classes.push('cell-grad', 'good');
                else if (quartile === 2) classes.push('cell-moderate');
                else if (quartile === 1) classes.push('cell-grad', 'bad');
            }
            
            return classes.join(' ');
        },
        getQuartile(value, colorData) {
            if (value == null) return 0;
            if (value >= colorData.p75) return 4;
            if (value >= colorData.p50) return 3;
            if (value >= colorData.p25) return 2;
            return 1;
        },
        getCellAlign(column) {
            // Default alignment rules:
            // 1. If column.align is explicitly provided, use it.
            // 2. Keep 'nickname' or columns marked with colClass 'col-name' left-aligned.
            // 3. Otherwise default to 'center' for all other columns (including numeric).
            if (column && column.align) return column.align;
            if (column && (column.key === 'nickname' || (column.colClass && column.colClass.indexOf('col-name') !== -1))) return 'left';
            return 'center';
        },
        formatCell(value, column) {
            if (value == null) return '-';
            if (column.format) return column.format(value);
            if (column.numeric && typeof value === 'number') {
                return column.decimals != null ? value.toFixed(column.decimals) : value;
            }
            return value;
        },
        resolveRowId(row, idx) {
            if (!row) return String(idx);
            if (row.id != null) return String(row.id);
            if (row.key != null) return String(row.key);
            if (row.team_id != null) return String(row.team_id);
            return String(idx);
        },
        isRowHighlighted(rowId) {
            if (this.highlightRowId == null) return false;
            return String(rowId) === String(this.highlightRowId);
        },
        getRowClass(row, idx) {
            const rowId = this.resolveRowId(row, idx);
            const classes = [];
            if (this.isRowHighlighted(rowId)) {
                classes.push(this.highlightRowClass);
            }
            return classes.join(' ');
        }
    },
    template: `
        <div class="table-container">
            <table :class="tableClass">
                <colgroup>
                    <col v-for="column in columns" :key="column.key" :style="column.width ? ('width:' + column.width) : null" :class="column.colClass || ''" />
                </colgroup>
                <thead>
                    <tr>
                        <th 
                            v-for="column in columns" 
                            :key="column.key"
                            :class="getHeaderClass(column)"
                            :style="{ textAlign: getCellAlign(column) }"
                            @click="handleSort(column)"
                            v-bind:data-sortable="column.sortable !== false ? true : null"
                            v-bind:data-sort-dir="(currentSort && currentSort.column === column.key) ? currentSort.order : null"
                            :title="column.sortable !== false ? 'Click to sort' : ''"
                        >
                            <span class="th-content">
                                {{ column.label }}
                                <span v-if="column.sortable !== false && getSortIndicator(column.key)" class="sort-indicator">
                                    {{ getSortIndicator(column.key) }}
                                </span>
                            </span>
                        </th>
                    </tr>
                </thead>
                <tbody>
                    <tr
                        v-for="(row, idx) in sortedRows"
                        :key="row.id || idx"
                        :class="getRowClass(row, idx)"
                        :data-row-id="resolveRowId(row, idx)"
                    >
                        <td 
                            v-for="column in columns" 
                            :key="column.key"
                            :class="[getCellClass(column, row), column.colClass || '']"
                            :style="{ textAlign: getCellAlign(column) }"
                        >
                            <!-- Special-case map_name column to use a static named slot and inline image rendering -->
                            <template v-if="column.key === 'map_name'">
                                <slot name="cell-map_name" :row="row" :value="row[column.key]">
                                    <div class="map-name">
                                        <img v-if="row.logo" :src="row.logo" alt="" class="map-logo" />
                                        {{ formatCell(row[column.key], column) }}
                                    </div>
                                </slot>
                            </template>
                            <template v-else>
                                <slot :name="'cell-' + column.key" :row="row" :value="row[column.key]">
                                    {{ formatCell(row[column.key], column) }}
                                </slot>
                            </template>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    `
};

