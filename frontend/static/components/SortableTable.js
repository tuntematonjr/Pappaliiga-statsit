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
        },
        headerGroups: {
            type: Array,
            default: () => []
            // Format: [{ label: 'Group', colSpan: 3, className: 'group-class' }]
        },
        collapsedGroups: {
            type: Set,
            default: () => new Set()
        },
        onToggleGroup: {
            type: Function,
            default: null
        },
        mobileColumnLimit: {
            type: Number,
            default: null
        },
        mobileBreakpoint: {
            type: Number,
            default: 768
        }
    },
    setup(props) {
        // Import composables
        const dataSource = Vue.computed(() => props.data);
        const { sortData, currentSort, toggleSort, applyDefaultSort, sortedData } = window.useTableSort(dataSource, null);
        const { colorizeColumn, getCellStyle } = window.useCellColorization();

        const hasAppliedDefault = Vue.ref(false);
        const isMobile = Vue.ref(window.innerWidth <= props.mobileBreakpoint);
        
        const hasDefaultColumn = Vue.computed(() => {
            if (!props.defaultSort || !Array.isArray(props.columns)) return false;
            return props.columns.some(col => col.key === props.defaultSort.column);
        });

        const visibleColumns = Vue.computed(() => {
            let cols = props.columns;
            
            // Filter by collapsed groups only
            if (props.collapsedGroups && props.collapsedGroups.size > 0) {
                cols = cols.filter(col => !props.collapsedGroups.has(col.group));
            }

            if (!isMobile.value) {
                return cols;
            }

            const responsiveCols = cols.filter(col => col.mobileHidden !== true);
            const hasMobileMetadata = responsiveCols.some(col => col.mobilePinned || Number.isFinite(col.mobilePriority));
            const mobileLimit = Number.isFinite(props.mobileColumnLimit) && props.mobileColumnLimit > 0
                ? props.mobileColumnLimit
                : null;

            if (!mobileLimit && !hasMobileMetadata) {
                return responsiveCols;
            }

            const limit = mobileLimit ? Math.max(mobileLimit, 1) : responsiveCols.length;
            const pinnedKeys = new Set(
                responsiveCols
                    .filter(col => col.mobilePinned)
                    .map(col => col.key)
            );
            const prioritized = responsiveCols
                .filter(col => !pinnedKeys.has(col.key))
                .map((col, index) => ({
                    col,
                    index,
                    priority: Number.isFinite(col.mobilePriority) ? col.mobilePriority : Number.MAX_SAFE_INTEGER
                }))
                .sort((left, right) => {
                    if (left.priority !== right.priority) return left.priority - right.priority;
                    return left.index - right.index;
                });
            const selectedKeys = new Set(Array.from(pinnedKeys));
            const remainingSlots = Math.max(0, limit - selectedKeys.size);

            prioritized.slice(0, remainingSlots).forEach(item => selectedKeys.add(item.col.key));

            return responsiveCols.filter(col => selectedKeys.has(col.key));
        });

        const visibleHeaderGroups = Vue.computed(() => {
            if (!Array.isArray(props.headerGroups) || !props.headerGroups.length) {
                return [];
            }

            const groupMeta = new Map(props.headerGroups.map(group => [group.key, group]));
            const groups = [];

            visibleColumns.value.forEach(column => {
                const key = column.group || '__ungrouped';
                const meta = groupMeta.get(key) || {};
                const last = groups[groups.length - 1];
                if (!last || last.key !== key) {
                    groups.push({
                        key,
                        label: meta.label || '',
                        className: meta.className || '',
                        colSpan: 1
                    });
                    return;
                }
                last.colSpan += 1;
            });

            return groups.filter(group => group.colSpan > 0);
        });

        // Update mobile state on resize
        const handleResize = () => {
            isMobile.value = window.innerWidth <= props.mobileBreakpoint;
        };
        
        Vue.onMounted(() => {
            window.addEventListener('resize', handleResize);
        });
        
        Vue.onUnmounted(() => {
            window.removeEventListener('resize', handleResize);
        });

        const runSortPipeline = (rows) => {
            if (!Array.isArray(rows)) {
                sortedData.value = [];
                return;
            }

            if (!rows.length) {
                sortedData.value = [];
                return;
            }

            if (!hasAppliedDefault.value && props.defaultSort && hasDefaultColumn.value) {
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
                sortedData.value = sortData([...rows]);
                return;
            }

            sortedData.value = [...rows];
        };

        Vue.watch(dataSource, (newVal) => {
            if (!Array.isArray(newVal)) {
                hasAppliedDefault.value = false;
                sortedData.value = [];
                return;
            }

            if (!newVal.length) {
                sortedData.value = [];
                return;
            }

            if (!props.sortReady) {
                sortedData.value = [...newVal];
                return;
            }

            runSortPipeline(newVal);
        }, { immediate: true, deep: true });

        Vue.watch(() => props.sortReady, (ready) => {
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
            hasAppliedDefault,
            hasDefaultColumn,
            visibleColumns,
            visibleHeaderGroups,
            isMobile
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
            if (this.headerGroups && this.headerGroups.length) classes.push('has-group-header');
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
        isDefaultSortColumn(column) {
            if (!this.defaultSort || !column) return false;
            return column.key === this.defaultSort.column;
        },
        getHeaderTooltip(column) {
            if (!column) return '';
            const base = column.tooltip || '';
            if (column.sortable === false) return base;
            const sortTip = `Lajittele: ${column.label}. Klikkaa vaihtaaksesi nouseva/laskeva.`;
            if (base) return `${base} · ${sortTip}`;
            return sortTip;
        },
        getAriaSort(column) {
            if (!column || column.sortable === false) return null;
            if (!this.currentSort || !this.currentSort.column || this.currentSort.column !== column.key) {
                return 'none';
            }
            return this.currentSort.order === 'asc' ? 'ascending' : 'descending';
        },
        handleSort(column) {
            if (column.sortable !== false) {
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
            if (this.isDefaultSortColumn(column)) classes.push('is-default');
            return classes.join(' ');
        },
        getColumnLabel(column) {
            if (!column) return '';
            if (this.isMobile && typeof column.mobileLabel === 'string' && column.mobileLabel) {
                return column.mobileLabel;
            }
            return column.label;
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
        getCellTooltip(column, row) {
            if (!column) return '';
            if (typeof column.cellTooltip === 'function') {
                try {
                    return column.cellTooltip(row, column) || '';
                } catch (_err) {
                    return '';
                }
            }
            if (typeof column.cellTooltip === 'string') return column.cellTooltip;
            if (typeof column.tooltip === 'string') return column.tooltip;
            return '';
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
        isGroupCollapsed(groupKey) {
            return this.collapsedGroups && this.collapsedGroups.has(groupKey);
        },
        handleGroupToggle(groupKey) {
            if (this.onToggleGroup) {
                this.onToggleGroup(groupKey);
            }
        },
        getGroupClass(group) {
            const classes = [group.className || ''];
            if (group.key && this.isGroupCollapsed(group.key)) {
                classes.push('table-group-header--collapsed');
            }
            if (group.key && this.onToggleGroup) {
                classes.push('table-group-header--collapsible');
            }
            return classes.filter(Boolean).join(' ');
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
            <div class="table-wrapper">
                <table :class="tableClass">
                    <colgroup>
                        <col v-for="column in visibleColumns" :key="column.key" :style="column.width ? ('width:' + column.width) : null" :class="column.colClass || ''" />
                    </colgroup>
                    <thead>
                        <tr v-if="visibleHeaderGroups && visibleHeaderGroups.length" class="table-group-header">
                            <th
                                v-for="(group, idx) in visibleHeaderGroups"
                                :key="group.label + '-' + idx"
                                :colspan="group.colSpan"
                                :class="getGroupClass(group)"
                                v-bind:data-group="group.key || null"
                                @click="group.key && onToggleGroup ? handleGroupToggle(group.key) : null"
                                :style="{ cursor: (group.key && onToggleGroup) ? 'pointer' : 'default' }"
                            >
                                <span class="group-label-content">
                                    <span v-if="group.key && onToggleGroup" class="group-collapse-icon">
                                        {{ isGroupCollapsed(group.key) ? '▶' : '▼' }}
                                    </span>
                                    {{ group.label }}
                                </span>
                            </th>
                        </tr>
                        <tr>
                            <th 
                                v-for="column in visibleColumns" 
                                :key="column.key"
                                :class="getHeaderClass(column)"
                                :style="{ textAlign: getCellAlign(column) }"
                                @click="handleSort(column)"
                                v-bind:data-sortable="column.sortable !== false ? true : null"
                                v-bind:data-sort-dir="(currentSort && currentSort.column === column.key) ? currentSort.order : null"
                                v-bind:data-key="column.key"
                                v-bind:data-label="column.label"
                                :title="getHeaderTooltip(column)"
                                :aria-sort="getAriaSort(column)"
                            >
                                <span class="th-content">
                                    {{ getColumnLabel(column) }}
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
                                v-for="column in visibleColumns" 
                                :key="column.key"
                                :class="[getCellClass(column, row), column.colClass || '']"
                                :style="{ textAlign: getCellAlign(column) }"
                                :data-label="getColumnLabel(column)"
                                :title="getCellTooltip(column, row)"
                            >
                                <!-- Special-case map_name column to use a static named slot and inline image rendering -->
                                <template v-if="column.key === 'map_name'">
                                    <slot name="cell-map_name" :row="row" :value="row[column.key]">
                                        <div class="map-name">
                                            <img v-if="row.logo" :src="row.logo" alt="" class="map-logo" loading="lazy" />
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
        </div>
    `
};

