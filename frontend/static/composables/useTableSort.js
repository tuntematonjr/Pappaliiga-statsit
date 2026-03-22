/**
 * useTableSort.js
 * Vue composable for table sorting functionality
 * 
 * Migrated from web_static/app.js sortTable() function
 * Provides sorting capabilities with asc/desc toggle, numeric/string support,
 * and visual indicators via data attributes
 */

/**
 * Composable for table sorting functionality
 * 
 * @returns {Object} Sorting state and methods
 */
window.useTableSort = function(initialData = [], defaultSort = null) {
	// Use Vue globals (scoped to function to avoid redeclaration)
	const { ref, computed, watch, unref } = Vue;

	// Support passing a ref/computed as the data source
	const isRefSource = initialData && typeof initialData === 'object' && 'value' in initialData;
	const dataSource = isRefSource ? initialData : ref(initialData || []);

    // Sorting state
    const sortColumn = ref(null);
    const sortDirection = ref('asc');
    const sortNumeric = ref(false);
    const sortedData = ref([]);

	/**
	 * Sort table data by column
	 * 
	 * @param {Array} data - Array of objects to sort
	 * @param {String} columnKey - Key to sort by
	 * @param {Boolean} isNumeric - Whether to sort as numbers (default: false)
	 * @returns {Array} Sorted array
	 */
    const sortData = (data = unref(dataSource), columnKey = null, isNumeric) => {
        if (!data || !Array.isArray(data)) {
            console.warn('[useTableSort] Invalid data provided');
            return [];
        }

        // If columnKey not provided, use currently selected column
        const key = columnKey || sortColumn.value;
        if (!key) {
            // Nothing to sort by; just return original
            sortedData.value = [...data];
            return sortedData.value;
        }

        let numeric;
        if (columnKey == null) {
            numeric = sortNumeric.value;
        } else if (isNumeric === undefined || isNumeric === null) {
            numeric = false;
        } else {
            numeric = !!isNumeric;
        }

        // Toggle direction if same column, otherwise default to ascending
        if (sortColumn.value === key && columnKey) {
            // If explicit column requested and it's the same, toggle
            sortDirection.value = sortDirection.value === 'asc' ? 'desc' : 'asc';
        } else if (columnKey) {
            sortColumn.value = key;
            sortDirection.value = 'asc';
        }

        if (columnKey) {
            sortNumeric.value = numeric;
        }

        // Create shallow copy to avoid mutating original
        const sorted = [...data].sort((a, b) => {
            let valA = a[columnKey];
            let valB = b[columnKey];

            // If using inferred key variable, fall back
            if (columnKey == null) {
                valA = a[key];
                valB = b[key];
            }

			// Handle null/undefined values
			if (valA == null && valB == null) return 0;
			if (valA == null) return 1;
			if (valB == null) return -1;

			// Numeric comparison
            if (numeric) {
                valA = parseFloat(valA);
                valB = parseFloat(valB);

				// Handle NaN
				if (isNaN(valA) && isNaN(valB)) return 0;
				if (isNaN(valA)) return 1;
				if (isNaN(valB)) return -1;

				const diff = valA - valB;
				return sortDirection.value === 'asc' ? diff : -diff;
			}

			// String comparison (case-insensitive)
			const strA = String(valA).toLowerCase();
			const strB = String(valB).toLowerCase();

			if (strA < strB) return sortDirection.value === 'asc' ? -1 : 1;
			if (strA > strB) return sortDirection.value === 'asc' ? 1 : -1;
			return 0;
		});

		sortedData.value = sorted;
        return sorted;
    };

	/**
	 * Reset sorting state
	 */
    const resetSort = () => {
        sortColumn.value = null;
        sortDirection.value = 'asc';
        sortNumeric.value = false;
        sortedData.value = [];
    };

	/**
	 * Get sort attributes for <th> elements
	 * Used with data-sortable, data-sort-dir attributes for CSS styling
	 * 
	 * @param {String} columnKey - Column key
	 * @returns {Object} Attributes object { 'data-sortable': true, 'data-sort-dir': 'asc'|'desc'|null }
	 */
	const getSortAttributes = (columnKey) => {
		const attrs = {
			'data-sortable': true,
			'data-sort-dir': sortColumn.value === columnKey ? sortDirection.value : null
		};
		return attrs;
	};

	/**
	 * Check if column is currently sorted
	 * 
	 * @param {String} columnKey - Column key
	 * @returns {Boolean}
	 */
	const isSorted = (columnKey) => {
		return sortColumn.value === columnKey;
	};

	/**
	 * Get sort indicator for UI (arrows)
	 * 
	 * @param {String} columnKey - Column key
	 * @returns {String} Sort indicator: '⇅' (sortable), '↑' (asc), '↓' (desc)
	 */
	const getSortIndicator = (columnKey) => {
		if (sortColumn.value !== columnKey) {
			return '⇅'; // Sortable but not currently sorted
		}
		return sortDirection.value === 'asc' ? '↑' : '↓';
	};

	/**
	 * Apply default sort to data (e.g., on component mount)
	 * 
	 * @param {Array} data - Data to sort
	 * @param {String} columnKey - Default sort column
	 * @param {Boolean} isNumeric - Whether to sort as numbers
	 * @param {String} direction - Default direction ('asc' or 'desc')
	 * @returns {Array} Sorted data
	 */
    const applyDefaultSort = (data, columnKey, isNumeric = false, direction = 'asc') => {
        sortColumn.value = columnKey;
        sortDirection.value = direction;
        sortNumeric.value = !!isNumeric;
        return sortData(data);
    };

	// Computed properties
	const currentColumn = computed(() => sortColumn.value);
	const currentDirection = computed(() => sortDirection.value);
	const hasSortedData = computed(() => sortedData.value.length > 0);

	// Compose a currentSort object for compatibility with the component
    const currentSort = computed(() => ({ column: sortColumn.value, order: sortDirection.value, numeric: sortNumeric.value }));

	// Toggle sort (component-friendly API)
    const toggleSort = (columnKey, isNumeric = false) => {
        // Call sortData with explicit columnKey to toggle or set
        return sortData(unref(dataSource), columnKey, isNumeric);
    };

    // If defaultSort provided, apply it initially
    if (defaultSort && defaultSort.column) {
        try {
            applyDefaultSort(unref(dataSource), defaultSort.column, defaultSort.numeric || false, defaultSort.order || 'asc');
        } catch (e) {
            // ignore
        }
    }

    // Watch the provided input data (initialData) and reapply current sort when it changes.
    // This is more reliable when the caller passes a reactive prop (e.g., props.data).
    watch(
        () => unref(dataSource),
        (newVal) => {
            if (!newVal) {
                sortedData.value = [];
                return;
            }
            if (sortColumn.value) {
                sortData(newVal);
            } else if (defaultSort && defaultSort.column) {
                applyDefaultSort(newVal, defaultSort.column, defaultSort.numeric || false, defaultSort.order || 'asc');
            } else {
                sortedData.value = Array.isArray(newVal) ? [...newVal] : [];
            }
        },
        { deep: true, immediate: true }
    );

	return {
		// State
		sortColumn,
		sortDirection,
		sortedData,
		// Computed
		currentColumn,
		currentDirection,
		hasSortedData,
		currentSort,

		// Methods
		sortData,
		toggleSort,
		resetSort,
		getSortAttributes,
		isSorted,
		getSortIndicator,
		applyDefaultSort
	};
}

/**
 * Usage Example:
 * 
 * <script setup>
 * import { ref, onMounted } from 'vue';
 * import { useTableSort } from './composables/useTableSort.js';
 * 
 * const rawData = ref([
 *   { name: 'Team A', wins: 10, losses: 5 },
 *   { name: 'Team B', wins: 8, losses: 7 }
 * ]);
 * 
 * const { sortData, getSortAttributes, getSortIndicator, applyDefaultSort } = useTableSort();
 * 
 * // Apply default sort on mount
 * onMounted(() => {
 *   const sorted = applyDefaultSort(rawData.value, 'wins', true, 'desc');
 *   displayData.value = sorted;
 * });
 * 
 * // Handle column click
 * const handleSort = (columnKey, isNumeric) => {
 *   const sorted = sortData(rawData.value, columnKey, isNumeric);
 *   displayData.value = sorted;
 * };
 * </script>
 * 
 * <template>
 *   <table>
 *     <thead>
 *       <tr>
 *         <th v-bind="getSortAttributes('name')" @click="handleSort('name', false)">
 *           Team {{ getSortIndicator('name') }}
 *         </th>
 *         <th v-bind="getSortAttributes('wins')" @click="handleSort('wins', true)">
 *           Wins {{ getSortIndicator('wins') }}
 *         </th>
 *       </tr>
 *     </thead>
 *     <tbody>
 *       <tr v-for="row in displayData" :key="row.name">
 *         <td>{{ row.name }}</td>
 *         <td>{{ row.wins }}</td>
 *       </tr>
 *     </tbody>
 *   </table>
 * </template>
 */
