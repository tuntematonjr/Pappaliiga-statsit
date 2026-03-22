/**
 * useCellColorization.js
 * Vue composable for continuous cell colorization based on quartile distribution
 * 
 * Migrated from web_static/app.js colorizeContinuous() function
 * Applies gradient background colors to table cells based on value distribution
 */

/**
 * Composable for continuous cell colorization
 * 
 * @returns {Object} Colorization methods
 */
window.useCellColorization = function() {
	// Use Vue globals (scoped to function to avoid redeclaration)
	const { computed } = Vue;

	/**
	 * Interpolate between two colors
	 * 
	 * @param {String} color1 - Start color (hex)
	 * @param {String} color2 - End color (hex)
	 * @param {Number} factor - Interpolation factor (0-1)
	 * @returns {String} Interpolated hex color
	 */
	const interpolateColor = (color1, color2, factor) => {
		const c1 = parseInt(color1.slice(1), 16);
		const c2 = parseInt(color2.slice(1), 16);
		
		const r1 = (c1 >> 16) & 0xff;
		const g1 = (c1 >> 8) & 0xff;
		const b1 = c1 & 0xff;
		
		const r2 = (c2 >> 16) & 0xff;
		const g2 = (c2 >> 8) & 0xff;
		const b2 = c2 & 0xff;
		
		const r = Math.round(r1 + (r2 - r1) * factor);
		const g = Math.round(g1 + (g2 - g1) * factor);
		const b = Math.round(b1 + (b2 - b1) * factor);
		
		return `#${((1 << 24) + (r << 16) + (g << 8) + b).toString(16).slice(1)}`;
	};

	/**
	 * Calculate quartiles from data array
	 * 
	 * @param {Array} values - Array of numeric values
	 * @returns {Object} { p25, p50, p75 } Quartile values
	 */
	const calculateQuartiles = (values) => {
		if (!values || values.length === 0) {
			return { p25: 0, p50: 0, p75: 0 };
		}

		const sorted = [...values].sort((a, b) => a - b);
		const len = sorted.length;

		const getPercentile = (p) => {
			const index = (p / 100) * (len - 1);
			const lower = Math.floor(index);
			const upper = Math.ceil(index);
			const weight = index % 1;
			
			if (lower === upper) return sorted[lower];
			return sorted[lower] * (1 - weight) + sorted[upper] * weight;
		};

		return {
			p25: getPercentile(25),
			p50: getPercentile(50),
			p75: getPercentile(75)
		};
	};

	/**
	 * Get cell background color based on value and quartiles
	 * 
	 * @param {Number} value - Cell value
	 * @param {Object} quartiles - { p25, p50, p75 }
	 * @param {Boolean} inverse - Invert color scale (lower = better)
	 * @returns {String} CSS color value (rgba)
	 */
	const getCellColor = (value, quartiles, inverse = false) => {
		const { p25, p50, p75 } = quartiles;
		const val = parseFloat(value);

		if (isNaN(val)) {
			return 'transparent';
		}

		// Define color scale (green = good, red = bad)
		const colors = {
			excellent: 'rgba(34, 197, 94, 0.25)',  // Green
			good: 'rgba(58, 163, 255, 0.18)',       // Blue
			moderate: 'rgba(244, 191, 79, 0.18)',   // Yellow
			poor: 'rgba(239, 68, 68, 0.22)'         // Red
		};

		let color;
		
		if (inverse) {
			// Lower values are better (e.g., deaths, loss%)
			if (val <= p25) {
				color = colors.excellent;
			} else if (val <= p50) {
				color = colors.good;
			} else if (val <= p75) {
				color = colors.moderate;
			} else {
				color = colors.poor;
			}
		} else {
			// Higher values are better (e.g., kills, win%)
			if (val >= p75) {
				color = colors.excellent;
			} else if (val >= p50) {
				color = colors.good;
			} else if (val >= p25) {
				color = colors.moderate;
			} else {
				color = colors.poor;
			}
		}

		return color;
	};

	/**
	 * Get gradient background style for cell
	 * 
	 * @param {Number} value - Cell value
	 * @param {Object} quartiles - { p25, p50, p75 }
	 * @param {Boolean} inverse - Invert color scale
	 * @returns {Object} Style object for Vue binding
	 */
	const getCellStyle = (value, quartiles, inverse = false) => {
		const color = getCellColor(value, quartiles, inverse);
		
		return {
			background: `linear-gradient(90deg, ${color}, transparent)`,
			transition: 'background 0.3s ease'
		};
	};

	/**
	 * Colorize entire column of data
	 * 
	 * @param {Array} data - Array of row objects
	 * @param {String} columnKey - Key to extract values
	 * @param {Boolean} inverse - Invert color scale
	 * @returns {Array} Array of style objects for each row
	 */
	const colorizeColumn = (data, columnKey, inverse = false) => {
		if (!data || !Array.isArray(data)) {
			return [];
		}

		// Extract values and calculate quartiles
		const values = data
			.map(row => parseFloat(row[columnKey]))
			.filter(val => !isNaN(val));

		if (values.length === 0) {
			return data.map(() => ({ background: 'transparent' }));
		}

		const quartiles = calculateQuartiles(values);

		// Generate styles for each row
		return data.map(row => {
			const value = parseFloat(row[columnKey]);
			return isNaN(value) 
				? { background: 'transparent' }
				: getCellStyle(value, quartiles, inverse);
		});
	};

	/**
	 * Process multiple columns for colorization
	 * 
	 * @param {Array} data - Array of row objects
	 * @param {Object} columnConfig - { columnKey: { inverse: boolean } }
	 * @returns {Object} { [columnKey]: Array<StyleObject> }
	 */
	const colorizeMultipleColumns = (data, columnConfig) => {
		const result = {};

		for (const [columnKey, config] of Object.entries(columnConfig)) {
			const inverse = config.inverse || false;
			result[columnKey] = colorizeColumn(data, columnKey, inverse);
		}

		return result;
	};

	/**
	 * Get text color class based on value comparison to median
	 * 
	 * @param {Number} value - Cell value
	 * @param {Number} median - Median value (p50)
	 * @param {Boolean} inverse - Invert color logic
	 * @returns {String} CSS class name
	 */
	const getTextColorClass = (value, median, inverse = false) => {
		const val = parseFloat(value);
		if (isNaN(val)) return 'cell-muted';

		if (inverse) {
			return val < median ? 'stat-positive' : 'stat-negative';
		} else {
			return val > median ? 'stat-positive' : 'stat-negative';
		}
	};

	/**
	 * Create color scale legend data
	 * 
	 * @param {Object} quartiles - { p25, p50, p75 }
	 * @param {Boolean} inverse - Invert scale
	 * @returns {Array} Legend items [{ label, color, value }]
	 */
	const createLegend = (quartiles, inverse = false) => {
		const { p25, p50, p75 } = quartiles;

		const items = [
			{ label: 'Excellent', color: 'rgba(34, 197, 94, 0.25)', value: inverse ? `≤ ${p25.toFixed(1)}` : `≥ ${p75.toFixed(1)}` },
			{ label: 'Good', color: 'rgba(58, 163, 255, 0.18)', value: inverse ? `≤ ${p50.toFixed(1)}` : `≥ ${p50.toFixed(1)}` },
			{ label: 'Moderate', color: 'rgba(244, 191, 79, 0.18)', value: inverse ? `≤ ${p75.toFixed(1)}` : `≥ ${p25.toFixed(1)}` },
			{ label: 'Poor', color: 'rgba(239, 68, 68, 0.22)', value: inverse ? `> ${p75.toFixed(1)}` : `< ${p25.toFixed(1)}` }
		];

		return items;
	};

	return {
		// Core colorization
		getCellColor,
		getCellStyle,
		colorizeColumn,
		colorizeMultipleColumns,

		// Utilities
		calculateQuartiles,
		interpolateColor,
		getTextColorClass,
		createLegend
	};
}

/**
 * Usage Example:
 * 
 * <script setup>
 * import { ref, computed } from 'vue';
 * import { useCellColorization } from './composables/useCellColorization.js';
 * 
 * const { colorizeColumn, calculateQuartiles, getCellStyle } = useCellColorization();
 * 
 * const playerData = ref([
 *   { name: 'Player 1', kd: 1.25, deaths: 45 },
 *   { name: 'Player 2', kd: 0.95, deaths: 52 },
 *   { name: 'Player 3', kd: 1.42, deaths: 38 }
 * ]);
 * 
 * // Colorize K/D column (higher is better)
 * const kdStyles = computed(() => colorizeColumn(playerData.value, 'kd', false));
 * 
 * // Colorize deaths column (lower is better)
 * const deathStyles = computed(() => colorizeColumn(playerData.value, 'deaths', true));
 * </script>
 * 
 * <template>
 *   <table>
 *     <thead>
 *       <tr>
 *         <th>Player</th>
 *         <th>K/D Ratio</th>
 *         <th>Deaths</th>
 *       </tr>
 *     </thead>
 *     <tbody>
 *       <tr v-for="(player, idx) in playerData" :key="player.name">
 *         <td>{{ player.name }}</td>
 *         <td :style="kdStyles[idx]">{{ player.kd.toFixed(2) }}</td>
 *         <td :style="deathStyles[idx]">{{ player.deaths }}</td>
 *       </tr>
 *     </tbody>
 *   </table>
 * </template>
 */
