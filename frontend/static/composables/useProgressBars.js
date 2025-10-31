/**
 * useProgressBars.js
 * Vue composable for progress bar rendering and win/loss split bars
 * 
 * Migrated from web_static/app.js:
 * - renderBar(), setBarWidth(), getProgressColor()
 * - renderSplitWR(), renderWRCell(), getWRColor()
 * 
 * Provides reactive progress bar creation with color gradients
 */

/**
 * Composable for progress bar functionality
 * 
 * @returns {Object} Progress bar methods and utilities
 */
window.useProgressBars = function() {
	// Use Vue globals
	const { computed } = Vue;

	/**
	 * Calculate progress bar color based on percentage
	 * 
	 * @param {Number} percent - Percentage value (0-100)
	 * @returns {String} CSS color value
	 */
	const getProgressColor = (percent) => {
		if (percent >= 75) return '#22c55e'; // Green (--ok equivalent)
		if (percent >= 50) return '#3aa3ff'; // Blue (--accent)
		if (percent >= 25) return '#f4bf4f'; // Yellow (--warn)
		return '#ef4444'; // Red (--err equivalent)
	};

	/**
	 * Calculate win rate color based on percentage
	 * 
	 * @param {Number} wrPercent - Win rate percentage (0-100)
	 * @returns {String} CSS color value
	 */
	const getWRColor = (wrPercent) => {
		if (wrPercent >= 60) return '#22c55e'; // Excellent
		if (wrPercent >= 50) return '#3aa3ff'; // Good
		if (wrPercent >= 40) return '#f4bf4f'; // Moderate
		return '#ef4444'; // Poor
	};

	/**
	 * Create progress bar data structure
	 * 
	 * @param {Number} value - Current value
	 * @param {Number} max - Maximum value (default: 100)
	 * @param {Boolean} showValue - Display value text (default: true)
	 * @returns {Object} Progress bar data { percent, color, displayValue, width }
	 */
	const createProgressBar = (value, max = 100, showValue = true) => {
		const numValue = parseFloat(value) || 0;
		const numMax = parseFloat(max) || 100;
		const percent = numMax > 0 ? Math.round((numValue / numMax) * 100) : 0;
		const clampedPercent = Math.max(0, Math.min(100, percent));

		return {
			percent: clampedPercent,
			color: getProgressColor(clampedPercent),
			displayValue: showValue ? `${numValue}/${numMax}` : null,
			width: `${clampedPercent}%`,
			value: numValue,
			max: numMax
		};
	};

	/**
	 * Create win/loss split bar data structure
	 * 
	 * @param {Number} wins - Number of wins
	 * @param {Number} losses - Number of losses
	 * @returns {Object} Split bar data { wins, losses, total, wrPercent, winWidth, lossWidth, color, displayText }
	 */
	const createSplitWRBar = (wins, losses) => {
		const numWins = parseInt(wins) || 0;
		const numLosses = parseInt(losses) || 0;
		const total = numWins + numLosses;

		if (total === 0) {
			return {
				wins: 0,
				losses: 0,
				total: 0,
				wrPercent: 0,
				winWidth: '0%',
				lossWidth: '0%',
				color: '#888',
				displayText: '0-0 (0%)'
			};
		}

		const wrPercent = Math.round((numWins / total) * 100);
		const winWidth = `${wrPercent}%`;
		const lossWidth = `${100 - wrPercent}%`;

		return {
			wins: numWins,
			losses: numLosses,
			total,
			wrPercent,
			winWidth,
			lossWidth,
			color: getWRColor(wrPercent),
			displayText: `${numWins}-${numLosses} (${wrPercent}%)`
		};
	};

	/**
	 * Create simple win rate bar (single color, percentage-based)
	 * 
	 * @param {Number} played - Total games played
	 * @param {Number} wrPercent - Win rate percentage (0-100)
	 * @returns {Object} WR bar data { played, wrPercent, width, color, displayText }
	 */
	const createWRBar = (played, wrPercent) => {
		const numPlayed = parseInt(played) || 0;
		const numWR = parseFloat(wrPercent) || 0;
		const clampedWR = Math.max(0, Math.min(100, numWR));

		return {
			played: numPlayed,
			wrPercent: clampedWR,
			width: `${clampedWR}%`,
			color: getWRColor(clampedWR),
			displayText: `${clampedWR}%`
		};
	};

	/**
	 * Get shimmer state for progress bar animation
	 * Shimmer should be visible at 0% and 100% completion
	 * 
	 * @param {Number} percent - Progress percentage (0-100)
	 * @returns {Boolean} Whether shimmer should be visible
	 */
	const shouldShowShimmer = (percent) => {
		return percent === 0 || percent === 100;
	};

	/**
	 * Generate CSS styles for progress bar fill
	 * 
	 * @param {Number} percent - Progress percentage
	 * @param {String} color - Bar color (optional, uses default if not provided)
	 * @returns {Object} Style object for Vue binding
	 */
	const getBarStyle = (percent, color = null) => {
		return {
			width: `${percent}%`,
			background: color || getProgressColor(percent),
			transition: 'width 0.6s ease'
		};
	};

	/**
	 * Generate CSS styles for split bar segments
	 * 
	 * @param {Number} wins - Number of wins
	 * @param {Number} losses - Number of losses
	 * @returns {Object} { winStyle, lossStyle } for Vue binding
	 */
	const getSplitBarStyles = (wins, losses) => {
		const splitData = createSplitWRBar(wins, losses);
		
		return {
			winStyle: {
				width: splitData.winWidth,
				background: '#22c55e',
				position: 'absolute',
				left: 0,
				top: 0,
				bottom: 0
			},
			lossStyle: {
				width: splitData.lossWidth,
				background: '#ef4444',
				position: 'absolute',
				right: 0,
				top: 0,
				bottom: 0
			}
		};
	};

	return {
		// Color utilities
		getProgressColor,
		getWRColor,

		// Bar creation
		createProgressBar,
		createSplitWRBar,
		createWRBar,

		// Style generation
		getBarStyle,
		getSplitBarStyles,

		// Animation utilities
		shouldShowShimmer
	};
}

/**
 * Usage Example:
 * 
 * <script setup>
 * import { ref, computed } from 'vue';
 * import { useProgressBars } from './composables/useProgressBars.js';
 * 
 * const { createProgressBar, createSplitWRBar, shouldShowShimmer } = useProgressBars();
 * 
 * // Simple progress bar
 * const progressData = computed(() => createProgressBar(75, 100, true));
 * 
 * // Win/Loss split bar
 * const teamStats = ref({ wins: 12, losses: 8 });
 * const splitData = computed(() => createSplitWRBar(teamStats.value.wins, teamStats.value.losses));
 * </script>
 * 
 * <template>
 *   <!-- Simple Progress Bar -->
 *   <div class="bar">
 *     <span :style="{ width: progressData.width, background: progressData.color }"></span>
 *     <span class="val">{{ progressData.displayValue }}</span>
 *   </div>
 * 
 *   <!-- Win/Loss Split Bar -->
 *   <div class="bar-split">
 *     <div class="win" :style="{ width: splitData.winWidth }"></div>
 *     <div class="loss" :style="{ width: splitData.lossWidth }"></div>
 *     <span class="val">{{ splitData.displayText }}</span>
 *   </div>
 * 
 *   <!-- Progress Bar with Shimmer (Vue component) -->
 *   <div class="progress-bar">
 *     <div 
 *       class="progress-fill"
 *       :class="{ 'progress-glow': shouldShowShimmer(progressData.percent) }"
 *       :style="{ width: progressData.width, background: progressData.color }"
 *     ></div>
 *   </div>
 *   <div class="progress-text">{{ progressData.displayValue }}</div>
 * </template>
 */
