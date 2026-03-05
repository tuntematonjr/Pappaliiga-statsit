window.RadarChart = {
    name: 'RadarChart',
    props: {
        metrics: {
            type: Array,
            default: () => []
        },
        comparisons: {
            type: Array,
            default: () => []
        },
        radius: {
            type: Number,
            default: 90
        },
        levels: {
            type: Number,
            default: 4
        },
        autoScale: {
            type: Boolean,
            default: true
        }
    },
    data() {
        return {
            containerWidth: 0,
            containerHeight: 0,
            pointTooltip: {
                visible: false,
                x: 0,
                y: 0,
                title: '',
                value: ''
            }
        };
    },
    computed: {
        chartSize() {
            const size = Math.min(this.containerWidth || 0, this.containerHeight || 0);
            if (size > 0) return size;
            return Math.max(160, this.radius * 2 + 40);
        },
        chartCenter() {
            return this.chartSize / 2;
        },
        radiusValue() {
            if (this.autoScale) {
                return Math.max(24, this.chartSize * 0.42);
            }
            return Math.min(this.radius, this.chartSize * 0.45);
        },
        labelOffset() {
            return Math.max(10, this.chartSize * 0.08);
        },
        normalizedMetrics() {
            return (this.metrics || []).map(metric => {
                const staticMax = Number(metric.max);
                const value = Number(metric.value) || 0;
                let max = Number.isFinite(staticMax) && staticMax > 0 ? staticMax : 1;
                if (metric.dynamicMax) {
                    const comparisonValues = (this.comparisons || [])
                        .map(series => Number(series?.values?.[metric.key]))
                        .filter(Number.isFinite);
                    const highestValue = Math.max(value, ...comparisonValues, 0);
                    const headroom = Number(metric.dynamicHeadroom);
                    const growthFactor = Number.isFinite(headroom) && headroom > 1 ? headroom : 1.15;
                    const minMax = Number(metric.dynamicMinMax);
                    const dynamicFloor = Number.isFinite(minMax) && minMax > 0 ? minMax : 1;
                    // Use a sensible floor for low-count metrics, but still scale up for larger seasons.
                    max = Math.max(dynamicFloor, highestValue * growthFactor);
                }
                return {
                    key: String(metric.key || ''),
                    label: metric.label || '',
                    value,
                    max,
                    decimals: Number.isInteger(metric.decimals) ? metric.decimals : 1,
                    percent: Boolean(metric.percent),
                    ratio: max > 0 ? Math.max(0, Math.min(1, value / max)) : 0
                };
            });
        },
        normalizedComparisons() {
            if (!Array.isArray(this.comparisons) || !this.comparisons.length || !this.normalizedMetrics.length) {
                return [];
            }
            return this.comparisons.map((series, idx) => {
                const values = series?.values && typeof series.values === 'object' ? series.values : {};
                const points = this.normalizedMetrics.map(metric => {
                    const raw = Number(values[metric.key]);
                    const value = Number.isFinite(raw) ? raw : 0;
                    const ratio = metric.max > 0 ? Math.max(0, Math.min(1, value / metric.max)) : 0;
                    return {
                        key: metric.key,
                        value,
                        ratio
                    };
                });
                const color = series?.color || '#60a5fa';
                return {
                    key: String(series?.key || `comparison-${idx}`),
                    label: String(series?.label || `Comparison ${idx + 1}`),
                    color,
                    points
                };
            });
        },
        metricComparisonRows() {
            if (!this.normalizedMetrics.length) return [];
            const seriesByKey = new Map(this.normalizedComparisons.map(series => [series.key, series]));
            const divisionSeries = seriesByKey.get('division_median') || this.normalizedComparisons[0] || null;
            const seasonSeries = seriesByKey.get('season_median') || this.normalizedComparisons[1] || null;
            const buildPointMap = series => new Map((series?.points || []).map(item => [item.key, item.value]));
            const divisionMap = buildPointMap(divisionSeries);
            const seasonMap = buildPointMap(seasonSeries);
            const buildDiff = (base, compare, fmt) => {
                const diff = Number(base || 0) - Number(compare || 0);
                const sign = diff > 0 ? '+' : diff < 0 ? '-' : '';
                return {
                    label: `${sign}${fmt(Math.abs(diff))}`,
                    className: diff > 0 ? 'is-pos' : diff < 0 ? 'is-neg' : 'is-zero'
                };
            };

            return this.normalizedMetrics.map(metric => {
                const decimals = Number.isInteger(metric.decimals) ? metric.decimals : 1;
                const percent = Boolean(metric.percent);
                const fmt = value => {
                    const numeric = Number(value || 0);
                    if (percent) return `${numeric.toFixed(decimals)}%`;
                    return numeric.toFixed(decimals);
                };
                const divisionValue = Number(divisionMap.get(metric.key) || 0);
                const seasonValue = Number(seasonMap.get(metric.key) || 0);
                const divisionDiff = buildDiff(metric.value, divisionValue, fmt);
                const seasonDiff = buildDiff(metric.value, seasonValue, fmt);
                return {
                    key: metric.key,
                    label: metric.label,
                    playerLabel: fmt(metric.value),
                    divisionLabel: fmt(divisionValue),
                    seasonLabel: fmt(seasonValue),
                    divisionDiffLabel: divisionDiff.label,
                    divisionDiffClass: divisionDiff.className,
                    seasonDiffLabel: seasonDiff.label,
                    seasonDiffClass: seasonDiff.className
                };
            });
        },
        polygonPoints() {
            if (!this.normalizedMetrics.length) return '';
            const angleStep = (Math.PI * 2) / this.normalizedMetrics.length;
            return this.normalizedMetrics
                .map((metric, index) => {
                    const angle = angleStep * index - Math.PI / 2;
                    const r = this.radiusValue * metric.ratio;
                    const x = this.chartCenter + r * Math.cos(angle);
                    const y = this.chartCenter + r * Math.sin(angle);
                    return `${x.toFixed(2)},${y.toFixed(2)}`;
                })
                .join(' ');
        },
        gridLevels() {
            return Array.from({ length: this.levels }, (_, idx) => (idx + 1) / this.levels);
        }
    },
    template: `
        <div ref="container" class="radar-chart__wrap">
            <svg v-if="normalizedMetrics.length" :viewBox="'0 0 ' + chartSize + ' ' + chartSize" class="radar-chart">
                <g v-for="level in gridLevels" :key="'grid-' + level" class="radar-chart__grid">
                    <polygon :points="gridPolygon(level)" fill="none" stroke="rgba(150, 200, 255, 0.4)" stroke-width="1" />
                </g>
                <g v-for="series in normalizedComparisons" :key="series.key">
                    <polygon
                        class="radar-chart__comparison"
                        :points="comparisonPolygonPoints(series)"
                        :fill="withAlpha(series.color, 0.09)"
                        :stroke="withAlpha(series.color, 0.92)"
                        :stroke-dasharray="comparisonStrokeDash(series)"
                        stroke-width="2"
                    />
                    <circle
                        v-for="(point, pointIndex) in series.points"
                        :key="series.key + '-pt-' + pointIndex"
                        class="radar-chart__point radar-chart__point--comparison"
                        :cx="metricPoint(pointIndex, point.ratio).x"
                        :cy="metricPoint(pointIndex, point.ratio).y"
                        r="2.6"
                        :fill="series.color"
                        :stroke="withAlpha(series.color, 0.98)"
                        stroke-width="1"
                        @mouseenter="showPointTooltip($event, series.label, normalizedMetrics[pointIndex], point.value)"
                        @mousemove="movePointTooltip($event)"
                        @mouseleave="hidePointTooltip"
                    ></circle>
                </g>
                <polygon
                    class="radar-chart__shape"
                    :points="polygonPoints"
                    fill="rgba(90, 160, 255, 0.2)"
                    stroke="rgba(170, 220, 255, 0.95)"
                    stroke-width="2"
                />
                <circle
                    v-for="(metric, index) in normalizedMetrics"
                    :key="'player-pt-' + metric.key + '-' + index"
                    class="radar-chart__point radar-chart__point--player"
                    :cx="metricPoint(index, metric.ratio).x"
                    :cy="metricPoint(index, metric.ratio).y"
                    r="3.1"
                    fill="#60a5fa"
                    stroke="rgba(214, 235, 255, 0.95)"
                    stroke-width="1.2"
                    @mouseenter="showPointTooltip($event, 'Pelaaja', metric, metric.value)"
                    @mousemove="movePointTooltip($event)"
                    @mouseleave="hidePointTooltip"
                ></circle>
                <g v-for="(metric, index) in normalizedMetrics" :key="metric.label || index">
                    <line
                        class="radar-chart__axis"
                        :x1="chartCenter"
                        :y1="chartCenter"
                        :x2="axisPoint(index).x"
                        :y2="axisPoint(index).y"
                        stroke="rgba(150, 180, 255, 0.45)"
                        stroke-width="1"
                    />
                    <text
                        class="radar-chart__label"
                        :x="axisLabelPoint(index).x"
                        :y="axisLabelPoint(index).y"
                        dominant-baseline="middle"
                    >
                        {{ metric.label }}
                    </text>
                </g>
            </svg>
            <div v-if="normalizedComparisons.length" class="radar-chart__legend">
                <span class="radar-chart__legend-item">
                    <i class="radar-chart__legend-dot" style="background: #60a5fa;"></i>
                    Pelaaja
                </span>
                <span v-for="series in normalizedComparisons" :key="'legend-' + series.key" class="radar-chart__legend-item">
                    <i class="radar-chart__legend-dot" :style="{ background: series.color }"></i>
                    {{ series.label }}
                </span>
            </div>
            <div v-if="metricComparisonRows.length" class="radar-chart__metric-values">
                <div class="radar-chart__metric-table">
                    <div class="radar-chart__metric-head radar-chart__metric-grid">
                        <span class="radar-chart__metric-col radar-chart__metric-col--name">Metric</span>
                        <span class="radar-chart__metric-col">P</span>
                        <span class="radar-chart__metric-col">D</span>
                        <span class="radar-chart__metric-col">ΔD</span>
                        <span class="radar-chart__metric-col">S</span>
                        <span class="radar-chart__metric-col">ΔS</span>
                    </div>
                    <div
                        v-for="row in metricComparisonRows"
                        :key="'mv-' + row.key"
                        class="radar-chart__metric-line radar-chart__metric-grid"
                    >
                        <span class="radar-chart__metric-col radar-chart__metric-col--name">{{ row.label }}</span>
                        <span class="radar-chart__metric-col radar-chart__metric-col--player">{{ row.playerLabel }}</span>
                        <span class="radar-chart__metric-col radar-chart__metric-col--division">{{ row.divisionLabel }}</span>
                        <span class="radar-chart__metric-col radar-chart__metric-diff" :class="row.divisionDiffClass">{{ row.divisionDiffLabel }}</span>
                        <span class="radar-chart__metric-col radar-chart__metric-col--season">{{ row.seasonLabel }}</span>
                        <span class="radar-chart__metric-col radar-chart__metric-diff" :class="row.seasonDiffClass">{{ row.seasonDiffLabel }}</span>
                    </div>
                </div>
            </div>
            <div
                v-if="pointTooltip.visible"
                class="radar-chart__tooltip"
                :style="{ left: pointTooltip.x + 'px', top: pointTooltip.y + 'px' }"
            >
                <div class="radar-chart__tooltip-title">{{ pointTooltip.title }}</div>
                <div class="radar-chart__tooltip-value">{{ pointTooltip.value }}</div>
            </div>
        </div>
    `,
    methods: {
        withAlpha(color, alpha) {
            if (typeof color !== 'string') return color;
            const hex = color.trim();
            if (/^#([0-9a-fA-F]{6})$/.test(hex)) {
                const r = parseInt(hex.slice(1, 3), 16);
                const g = parseInt(hex.slice(3, 5), 16);
                const b = parseInt(hex.slice(5, 7), 16);
                return `rgba(${r}, ${g}, ${b}, ${alpha})`;
            }
            return color;
        },
        comparisonPolygonPoints(series) {
            if (!series || !Array.isArray(series.points) || !series.points.length) return '';
            const angleStep = (Math.PI * 2) / series.points.length;
            return series.points
                .map((metric, index) => {
                    const angle = angleStep * index - Math.PI / 2;
                    const r = this.radiusValue * metric.ratio;
                    const x = this.chartCenter + r * Math.cos(angle);
                    const y = this.chartCenter + r * Math.sin(angle);
                    return `${x.toFixed(2)},${y.toFixed(2)}`;
                })
                .join(' ');
        },
        comparisonStrokeDash(series) {
            const key = String(series?.key || '');
            if (key === 'division_median') return '0';
            if (key === 'season_median') return '6 4';
            return '3 3';
        },
        metricPoint(index, ratio) {
            const angle = (Math.PI * 2 * index) / this.normalizedMetrics.length - Math.PI / 2;
            const r = this.radiusValue * ratio;
            return {
                x: this.chartCenter + r * Math.cos(angle),
                y: this.chartCenter + r * Math.sin(angle)
            };
        },
        formatMetricValue(metric, value) {
            const decimals = Number.isInteger(metric?.decimals) ? metric.decimals : 1;
            const numeric = Number(value || 0);
            if (metric?.percent) {
                return `${numeric.toFixed(decimals)}%`;
            }
            return numeric.toFixed(decimals);
        },
        showPointTooltip(event, seriesLabel, metric, value) {
            if (!event) return;
            this.pointTooltip.visible = true;
            this.pointTooltip.title = `${seriesLabel} - ${metric?.label || ''}`;
            this.pointTooltip.value = this.formatMetricValue(metric, value);
            this.movePointTooltip(event);
        },
        movePointTooltip(event) {
            if (!event || !this.$refs.container) return;
            const rect = this.$refs.container.getBoundingClientRect();
            const rawX = (event.clientX - rect.left) + 10;
            const rawY = (event.clientY - rect.top) - 12;
            const maxX = Math.max(16, rect.width - 12);
            const maxY = Math.max(16, rect.height - 12);
            this.pointTooltip.x = Math.min(maxX, Math.max(12, rawX));
            this.pointTooltip.y = Math.min(maxY, Math.max(12, rawY));
        },
        hidePointTooltip() {
            this.pointTooltip.visible = false;
        },
        axisPoint(index) {
            const angle = (Math.PI * 2 * index) / this.normalizedMetrics.length - Math.PI / 2;
            return {
                x: this.chartCenter + this.radiusValue * Math.cos(angle),
                y: this.chartCenter + this.radiusValue * Math.sin(angle)
            };
        },
        axisLabelPoint(index) {
            const angle = (Math.PI * 2 * index) / this.normalizedMetrics.length - Math.PI / 2;
            const distance = this.radiusValue + this.labelOffset;
            const clamp = (value, min, max) => Math.min(Math.max(value, min), max);
            const padding = Math.max(6, this.chartSize * 0.03);
            return {
                x: clamp(this.chartCenter + distance * Math.cos(angle), padding, this.chartSize - padding),
                y: clamp(this.chartCenter + distance * Math.sin(angle), padding, this.chartSize - padding)
            };
        },
        gridPolygon(level) {
            if (!this.normalizedMetrics.length) return '';
            const angleStep = (Math.PI * 2) / this.normalizedMetrics.length;
            return this.normalizedMetrics
                .map((_, index) => {
                    const angle = angleStep * index - Math.PI / 2;
                    const r = this.radiusValue * level;
                    const x = this.chartCenter + r * Math.cos(angle);
                    const y = this.chartCenter + r * Math.sin(angle);
                    return `${x.toFixed(2)},${y.toFixed(2)}`;
                })
                .join(' ');
        },
        updateSize() {
            if (!this.$refs.container) return;
            const rect = this.$refs.container.getBoundingClientRect();
            this.containerWidth = rect.width;
            this.containerHeight = rect.height;
        }
    },
    mounted() {
        this.updateSize();
        if (typeof ResizeObserver !== 'undefined') {
            this._resizeObserver = new ResizeObserver(() => this.updateSize());
            this._resizeObserver.observe(this.$refs.container);
        } else {
            this._resizeHandler = () => this.updateSize();
            window.addEventListener('resize', this._resizeHandler);
        }
    },
    beforeUnmount() {
        if (this._resizeObserver) {
            this._resizeObserver.disconnect();
            this._resizeObserver = null;
        }
        if (this._resizeHandler) {
            window.removeEventListener('resize', this._resizeHandler);
            this._resizeHandler = null;
        }
    }
};

