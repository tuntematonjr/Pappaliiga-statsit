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
            containerHeight: 0
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
                const max = Number(metric.max) || 1;
                const value = Number(metric.value) || 0;
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
        primaryComparison() {
            return this.normalizedComparisons?.[0] || null;
        },
        metricComparisonRows() {
            if (!this.normalizedMetrics.length) return [];
            const comparisonByKey = new Map(
                (this.primaryComparison?.points || []).map(item => [item.key, item.value])
            );
            return this.normalizedMetrics.map(metric => {
                const compare = Number(comparisonByKey.get(metric.key) || 0);
                const diff = metric.value - compare;
                const decimals = Number.isInteger(metric.decimals) ? metric.decimals : 1;
                const percent = Boolean(metric.percent);
                const fmt = value => {
                    const numeric = Number(value || 0);
                    if (percent) return `${numeric.toFixed(decimals)}%`;
                    return numeric.toFixed(decimals);
                };
                const diffAbs = Math.abs(diff);
                const diffLabel = `${diff > 0 ? '+' : diff < 0 ? '-' : ''}${fmt(diffAbs)}`;
                return {
                    key: metric.key,
                    label: metric.label,
                    playerLabel: fmt(metric.value),
                    medianLabel: fmt(compare),
                    diffLabel,
                    diffClass: diff > 0 ? 'is-pos' : diff < 0 ? 'is-neg' : 'is-zero'
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
                        :fill="withAlpha(series.color, 0.16)"
                        :stroke="withAlpha(series.color, 0.75)"
                        stroke-width="2"
                    />
                </g>
                <polygon
                    class="radar-chart__shape"
                    :points="polygonPoints"
                    fill="rgba(90, 160, 255, 0.5)"
                    stroke="rgba(170, 220, 255, 0.95)"
                    stroke-width="2"
                />
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
                        <title>{{ metric.label }}: {{ metric.value.toFixed(1) }}</title>
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
                <div v-for="row in metricComparisonRows" :key="'mv-' + row.key" class="radar-chart__metric-row">
                    <span class="radar-chart__metric-name">{{ row.label }}</span>
                    <span class="radar-chart__metric-text">{{ row.playerLabel }} / {{ row.medianLabel }} / </span>
                    <span class="radar-chart__metric-diff" :class="row.diffClass">{{ row.diffLabel }}</span>
                </div>
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

