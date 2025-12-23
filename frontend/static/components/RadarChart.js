window.RadarChart = {
    name: 'RadarChart',
    props: {
        metrics: {
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
                    label: metric.label || '',
                    value,
                    max,
                    ratio: max > 0 ? Math.max(0, Math.min(1, value / max)) : 0
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
        </div>
    `,
    methods: {
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

