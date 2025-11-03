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
        }
    },
    computed: {
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
            const center = 100;
            const angleStep = (Math.PI * 2) / this.normalizedMetrics.length;
            return this.normalizedMetrics
                .map((metric, index) => {
                    const angle = angleStep * index - Math.PI / 2;
                    const r = this.radius * metric.ratio;
                    const x = center + r * Math.cos(angle);
                    const y = center + r * Math.sin(angle);
                    return `${x.toFixed(2)},${y.toFixed(2)}`;
                })
                .join(' ');
        },
        gridLevels() {
            return Array.from({ length: this.levels }, (_, idx) => (idx + 1) / this.levels);
        }
    },
    template: `
        <svg v-if="normalizedMetrics.length" viewBox="0 0 200 200" class="radar-chart">
            <g v-for="level in gridLevels" :key="'grid-' + level" class="radar-chart__grid">
                <polygon :points="gridPolygon(level)" fill="none" stroke="rgba(160, 190, 255, 0.25)" stroke-width="1" />
            </g>
            <polygon
                class="radar-chart__shape"
                :points="polygonPoints"
                fill="rgba(90, 160, 255, 0.35)"
                stroke="rgba(130, 190, 255, 0.8)"
                stroke-width="2"
            />
            <g v-for="(metric, index) in normalizedMetrics" :key="metric.label || index">
                <line
                    class="radar-chart__axis"
                    x1="100"
                    y1="100"
                    :x2="axisPoint(index).x"
                    :y2="axisPoint(index).y"
                    stroke="rgba(150, 180, 255, 0.3)"
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
    `,
    methods: {
        axisPoint(index) {
            const angle = (Math.PI * 2 * index) / this.normalizedMetrics.length - Math.PI / 2;
            return {
                x: 100 + this.radius * Math.cos(angle),
                y: 100 + this.radius * Math.sin(angle)
            };
        },
        axisLabelPoint(index) {
            const angle = (Math.PI * 2 * index) / this.normalizedMetrics.length - Math.PI / 2;
            const distance = this.radius + 18;
            return {
                x: 100 + distance * Math.cos(angle),
                y: 100 + distance * Math.sin(angle)
            };
        },
        gridPolygon(level) {
            if (!this.normalizedMetrics.length) return '';
            const center = 100;
            const angleStep = (Math.PI * 2) / this.normalizedMetrics.length;
            return this.normalizedMetrics
                .map((_, index) => {
                    const angle = angleStep * index - Math.PI / 2;
                    const r = this.radius * level;
                    const x = center + r * Math.cos(angle);
                    const y = center + r * Math.sin(angle);
                    return `${x.toFixed(2)},${y.toFixed(2)}`;
                })
                .join(' ');
        }
    }
};

