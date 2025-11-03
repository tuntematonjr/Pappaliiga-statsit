window.SparklineChart = {
    name: 'SparklineChart',
    props: {
        points: {
            type: Array,
            default: () => []
        },
        width: {
            type: Number,
            default: 160
        },
        height: {
            type: Number,
            default: 60
        },
        strokeWidth: {
            type: Number,
            default: 3
        },
        positiveColor: {
            type: String,
            default: '#5ad4ff'
        },
        negativeColor: {
            type: String,
            default: '#7dd3fc'
        }
    },
    computed: {
        path() {
            if (!Array.isArray(this.points) || !this.points.length) {
                return '';
            }
            const step = this.points.length > 1 ? this.width / (this.points.length - 1) : this.width;
            return this.points
                .map((value, index) => {
                    const normalized = Math.max(-1, Math.min(1, Number(value)));
                    const x = index * step;
                    const y = this.height - (normalized + 1) * (this.height / 2);
                    return `${index === 0 ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)}`;
                })
                .join(' ');
        }
    },
    template: `
        <svg
            v-if="path"
            :viewBox="'0 0 ' + width + ' ' + height"
            preserveAspectRatio="none"
            class="sparkline-chart"
        >
            <defs>
                <linearGradient :id="$attrs.id || 'sparklineGradient'" x1="0%" y1="0%" x2="100%" y2="0%">
                    <stop offset="0%" :stop-color="positiveColor" />
                    <stop offset="100%" :stop-color="negativeColor" />
                </linearGradient>
            </defs>
            <path :d="path" fill="none" :stroke="'url(#' + ($attrs.id || 'sparklineGradient') + ')'" :stroke-width="strokeWidth" />
        </svg>
    `
};

