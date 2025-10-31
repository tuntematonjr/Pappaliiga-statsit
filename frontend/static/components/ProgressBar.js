// ProgressBar Component - Reusable progress bar with animations
window.ProgressBar = {
    name: 'ProgressBar',
    props: {
        value: {
            type: Number,
            required: true,
            validator: val => val >= 0
        },
        max: {
            type: Number,
            default: 100,
            validator: val => val > 0
        },
        color: {
            type: String,
            default: 'default',
            validator: val => ['default', 'ok', 'warn', 'err', 'accent'].includes(val)
        },
        showShimmer: {
            type: Boolean,
            default: true
        },
        height: {
            type: String,
            default: '32px'
        },
        label: {
            type: String,
            default: ''
        },
        showPercentage: {
            type: Boolean,
            default: false
        }
    },
    computed: {
        percentage() {
            return Math.min(100, Math.max(0, (this.value / this.max) * 100));
        },
        barStyle() {
            return {
                width: `${this.percentage}%`,
                height: this.height
            };
        },
        barClass() {
            const classes = ['progress-fill'];
            if (this.showShimmer && this.percentage > 0 && this.percentage < 100) {
                classes.push('progress-glow');
            }
            if (this.color !== 'default') {
                classes.push(`progress-${this.color}`);
            }
            return classes.join(' ');
        },
        displayText() {
            if (this.label) return this.label;
            if (this.showPercentage) return `${this.percentage.toFixed(0)}%`;
            return '';
        }
    },
    template: `
        <div class="progress-wrapper">
            <div class="progress-bar progress-base" :style="{ height: height }" role="progressbar" :aria-valuemin="0" :aria-valuemax="max" :aria-valuenow="Math.round(percentage)" :aria-valuetext="displayText || (Math.round(percentage) + '%')">
                <div :class="barClass" :style="barStyle"></div>
                <div v-if="displayText" class="progress-label">{{ displayText }}</div>
            </div>
        </div>
    `
};
