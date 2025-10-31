// DeltaIndicator Component - Shows curr/prev/delta with arrows
window.DeltaIndicator = {
    name: 'DeltaIndicator',
    template: `
        <span class="delta-indicator" :class="deltaClass" :title="tooltip">
            <span class="delta-value">{{ displayValue }}</span>
            <span v-if="showDelta && delta !== null" class="delta-arrow">
                {{ deltaSymbol }}
            </span>
        </span>
    `,
    props: {
        value: {
            type: [Number, String],
            required: true
        },
        delta: {
            type: Number,
            default: null
        },
        prev: {
            type: [Number, String],
            default: null
        },
        format: {
            type: String,
            default: 'number' // 'number', 'percent', 'decimal'
        },
        decimals: {
            type: Number,
            default: 2
        },
        showDelta: {
            type: Boolean,
            default: true
        },
        invertColors: {
            type: Boolean,
            default: false // For deaths, losses, etc.
        }
    },
    computed: {
        displayValue() {
            const val = parseFloat(this.value);
            if (isNaN(val)) return this.value;

            switch (this.format) {
                case 'percent':
                    return `${val.toFixed(this.decimals)}%`;
                case 'decimal':
                    return val.toFixed(this.decimals);
                default:
                    return Number.isInteger(val) ? val : val.toFixed(this.decimals);
            }
        },
        deltaClass() {
            if (!this.showDelta || this.delta === null) return '';
            
            const isPositive = this.delta > 0;
            const isNegative = this.delta < 0;
            
            if (this.invertColors) {
                if (isPositive) return 'delta-negative';
                if (isNegative) return 'delta-positive';
            } else {
                if (isPositive) return 'delta-positive';
                if (isNegative) return 'delta-negative';
            }
            
            return '';
        },
        deltaSymbol() {
            if (this.delta === null || this.delta === 0) return '';
            return this.delta > 0 ? '↑' : '↓';
        },
        tooltip() {
            if (!this.showDelta || this.prev === null) return '';
            
            const prevFormatted = this.formatValue(this.prev);
            const deltaFormatted = this.formatValue(Math.abs(this.delta));
            
            return `Previous: ${prevFormatted} (${this.delta > 0 ? '+' : '-'}${deltaFormatted})`;
        }
    },
    methods: {
        formatValue(val) {
            const num = parseFloat(val);
            if (isNaN(num)) return val;

            switch (this.format) {
                case 'percent':
                    return `${num.toFixed(this.decimals)}%`;
                case 'decimal':
                    return num.toFixed(this.decimals);
                default:
                    return Number.isInteger(num) ? num : num.toFixed(this.decimals);
            }
        }
    }
};
