// SplitBar Component - shows win/loss split using existing .bar-split styles
// SplitBar Component - Win/Loss split bar visualization
window.SplitBar = {
    name: 'SplitBar',
    props: {
        wins: {
            type: Number,
            required: true,
            validator: val => val >= 0
        },
        losses: {
            type: Number,
            required: true,
            validator: val => val >= 0
        },
        height: {
            type: String,
            default: '32px'
        },
        // optional custom text to render inside the left (win) segment
        leftText: {
            type: String,
            default: ''
        },
        // optional custom text to render inside the right (loss) segment
        rightText: {
            type: String,
            default: ''
        },
        // show percentage text centered over the bar (default: true)
        showPercent: {
            type: Boolean,
            default: true
        },
        showLabels: {
            type: Boolean,
            default: true
        },
        showShimmer: {
            type: Boolean,
            default: true
        }
    },
    data() {
        return {
            segmentWidths: {
                win: '0%',
                loss: '0%'
            },
            initialised: false,
            isAnimating: false
        };
    },
    computed: {
        total() {
            return this.wins + this.losses;
        },
        winPercentage() {
            if (this.total === 0) return 50;
            return (this.wins / this.total) * 100;
        },
        lossPercentage() {
            if (this.total === 0) return 50;
            return (this.losses / this.total) * 100;
        },
        winTargetWidth() {
            if (this.total === 0) return '50%';
            if (this.wins === this.total) return '100%';
            if (this.wins === 0) return '0%';
            return `${Math.round(this.winPercentage)}%`;
        },
        lossTargetWidth() {
            if (this.total === 0) return '50%';
            if (this.losses === this.total) return '100%';
            if (this.losses === 0) return '0%';
            return `${Math.round(this.lossPercentage)}%`;
        },
        segmentTargets() {
            return {
                win: this.winTargetWidth,
                loss: this.lossTargetWidth
            };
        },
        winStyle() {
            return { width: this.segmentWidths.win };
        },
        lossStyle() {
            return { width: this.segmentWidths.loss };
        },
        winClass() {
            // Only apply tiny helper when there is a small but non-zero win portion
            return (this.total > 0 && this.wins > 0 && this.winPercentage < 8) ? 'win tiny' : 'win';
        },
        lossClass() {
            return (this.total > 0 && this.losses > 0 && this.lossPercentage < 8) ? 'loss tiny' : 'loss';
        },
        centerPercent() {
            if (!this.total) return '50%';
            return `${Math.round(this.winPercentage)}%`;
        },
        barClass() {
            const classes = ['bar-split'];
            if (this.showShimmer && this.total > 0) classes.push('bar-shimmer');
            if (this.isAnimating) classes.push('bar-split--animating');
            return classes.join(' ');
        }
    },
    watch: {
        segmentTargets: {
            immediate: true,
            deep: true,
            handler(newTargets) {
                this.updateSegmentWidths(newTargets);
            }
        }
    },
    methods: {
        updateSegmentWidths(targets) {
            const nextTargets = {
                win: targets?.win ?? '0%',
                loss: targets?.loss ?? '0%'
            };

            if (this.initialised) {
                this.segmentWidths = nextTargets;
                return;
            }

            this.segmentWidths = { win: '0%', loss: '0%' };
            this.$nextTick(() => {
                const queue = (typeof window !== 'undefined' && typeof window.requestAnimationFrame === 'function')
                    ? window.requestAnimationFrame
                    : (cb) => setTimeout(cb, 16);
                queue(() => {
                    this.isAnimating = true;
                    this.segmentWidths = nextTargets;
                    setTimeout(() => {
                        this.isAnimating = false;
                        this.initialised = true;
                    }, 900);
                });
            });
        }
    },
    template: `
        <div :class="barClass" :style="{ height: height }">
            <div :class="winClass" :style="winStyle"></div>
            <div :class="lossClass" :style="lossStyle"></div>
            <span v-if="showLabels" class="label label-left">{{ leftText || (wins + 'W') }}</span>
            <span v-if="showLabels" class="label label-right">{{ rightText || (losses + 'L') }}</span>
            <span v-if="showPercent && total>0" class="label label-center">{{ centerPercent }}</span>
        </div>
    `
};
