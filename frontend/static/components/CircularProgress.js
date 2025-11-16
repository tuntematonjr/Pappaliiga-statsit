(function () {
    'use strict';

    /**
     * CircularProgress - Ring-style circular progress indicator
     * 
     * Props:
     * - value: Number - Current value (played matches)
     * - max: Number - Maximum value (total matches)
     * - size: Number - Diameter of the circle in pixels (default: 120)
     * - strokeWidth: Number - Width of the ring stroke (default: 8)
     * - label: String - Text label below the circle
     * - sublabel: String - Secondary text below the label
     * - animationDelay: Number - Delay before animation starts in seconds
     * - color: String - Color variant: 'regular', 'playoff', 'overall' (default: 'regular')
     */
    
    window.CircularProgress = {
        name: 'CircularProgress',
        props: {
            value: { type: Number, default: 0 },
            max: { type: Number, default: 100 },
            size: { type: Number, default: 120 },
            strokeWidth: { type: Number, default: 8 },
            label: { type: String, default: '' },
            sublabel: { type: String, default: '' },
            animationDelay: { type: Number, default: 0 },
            color: { type: String, default: 'regular' }
        },
        computed: {
            percent() {
                const safeMax = Number.isFinite(this.max) ? this.max : 0;
                const safeValue = Number.isFinite(this.value) ? this.value : 0;
                if (safeMax <= 0) return 0;
                return Math.min(100, Math.max(0, Math.round((safeValue / safeMax) * 100)));
            },
            percentText() {
                return `${this.percent}%`;
            },
            radius() {
                return (this.size - this.strokeWidth) / 2;
            },
            circumference() {
                return 2 * Math.PI * this.radius;
            },
            strokeDashoffset() {
                const progress = this.percent / 100;
                return this.circumference * (1 - progress);
            },
            viewBox() {
                return `0 0 ${this.size} ${this.size}`;
            },
            center() {
                return this.size / 2;
            },
            colorClass() {
                return `circular-progress--${this.color}`;
            },
            containerStyle() {
                return {
                    '--animation-delay': `${this.animationDelay}s`,
                    '--circle-size': `${this.size}px`
                };
            },
            circleStyle() {
                return {
                    strokeDasharray: this.circumference,
                    strokeDashoffset: this.strokeDashoffset,
                    strokeWidth: this.strokeWidth
                };
            },
            playedText() {
                const safeMax = Number.isFinite(this.max) ? Math.round(this.max) : 0;
                const safeValue = Number.isFinite(this.value) ? Math.round(this.value) : 0;
                if (safeMax <= 0) {
                    return `${safeValue} Ottelut`;
                }
                const clamped = Math.min(safeValue, safeMax);
                return `${clamped} / ${safeMax} Ottelut`;
            },
            remainingValue() {
                const total = Number.isFinite(this.max) ? this.max : 0;
                const played = Number.isFinite(this.value) ? this.value : 0;
                const remaining = total - played;
                if (!Number.isFinite(remaining)) {
                    return 0;
                }
                return remaining > 0 ? Math.round(remaining) : 0;
            },
            remainingText() {
                return `${this.remainingValue} jäljellä`;
            },
            titleClassList() {
                const base = ['circular-progress__title', 'title-accent', 'titleUnderlineCard'];
                const delayMap = {
                    regular: 'title-delay-0',
                    playoff: 'title-delay-1',
                    overall: 'title-delay-2'
                };
                base.push(delayMap[this.color] || 'title-delay-3');
                return base;
            }
        },
        template: `
            <div class="circular-progress" :class="colorClass" :style="containerStyle">
                <div class="circular-progress__circle-container">
                    <svg 
                        class="circular-progress__svg" 
                        :width="size" 
                        :height="size" 
                        :viewBox="viewBox"
                        role="img"
                        :aria-label="label + ': ' + percent + '%'"
                    >
                        <defs>
                            <linearGradient id="gradient-regular" x1="0%" y1="0%" x2="100%" y2="100%">
                                <stop offset="0%" stop-color="rgba(120, 180, 255, 1)" />
                                <stop offset="100%" stop-color="rgba(60, 120, 255, 1)" />
                            </linearGradient>
                            <linearGradient id="gradient-playoff" x1="0%" y1="0%" x2="100%" y2="100%">
                                <stop offset="0%" stop-color="rgba(180, 120, 255, 1)" />
                                <stop offset="100%" stop-color="rgba(120, 80, 255, 1)" />
                            </linearGradient>
                            <linearGradient id="gradient-overall" x1="0%" y1="0%" x2="100%" y2="100%">
                                <stop offset="0%" stop-color="rgba(120, 240, 190, 1)" />
                                <stop offset="100%" stop-color="rgba(80, 200, 150, 1)" />
                            </linearGradient>
                        </defs>
                        <!-- Background ring -->
                        <circle
                            class="circular-progress__ring circular-progress__ring--bg"
                            :cx="center"
                            :cy="center"
                            :r="radius"
                            :stroke-width="strokeWidth"
                            fill="none"
                        />
                        <!-- Progress ring -->
                        <circle
                            class="circular-progress__ring circular-progress__ring--progress"
                            :cx="center"
                            :cy="center"
                            :r="radius"
                            :style="circleStyle"
                            fill="none"
                            stroke-linecap="round"
                        />
                    </svg>
                    <div class="circular-progress__label">
                        <span class="circular-progress__percent">{{ percentText }}</span>
                        <span class="circular-progress__played">{{ playedText }}</span>
                        <span class="circular-progress__remaining">{{ remainingText }}</span>
                    </div>
                </div>
                <div class="circular-progress__text">
                    <div :class="titleClassList">{{ label }}</div>
                </div>
            </div>
        `
    };
})();
