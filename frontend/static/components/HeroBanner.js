// HeroBanner - glassmorphism hero section for landing views
window.HeroBanner = {
    name: 'HeroBanner',
    props: {
        eyebrow: {
            type: String,
            default: ''
        },
        title: {
            type: String,
            required: true
        },
        subtitle: {
            type: String,
            default: ''
        },
        background: {
            type: String,
            default: ''
        },
        align: {
            type: String,
            default: 'left'
        }
    },
    computed: {
        hasMeta() {
            return Boolean(this.$slots.meta);
        },
        bannerClasses() {
            return [
                'hero-banner',
                `hero-banner--align-${this.align}`
            ];
        },
        backgroundStyle() {
            if (!this.background) {
                return {};
            }
            return {
                '--hero-banner-bg': `url('${this.background}')`
            };
        }
    },
    template: `
        <section
            class="hero-banner-wrapper"
            :class="bannerClasses"
            :style="backgroundStyle"
            role="region"
            aria-labelledby="hero-title"
        >
            <div class="hero-banner__overlay"></div>
            <div class="hero-banner__content">
                <p v-if="eyebrow" class="hero-banner__eyebrow">{{ eyebrow }}</p>
                <h1 class="hero-banner__title" id="hero-title">{{ title }}</h1>
                <p v-if="subtitle" class="hero-banner__subtitle">{{ subtitle }}</p>

                <div v-if="$slots.actions" class="hero-banner__actions">
                    <slot name="actions"></slot>
                </div>

                <div v-if="hasMeta" class="hero-banner__meta" aria-live="polite">
                    <slot name="meta"></slot>
                </div>
            </div>

            <div v-if="$slots.after" class="hero-banner__aside">
                <slot name="after"></slot>
            </div>
        </section>
    `
};

