// CopyLink Component - Button to copy current page URL to clipboard
window.CopyLink = {
    name: 'CopyLink',
    props: {
        url: {
            type: String,
            default: null
        },
        label: {
            type: String,
            default: 'Copy Link'
        },
        compact: {
            type: Boolean,
            default: false
        }
    },
    data() {
        return {
            copied: false,
            copyTimeout: null
        };
    },
    computed: {
        linkUrl() {
            return this.url || window.location.href;
        },
        buttonClass() {
            const classes = ['btn', 'btn-ghost', 'copy-link-btn'];
            if (this.compact) classes.push('btn-compact');
            if (this.copied) classes.push('copied');
            return classes.join(' ');
        },
        buttonText() {
            return this.copied ? '✓ Copied!' : this.label;
        }
    },
    methods: {
        async copyLink() {
            try {
                await navigator.clipboard.writeText(this.linkUrl);
                this.copied = true;
                
                // Clear any existing timeout
                if (this.copyTimeout) {
                    clearTimeout(this.copyTimeout);
                }
                
                // Reset after 2 seconds
                this.copyTimeout = setTimeout(() => {
                    this.copied = false;
                }, 2000);
                
                this.$emit('copied', this.linkUrl);
            } catch (err) {
                console.error('Failed to copy:', err);
                this.$emit('error', err);
            }
        }
    },
    beforeUnmount() {
        if (this.copyTimeout) {
            clearTimeout(this.copyTimeout);
        }
    },
    template: `
        <button :class="buttonClass" @click="copyLink" :title="'Copy link: ' + linkUrl">
            <span class="copy-icon">🔗</span>
            <span class="copy-text">{{ buttonText }}</span>
        </button>
    `
};
