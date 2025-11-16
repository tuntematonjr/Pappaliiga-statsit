// ErrorMessage Component
window.ErrorMessage = {
    name: 'ErrorMessage',
    template: `
        <div class="error-message">
            <div class="error-icon">⚠️</div>
            <h3 class="title-accent titleUnderlineCard title-delay-2">{{ title }}</h3>
            <p>{{ message }}</p>
            <button v-if="retry" @click="$emit('retry')" class="btn-retry">
                Try Again
            </button>
        </div>
    `,
    props: {
        title: {
            type: String,
            default: 'Error'
        },
        message: {
            type: String,
            required: true
        },
        retry: {
            type: Boolean,
            default: true
        }
    },
    emits: ['retry']
};
