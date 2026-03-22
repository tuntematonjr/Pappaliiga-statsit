// LoadingSpinner Component
window.LoadingSpinner = {
    name: 'LoadingSpinner',
    template: `
        <div class="loading-spinner">
            <div class="spinner"></div>
            <p v-if="message">{{ message }}</p>
        </div>
    `,
    props: {
        message: {
            type: String,
            default: 'Ladataan...'
        }
    }
};
