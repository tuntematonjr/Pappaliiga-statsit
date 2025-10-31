// Seasons View - List all seasons and their divisions
window.SeasonsView = {
    name: 'SeasonsView',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; }
    },
    template: `
        <div class="seasons-view">
            <masthead></masthead>
            <h1>Seasons & Divisions</h1>
            
            <loading-spinner v-if="loading" message="Kausia ladataan..."></loading-spinner>
            <error-message v-else-if="error" :message="error" @retry="loadSeasons"></error-message>
            
            <div v-else class="seasons-grid home-section">
                <div v-for="season in seasons" :key="season.season" class="season-card home-section">
                    <h2 class="section-title">Season {{ season.season }}</h2>
                    <p class="season-meta">{{ season.divisions.length }} divisions</p>
                    
                    <div class="divisions-list">
                        <router-link 
                            v-for="(divNum, idx) in season.divisions" 
                            :key="idx"
                            :to="'/division/' + (season.championship_ids && season.championship_ids[idx] ? season.championship_ids[idx] : ('div' + divNum + '-s' + season.season))"
                            class="division-link"
                        >
                            Division {{ divNum }}
                        </router-link>
                    </div>
                </div>
            </div>
        </div>
    `,
    data() {
        return {
            loading: true,
            error: null,
            seasons: []
        };
    },
    async mounted() {
        await this.loadSeasons();
    },
    methods: {
        async loadSeasons() {
            this.loading = true;
            this.error = null;
            
            try {
                this.seasons = await window.apiClient.getSeasons();
            } catch (err) {
                this.error = err.message || 'Kausien lataus epäonnistui';
            } finally {
                this.loading = false;
            }
        }
    }
};
