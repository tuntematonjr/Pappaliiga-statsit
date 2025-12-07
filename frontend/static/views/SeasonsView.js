// Seasons View - List all seasons and their divisions
window.SeasonsView = {
    name: 'SeasonsView',
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; }
    },
    template: `
        <div class="seasons-view">
            <h1 class="title-accent titleUnderlinePage">Seasons & Divisions</h1>
            
            <loading-spinner v-if="loading" message="Kausia ladataan..."></loading-spinner>
            <error-message v-else-if="error" :message="error" @retry="loadSeasons"></error-message>
            
            <div v-else class="seasons-grid home-section">
                <p v-if="!seasons.length" class="season-meta">Ei kausia saatavilla.</p>
                <template v-else>
                    <div
                        v-for="(season, idx) in seasons"
                        :key="season.season || idx"
                        class="season-card home-section"
                    >
                        <h2 class="section-title title-accent titleUnderlineCard">
                            Season {{ season.season }}
                        </h2>
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
                </template>
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
