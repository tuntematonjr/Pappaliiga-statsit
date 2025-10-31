// SeasonsNav - season selector/tabs
window.SeasonsNav = {
    name: 'SeasonsNav',
    props: {
        seasons: { type: Array, required: true },
        // Format: [{ season: 11, divisions: [0,1,2,...] }]
        currentSeason: { type: Number, required: true }
    },
    emits: ['season-change'],
    template: `
        <div class="seasons-nav">
            <h3>Select Season</h3>
            <div class="season-tabs">
                <button 
                    v-for="s in seasons" 
                    :key="s.season"
                    :class="['season-tab', { active: s.season === currentSeason }]"
                    @click="$emit('season-change', s.season)"
                >
                    Season {{ s.season }}
                </button>
            </div>
        </div>
    `
};
