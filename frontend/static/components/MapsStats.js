// MapsStats - wrapper card for MapStatsTable with consistent header styling
window.MapsStats = {
    name: 'MapsStats',
    components: {
        get MapStatsTable() { return window.MapStatsTable; },
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; }
    },
    props: {
        title: {
            type: String,
            default: 'Karttatilastot'
        },
        subtitle: {
            type: String,
            default: ''
        },
        mapStats: {
            type: Array,
            default: () => []
        },
        loading: {
            type: Boolean,
            default: false
        },
        error: {
            type: String,
            default: null
        },
        columns: {
            type: Array,
            default: null
        }
    },
    template: `
        <section class="maps-stats card">
            <header class="card-head">
                <div>
                    <h2 class="title title-accent titleUnderlineCard title-delay-0">{{ title }}</h2>
                    <p v-if="subtitle" class="subtitle muted">{{ subtitle }}</p>
                </div>
            </header>
            <div class="card-content">
                <loading-spinner v-if="loading" message="Karttatilastoja ladataan..."></loading-spinner>
                <error-message v-else-if="error" :message="error"></error-message>
                <map-stats-table
                    v-else
                    :map-stats="mapStats"
                    :columns-config="columns"
                ></map-stats-table>
            </div>
        </section>
    `
};

