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
        },
        headingVariant: {
            type: String,
            default: 'card'
        },
        stickyHeader: {
            type: Boolean,
            default: false
        },
        showHeader: {
            type: Boolean,
            default: true
        }
    },
    computed: {
        headingClass() {
            return this.headingVariant === 'main' ? 'titleUnderlineMain' : 'titleUnderlineCard';
        }
    },
    template: `
        <section class="maps-stats card">
            <header v-if="showHeader" class="card-head">
                <div>
                    <h2 :class="['title', 'title-accent', headingClass]">{{ title }}</h2>
                    <p v-if="subtitle" class="subtitle muted">{{ subtitle }}</p>
                </div>
            </header>
            <div class="card-content">
                <loading-spinner v-if="loading" message="Karttatilastoja ladataan..."></loading-spinner>
                <error-message v-else-if="error" :message="error"></error-message>
                <template v-else>
                    <map-stats-table
                        v-if="mapStats && mapStats.length"
                        :map-stats="mapStats"
                        :columns-config="columns"
                        :sticky-header="stickyHeader"
                    ></map-stats-table>
                    <p v-else class="muted empty">Ei karttatilastoja saatavilla</p>
                </template>
            </div>
        </section>
    `
};

