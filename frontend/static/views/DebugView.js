window.DebugView = {
    name: 'DebugView',
    template: `
        <div class="seasons-view">
            <h1 class="title-accent titleUnderlinePage">Debug Status</h1>
            <p class="season-meta">Cache, sync queue and data revision snapshot.</p>

            <loading-spinner v-if="loading" message="Loading debug status..."></loading-spinner>
            <error-message v-else-if="error" :message="error" @retry="loadStatus"></error-message>

            <div v-else class="home-section">
                <p class="season-meta">
                    Generated: {{ formatTs(status.generated_at) }} |
                    Last sync: {{ formatTs(status.sync_queue?.last_job_finished_at_iso) }} |
                    Last status: {{ status.sync_queue?.last_job_status || 'n/a' }}
                </p>

                <div class="seasons-grid home-section">
                    <div class="season-card home-section">
                        <h2 class="section-title title-accent titleUnderlineCard">Queue</h2>
                        <p class="season-meta">Worker: {{ yesNo(status.sync_queue?.worker_running) }}</p>
                        <p class="season-meta">Queue size: {{ status.sync_queue?.queue_size ?? 0 }}</p>
                        <p class="season-meta">Queued keys: {{ status.sync_queue?.queued_keys ?? 0 }}</p>
                        <p class="season-meta">Processing keys: {{ status.sync_queue?.processing_keys ?? 0 }}</p>
                        <p class="season-meta">Last error: {{ status.sync_queue?.last_job_error || 'none' }}</p>
                    </div>

                    <div class="season-card home-section">
                        <h2 class="section-title title-accent titleUnderlineCard">Data</h2>
                        <p class="season-meta">Global revision: {{ status.data?.global_revision || 'n/a' }}</p>
                        <p class="season-meta">Last started: {{ formatTs(status.sync_queue?.last_job_started_at_iso) }}</p>
                        <p class="season-meta">Last finished: {{ formatTs(status.sync_queue?.last_job_finished_at_iso) }}</p>
                    </div>
                </div>

                <div class="season-card home-section" v-if="cacheRows.length">
                    <h2 class="section-title title-accent titleUnderlineCard">Caches</h2>
                    <div class="table-scroller">
                        <table class="stats-table">
                            <thead>
                                <tr>
                                    <th>Cache</th>
                                    <th>Size</th>
                                    <th>Hit rate</th>
                                    <th>Hits</th>
                                    <th>Misses</th>
                                    <th>Sets</th>
                                </tr>
                            </thead>
                            <tbody>
                                <tr v-for="row in cacheRows" :key="row.name">
                                    <td>{{ row.name }}</td>
                                    <td>{{ row.size }}</td>
                                    <td>{{ row.hitRate }}%</td>
                                    <td>{{ row.hits }}</td>
                                    <td>{{ row.misses }}</td>
                                    <td>{{ row.sets }}</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>
    `,
    components: {
        get LoadingSpinner() { return window.LoadingSpinner; },
        get ErrorMessage() { return window.ErrorMessage; }
    },
    data() {
        return {
            loading: true,
            error: null,
            status: {},
            pollTimer: null
        };
    },
    computed: {
        cacheRows() {
            const cache = this.status?.cache || {};
            return Object.entries(cache).map(([name, entry]) => {
                const stats = entry?.stats || {};
                return {
                    name,
                    size: entry?.size ?? 0,
                    hitRate: Number(stats?.hit_rate ?? 0),
                    hits: Number(stats?.hits ?? 0),
                    misses: Number(stats?.misses ?? 0),
                    sets: Number(stats?.sets ?? 0)
                };
            });
        }
    },
    async mounted() {
        await this.loadStatus();
        this.pollTimer = window.setInterval(() => {
            this.loadStatus({ silent: true });
        }, 15000);
    },
    beforeUnmount() {
        if (this.pollTimer) {
            window.clearInterval(this.pollTimer);
            this.pollTimer = null;
        }
    },
    methods: {
        async loadStatus(options = {}) {
            const silent = Boolean(options.silent);
            if (!silent) {
                this.loading = true;
                this.error = null;
            }
            try {
                this.status = await window.apiClient.getDebugStatus({ retries: 1, persistCache: false });
            } catch (err) {
                if (!silent) {
                    this.error = err?.message || 'Failed to load debug status';
                }
            } finally {
                if (!silent) {
                    this.loading = false;
                }
            }
        },
        formatTs(value) {
            if (!value) return 'n/a';
            const dt = new Date(value);
            if (Number.isNaN(dt.getTime())) return String(value);
            return dt.toLocaleString();
        },
        yesNo(value) {
            return value ? 'yes' : 'no';
        }
    }
};
