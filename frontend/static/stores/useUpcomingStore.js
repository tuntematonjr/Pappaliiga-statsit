(function () {
    const { defineStore } = Pinia;

    const FRESH_MS = 2 * 60 * 1000;

    function now() {
        return Date.now();
    }

    function createSegment() {
        return {
            data: [],
            meta: null,
            loading: false,
            error: null,
            fetchedAt: null,
            params: null
        };
    }

    function isFresh(segment) {
        return !!(segment && segment.fetchedAt && now() - segment.fetchedAt < FRESH_MS && !segment.error);
    }

    function normalizeKeyPart(value) {
        if (value === null || value === undefined || value === '') return '_';
        return String(value);
    }

    function buildKey(params) {
        if (!params || typeof params !== 'object') return 'upcoming::default';
        const parts = [
            normalizeKeyPart(params.championshipId ?? params.championship_id),
            normalizeKeyPart(params.teamId ?? params.team_id),
            normalizeKeyPart(params.season ?? params.seasonId ?? params.season_id),
            normalizeKeyPart(
                typeof params.includePlayoffs === 'boolean'
                    ? params.includePlayoffs
                    : (typeof params.include_playoffs === 'boolean' ? params.include_playoffs : 'default')
            ),
            normalizeKeyPart(params.limit),
            normalizeKeyPart(params.offset)
        ];
        return `upcoming::${parts.join(':')}`;
    }

    window.useUpcomingStore = defineStore('upcoming', {
        state: () => ({
            entries: {}
        }),
        getters: {
            getEntry(state) {
                return key => state.entries[key] || createSegment();
            },
            getEntryForParams(state) {
                return params => {
                    const key = buildKey(params);
                    return state.entries[key] || createSegment();
                };
            }
        },
        actions: {
            ensureEntry(key) {
                if (!this.entries[key]) {
                    this.entries[key] = createSegment();
                }
                return this.entries[key];
            },
            async fetchUpcomingMatches(params = {}, options = {}) {
                const { force = false } = options;
                const key = buildKey(params);
                const entry = this.ensureEntry(key);

                if (entry.loading) {
                    return entry.data || [];
                }
                if (!force && isFresh(entry)) {
                    return entry.data || [];
                }

                entry.loading = true;
                entry.error = null;
                entry.params = params;
                try {
                    const payload = await window.apiClient.getUpcomingMatches(params);
                    entry.data = Array.isArray(payload?.items) ? payload.items : [];
                    entry.meta = payload?.meta || null;
                    entry.fetchedAt = now();
                    return entry.data;
                } catch (error) {
                    entry.error = error?.message || 'Tulevien otteluiden haku epäonnistui';
                    throw error;
                } finally {
                    entry.loading = false;
                }
            },
            seedUpcoming(params, items, meta = null) {
                const key = buildKey(params);
                const entry = this.ensureEntry(key);
                if (isFresh(entry)) return;
                entry.data = Array.isArray(items) ? items : [];
                entry.meta = meta;
                entry.fetchedAt = now();
                entry.params = params;
            }
        }
    });
})();
