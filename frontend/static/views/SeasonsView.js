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
                        :key="season.season || season.id || idx"
                        class="season-card home-section"
                    >
                        <h2 class="section-title title-accent titleUnderlineCard">
                            Season {{ getSeasonLabel(season) }}
                        </h2>
                        <p class="season-meta">{{ getDivisionCount(season) }} divisions</p>
                        
                        <div class="divisions-list">
                            <div 
                                v-for="(div, idx) in getDivisionList(season)" 
                                :key="div.id || idx"
                                class="division-row"
                            >
                                <router-link 
                                    :to="getDivisionHref(div)"
                                    class="division-link division-link--regular"
                                >
                                    {{ div.name }}
                                </router-link>
                                <router-link 
                                    v-if="div.hasPlayoffsStarted"
                                    :to="getPlayoffsHref(div)"
                                    class="division-link division-link--playoffs"
                                >
                                    PO
                                </router-link>
                            </div>
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
                const seasons = await window.apiClient.getSeasons();
                const normalized = await Promise.all(
                    (Array.isArray(seasons) ? seasons : []).map(async season => {
                        const seasonId = season.season ?? season.id ?? season.season_id ?? season.seasonId;
                        let divisions = [];

                        if (seasonId != null) {
                            try {
                                const response = await window.apiClient.getDivisions(seasonId);
                                const fullList = response?.data ?? response ?? [];
                                const hasPlayoffsStarted =
                                    typeof window !== 'undefined' && window.divisionNormalizer?.hasPlayoffsStarted
                                        ? window.divisionNormalizer.hasPlayoffsStarted
                                        : null;

                                // Process regular divisions and embed playoff info
                                if (Array.isArray(fullList)) {
                                    divisions = fullList
                                        .filter(div => !div.is_playoff && !div.isPlayoff)
                                        .map(div => {
                                            const divName = (div.name || '').replace(/\s+S\d+$/i, '');
                                            const playoffData = div.playoffs || {};
                                            const fallbackStarted = (playoffData.matches_played || playoffData.matchesPlayed || 0) > 0;
                                            const hasPlayoffsStartedValue = hasPlayoffsStarted
                                                ? hasPlayoffsStarted(div)
                                                : fallbackStarted;

                                            return {
                                                id: div.divisionId || div.division_id || div.id,
                                                name: divName,
                                                tier: div.tier || div.division_num || 0,
                                                status: div.status,
                                                season: seasonId,
                                                hasPlayoffsStarted: hasPlayoffsStartedValue,
                                                raw: div
                                            };
                                        })
                                        .sort((a, b) => (a.tier || 0) - (b.tier || 0));
                                }
                            } catch (err) {
                                console.warn(`Failed to fetch divisions for season ${seasonId}:`, err);
                                divisions = [];
                            }
                        }

                        const divisionsCount = typeof season.divisions_count === 'number'
                            ? season.divisions_count
                            : divisions.length;

                        return {
                            ...season,
                            season: season.season ?? seasonId,
                            divisions,
                            divisions_count: divisionsCount
                        };
                    })
                );
                this.seasons = normalized;
            } catch (err) {
                this.error = err.message || 'Kausien lataus epäonnistui';
            } finally {
                this.loading = false;
            }
        },
        getSeasonLabel(season) {
            return season.name || season.season || season.id || '—';
        },
        getDivisionList(season) {
            return Array.isArray(season.divisions) ? season.divisions : [];
        },
        getDivisionCount(season) {
            if (typeof season.divisions_count === 'number') {
                return season.divisions_count;
            }
            return this.getDivisionList(season).length;
        },
        resolveDivisionSlug(division) {
            const normalizer = typeof window !== 'undefined' ? window.divisionNormalizer : null;
            const slug = normalizer?.getDivisionSlug ? normalizer.getDivisionSlug(division) : null;
            return slug || division?.slug || division?.id || null;
        },
        getDivisionTitle(division) {
            const normalizer = typeof window !== 'undefined' ? window.divisionNormalizer : null;
            if (normalizer?.cleanDivisionName) {
                const clean = normalizer.cleanDivisionName(division?.name || '');
                if (clean) return clean;
            }
            return division?.name || '';
        },
        buildDivisionRoute(division, options = {}) {
            const divisionId = division?.divisionId || division?.division_id || division?.id;
            const slug = this.resolveDivisionSlug(division) || divisionId;
            if (!slug) return '/division';
            const name = this.getDivisionTitle(division);
            const seasonValue = division?.season ?? division?.seasonNumber ?? division?.season_number ?? division?.season_id ?? null;
            const query = {};
            if (divisionId != null) {
                query.championship = String(divisionId);
            }
            if (name) {
                query.championship_name = String(name);
            }
            if (seasonValue != null) {
                query.championship_season = String(seasonValue);
            }
            return {
                name: options.playoffs ? 'division-playoffs' : 'division',
                params: { championshipId: String(slug) },
                query
            };
        },
        getDivisionHref(division) {
            return this.buildDivisionRoute(division, { playoffs: false });
        },
        getPlayoffsHref(division) {
            const hrefId =
                (typeof window !== 'undefined' && window.divisionNormalizer?.getPlayoffsHrefId
                    ? window.divisionNormalizer.getPlayoffsHrefId(division)
                    : null) ||
                null;
            if (!hrefId) return '/division';
            const name = this.getDivisionTitle(division);
            const query = {};
            query.championship = String(hrefId);
            if (name) {
                query.championship_name = String(name);
            }
            const seasonValue = division?.season ?? division?.seasonNumber ?? division?.season_number ?? division?.season_id ?? null;
            if (seasonValue != null) {
                query.championship_season = String(seasonValue);
            }
            return {
                name: 'division-playoffs',
                params: { championshipId: String(hrefId) },
                query
            };
        }
    }
};
