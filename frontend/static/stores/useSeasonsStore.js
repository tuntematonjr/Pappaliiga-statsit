(function () {
    const { defineStore } = Pinia;

    const STATUS_ARCHIVE_PATTERN = /(archive|finished|complete|closed|done|past|ended|final)/i;
    const STATUS_ACTIVE_PATTERN = /(active|ongoing|current|live|running|progress)/i;

    function normalizeSeason(raw, index) {
        if (!raw || typeof raw !== 'object') {
            return null;
        }

        const fallbackKey = `season-${index}`;
        const slug = raw.slug || raw.season_slug || raw.seasonSlug || raw.code;
        const identifier = raw.id ?? raw.championship_id ?? raw.championshipId ?? raw.season_id ?? raw.seasonId;
        const numberish = raw.season ?? raw.year ?? raw.number ?? raw.ordinal ?? raw.index;
        const label =
            raw.name ||
            raw.display_name ||
            raw.title ||
            (numberish != null ? `Season ${numberish}` : null) ||
            (slug ? `Season ${slug}` : `Season ${index + 1}`);

        const keySource = raw.key ?? raw.uid ?? slug ?? identifier ?? numberish ?? fallbackKey;
        const key = keySource != null ? String(keySource) : fallbackKey;

        const status = (raw.status || raw.state || '').toString().toLowerCase();
        const classification = (raw.classification || '').toString().toLowerCase();

        const isCurrent = Boolean(raw.is_current || raw.current || raw.isCurrent);
        const activeFlag = raw.is_active ?? raw.active ?? raw.isActive ?? null;
        const archivedFlag = raw.is_archived ?? raw.archived ?? raw.isArchived ?? null;

        const inferredArchived =
            archivedFlag != null ? Boolean(archivedFlag) : STATUS_ARCHIVE_PATTERN.test(status) || STATUS_ARCHIVE_PATTERN.test(classification);
        const inferredActive =
            activeFlag != null
                ? Boolean(activeFlag)
                : isCurrent || STATUS_ACTIVE_PATTERN.test(status) || STATUS_ACTIVE_PATTERN.test(classification);

        const isArchived = inferredArchived && !isCurrent;
        const isActive = inferredActive && !isArchived;
        const category = isArchived ? 'archived' : 'active';
        const isPlayoff = Boolean(raw.is_playoff || raw.phase === 'playoffs' || raw.stage === 'playoffs');

        const apiParam = raw.api_param ?? raw.apiParam ?? slug ?? identifier ?? numberish ?? key;

        const seasonNumber =
            typeof numberish === 'number'
                ? numberish
                : Number(numberish != null ? String(numberish).replace(',', '.') : Number.NaN);

        return {
            key,
            label,
            shortLabel: raw.short_name || raw.abbreviation || label,
            seasonNumber: Number.isFinite(seasonNumber) ? seasonNumber : null,
            status,
            category,
            isActive,
            isArchived,
            isCurrent,
            isPlayoff,
            phase: raw.phase || raw.stage || (isPlayoff ? 'Playoffs' : null),
            startsAt: raw.starts_at || raw.start_at || raw.start_date || raw.startDate || null,
            endsAt: raw.ends_at || raw.end_at || raw.end_date || raw.endDate || null,
            apiParam: apiParam != null ? apiParam : key,
            raw
        };
    }

    window.useSeasonsStore = defineStore('seasons', {
        state: () => ({
            seasons: [],
            loading: false,
            error: null,
            fetchedAt: null,
            selectedSeasonKey: null,
            selectedSegment: 'active'
        }),
        getters: {
            sortedSeasons(state) {
                return [...state.seasons].sort((a, b) => {
                    const aVal = a.seasonNumber ?? Number.NEGATIVE_INFINITY;
                    const bVal = b.seasonNumber ?? Number.NEGATIVE_INFINITY;
                    if (Number.isFinite(aVal) && Number.isFinite(bVal) && aVal !== bVal) {
                        return bVal - aVal;
                    }
                    return a.label.localeCompare(b.label, 'fi');
                });
            },
            activeSeasons(state) {
                return state.seasons.filter(
                    season => season && (season.isCurrent || season.isActive || (!season.isArchived && !season.isPlayoff))
                );
            },
            archivedSeasons(state) {
                return state.seasons.filter(season => season && season.isArchived);
            },
            currentSeason(state) {
                return state.seasons.find(season => season.isCurrent) || null;
            },
            selectedSeason(state) {
                return state.seasons.find(season => season.key === state.selectedSeasonKey) || null;
            },
            getSeasonByKey: state => key => state.seasons.find(season => season.key === key) || null,
            hasData(state) {
                return state.seasons.length > 0;
            }
        },
        actions: {
            async fetchSeasons(options = {}) {
                const { force = false } = options;

                if (this.loading) {
                    return this.seasons;
                }

                const isFresh = this.fetchedAt && Date.now() - this.fetchedAt < 5 * 60 * 1000;
                if (!force && this.seasons.length && isFresh) {
                    return this.seasons;
                }

                this.loading = true;
                this.error = null;

                try {
                    const payload = await window.apiClient.getSeasons();
                    const normalized = Array.isArray(payload)
                        ? payload.map((season, index) => normalizeSeason(season, index)).filter(Boolean)
                        : [];

                    normalized.sort((a, b) => {
                        const aVal = a.seasonNumber ?? Number.NEGATIVE_INFINITY;
                        const bVal = b.seasonNumber ?? Number.NEGATIVE_INFINITY;
                        if (Number.isFinite(aVal) && Number.isFinite(bVal) && aVal !== bVal) {
                            return bVal - aVal;
                        }
                        return a.label.localeCompare(b.label, 'fi');
                    });

                    this.seasons = normalized;
                    this.fetchedAt = Date.now();

                    if (!this.selectedSeasonKey && normalized.length) {
                        const preferred =
                            normalized.find(season => season.isCurrent) ||
                            normalized.find(season => season.isActive) ||
                            normalized[0];
                        if (preferred) {
                            this.selectedSeasonKey = preferred.key;
                            this.selectedSegment = preferred.isArchived ? 'archived' : 'active';
                        }
                    } else if (this.selectedSeasonKey) {
                        const found = normalized.some(season => season.key === this.selectedSeasonKey);
                        if (!found && normalized.length) {
                            this.selectedSeasonKey = normalized[0].key;
                            this.selectedSegment = normalized[0].isArchived ? 'archived' : 'active';
                        }
                    }

                    return this.seasons;
                } catch (error) {
                    this.error = error?.message || 'Kausilistan lataus epäonnistui';
                    throw error;
                } finally {
                    this.loading = false;
                }
            },
            selectSeason(key) {
                if (!key) return;
                this.selectedSeasonKey = key;
                const season = this.seasons.find(entry => entry.key === key);
                if (season) {
                    this.selectedSegment = season.isArchived ? 'archived' : 'active';
                }
            },
            setSegment(segment) {
                if (!segment) return;
                const normalized = segment === 'archived' ? 'archived' : 'active';
                this.selectedSegment = normalized;
            },
            ensureSelectedSeason() {
                if (this.selectedSeasonKey && this.seasons.some(entry => entry.key === this.selectedSeasonKey)) {
                    return this.selectedSeasonKey;
                }
                const preferred =
                    this.seasons.find(entry => entry.isCurrent) ||
                    this.seasons.find(entry => entry.isActive) ||
                    this.seasons[0];
                if (preferred) {
                    this.selectedSeasonKey = preferred.key;
                    this.selectedSegment = preferred.isArchived ? 'archived' : 'active';
                    return preferred.key;
                }
                return null;
            },
            clear() {
                this.seasons = [];
                this.loading = false;
                this.error = null;
                this.fetchedAt = null;
                this.selectedSeasonKey = null;
                this.selectedSegment = 'active';
            }
        }
    });
})();

