(function () {
    const { defineStore } = Pinia;

    const STATUS_ARCHIVE_PATTERN = /(archive|finished|complete|closed|done|past|ended|final)/i;
    const STATUS_ACTIVE_PATTERN = /(active|ongoing|current|live|running|progress)/i;

    function toNumber(value, fallback = null) {
        if (value === null || value === undefined) {
            return fallback;
        }
        const numeric = Number(value);
        if (Number.isFinite(numeric)) {
            return numeric;
        }
        const coerced = Number(String(value).replace(',', '.'));
        return Number.isFinite(coerced) ? coerced : fallback;
    }

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
        const seasonId = toNumber(raw.id ?? identifier ?? numberish, null);
        const seasonNumber =
            typeof numberish === 'number'
                ? numberish
                : Number(numberish != null ? String(numberish).replace(',', '.') : Number.NaN);
        const shortLabelSource = raw.short_name || raw.abbreviation || raw.code;
        const derivedShort = seasonNumber != null ? `S${seasonNumber}` : seasonId != null ? `S${seasonId}` : null;

        return {
            key,
            label,
            id: seasonId,
            shortLabel: shortLabelSource || derivedShort || label,
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
            selectedSeasonKey: null
        }),
        getters: {
            sortedSeasons(state) {
                return [...state.seasons].sort((a, b) => {
                    const aId = Number.isFinite(a.id) ? a.id : Number.NEGATIVE_INFINITY;
                    const bId = Number.isFinite(b.id) ? b.id : Number.NEGATIVE_INFINITY;
                    if (aId !== bId) {
                        return bId - aId;
                    }
                    const aVal = a.seasonNumber ?? Number.NEGATIVE_INFINITY;
                    const bVal = b.seasonNumber ?? Number.NEGATIVE_INFINITY;
                    if (Number.isFinite(aVal) && Number.isFinite(bVal) && aVal !== bVal) {
                        return bVal - aVal;
                    }
                    return a.label.localeCompare(b.label, 'fi');
                });
            },
            currentSeason(state) {
                return state.seasons.find(season => season.isCurrent) || null;
            },
            newestSeason(state) {
                return state.seasons.reduce((latest, season) => {
                    if (!season) return latest;
                    if (!latest) return season;
                    const latestId = Number.isFinite(latest.id) ? latest.id : Number.NEGATIVE_INFINITY;
                    const currentId = Number.isFinite(season.id) ? season.id : Number.NEGATIVE_INFINITY;
                    if (currentId !== latestId) {
                        return currentId > latestId ? season : latest;
                    }
                    const latestNumber = Number.isFinite(latest.seasonNumber) ? latest.seasonNumber : Number.NEGATIVE_INFINITY;
                    const currentNumber = Number.isFinite(season.seasonNumber) ? season.seasonNumber : Number.NEGATIVE_INFINITY;
                    return currentNumber > latestNumber ? season : latest;
                }, null);
            },
            latestSeasonKey(state) {
                const newest = this.newestSeason;
                return newest ? newest.key : null;
            },
            selectedSeason(state) {
                return state.seasons.find(season => season.key === state.selectedSeasonKey) || null;
            },
            getSeasonByKey: state => key => state.seasons.find(season => season.key === key) || null,
            getSeasonById: state => id => {
                if (id === undefined || id === null) return null;
                const numeric = Number(id);
                const target = String(id);
                return (
                    state.seasons.find(season => {
                        if (!season) return false;
                        if (String(season.key) === target) return true;
                        if (season.apiParam != null && String(season.apiParam) === target) return true;
                        if (Number.isFinite(numeric) && Number.isFinite(season.id) && season.id === numeric) return true;
                        if (Number.isFinite(numeric) && Number.isFinite(season.seasonNumber) && season.seasonNumber === numeric) return true;
                        return false;
                    }) || null
                );
            },
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
                    if (typeof window !== 'undefined' && window.console) {
                        console.info('[seasonsStore] fetchSeasons start', { force });
                    }
                    const payload = await window.apiClient.getSeasons();
                    const normalized = Array.isArray(payload)
                        ? payload.map((season, index) => normalizeSeason(season, index)).filter(Boolean)
                        : [];

                    normalized.sort((a, b) => {
                        const aId = Number.isFinite(a.id) ? a.id : Number.NEGATIVE_INFINITY;
                        const bId = Number.isFinite(b.id) ? b.id : Number.NEGATIVE_INFINITY;
                        if (aId !== bId) {
                            return bId - aId;
                        }
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
                        this.selectedSeasonKey = normalized[0].key;
                    } else if (this.selectedSeasonKey) {
                        const found = normalized.some(season => season.key === this.selectedSeasonKey);
                        if (!found && normalized.length) {
                            this.selectedSeasonKey = normalized[0].key;
                        }
                    }

                    if (typeof window !== 'undefined' && window.console) {
                        console.info('[seasonsStore] fetchSeasons success', {
                            count: normalized.length,
                            selectedSeasonKey: this.selectedSeasonKey
                        });
                    }
                    return this.seasons;
                } catch (error) {
                    console.error('[seasonsStore] fetchSeasons failed', error);
                    this.error = error?.message || 'Kausilistan lataus epäonnistui';
                    throw error;
                } finally {
                    this.loading = false;
                }
            },
            selectSeason(key) {
                if (!key) return;
                this.selectedSeasonKey = key;
            },
            ensureSelectedSeason() {
                if (this.selectedSeasonKey && this.seasons.some(entry => entry.key === this.selectedSeasonKey)) {
                    return this.selectedSeasonKey;
                }
                const fallback = this.seasons[0];
                if (fallback) {
                    this.selectedSeasonKey = fallback.key;
                    return fallback.key;
                }
                return null;
            },
            clear() {
                this.seasons = [];
                this.loading = false;
                this.error = null;
                this.fetchedAt = null;
                this.selectedSeasonKey = null;
            }
        }
    });
})();

