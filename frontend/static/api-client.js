/**
 * Resilient API client with caching, retries, validation, and circuit breaker support.
 * Exposed globally as window.apiClient.
 */
(function () {
    const DEFAULTS = Object.freeze({
        baseUrl: (typeof window !== 'undefined' && window.PL_API_URL) || (typeof window !== 'undefined' && window.__API_BASE__) || (typeof window !== 'undefined' ? `${window.location.origin}/api` : '/api'),
        timeoutMs: Number((typeof window !== 'undefined' && window.PL_API_TIMEOUT_MS) || 8000),
        retries: Number((typeof window !== 'undefined' && window.PL_API_RETRY) || 2),
        cacheTtlMs: Number((typeof window !== 'undefined' && window.PL_API_CACHE_TTL_MS) || 300000)
    });

    const MEMORY_CACHE = new Map();
    const PERSIST_KEY = 'pl:cache:v1';
    const CACHE_META_KEY = 'pl:cache:meta';
    const BREAKERS = new Map();
    const CONSECUTIVE_FAILURE_LIMIT = 3;
    const FAILURE_WINDOW_MS = 60000;
    const BREAKER_COOLDOWN_MS = 30000;
    const isDev = typeof window !== 'undefined' && ['localhost', '127.0.0.1'].includes(window.location.hostname);

    function now() {
        return Date.now();
    }

    function sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    function readPersistentCache() {
        if (typeof window === 'undefined' || !window.localStorage) return { data: {}, meta: {} };
        try {
            const raw = window.localStorage.getItem(PERSIST_KEY);
            const metaRaw = window.localStorage.getItem(CACHE_META_KEY);
            return {
                data: raw ? JSON.parse(raw) : {},
                meta: metaRaw ? JSON.parse(metaRaw) : {}
            };
        } catch (error) {
            console.warn('[apiClient] Failed to read persistent cache', error);
            return { data: {}, meta: {} };
        }
    }

    function writePersistentCache(cache, meta) {
        if (typeof window === 'undefined' || !window.localStorage) return;
        try {
            window.localStorage.setItem(PERSIST_KEY, JSON.stringify(cache));
            window.localStorage.setItem(CACHE_META_KEY, JSON.stringify(meta));
        } catch (error) {
            console.warn('[apiClient] Failed to write persistent cache', error);
        }
    }

    const persistentStore = readPersistentCache();

    function makeCacheKey(path) {
        return `${DEFAULTS.baseUrl}|${path}`;
    }

    function getCached(path) {
        const key = makeCacheKey(path);
        const mem = MEMORY_CACHE.get(key);
        if (mem && now() - mem.timestamp <= DEFAULTS.cacheTtlMs) {
            return { ...mem, source: 'memory' };
        }
        const persisted = persistentStore.data[key];
        if (persisted && now() - persisted.timestamp <= DEFAULTS.cacheTtlMs) {
            MEMORY_CACHE.set(key, { ...persisted, source: 'storage' });
            return { ...persisted, source: 'storage' };
        }
        return null;
    }

    function storeCache(path, payload, etag) {
        const key = makeCacheKey(path);
        const entry = { data: payload, timestamp: now(), etag: etag || null };
        MEMORY_CACHE.set(key, entry);
        persistentStore.data[key] = entry;
        persistentStore.meta[key] = { cachedAt: entry.timestamp };
        writePersistentCache(persistentStore.data, persistentStore.meta);
    }

    function circuitBreaker(path) {
        if (!BREAKERS.has(path)) {
            BREAKERS.set(path, {
                failures: [],
                openAt: null
            });
        }
        const breaker = BREAKERS.get(path);
        return {
            isOpen() {
                if (breaker.openAt && now() - breaker.openAt < BREAKER_COOLDOWN_MS) {
                    return true;
                }
                breaker.openAt = null;
                return false;
            },
            recordSuccess() {
                breaker.failures = [];
                breaker.openAt = null;
            },
            recordFailure() {
                const cutoff = now() - FAILURE_WINDOW_MS;
                breaker.failures = breaker.failures.filter(ts => ts > cutoff);
                breaker.failures.push(now());
                if (breaker.failures.length >= CONSECUTIVE_FAILURE_LIMIT) {
                    breaker.openAt = now();
                }
            },
            openedAt() {
                return breaker.openAt;
            }
        };
    }

    function shouldRetry(status, error) {
        if (error && error.name === 'AbortError') {
            return true;
        }
        if (!status) {
            return true;
        }
        if (status === 429) {
            return true;
        }
        return status >= 500;
    }

    function parseRetryAfter(headers) {
        const retryHeader = headers.get('Retry-After');
        if (!retryHeader) return null;
        const seconds = Number(retryHeader);
        if (Number.isFinite(seconds)) {
            return seconds * 1000;
        }
        const dateMs = Date.parse(retryHeader);
        if (Number.isFinite(dateMs)) {
            return Math.max(0, dateMs - now());
        }
        return null;
    }

    function clampNumber(value, min, max) {
        const num = Number(value);
        if (!Number.isFinite(num)) return min;
        return Math.min(max, Math.max(min, num));
    }

    class ApiValidationError extends Error {
        constructor(message, path, value) {
            super(message);
            this.name = 'ApiValidationError';
            this.path = path;
            this.value = value;
        }
    }

    function validateSeasonSummary(payload) {
        if (!payload || typeof payload !== 'object') {
            throw new ApiValidationError('Season summary must be an object', 'root', payload);
        }
        return payload;
    }

    function validateDivisionRecord(record, index) {
        if (!record || typeof record !== 'object') {
            throw new ApiValidationError('Division entry must be object', `divisions[${index}]`, record);
        }
        if (!record.division_id && record.division_id !== 0) {
            throw new ApiValidationError('division_id missing', `divisions[${index}].division_id`, record);
        }
        const normalized = {
            id: String(record.division_id),
            name: String(record.name || `Division ${record.division_id}`),
            tier: Number(record.tier || 0),
            season: normalizeSeason(record.season, index),
            playoffs: normalizePlayoffs(record.playoffs, index)
        };
        return normalized;
    }

    function normalizeSeason(season, index) {
        if (!season || typeof season !== 'object') {
            throw new ApiValidationError('season missing', `divisions[${index}].season`, season);
        }
        const teams = Number(season.teams);
        if (!Number.isFinite(teams) || teams < 0) {
            throw new ApiValidationError('season.teams invalid', `divisions[${index}].season.teams`, season.teams);
        }
        const matchesTotal = Number(season.matches_total);
        const matchesPlayed = clampNumber(season.matches_played, 0, Number.isFinite(matchesTotal) ? matchesTotal : 100);
        const status = String(season.status || '').toLowerCase();
        if (!['waiting', 'active', 'finished'].includes(status)) {
            throw new ApiValidationError('season.status invalid', `divisions[${index}].season.status`, season.status);
        }
        return {
            teams,
            matches_played: matchesPlayed,
            matches_total: Number.isFinite(matchesTotal) ? matchesTotal : 0,
            status,
            winner: season.winner ? String(season.winner) : null
        };
    }

    function normalizePlayoffs(playoffs, index) {
        const fallback = {
            teams: 8,
            matches_played: 0,
            matches_total: 7,
            status: 'waiting',
            winner: null
        };
        if (!playoffs || typeof playoffs !== 'object') {
            return fallback;
        }
        const status = String(playoffs.status || 'waiting').toLowerCase();
        if (!['waiting', 'active', 'finished'].includes(status)) {
            throw new ApiValidationError('playoffs.status invalid', `divisions[${index}].playoffs.status`, playoffs.status);
        }
        const matchesTotal = Number(playoffs.matches_total ?? 7) || 7;
        const matchesPlayed = clampNumber(playoffs.matches_played, 0, matchesTotal);
        return {
            teams: Number(playoffs.teams) || 8,
            matches_played: matchesPlayed,
            matches_total: matchesTotal,
            status,
            winner: playoffs.winner ? String(playoffs.winner) : null
        };
    }

    let validationCounts = {};

    function recordValidationError(error) {
        const key = error.path || 'unknown';
        validationCounts[key] = (validationCounts[key] || 0) + 1;
        if (isDev) {
            console.warn(`[apiClient] Validation error at ${key}`, error.value);
        }
    }

    function resetValidationCounts() {
        validationCounts = {};
    }

    async function fetchJson(path, options = {}) {
        const breaker = circuitBreaker(path);
        let cacheEntry = getCached(path);
        const requestId = `req_${Math.random().toString(36).slice(2, 8)}`;
        const meta = {
            requestId,
            attempts: 0,
            fromCache: false,
            cacheTimestamp: cacheEntry?.timestamp ?? null,
            usedCacheDueToError: false,
            breakerOpen: false,
            telemetry: []
        };

        if (breaker.isOpen()) {
            meta.breakerOpen = true;
            if (cache) {
                meta.fromCache = true;
                meta.usedCacheDueToError = true;
                if (isDev) {
                    console.debug(`[apiClient] breaker open for ${path}, serving cache`, meta);
                }
                return { data: cache.data, meta };
            }
        }

        const retries = options.retries != null ? options.retries : DEFAULTS.retries;
        const timeoutMs = options.timeoutMs || DEFAULTS.timeoutMs;
        let lastError = null;
        let triedWithoutEtag = false;

        for (let attempt = 0; attempt <= retries; attempt += 1) {
            meta.attempts = attempt + 1;
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
            const headers = new Headers(options.headers || {});
            headers.set('Accept', 'application/json');
            const shouldSendEtag = cacheEntry?.etag && !triedWithoutEtag;
            if (shouldSendEtag) {
                headers.set('If-None-Match', cacheEntry.etag);
            }
            const start = performance.now();
            try {
                const response = await fetch(`${DEFAULTS.baseUrl}${path}`, {
                    method: options.method || 'GET',
                    signal: options.signal || controller.signal,
                    headers,
                    cache: 'no-cache',
                    credentials: options.credentials || 'same-origin'
                });
                clearTimeout(timeoutId);
                const attemptMeta = {
                    attempt,
                    status: response.status,
                    latency: performance.now() - start
                };
                meta.telemetry.push(attemptMeta);
                const retryAfter = parseRetryAfter(response.headers);
                if (response.status === 304) {
                    if (cacheEntry && cacheEntry.data) {
                        breaker.recordSuccess();
                        meta.fromCache = true;
                        meta.cacheTimestamp = cacheEntry.timestamp;
                        meta.cacheSource = cacheEntry.source || 'memory';
                        if (retryAfter) {
                            meta.retryAfter = retryAfter;
                        }
                        return { data: cacheEntry.data, meta };
                    }
                    if (shouldSendEtag && !triedWithoutEtag) {
                        triedWithoutEtag = true;
                        cacheEntry = null;
                        attempt -= 1;
                        continue;
                    }
                    lastError = new Error('Not Modified response without cached payload');
                    break;
                }
                if (!response.ok) {
                    if (shouldRetry(response.status)) {
                        if (retryAfter) {
                            await sleep(retryAfter);
                        } else if (attempt < retries) {
                            await sleep(Math.pow(2, attempt) * 150);
                        }
                        lastError = new Error(`HTTP ${response.status}`);
                        continue;
                    }
                    const bodyText = await response.text().catch(() => '');
                    const error = new Error(bodyText || `HTTP ${response.status}`);
                    error.status = response.status;
                    throw error;
                }
                const text = await response.text();
                const data = text ? JSON.parse(text) : null;
                const etag = response.headers.get('ETag');
                storeCache(path, data, etag);
                breaker.recordSuccess();
                meta.fromCache = false;
                meta.cacheTimestamp = now();
                if (retryAfter) {
                    meta.retryAfter = retryAfter;
                }
                return { data, meta };
            } catch (error) {
                clearTimeout(timeoutId);
                lastError = error;
                if (isDev) {
                    console.warn(`[apiClient] request error ${path}`, { requestId, attempt, error });
                }
                if (attempt < retries && shouldRetry(error.status, error)) {
                    await sleep(Math.pow(2, attempt) * 150);
                    continue;
                }
                breaker.recordFailure();
                break;
            }
        }

        if (cacheEntry) {
            meta.fromCache = true;
            meta.usedCacheDueToError = true;
            meta.cacheTimestamp = cacheEntry.timestamp;
            meta.cacheSource = cacheEntry.source || 'memory';
            if (isDev) {
                console.debug(`[apiClient] serving cache after failure for ${path}`, meta);
            }
            return { data: cacheEntry.data, meta };
        }
        throw lastError || new Error('Network request failed');
    }

    async function healthCheck(seasonId) {
        try {
            const { data } = await fetchJson('/health', { retries: 0, timeoutMs: 2000 });
            return !!(data && data.ok);
        } catch (error) {
            if (isDev) {
                console.warn('[apiClient] /health unavailable, falling back to HEAD probe', error);
            }
            try {
                const season = encodeURIComponent(seasonId || 'current');
                const response = await fetch(`${DEFAULTS.baseUrl}/seasons/${season}/divisions`, {
                    method: 'HEAD'
                });
                if (response.status === 405) {
                    return true;
                }
                return response.status >= 200 && response.status < 400;
            } catch (probeError) {
                return false;
            }
        }
    }

    class ApiClient {
        constructor() {
            this.fetchJson = fetchJson;
            this.healthCheck = healthCheck;
        }

        proxyAvatar(url) {
            const fallback = window.PAPPALIIGA_DEFAULT_LOGO;
            if (!url) return fallback;
            try {
                const parsed = new URL(url, window.location.origin);
                if (parsed.hostname.endsWith('faceit-cdn.net')) {
                    const params = new URLSearchParams({ url, fallback });
                    return `${DEFAULTS.baseUrl}/proxy-image?${params.toString()}`;
                }
                return url;
            } catch (error) {
                return url;
            }
        }

        async getSeasonSummary(seasonId) {
            const path = `/seasons/${encodeURIComponent(seasonId)}/summary`;
            try {
                const result = await fetchJson(path, {});
                const payload = result?.data ?? result;
                return { data: payload || {}, meta: result?.meta || {} };
            } catch (error) {
                if (error && error.status === 404) {
                    if (isDev) {
                        console.warn('[apiClient] /seasons/:id/summary missing, falling back to stats endpoint');
                    }
                    const fallback = await this.getSeasonStats(seasonId).catch(() => ({}));
                    const payload = fallback?.data ?? fallback ?? {};
                    return { data: payload, meta: { fallback: 'stats' } };
                }
                throw error;
            }
        }

        async getDivisions(seasonId) {
            const path = `/seasons/${encodeURIComponent(seasonId)}/divisions`;
            try {
                const result = await fetchJson(path, {});
                const payload = Array.isArray(result?.data)
                    ? result.data
                    : Array.isArray(result)
                        ? result
                        : [];
                return { data: payload, meta: result?.meta || {}, errors: [], validationCounts: {} };
            } catch (error) {
                if (error && error.status === 404) {
                    if (isDev) {
                        console.warn('[apiClient] /seasons/:id/divisions missing, falling back to legacy endpoint');
                    }
                    let legacyPayload = null;
                    try {
                        legacyPayload = await fetchJson(`/divisions/season/${encodeURIComponent(seasonId)}`);
                    } catch (legacyError) {
                        if (isDev) {
                            console.error('[apiClient] legacy divisions endpoint also failed', legacyError);
                        }
                        return {
                            data: [],
                            meta: { fallback: 'legacy-error' },
                            errors: [],
                            validationCounts: {}
                        };
                    }
                    const payload = Array.isArray(legacyPayload?.data)
                        ? legacyPayload.data
                        : Array.isArray(legacyPayload)
                            ? legacyPayload
                            : [];
                    return {
                        data: payload,
                        meta: { ...(legacyPayload?.meta || {}), fallback: 'legacy' },
                        errors: [],
                        validationCounts: {}
                    };
                }
                throw error;
            }
        }

        async fetchLifetimeSummary() {
            return fetchJson('/home');
        }

        async getStatsOverview() {
            return fetchJson('/stats/overview');
        }

        async getSeasons() {
            const result = await fetchJson('/divisions/seasons');
            return result?.data ?? result;
        }

        async getSeasonStats(seasonId) {
            return fetchJson(`/seasons/${encodeURIComponent(seasonId)}/stats`);
        }

        async getDivisionsBySeason(seasonId) {
            return fetchJson(`/divisions/season/${encodeURIComponent(seasonId)}`);
        }
    }

    window.ApiValidationError = ApiValidationError;
    window.apiClient = new ApiClient();
})();
