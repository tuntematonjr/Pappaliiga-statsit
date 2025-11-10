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
    const API_ROOT = (() => {
        if (typeof window === 'undefined') {
            return { origin: '', path: '' };
        }
        try {
            const parsed = new URL(DEFAULTS.baseUrl, window.location.origin);
            return {
                origin: `${parsed.protocol}//${parsed.host}`,
                path: parsed.pathname.replace(/\/$/, '') || ''
            };
        } catch (error) {
            return {
                origin: window.location.origin,
                path: ''
            };
        }
    })();

    class ApiEndpointNotFound extends Error {
        constructor(message, details = {}) {
            super(message);
            this.name = 'ApiEndpointNotFound';
            this.paths = details.paths || [];
        }
    }

    const ROUTE_MAP = Object.freeze({
        seasons: () => [
            `/api/seasons`,
            `/api/v1/seasons`
        ],
        seasonSummary: seasonId => [
            `/api/v3/summary/${seasonId}`,
            `/api/seasons/${seasonId}/summary`,
            `/api/v1/seasons/${seasonId}/summary`,
            `/api/seasons/${seasonId}/stats/summary`
        ],
        seasonDivisions: seasonId => [
            `/api/v3/divisions/${seasonId}`,
            `/api/seasons/${seasonId}/divisions`,
            `/api/v1/seasons/${seasonId}/divisions`,
            `/api/divisions/season/${seasonId}`,
            `/api/v1/divisions/season/${seasonId}`,
            `/api/divisions?season=${seasonId}`,
            `/api/v1/divisions?season=${seasonId}`
        ],
        divisionStats: (seasonId, divisionId) => [
            `/api/seasons/${seasonId}/divisions/${divisionId}/stats`,
            `/api/v1/seasons/${seasonId}/divisions/${divisionId}/stats`,
            `/api/divisions/${divisionId}/stats`
        ],
        // Legacy compatibility
        divisions: seasonId => [
            `/api/v3/divisions/${seasonId}`,
            `/api/divisions/season/${seasonId}`,
            `/api/seasons/${seasonId}/divisions`,
            `/api/v1/divisions/season/${seasonId}`,
            `/api/v1/seasons/${seasonId}/divisions`,
            `/api/divisions?season=${seasonId}`,
            `/api/v1/divisions?season=${seasonId}`
        ],
        summary: seasonId => [
            `/api/v3/summary/${seasonId}`,
            `/api/seasons/${seasonId}/summary`,
            `/api/v1/seasons/${seasonId}/summary`,
            `/api/seasons/${seasonId}/stats/summary`
        ],
        health: () => [`/api/health`, `/api/v1/health`, `/healthz`]
    });

    function encodeSeasonId(value) {
        return encodeURIComponent(value == null ? '' : value);
    }

    function normalizeRouteCandidate(route) {
        if (!route) return null;
        const raw = String(route).trim();
        if (!raw) return null;
        if (/^https?:\/\//i.test(raw)) {
            return {
                absoluteUrl: raw,
                displayPath: raw
            };
        }
        const normalized = raw.startsWith('/') ? raw : `/${raw}`;
        const origin = API_ROOT.origin || (typeof window !== 'undefined' && window.location ? window.location.origin : '');
        if (!origin) {
            return {
                absoluteUrl: `${DEFAULTS.baseUrl}${normalized}`,
                displayPath: normalized
            };
        }
        return {
            absoluteUrl: `${origin}${normalized}`,
            displayPath: normalized
        };
    }

    function buildRouteCandidates(key, ...args) {
        const resolver = ROUTE_MAP[key];
        if (!resolver) {
            return [];
        }
        const list = resolver(...args);
        if (!Array.isArray(list)) {
            return [];
        }
        return list.map(normalizeRouteCandidate).filter(Boolean);
    }

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

    function readCacheEntry(key) {
        if (!key) return null;
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

    function writeCacheEntry(key, payload, etag) {
        if (!key) return;
        const entry = { data: payload, timestamp: now(), etag: etag || null };
        MEMORY_CACHE.set(key, entry);
        persistentStore.data[key] = entry;
        persistentStore.meta[key] = { cachedAt: entry.timestamp };
        writePersistentCache(persistentStore.data, persistentStore.meta);
    }

    function resolveRequestTarget(path, options = {}) {
        const rawPath = typeof path === 'string' ? path : String(path || '');
        const absoluteOverride = options.absoluteUrl;
        const pathLabel = rawPath || '/';
        if (absoluteOverride) {
            return {
                url: absoluteOverride,
                cacheKey: absoluteOverride,
                breakerKey: absoluteOverride,
                displayPath: pathLabel || absoluteOverride,
                isAbsolute: true
            };
        }
        if (/^https?:\/\//i.test(pathLabel)) {
            return {
                url: pathLabel,
                cacheKey: pathLabel,
                breakerKey: pathLabel,
                displayPath: pathLabel,
                isAbsolute: true
            };
        }
        const normalizedPath = pathLabel.startsWith('/') ? pathLabel : `/${pathLabel}`;
        return {
            url: `${DEFAULTS.baseUrl}${normalizedPath}`,
            cacheKey: makeCacheKey(normalizedPath),
            breakerKey: normalizedPath,
            displayPath: normalizedPath,
            isAbsolute: false
        };
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

    function extractDivisionArray(raw) {
        const metaHints = {};
        if (!raw || typeof raw !== 'object') {
            return { list: Array.isArray(raw) ? raw : [], meta: metaHints };
        }
        if (Array.isArray(raw)) {
            return { list: raw, meta: metaHints };
        }
        if (Array.isArray(raw.items)) {
            return { list: raw.items, meta: raw.meta || raw.pagination || metaHints };
        }
        if (Array.isArray(raw.divisions)) {
            return { list: raw.divisions, meta: raw.meta || metaHints };
        }
        if (Array.isArray(raw.results)) {
            return { list: raw.results, meta: raw.meta || metaHints };
        }
        if (Array.isArray(raw.records)) {
            return { list: raw.records, meta: raw.meta || metaHints };
        }
        const nested = raw.data || raw.payload || raw.body || raw.result;
        if (nested && nested !== raw) {
            const nestedExtract = extractDivisionArray(nested);
            return {
                list: nestedExtract.list,
                meta: nestedExtract.meta || raw.meta || metaHints
            };
        }
        return { list: [], meta: raw.meta || metaHints };
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
        const target = resolveRequestTarget(path, options);
        const breaker = circuitBreaker(target.breakerKey);
        let cacheEntry = readCacheEntry(target.cacheKey);
        const requestId = `req_${Math.random().toString(36).slice(2, 8)}`;
        const meta = {
            requestId,
            attempts: 0,
            fromCache: false,
            cacheTimestamp: cacheEntry?.timestamp ?? null,
            cacheSource: cacheEntry?.source || null,
            usedCacheDueToError: false,
            breakerOpen: false,
            telemetry: [],
            path: target.displayPath,
            requestUrl: target.url
        };

        if (breaker.isOpen()) {
            meta.breakerOpen = true;
            if (cacheEntry) {
                meta.fromCache = true;
                meta.usedCacheDueToError = true;
                if (isDev) {
                    console.debug(`[apiClient] breaker open for ${target.displayPath}, serving cache`, meta);
                }
                return { data: cacheEntry.data, meta };
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
                const response = await fetch(target.url, {
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
                    latency: performance.now() - start,
                    path: target.displayPath
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
                        lastError.status = response.status;
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
                writeCacheEntry(target.cacheKey, data, etag);
                breaker.recordSuccess();
                meta.fromCache = false;
                meta.cacheTimestamp = now();
                meta.cacheSource = 'network';
                if (retryAfter) {
                    meta.retryAfter = retryAfter;
                }
                return { data, meta };
            } catch (error) {
                clearTimeout(timeoutId);
                lastError = error;
                if (isDev) {
                    console.warn(`[apiClient] request error ${target.displayPath}`, { requestId, attempt, error });
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
                console.debug(`[apiClient] serving cache after failure for ${target.displayPath}`, meta);
            }
            return { data: cacheEntry.data, meta };
        }
        throw lastError || new Error('Network request failed');
    }

    async function fetchWithFallback(paths, options = {}) {
        const candidates = (Array.isArray(paths) ? paths : [])
            .map(entry => (typeof entry === 'string' ? normalizeRouteCandidate(entry) : entry))
            .filter(Boolean);
        if (!candidates.length) {
            throw new ApiEndpointNotFound('No API routes configured', { paths: [] });
        }
        const tried = [];
        let lastError = null;
        const initialMethod = options.method || 'GET';

        for (let index = 0; index < candidates.length; index += 1) {
            const candidate = candidates[index];
            try {
                const result = await fetchJson(candidate.displayPath, {
                    ...options,
                    absoluteUrl: candidate.absoluteUrl
                });
                result.meta = {
                    ...(result.meta || {}),
                    resolvedPath: candidate.displayPath
                };
                if (index > 0 && typeof console !== 'undefined') {
                    console.info(`[apiClient] fallback resolved via ${candidate.displayPath}`);
                }
                return result;
            } catch (error) {
                const status = error?.status;
                tried.push(candidate.displayPath);
                lastError = error;
                if (status === 404 || status === 410 || status === 501) {
                    continue;
                }
                if (status === 405 && initialMethod.toUpperCase() === 'HEAD') {
                    try {
                        const retryResult = await fetchJson(candidate.displayPath, {
                            ...options,
                            method: 'GET',
                            absoluteUrl: candidate.absoluteUrl
                        });
                        retryResult.meta = {
                            ...(retryResult.meta || {}),
                            resolvedPath: candidate.displayPath,
                            methodOverride: 'GET'
                        };
                        if (typeof console !== 'undefined') {
                            console.info(`[apiClient] HEAD rejected for ${candidate.displayPath}, retried with GET`);
                        }
                        return retryResult;
                    } catch (retryError) {
                        lastError = retryError;
                        continue;
                    }
                }
                throw error;
            }
        }
        const error = new ApiEndpointNotFound('API endpoints not found', { paths: tried });
        error.paths = tried;
        if (lastError) {
            error.cause = lastError;
        }
        throw error;
    }

    async function healthCheck(seasonId) {
        const healthRoutes = buildRouteCandidates('health');
        for (const candidate of healthRoutes) {
            try {
                const response = await fetch(candidate.absoluteUrl, {
                    method: 'GET',
                    credentials: 'same-origin',
                    headers: { Accept: 'application/json' }
                });
                if (response.status === 404) {
                    continue;
                }
                if (response.ok) {
                    let payload = null;
                    try {
                        payload = await response.json();
                    } catch (parseError) {
                        payload = null;
                    }
                    const ok = payload && Object.prototype.hasOwnProperty.call(payload, 'ok') ? Boolean(payload.ok) : true;
                    return { ok, status: response.status, route: candidate.displayPath, source: 'health', probableRoute: false };
                }
                if (response.status === 204) {
                    return { ok: true, status: 204, route: candidate.displayPath, source: 'health', probableRoute: false };
                }
                if (response.status >= 200 && response.status < 500) {
                    return { ok: true, status: response.status, route: candidate.displayPath, source: 'health', probableRoute: false };
                }
            } catch (error) {
                // ignore individual health errors, fall through to next candidate
            }
        }

        const fallbackSeason = encodeSeasonId(seasonId || 'current');
        const divisionRoutes = buildRouteCandidates('divisions', fallbackSeason);
        if (divisionRoutes.length) {
            const primary = divisionRoutes[0];
            try {
                const response = await fetch(primary.absoluteUrl, {
                    method: 'HEAD',
                    credentials: 'same-origin'
                });
                const allow = response.headers ? response.headers.get('Allow') || '' : '';
                if (response.status === 405 && /GET/i.test(allow)) {
                    return {
                        ok: true,
                        probableRoute: true,
                        status: response.status,
                        route: primary.displayPath,
                        source: 'divisions-head'
                    };
                }
                    return {
                        ok: response.ok,
                        status: response.status,
                        route: primary.displayPath,
                        source: 'divisions-head',
                        probableRoute: false
                    };
            } catch (error) {
                // ignore and fall through
            }
        }

        return { ok: false, status: null, route: null, probableRoute: false, source: 'health' };
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
            const identifier = encodeSeasonId(seasonId);
            const routes = buildRouteCandidates('summary', identifier);
            try {
                const result = await fetchWithFallback(routes);
                const payload = result?.data ?? result;
                return { data: payload || {}, meta: result?.meta || {} };
            } catch (error) {
                if (error instanceof ApiEndpointNotFound) {
                    if (isDev) {
                        console.warn('[apiClient] summary routes missing, falling back to stats summary');
                    }
                    const fallback = await this.getSeasonStats(seasonId).catch(() => ({}));
                    const payload = fallback?.data ?? fallback ?? {};
                    return { data: payload, meta: { fallback: 'stats', error: 'summary-routes-missing' } };
                }
                throw error;
            }
        }

        async getDivisions(seasonId) {
            const identifier = encodeSeasonId(seasonId);
            const routes = buildRouteCandidates('divisions', identifier);
            const result = await fetchWithFallback(routes);
            const basePayload = result?.data ?? result;
            const { list, meta: payloadMeta } = extractDivisionArray(basePayload);
            const meta = { ...(payloadMeta || {}), ...(result?.meta || {}) };
            if (!list.length && isDev) {
                console.warn('[apiClient] Division payload empty from', meta.resolvedPath || 'unknown route');
            }
            return { data: list, meta, errors: [], validationCounts: {} };
        }

        async fetchLifetimeSummary() {
            return fetchJson('/home');
        }

        async getStatsOverview() {
            return fetchJson('/stats/overview');
        }

        async getSeasons() {
            const routes = buildRouteCandidates('seasons');
            try {
                const result = await fetchWithFallback(routes);
                const payload = result?.data ?? result;
                return Array.isArray(payload) ? payload : [];
            } catch (error) {
                if (isDev) {
                    console.warn('[apiClient] seasons endpoint failed, falling back to legacy', error);
                }
                // Fallback to legacy endpoint
                const legacyResult = await fetchJson('/divisions/seasons').catch(() => []);
                return legacyResult?.data ?? legacyResult ?? [];
            }
        }

        async getSeasonStats(seasonId) {
            return fetchJson(`/seasons/${encodeURIComponent(seasonId)}/stats`);
        }

        async getDivisionsBySeason(seasonId) {
            return fetchJson(`/divisions/season/${encodeURIComponent(seasonId)}`);
        }

        async getDivisionDetailedStats(seasonId, divisionId) {
            const encodedSeason = encodeURIComponent(seasonId);
            const encodedDivision = encodeURIComponent(divisionId);
            const routes = buildRouteCandidates('divisionStats', encodedSeason, encodedDivision);
            try {
                const result = await fetchWithFallback(routes);
                return result?.data ?? result;
            } catch (error) {
                if (isDev) {
                    console.warn('[apiClient] divisionStats endpoint failed', error);
                }
                throw error;
            }
        }
    }

    window.ApiValidationError = ApiValidationError;
    window.ApiEndpointNotFound = ApiEndpointNotFound;
    window.apiClient = new ApiClient();
})();
