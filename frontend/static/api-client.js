/**
 * Resilient API client with caching, retries, validation, and circuit breaker support.
 * Exposed globally as window.apiClient.
 */
(function () {
    const BROWSER_ORIGIN =
        typeof window !== 'undefined' && /^https?:\/\//i.test(window.location.origin || '')
            ? window.location.origin
            : '';

    const DEFAULTS = Object.freeze({
        baseUrl:
            (typeof window !== 'undefined' && window.PL_API_URL) ||
            (typeof window !== 'undefined' && window.__API_BASE__) ||
            (BROWSER_ORIGIN ? `${BROWSER_ORIGIN}/api` : '/api'),
        timeoutMs: Number((typeof window !== 'undefined' && window.PL_API_TIMEOUT_MS) || 8000),
        retries: Number((typeof window !== 'undefined' && window.PL_API_RETRY) || 2),
        cacheTtlMs: Number((typeof window !== 'undefined' && window.PL_API_CACHE_TTL_MS) || 300000)
    });

    const MEMORY_CACHE = new Map();
    const PERSIST_KEY = 'pl:cache:v1';
    const CACHE_META_KEY = 'pl:cache:meta';
    const MAX_PERSISTED_ENTRIES = Math.max(
        25,
        Number((typeof window !== 'undefined' && window.PL_API_PERSIST_MAX_KEYS) || 120)
    );
    const MAX_PERSISTED_ENTRY_CHARS = Math.max(
        4096,
        Number((typeof window !== 'undefined' && window.PL_API_PERSIST_MAX_ENTRY_CHARS) || 120000)
    );
    const persistentCacheState = {
        enabled: true,
        quotaWarned: false
    };
    const BREAKERS = new Map();
    const CONSECUTIVE_FAILURE_LIMIT = 3;
    const FAILURE_WINDOW_MS = 60000;
    const BREAKER_COOLDOWN_MS = 30000;
    const DIVISION_CACHE_TTL_MS = 2 * 60 * 1000;
    const isDev = typeof window !== 'undefined' && ['localhost', '127.0.0.1'].includes(window.location.hostname);
    const API_ROOT = (() => {
        if (typeof window === 'undefined') {
            return { origin: '', path: '' };
        }
        try {
            const parsed = new URL(DEFAULTS.baseUrl, window.location.origin);
            const origin = `${parsed.protocol}//${parsed.host}`;
            if (!/^https?:\/\//i.test(origin)) {
                return { origin: '', path: parsed.pathname.replace(/\/$/, '') || '' };
            }
            return {
                origin,
                path: parsed.pathname.replace(/\/$/, '') || ''
            };
        } catch (error) {
            return {
                origin: BROWSER_ORIGIN,
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
            `/api/seasons`
        ],
        divisionDetails: divisionId => [
            `/api/divisions/${divisionId}`
        ],
        divisionMatches: divisionId => [
            `/api/matches/division/${divisionId}`
        ],
        divisionAverages: championshipId => [
            `/api/stats/division/${championshipId}/averages`
        ],
        teamsList: query => [
            `/api/teams${query}`
        ],
        teamPage: (teamId, seasonId) => {
            const seasonQuery = seasonId ? `?championship_id=${seasonId}` : '';
            return [`/api/teams/${teamId}/page${seasonQuery}`];
        },
        playersList: query => [
            `/api/players${query}`
        ],
        playerBundle: (playerId, championshipId = null) => {
            const champParam = championshipId ? `?championship_id=${encodeURIComponent(championshipId)}` : '';
            return [
                `/api/players/${playerId}/bundle${champParam}`
            ];
        },
        teamMatchPlayerStats: (teamId, championshipId) => [
            `/api/teams/${teamId}/match-player-stats/${championshipId}`
        ],
        divisions: seasonId => [
            `/api/season-view/divisions/${seasonId}`
        ],
        summary: seasonId => [
            `/api/stats/summary/season/${seasonId}`
        ],
        health: () => [`/api/health`],
        debugStatus: () => [`/api/debug/status`],
        mapsCatalog: () => [
            `/api/maps`,
            `/api/maps/`
        ],
        upcomingMatches: query => [
            `/api/matches/upcoming${query}`
        ],
        matchBundle: matchId => [
            `/api/matches/${matchId}/bundle`
        ],
        matchDemos: (matchId, query) => [
            `/api/matches/${matchId}/demos${query}`
        ]
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
        const origin = API_ROOT.origin || BROWSER_ORIGIN;
        if (!origin || !/^https?:\/\//i.test(origin)) {
            return {
                absoluteUrl: normalized,
                displayPath: normalized
            };
        }
        return {
            absoluteUrl: `${origin}${normalized}`,
            displayPath: normalized
        };
    }

    function buildQueryString(params) {
        if (!params || typeof params !== 'object') {
            return '';
        }
        const entries = Object.entries(params)
            .filter(([, value]) => value !== undefined && value !== null && value !== '');
        if (!entries.length) {
            return '';
        }
        const query = entries
            .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`)
            .join('&');
        return query ? `?${query}` : '';
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

    function toSnakeCaseKey(key) {
        if (!key || typeof key !== 'string') {
            return key;
        }
        return key.replace(/([A-Z])/g, '_$1').toLowerCase();
    }

    function ensureSnakeCaseDeep(value) {
        if (Array.isArray(value)) {
            return value.map(item => ensureSnakeCaseDeep(item));
        }
        if (!value || typeof value !== 'object') {
            return value;
        }
        const result = {};
        for (const [key, nested] of Object.entries(value)) {
            const normalizedValue = ensureSnakeCaseDeep(nested);
            result[key] = normalizedValue;
            if (/[A-Z]/.test(key)) {
                const snakeKey = toSnakeCaseKey(key);
                if (!(snakeKey in result)) {
                    result[snakeKey] = normalizedValue;
                }
            }
        }
        return result;
    }

    function now() {
        return Date.now();
    }

    function isQuotaExceededError(error) {
        if (!error) return false;
        if (error.name === 'QuotaExceededError') return true;
        if (error.code === 22 || error.code === 1014) return true;
        const message = String(error.message || '').toLowerCase();
        return message.includes('quota') || message.includes('storage full');
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
        if (typeof window === 'undefined' || !window.localStorage || !persistentCacheState.enabled) {
            return false;
        }
        try {
            window.localStorage.setItem(PERSIST_KEY, JSON.stringify(cache));
            window.localStorage.setItem(CACHE_META_KEY, JSON.stringify(meta));
            return true;
        } catch (error) {
            if (isQuotaExceededError(error)) {
                const prunedCount = prunePersistentStore(Math.max(25, Math.floor(MAX_PERSISTED_ENTRIES * 0.6)));
                if (prunedCount > 0) {
                    try {
                        window.localStorage.setItem(PERSIST_KEY, JSON.stringify(cache));
                        window.localStorage.setItem(CACHE_META_KEY, JSON.stringify(meta));
                        return true;
                    } catch (retryError) {
                        error = retryError;
                    }
                }
                persistentCacheState.enabled = false;
                if (!persistentCacheState.quotaWarned) {
                    persistentCacheState.quotaWarned = true;
                    console.warn(
                        '[apiClient] Persistent cache quota exceeded, disabling localStorage cache for this session.',
                        error
                    );
                }
                return false;
            }
            console.warn('[apiClient] Failed to write persistent cache', error);
            return false;
        }
    }

    const persistentStore = readPersistentCache();
    prunePersistentStore(MAX_PERSISTED_ENTRIES);

    function prunePersistentStore(maxEntries = MAX_PERSISTED_ENTRIES) {
        const data = persistentStore?.data;
        const meta = persistentStore?.meta;
        if (!data || !meta) {
            return 0;
        }
        const ttlCutoff = now() - DEFAULTS.cacheTtlMs;
        let removed = 0;
        const keys = Object.keys(data);
        keys.forEach(key => {
            const entry = data[key];
            const timestamp = Number(entry?.timestamp || 0);
            if (!entry || !Number.isFinite(timestamp) || timestamp < ttlCutoff) {
                delete data[key];
                delete meta[key];
                removed += 1;
            }
        });
        const remainingKeys = Object.keys(data);
        if (remainingKeys.length > maxEntries) {
            const ordered = remainingKeys.sort((a, b) => {
                const aTs = Number(data[a]?.timestamp || 0);
                const bTs = Number(data[b]?.timestamp || 0);
                return aTs - bTs;
            });
            const toDrop = ordered.slice(0, remainingKeys.length - maxEntries);
            toDrop.forEach(key => {
                delete data[key];
                delete meta[key];
                removed += 1;
            });
        }
        return removed;
    }

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

    function writeCacheEntry(key, payload, etag, options = {}) {
        if (!key) return;
        const entry = { data: payload, timestamp: now(), etag: etag || null };
        MEMORY_CACHE.set(key, entry);
        if (!persistentCacheState.enabled || options.persistCache === false) {
            return;
        }
        const payloadSize = Number(options.payloadSize || 0);
        if (Number.isFinite(payloadSize) && payloadSize > MAX_PERSISTED_ENTRY_CHARS) {
            return;
        }
        persistentStore.data[key] = entry;
        persistentStore.meta[key] = { cachedAt: entry.timestamp };
        prunePersistentStore(MAX_PERSISTED_ENTRIES);
        const wrote = writePersistentCache(persistentStore.data, persistentStore.meta);
        if (!wrote && !persistentCacheState.enabled) {
            delete persistentStore.data[key];
            delete persistentStore.meta[key];
        }
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
                writeCacheEntry(target.cacheKey, data, etag, {
                    persistCache: options.persistCache !== false,
                    payloadSize: text ? text.length : 0
                });
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
                    console.warn(`[apiClient] request error ${target.displayPath}`, {
                        requestId,
                        attempt,
                        requestUrl: target.url,
                        error,
                        message: error?.message || String(error || '')
                    });
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
        if (candidates.length === 1 && lastError) {
            throw lastError;
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
            this._divisionDetailsCache = new Map();
            this._divisionCacheTtlMs = DIVISION_CACHE_TTL_MS;
            this._mapCatalogCache = null;
            this._divisionTeamCountCache = new Map();
            this._seasonTeamCountCache = new Map();
            this._lifetimeUniqueTeamCountCache = null;
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
            const result = await fetchWithFallback(routes);
            const payload = result?.data ?? result;
            return { data: payload || {}, meta: result?.meta || {} };
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
            return await fetchJson('/stats/summary/all');
        }

        async getSeasons() {
            const routes = buildRouteCandidates('seasons');
            const result = await fetchWithFallback(routes);
            const payload = result?.data ?? result;
            return Array.isArray(payload) ? payload : [];
        }

        async getDebugStatus(options = {}) {
            const routes = buildRouteCandidates('debugStatus');
            const result = await fetchWithFallback(routes, {
                persistCache: false,
                ...options
            });
            const payload = result?.data ?? result;
            return payload || {};
        }

        async getMapsCatalog(options = {}) {
            const { force = false, cacheTtlMs = 5 * 60 * 1000, ...requestOptions } = options || {};
            const cached = this._mapCatalogCache;
            if (!force && cached && cached.data && now() - cached.timestamp < cacheTtlMs) {
                return cached.data;
            }
            const routes = buildRouteCandidates('mapsCatalog');
            const result = await fetchWithFallback(routes, requestOptions);
            const payload = result?.data ?? result ?? [];
            const normalized = ensureSnakeCaseDeep(payload);
            const data = Array.isArray(normalized) ? normalized : [];
            this._mapCatalogCache = { data, timestamp: now() };
            return data;
        }

        async getDivisionById(championshipId, options = {}) {
            if (!championshipId) {
                throw new Error('championshipId is required');
            }
            const cacheKey = String(championshipId);
            const {
                force = false,
                noCache = false,
                cacheTtlMs = this._divisionCacheTtlMs,
                ...requestOptions
            } = options || {};
            const skipCache = force === true || noCache === true;
            if (!skipCache) {
                const cached = this._divisionDetailsCache.get(cacheKey);
                if (cached) {
                    if (cached.promise) {
                        return cached.promise;
                    }
                    if (cached.data && now() - cached.timestamp < cacheTtlMs) {
                        return cached.data;
                    }
                    this._divisionDetailsCache.delete(cacheKey);
                }
            }
            const fetchPromise = this._fetchDivisionDetails(cacheKey, requestOptions);
            if (!skipCache) {
                this._divisionDetailsCache.set(cacheKey, { promise: fetchPromise });
            }
            try {
                const data = await fetchPromise;
                if (!skipCache) {
                    this._divisionDetailsCache.set(cacheKey, { data, timestamp: now() });
                }
                return data;
            } catch (error) {
                if (!skipCache) {
                    const tracked = this._divisionDetailsCache.get(cacheKey);
                    if (tracked && tracked.promise === fetchPromise) {
                        this._divisionDetailsCache.delete(cacheKey);
                    }
                }
                throw error;
            }
        }

        async getDivisionAverages(championshipId, options = {}) {
            if (!championshipId) {
                throw new Error('championshipId is required');
            }
            const encodedId = encodeURIComponent(championshipId);
            const routes = buildRouteCandidates('divisionAverages', encodedId);
            const result = await fetchWithFallback(routes, options);
            const payload = result?.data ?? result ?? {};
            return ensureSnakeCaseDeep(payload) || {};
        }

        async getDivisionTeamCount(championshipId, options = {}) {
            if (!championshipId) {
                throw new Error('championshipId is required');
            }
            const cacheKey = String(championshipId);
            const {
                force = false,
                noCache = false,
                cacheTtlMs = this._divisionCacheTtlMs,
                ...requestOptions
            } = options || {};
            const skipCache = force === true || noCache === true;
            if (!skipCache) {
                const cached = this._divisionTeamCountCache.get(cacheKey);
                if (cached) {
                    if (cached.promise) {
                        return cached.promise;
                    }
                    if (cached.data !== undefined && now() - cached.timestamp < cacheTtlMs) {
                        return cached.data;
                    }
                    this._divisionTeamCountCache.delete(cacheKey);
                }
            }
            const fetchPromise = this.getDivisionById(cacheKey, requestOptions)
                .then(details => {
                    const aggregates = details?.aggregates || {};
                    const aggregateCount = Number(aggregates.team_count ?? aggregates.teams);
                    const directCount = Number(details?.team_count ?? details?.teams_count ?? details?.total_teams);
                    const teams = Array.isArray(details?.teams) ? details.teams : [];
                    const resolved =
                        (Number.isFinite(aggregateCount) && aggregateCount > 0
                            ? aggregateCount
                            : Number.isFinite(directCount) && directCount > 0
                                ? directCount
                                : teams.length) || 0;
                    return Number.isFinite(resolved) ? resolved : 0;
                });
            if (!skipCache) {
                this._divisionTeamCountCache.set(cacheKey, { promise: fetchPromise });
            }
            try {
                const count = await fetchPromise;
                if (!skipCache) {
                    this._divisionTeamCountCache.set(cacheKey, { data: count, timestamp: now() });
                }
                return count;
            } catch (error) {
                if (!skipCache) {
                    const tracked = this._divisionTeamCountCache.get(cacheKey);
                    if (tracked && tracked.promise === fetchPromise) {
                        this._divisionTeamCountCache.delete(cacheKey);
                    }
                }
                throw error;
            }
        }

        async getDivisionTeamIds(championshipId, options = {}) {
            if (!championshipId) {
                throw new Error('championshipId is required');
            }
            const details = await this.getDivisionById(championshipId, options);
            const teams = Array.isArray(details?.teams) ? details.teams : [];
            const ids = teams
                .map(team => {
                    if (team == null) return null;
                    if (typeof team === 'string' || typeof team === 'number') return team;
                    return (
                        team.team_id ??
                        team.teamId ??
                        team.id ??
                        team.teamID ??
                        team.team ??
                        team.name ??
                        null
                    );
                })
                .filter(value => value !== undefined && value !== null)
                .map(value => String(value));
            return ids;
        }

        async getSeasonTeamCount(seasonId, options = {}) {
            const {
                force = false,
                noCache = false,
                cacheTtlMs = this._divisionCacheTtlMs,
                divisions,
                ...requestOptions
            } = options || {};
            const cacheKey = seasonId != null ? String(seasonId) : null;
            const skipCache = force === true || noCache === true || !cacheKey;
            if (!skipCache) {
                const cached = this._seasonTeamCountCache.get(cacheKey);
                if (cached) {
                    if (cached.promise) {
                        return cached.promise;
                    }
                    if (cached.data !== undefined && now() - cached.timestamp < cacheTtlMs) {
                        return cached.data;
                    }
                    this._seasonTeamCountCache.delete(cacheKey);
                }
            }
            const fetchPromise = (async () => {
                let list = Array.isArray(divisions) ? divisions : null;
                if (!list) {
                    if (!cacheKey) return 0;
                    const result = await this.getDivisions(cacheKey);
                    list = Array.isArray(result?.data) ? result.data : Array.isArray(result) ? result : [];
                }
                const filtered = list.filter(entry => !(entry?.is_playoff || entry?.isPlayoff));
                const ids = filtered
                    .map(entry => entry?.division_id ?? entry?.divisionId ?? entry?.id ?? entry?.slug)
                    .filter(value => value !== undefined && value !== null)
                    .map(value => String(value));
                const uniqueIds = Array.from(new Set(ids));
                if (!uniqueIds.length) return 0;
                const counts = await Promise.all(
                    uniqueIds.map(id =>
                        this.getDivisionTeamCount(id, requestOptions).catch(() => 0)
                    )
                );
                return counts.reduce((sum, value) => sum + (Number.isFinite(value) ? value : 0), 0);
            })();
            if (!skipCache) {
                this._seasonTeamCountCache.set(cacheKey, { promise: fetchPromise });
            }
            try {
                const total = await fetchPromise;
                if (!skipCache) {
                    this._seasonTeamCountCache.set(cacheKey, { data: total, timestamp: now() });
                }
                return total;
            } catch (error) {
                if (!skipCache) {
                    const tracked = this._seasonTeamCountCache.get(cacheKey);
                    if (tracked && tracked.promise === fetchPromise) {
                        this._seasonTeamCountCache.delete(cacheKey);
                    }
                }
                throw error;
            }
        }

        async getLifetimeUniqueTeamCount(options = {}) {
            const {
                force = false,
                noCache = false,
                cacheTtlMs = this._divisionCacheTtlMs,
                seasons,
                includePlayoffs = false,
                ...requestOptions
            } = options || {};
            const skipCache = force === true || noCache === true;
            if (!skipCache && this._lifetimeUniqueTeamCountCache) {
                if (this._lifetimeUniqueTeamCountCache.promise) {
                    return this._lifetimeUniqueTeamCountCache.promise;
                }
                if (
                    this._lifetimeUniqueTeamCountCache.data !== undefined &&
                    now() - this._lifetimeUniqueTeamCountCache.timestamp < cacheTtlMs
                ) {
                    return this._lifetimeUniqueTeamCountCache.data;
                }
                this._lifetimeUniqueTeamCountCache = null;
            }
            const fetchPromise = (async () => {
                const seasonList = Array.isArray(seasons) ? seasons : await this.getSeasons();
                const seasonIds = seasonList
                    .map(entry =>
                        entry?.api_param ??
                        entry?.apiParam ??
                        entry?.id ??
                        entry?.season_id ??
                        entry?.seasonId ??
                        entry?.season ??
                        entry?.number ??
                        entry?.key
                    )
                    .filter(value => value !== undefined && value !== null)
                    .map(value => String(value));
                const uniqueSeasonIds = Array.from(new Set(seasonIds));
                if (!uniqueSeasonIds.length) return 0;

                const uniqueTeams = new Set();
                for (const seasonId of uniqueSeasonIds) {
                    let list = [];
                    try {
                        const result = await this.getDivisions(seasonId);
                        list = Array.isArray(result?.data) ? result.data : Array.isArray(result) ? result : [];
                    } catch (error) {
                        // skip season on fetch error
                        continue;
                    }
                    const filtered = includePlayoffs
                        ? list
                        : list.filter(entry => !(entry?.is_playoff || entry?.isPlayoff));
                    const divisionIds = filtered
                        .map(entry => entry?.division_id ?? entry?.divisionId ?? entry?.id ?? entry?.slug)
                        .filter(value => value !== undefined && value !== null)
                        .map(value => String(value));
                    const uniqueDivisionIds = Array.from(new Set(divisionIds));
                    if (!uniqueDivisionIds.length) continue;
                    const teamIdLists = await Promise.all(
                        uniqueDivisionIds.map(id =>
                            this.getDivisionTeamIds(id, requestOptions).catch(() => [])
                        )
                    );
                    teamIdLists.flat().forEach(teamId => {
                        if (teamId != null) {
                            uniqueTeams.add(String(teamId));
                        }
                    });
                }
                return uniqueTeams.size;
            })();
            if (!skipCache) {
                this._lifetimeUniqueTeamCountCache = { promise: fetchPromise };
            }
            try {
                const total = await fetchPromise;
                if (!skipCache) {
                    this._lifetimeUniqueTeamCountCache = { data: total, timestamp: now() };
                }
                return total;
            } catch (error) {
                if (!skipCache && this._lifetimeUniqueTeamCountCache?.promise === fetchPromise) {
                    this._lifetimeUniqueTeamCountCache = null;
                }
                throw error;
            }
        }

        async _fetchDivisionDetails(championshipId, requestOptions = {}) {
            const encodedId = encodeURIComponent(championshipId);
            const routes = buildRouteCandidates('divisionDetails', encodedId);
            const result = await fetchWithFallback(routes, {
                ...requestOptions,
                persistCache: false
            });
            const payload = result?.data ?? result ?? {};
            const normalized = ensureSnakeCaseDeep(payload) || {};
            if (!Array.isArray(normalized.teams)) {
                normalized.teams = [];
            }
            if (!Array.isArray(normalized.map_stats)) {
                normalized.map_stats = [];
            }
            if (!Array.isArray(normalized.excluded_team_ids)) {
                normalized.excluded_team_ids = [];
            }
            normalized._meta = result?.meta || {};
            return normalized;
        }

        async getDivisionMapStats(championshipId, options = {}) {
            const details = await this.getDivisionById(championshipId, options);
            const stats = details?.map_stats ?? [];
            return Array.isArray(stats) ? stats : [];
        }

        async getDivisionMatches(championshipId, options = {}) {
            if (!championshipId) {
                throw new Error('championshipId is required');
            }
            const encodedId = encodeURIComponent(championshipId);
            const routes = buildRouteCandidates('divisionMatches', encodedId);
            const result = await fetchWithFallback(routes, options);
            const payload = result?.data ?? result ?? {};
            const items = Array.isArray(payload?.items) ? payload.items : (Array.isArray(payload) ? payload : []);
            const targetId = String(championshipId);
            return items.filter(match => String(match?.championship_id ?? match?.championshipId ?? '') === targetId);
        }

        async getUpcomingMatches(params = {}, options = {}) {
            const queryParams = {
                championship_id: params.championshipId ?? params.championship_id ?? null,
                team_id: params.teamId ?? params.team_id ?? null,
                season: params.season ?? params.seasonId ?? params.season_id ?? null,
                include_playoffs:
                    typeof params.includePlayoffs === 'boolean'
                        ? params.includePlayoffs
                        : (typeof params.include_playoffs === 'boolean' ? params.include_playoffs : null),
                limit: params.limit ?? null,
                offset: params.offset ?? null
            };
            const query = buildQueryString(queryParams);
            const routes = buildRouteCandidates('upcomingMatches', query);
            const result = await fetchWithFallback(routes, options);
            const payload = result?.data ?? result ?? {};
            const items = Array.isArray(payload?.items) ? payload.items : (Array.isArray(payload) ? payload : []);
            return {
                items,
                meta: payload?.meta || result?.meta || null
            };
        }

        async getMatchDemos(params = {}, options = {}) {
            const championshipId = params.championshipId ?? params.championship_id ?? null;
            const matchId = params.matchId ?? params.match_id ?? null;
            const expectedCount = params.expectedCount ?? params.expected_count ?? null;
            if (!championshipId || !matchId) {
                throw new Error('championshipId and matchId are required');
            }

            const cacheBypass = options?.forceRefresh === true
                ? `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
                : null;

            const query = buildQueryString({
                championship_id: championshipId,
                expected_count: expectedCount,
                force: options?.forceRefresh === true ? true : null,
                _cb: cacheBypass
            });
            const encMatchId = encodeURIComponent(matchId);
            const routes = buildRouteCandidates('matchDemos', encMatchId, query);
            const result = await fetchWithFallback(routes, {
                ...options,
                persistCache: false
            });
            const payload = ensureSnakeCaseDeep(result?.data ?? result ?? {});
            const items = Array.isArray(payload?.items) ? payload.items : [];
            return items
                .map(item => ({
                    demo_index: Number(item?.demo_index ?? item?.demoIndex ?? -1),
                    url: String(item?.url || '')
                }))
                .filter(item => item.demo_index >= 0 && item.url);
        }

        async getMatchBundle(matchId, options = {}) {
            if (!matchId) {
                throw new Error('matchId is required');
            }
            const encMatchId = encodeURIComponent(matchId);
            const routes = buildRouteCandidates('matchBundle', encMatchId);
            const result = await fetchWithFallback(routes, {
                ...options,
                persistCache: false
            });
            const payload = ensureSnakeCaseDeep(result?.data ?? result ?? {});
            return {
                details: payload?.details || {},
                playerStats: Array.isArray(payload?.player_stats) ? payload.player_stats : []
            };
        }

        async getTeams(params = {}, options = {}) {
            const queryParams = {
                season: params.season ?? params.seasonId ?? params.season_id ?? null,
                division: params.division ?? params.divisionNum ?? params.division_num ?? null,
                limit: params.limit ?? null
            };
            const query = buildQueryString(queryParams);
            const routes = buildRouteCandidates('teamsList', query);
            const result = await fetchWithFallback(routes, options);
            const payload = result?.data ?? result ?? [];
            const normalized = ensureSnakeCaseDeep(payload);
            return Array.isArray(normalized) ? normalized : [];
        }

        async getPlayers(params = {}, options = {}) {
            const queryParams = {
                season: params.season ?? params.seasonId ?? params.season_id ?? null,
                division: params.division ?? params.divisionNum ?? params.division_num ?? null,
                limit: params.limit ?? null
            };
            const query = buildQueryString(queryParams);
            const routes = buildRouteCandidates('playersList', query);
            const result = await fetchWithFallback(routes, options);
            const payload = result?.data ?? result ?? [];
            const normalized = ensureSnakeCaseDeep(payload);
            return Array.isArray(normalized) ? normalized : [];
        }

        async getTeamPage(teamId, seasonId, options = {}) {
            if (!teamId) {
                throw new Error('teamId is required');
            }
            const encTeamId = encodeURIComponent(teamId);
            const encSeasonId = seasonId ? encodeURIComponent(seasonId) : null;
            const routes = buildRouteCandidates('teamPage', encTeamId, encSeasonId);
            try {
                const result = await fetchWithFallback(routes, options);
                return result?.data ?? result ?? {};
            } catch (error) {
                if (isDev) {
                    console.warn('[apiClient] teamPage endpoint failed', error);
                }
                throw error;
            }
        }

        async getTeamMatchPlayerStats(teamId, championshipId, options = {}) {
            if (!teamId || !championshipId) {
                throw new Error('teamId and championshipId are required');
            }
            const encTeamId = encodeURIComponent(teamId);
            const encChampId = encodeURIComponent(championshipId);
            const routes = buildRouteCandidates('teamMatchPlayerStats', encTeamId, encChampId);
            try {
                const result = await fetchWithFallback(routes, options);
                const payload = result?.data ?? result ?? [];
                const normalized = ensureSnakeCaseDeep(payload);
                return Array.isArray(normalized) ? normalized : [];
            } catch (error) {
                if (isDev) {
                    console.warn('[apiClient] teamMatchPlayerStats endpoint failed', error);
                }
                return [];
            }
        }

        async getPlayerBundle(playerId, championshipId = null, options = {}) {
            if (!playerId) {
                throw new Error('playerId is required');
            }
            const encPlayerId = encodeURIComponent(playerId);
            const encChampionshipId = championshipId ? encodeURIComponent(championshipId) : null;
            const routes = buildRouteCandidates('playerBundle', encPlayerId, encChampionshipId);
            try {
                const result = await fetchWithFallback(routes, options);
                return result?.data ?? result ?? {};
            } catch (error) {
                if (isDev) {
                    console.warn('[apiClient] playerBundle endpoint failed', error);
                }
                throw error;
            }
        }
    }

    window.apiClient = new ApiClient();
})();
