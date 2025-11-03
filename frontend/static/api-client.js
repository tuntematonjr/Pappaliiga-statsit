// API Client for Pappaliiga Stats
// Allow an explicit runtime override. Backend or embedding code may write
// `window.__API_BASE__ = 'https://example.com/api'` before loading the client.
// Fallback to hostname heuristic for local development, then to origin + '/api'.
const API_BASE_URL = (() => {
    if (typeof window !== 'undefined' && window.__API_BASE__) {
        try { return window.__API_BASE__; } catch (e) {}
    }
    // In production the API is served from the same origin under /api.
    // For local development the frontend may be served from a different port (e.g. 8080).
    // When running on localhost, prefer the backend at port 8000 so requests go to uvicorn.
    try {
        const host = window.location.hostname;
        const port = window.location.port;
        // If we're on localhost/127.0.0.1 and not already on backend port, route to backend
        if ((host === 'localhost' || host === '127.0.0.1') && port && port !== '8000') {
            return `http://${host}:8000/api`;
        }
    } catch (e) {
        // Fallback
    }
    return window.location.origin + '/api';
})();

class ApiClient {
    // Return a proxied avatar URL for whitelisted hosts to avoid client-side opaque responses
    proxyAvatar(url) {
        const DEFAULT_AVATAR = new URL('/static/pappaliiga-logo-white-bg.png', window.location.origin).href;
        if (!url) {
            return DEFAULT_AVATAR;
        }
        try {
            const parsed = new URL(url, window.location.origin);
            const host = parsed.hostname.toLowerCase();
            if (host.endsWith('faceit-cdn.net')) {
                const params = new URLSearchParams({
                    url,
                    fallback: DEFAULT_AVATAR
                });
                const base = API_BASE_URL.endsWith('/') ? API_BASE_URL.slice(0, -1) : API_BASE_URL;
                return `${base}/proxy-image?${params.toString()}`;
            }
        } catch (e) {
            // Fall through to return the original URL when parsing fails
        }
        return url;
    }

    async request(endpoint, options = {}) {
        const url = `${API_BASE_URL}${endpoint}`;
        try {
            const response = await fetch(url, {
                headers: {
                    'Content-Type': 'application/json',
                    ...options.headers
                },
                ...options
            });

            if (!response.ok) {
                // Try to parse a JSON error body, but fall back to text if not JSON
                const errorBody = await response.json().catch(async () => {
                    const text = await response.text().catch(() => '');
                    return { _raw: text };
                });
                const message = (errorBody && (errorBody.detail || errorBody.message)) || (errorBody && errorBody._raw) || `HTTP ${response.status}`;
                const e = new Error(message);
                e.status = response.status;
                e.body = errorBody;
                throw e;
            }

            // Try to parse JSON, but if the server returns non-JSON (HTML, plaintext), include raw text in the error
            const text = await response.text();
            try {
                return text ? JSON.parse(text) : null;
            } catch (parseErr) {
                const e = new Error('Invalid JSON response from API');
                e.status = response.status;
                e.body = text;
                console.error('API parse error:', parseErr, 'raw response:', text);
                throw e;
            }
        } catch (error) {
            console.error(`API Error (${endpoint}):`, error);
            throw error;
        }
    }

    // Home
    async getHome() {
        return this.request('/home');
    }

    // Seasons & Divisions
    async getSeasons() {
        return this.request('/divisions/seasons');
    }

    async getDivisions() {
        return this.request('/divisions');
    }

    async getDivisionsBySeason(season) {
        return this.request(`/divisions/season/${season}`);
    }

    async getDivisionBySlug(slug) {
        return this.request(`/divisions/by-slug/${slug}`);
    }

    async getDivisionById(championshipId) {
        return this.request(`/divisions/${championshipId}`);
    }

    async getSeasonStats(season) {
        return this.request(`/seasons/${season}/stats`);
    }

    async getDivisionMapStats(championshipId) {
        return this.request(`/divisions/${championshipId}/map-stats`);
    }

    async getDivisionStandings(championshipId) {
        return this.request(`/divisions/${championshipId}/standings`);
    }

    async getDivisionHighlights(championshipId) {
        return this.request(`/divisions/${championshipId}/highlights`);
    }

    // Teams
    async getTeamInfo(teamId) {
        return this.request(`/teams/${teamId}`);
    }

    async getTeamSeasons(teamId) {
        return this.request(`/teams/${teamId}/seasons`);
    }

    async getTeamSeasonStats(teamId) {
        return this.request(`/teams/${teamId}/seasons`);
    }

    async getTeamDetails(championshipId, teamId) {
        if (championshipId) {
            return this.request(`/championships/${championshipId}/teams/${teamId}`);
        }
        return this.getTeamInfo(teamId);
    }

    async getTeamMapStats(teamId, championshipId) {
        return this.request(`/teams/${teamId}/map-stats/${championshipId}`);
    }

    async getTeamMatches(championshipId, teamId) {
        if (championshipId) {
            return this.request(`/championships/${championshipId}/teams/${teamId}/matches`);
        }
        return this.request(`/teams/${teamId}/matches`);
    }

    async listTeams(params = {}) {
        const query = new URLSearchParams(params).toString();
        return this.request(`/teams/${query ? '?' + query : ''}`);
    }

    // Players
    async getPlayerInfo(playerId) {
        return this.request(`/players/${playerId}`);
    }

    async getPlayerSeasonStats(playerId) {
        return this.request(`/players/${playerId}/seasons`);
    }

    async getPlayerMapStats(playerId, championshipId) {
        return this.request(`/players/${playerId}/map-stats/${championshipId}`);
    }

    async listPlayers(params = {}) {
        const query = new URLSearchParams(params).toString();
        return this.request(`/players/${query ? '?' + query : ''}`);
    }

    // Matches
    async getDivisionMatches(championshipId) {
        return this.request(`/matches/division/${championshipId}`);
    }

    async getMatchDetails(matchId) {
        return this.request(`/matches/${matchId}`);
    }

    async getMatchPlayerStats(matchId) {
        return this.request(`/matches/${matchId}/player-stats`);
    }

    // Maps
    async getMaps() {
        return this.request('/maps');
    }

    async getMapInfo(mapId) {
        return this.request(`/maps/${mapId}`);
    }

    async getDivisionMapVotes(championshipId) {
        return this.request(`/divisions/${championshipId}/map-votes`);
    }

    // Stats
    async getStatsOverview() {
        return this.request('/stats/overview');
    }

    async getTopPlayers(stat, params = {}) {
        const query = new URLSearchParams(params).toString();
        return this.request(`/stats/top-players/${stat}${query ? '?' + query : ''}`);
    }

    async getDivisionAverages(championshipId) {
        return this.request(`/stats/division/${championshipId}/averages`);
    }
}

// Global API instance
window.apiClient = new ApiClient();
