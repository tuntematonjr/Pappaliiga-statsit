window.MatchExpandedDetails = {
    name: 'MatchExpandedDetails',
    components: {
        get PickBanFlow() { return window.PickBanFlow; }
    },
    props: {
        summary: {
            type: Object,
            required: true
        },
        details: {
            type: Object,
            default: null
        },
        vetoEntry: {
            type: Object,
            default: null
        },
        playerStats: {
            type: Array,
            default: () => []
        },
        mapCatalog: {
            type: Array,
            default: () => []
        },
        loading: {
            type: Boolean,
            default: false
        }
    },
    data() {
        return {
            imageErrors: {}
        };
    },
    computed: {
        matchDetails() {
            return this.details || this.summary || {};
        },
        teamId() {
            return this.summary?.me?.team_id || this.matchDetails?.team1Id || this.matchDetails?.team1_id || null;
        },
        opponentId() {
            return this.summary?.opponent?.team_id || this.matchDetails?.team2Id || this.matchDetails?.team2_id || null;
        },
        teamName() {
            return this.summary?.me?.team_name || this.matchDetails?.team1Name || this.matchDetails?.team1_name || 'Oma joukkue';
        },
        opponentName() {
            return this.summary?.opponentName || this.summary?.team2Name || this.matchDetails?.team2Name || this.matchDetails?.team2_name || 'Vastustaja';
        },
        formatLabel() {
            if (this.vetoEntry?.format) {
                return String(this.vetoEntry.format).toUpperCase();
            }
            const bestOf = Number(this.summary?.bestOf || this.matchDetails?.bestOf || 0);
            return bestOf ? `BO${bestOf}` : '';
        },
        dateLabel() {
            const ts = this.summary?.ts || this.matchDetails?.ts || null;
            return ts ? this.formatDate(ts) : '';
        },
        scoreLabel() {
            const teamScore = this.summary?.teamScore;
            const oppScore = this.summary?.oppScore;
            if (teamScore == null || oppScore == null) return '';
            return `${teamScore}-${oppScore}`;
        },
        mapEntries() {
            const maps = Array.isArray(this.matchDetails?.maps) ? this.matchDetails.maps : [];
            return maps.filter(map => map && (map.map || map.map_name));
        },
        catalogLookup() {
            const lookup = {};
            this.mapCatalog.forEach(item => {
                const key = this.mapKey(item?.map_id || item?.pretty_name);
                if (!key) return;
                lookup[key] = item;
            });
            return lookup;
        }
    },
    methods: {
        mapKey(name) {
            if (!name) return null;
            return String(name).trim().toLowerCase();
        },
        formatDate(ts) {
            if (!ts) return '';
            const d = new Date(ts * 1000);
            return d.toLocaleDateString('fi-FI', { year: 'numeric', month: 'short', day: 'numeric' });
        },
        formatNumber(value, decimals = 0) {
            const num = Number(value);
            if (!Number.isFinite(num)) return '0';
            return num.toFixed(decimals);
        },
        formatSigned(value) {
            const num = Number(value) || 0;
            return `${num >= 0 ? '+' : ''}${Math.round(num)}`;
        },
        beautifyMapName(raw) {
            if (!raw) return 'Kartta';
            const value = String(raw).trim();
            const lower = value.toLowerCase();
            if (lower === 'forfeit') return 'Forfeit';
            const core = lower.startsWith('de_') ? lower.slice(3) : lower;
            const parts = core.split(/[_-]/).filter(Boolean);
            if (!parts.length) return value;
            return parts.map(part => part.charAt(0).toUpperCase() + part.slice(1)).join(' ');
        },
        resolveMapImage(map) {
            // First try direct image from map data (backend-provided), prefer large variant
            const direct = map?.image_lg || map?.image_sm || map?.imageLg || map?.imageSm;
            if (direct) {
                try {
                    return window.apiClient && typeof window.apiClient.proxyAvatar === 'function'
                        ? window.apiClient.proxyAvatar(direct)
                        : direct;
                } catch (error) {
                    return direct;
                }
            }
            
            // Fallback to catalog lookup, prefer large variant
            const mapName = this.mapName(map);
            const key = this.mapKey(mapName);
            if (!key) return null;
            if (this.imageErrors[key]) return null;
            const entry = this.catalogLookup[key];
            const url = entry?.image_lg || entry?.image_sm || null;
            if (!url) return null;
            try {
                return window.apiClient && typeof window.apiClient.proxyAvatar === 'function'
                    ? window.apiClient.proxyAvatar(url)
                    : url;
            } catch (error) {
                return url;
            }
        },
        mapRoundIndex(map) {
            return map?.round_index ?? map?.roundIndex ?? 0;
        },
        mapScoreFor(map) {
            return map?.rf ?? map?.score_team1 ?? map?.scoreFor ?? 0;
        },
        mapScoreAgainst(map) {
            return map?.ra ?? map?.score_team2 ?? map?.scoreAgainst ?? 0;
        },
        mapName(map) {
            return map?.map || map?.map_name || map?.mapName || '';
        },
        onImageError(mapName) {
            const key = this.mapKey(mapName);
            if (!key) return;
            this.imageErrors = { ...this.imageErrors, [key]: true };
        },
        playersForTeam(roundIndex, teamId) {
            if (!teamId) return [];
            return this.playerStats.filter(entry =>
                String(entry?.teamId || entry?.team_id || '') === String(teamId)
                && Number(entry?.roundIndex ?? entry?.round_index ?? -1) === Number(roundIndex ?? -1)
            );
        },
        playersAvailable(roundIndex) {
            return this.playerStats.some(entry => Number(entry?.roundIndex ?? entry?.round_index ?? -1) === Number(roundIndex ?? -1));
        }
    },
    template: `
        <div class="match-details-panel">
            <div class="match-details-header">
                <div class="match-details-pills">
                    <span v-if="formatLabel" class="pill">{{ formatLabel }}</span>
                    <span class="pill" v-if="opponentName">vs {{ opponentName }}</span>
                    <span class="pill" v-if="dateLabel">{{ dateLabel }}</span>
                    <span class="pill" v-if="scoreLabel">Score {{ scoreLabel }}</span>
                </div>
            </div>

            <pick-ban-flow :entry="vetoEntry" :map-catalog="mapCatalog"></pick-ban-flow>

            <div class="match-maps">
                <div class="section-heading">
                    <div>
                        <h4 class="section-title">Kartat</h4>
                        <span class="section-sub">Ottelukarttakohtaiset tilastot ja pelaajat</span>
                    </div>
                </div>

                <div v-if="loading" class="match-details-skeleton">
                    <div class="skeleton-line"></div>
                    <div class="skeleton-line"></div>
                </div>

                <div v-if="mapEntries.length" class="match-maps-grid">
                    <div v-for="map in mapEntries" :key="mapRoundIndex(map) || mapName(map)" class="match-map-card">
                        <div class="match-map-header">
                            <div class="match-map-thumb">
                                <img
                                    v-if="resolveMapImage(map)"
                                    :src="resolveMapImage(map)"
                                    @error="onImageError(mapName(map))"
                                    alt=""
                                />
                                <div v-else class="map-thumb map-thumb--placeholder">No image</div>
                            </div>
                            <div class="match-map-meta">
                                <div class="match-map-name">{{ beautifyMapName(mapName(map)) }}</div>
                                <div class="match-map-score">{{ mapScoreFor(map) }} - {{ mapScoreAgainst(map) }}</div>
                            </div>
                        </div>
                        <div class="match-map-metrics">
                            <div class="metric-row">
                                <span class="metric-label">RD +/-</span>
                                <span class="metric-value">{{ formatSigned(mapScoreFor(map) - mapScoreAgainst(map)) }}</span>
                            </div>
                            <div class="metric-grid">
                                <div class="metric-col">
                                    <div class="metric-col__title">{{ teamName }}</div>
                                    <div class="metric-row">
                                        <span class="metric-label">ADR</span>
                                        <span class="metric-value">{{ formatNumber(map.left?.adr, 1) }}</span>
                                    </div>
                                    <div class="metric-row">
                                        <span class="metric-label">avgKD</span>
                                        <span class="metric-value">{{ formatNumber(map.left?.kd, 2) }}</span>
                                    </div>
                                    <div class="metric-row">
                                        <span class="metric-label">Damage</span>
                                        <span class="metric-value">{{ formatNumber(map.left?.dmg, 0) }}</span>
                                    </div>
                                </div>
                                <div class="metric-col">
                                    <div class="metric-col__title">{{ opponentName }}</div>
                                    <div class="metric-row">
                                        <span class="metric-label">ADR</span>
                                        <span class="metric-value">{{ formatNumber(map.right?.adr, 1) }}</span>
                                    </div>
                                    <div class="metric-row">
                                        <span class="metric-label">avgKD</span>
                                        <span class="metric-value">{{ formatNumber(map.right?.kd, 2) }}</span>
                                    </div>
                                    <div class="metric-row">
                                        <span class="metric-label">Damage</span>
                                        <span class="metric-value">{{ formatNumber(map.right?.dmg, 0) }}</span>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div class="match-map-players">
                            <div class="map-players" v-if="playersAvailable(mapRoundIndex(map))">
                                <div class="map-players__team">
                                    <div class="map-players__label">{{ teamName }}</div>
                                    <div class="map-players__list">
                                        <span v-for="player in playersForTeam(mapRoundIndex(map), teamId)" :key="player.playerId || player.player_id" class="map-player-pill">{{ player.nickname || player.playerId || player.player_id }}</span>
                                    </div>
                                </div>
                                <div class="map-players__team">
                                    <div class="map-players__label">{{ opponentName }}</div>
                                    <div class="map-players__list">
                                        <span v-for="player in playersForTeam(mapRoundIndex(map), opponentId)" :key="player.playerId || player.player_id" class="map-player-pill">{{ player.nickname || player.playerId || player.player_id }}</span>
                                    </div>
                                </div>
                            </div>
                            <div v-else-if="!loading" class="match-map-empty">No player stats available</div>
                        </div>
                    </div>
                </div>
                <div v-else class="match-map-empty">No map stats available</div>
            </div>
        </div>
    `
};
