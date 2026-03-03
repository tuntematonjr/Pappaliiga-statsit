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
        championshipId() {
            return this.$route?.params?.championshipId || this.$route?.query?.championship || null;
        },
        playerRoute(player) {
            const playerId = player?.playerId || player?.player_id || null;
            if (!playerId) return null;
            const championshipId = this.championshipId();
            return {
                name: 'player',
                params: { playerId: String(playerId) },
                query: championshipId ? { championship: String(championshipId) } : {}
            };
        },
        teamRoute(teamId, teamName = '') {
            const championshipId = this.championshipId();
            if (!championshipId || !teamId) return null;
            return {
                name: 'team-detail',
                params: { championshipId: String(championshipId), teamId: String(teamId) }
            };
        },
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
        mapScoreClass(map, side = 'left') {
            const left = Number(this.mapScoreFor(map) ?? 0);
            const right = Number(this.mapScoreAgainst(map) ?? 0);
            if (!Number.isFinite(left) || !Number.isFinite(right) || left === right) return '';
            const leftWins = left > right;
            if (side === 'left') {
                return leftWins ? 'match-map-score__value--win' : 'match-map-score__value--loss';
            }
            return leftWins ? 'match-map-score__value--loss' : 'match-map-score__value--win';
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
            <pick-ban-flow :entry="vetoEntry" :map-catalog="mapCatalog" :match-maps="mapEntries"></pick-ban-flow>

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
                                <div class="match-map-score">
                                    <router-link v-if="teamRoute(teamId, teamName)" :to="teamRoute(teamId, teamName)" class="match-map-score__team team-link">{{ teamName }}</router-link>
                                    <span v-else class="match-map-score__team">{{ teamName }}</span>
                                    <span class="match-map-score__rounds">
                                        <span :class="['match-map-score__value', mapScoreClass(map, 'left')]">{{ mapScoreFor(map) }}</span>
                                        <span class="match-map-score__sep"> - </span>
                                        <span :class="['match-map-score__value', mapScoreClass(map, 'right')]">{{ mapScoreAgainst(map) }}</span>
                                    </span>
                                    <router-link v-if="teamRoute(opponentId, opponentName)" :to="teamRoute(opponentId, opponentName)" class="match-map-score__team team-link">{{ opponentName }}</router-link>
                                    <span v-else class="match-map-score__team">{{ opponentName }}</span>
                                </div>
                            </div>
                        </div>
                        <div class="match-map-metrics">
                            <div class="metric-row metric-row--rd">
                                <span class="metric-label metric-label--rd">RD +/-</span>
                                <span class="metric-value">{{ formatSigned(mapScoreFor(map) - mapScoreAgainst(map)) }}</span>
                            </div>
                            <div class="metric-grid metric-grid--compare">
                                <div class="metric-col__title metric-col__title--left">
                                    <router-link v-if="teamRoute(teamId, teamName)" :to="teamRoute(teamId, teamName)" class="team-link">{{ teamName }}</router-link>
                                    <span v-else>{{ teamName }}</span>
                                </div>
                                <div class="metric-col__title metric-col__title--center">Stat</div>
                                <div class="metric-col__title metric-col__title--right">
                                    <router-link v-if="teamRoute(opponentId, opponentName)" :to="teamRoute(opponentId, opponentName)" class="team-link">{{ opponentName }}</router-link>
                                    <span v-else>{{ opponentName }}</span>
                                </div>

                                <span class="metric-value metric-value--left">{{ formatNumber(map.left?.adr, 1) }}</span>
                                <span class="metric-label metric-label--center">ADR</span>
                                <span class="metric-value metric-value--right">{{ formatNumber(map.right?.adr, 1) }}</span>

                                <span class="metric-value metric-value--left">{{ formatNumber(map.left?.kd, 2) }}</span>
                                <span class="metric-label metric-label--center">avgKD</span>
                                <span class="metric-value metric-value--right">{{ formatNumber(map.right?.kd, 2) }}</span>

                                <span class="metric-value metric-value--left">{{ formatNumber(map.left?.dmg, 0) }}</span>
                                <span class="metric-label metric-label--center">Damage</span>
                                <span class="metric-value metric-value--right">{{ formatNumber(map.right?.dmg, 0) }}</span>
                            </div>
                        </div>
                        <div class="match-map-players">
                            <div class="map-players" v-if="playersAvailable(mapRoundIndex(map))">
                                <div class="map-players__team">
                                    <div class="map-players__label">
                                        <router-link v-if="teamRoute(teamId, teamName)" :to="teamRoute(teamId, teamName)" class="team-link">{{ teamName }}</router-link>
                                        <span v-else>{{ teamName }}</span>
                                    </div>
                                    <div class="map-players__list">
                                        <template v-for="player in playersForTeam(mapRoundIndex(map), teamId)" :key="player.playerId || player.player_id || player.nickname">
                                            <router-link
                                                v-if="playerRoute(player)"
                                                :to="playerRoute(player)"
                                                class="map-player-pill player-link"
                                            >{{ player.nickname || player.playerId || player.player_id }}</router-link>
                                            <span v-else class="map-player-pill">{{ player.nickname || player.playerId || player.player_id }}</span>
                                        </template>
                                    </div>
                                </div>
                                <div class="map-players__team">
                                    <div class="map-players__label">
                                        <router-link v-if="teamRoute(opponentId, opponentName)" :to="teamRoute(opponentId, opponentName)" class="team-link">{{ opponentName }}</router-link>
                                        <span v-else>{{ opponentName }}</span>
                                    </div>
                                    <div class="map-players__list">
                                        <template v-for="player in playersForTeam(mapRoundIndex(map), opponentId)" :key="player.playerId || player.player_id || player.nickname">
                                            <router-link
                                                v-if="playerRoute(player)"
                                                :to="playerRoute(player)"
                                                class="map-player-pill player-link"
                                            >{{ player.nickname || player.playerId || player.player_id }}</router-link>
                                            <span v-else class="map-player-pill">{{ player.nickname || player.playerId || player.player_id }}</span>
                                        </template>
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
