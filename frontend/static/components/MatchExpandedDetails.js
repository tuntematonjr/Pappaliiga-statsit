window.MatchExpandedDetails = {
    name: 'MatchExpandedDetails',
    components: {
        get PickBanFlow() { return window.PickBanFlow; },
        get PlayerStatsTable() { return window.PlayerStatsTable; }
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
        },
        vetoLookup() {
            const lookup = {};
            const steps = Array.isArray(this.vetoEntry?.steps) ? this.vetoEntry.steps : [];
            steps.forEach(step => {
                const key = this.mapKey(step?.mapName || step?.map_name || step?.map);
                if (!key || lookup[key]) return;
                lookup[key] = step;
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
        teamRoute(teamId) {
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
        mapsWon() {
            return this.mapEntries.filter(map => Number(this.mapScoreFor(map) || 0) > Number(this.mapScoreAgainst(map) || 0)).length;
        },
        mapsLost() {
            return this.mapEntries.filter(map => Number(this.mapScoreFor(map) || 0) < Number(this.mapScoreAgainst(map) || 0)).length;
        },
        matchOutcomeClass() {
            const teamScore = Number(this.summary?.teamScore ?? this.mapsWon());
            const oppScore = Number(this.summary?.oppScore ?? this.mapsLost());
            if (!Number.isFinite(teamScore) || !Number.isFinite(oppScore) || teamScore === oppScore) return 'match-overview__result--neutral';
            return teamScore > oppScore ? 'match-overview__result--win' : 'match-overview__result--loss';
        },
        matchOutcomeLabel() {
            const teamScore = Number(this.summary?.teamScore ?? this.mapsWon());
            const oppScore = Number(this.summary?.oppScore ?? this.mapsLost());
            if (!Number.isFinite(teamScore) || !Number.isFinite(oppScore) || teamScore === oppScore) return 'Tasapeli';
            return teamScore > oppScore ? 'Voitto' : 'Tappio';
        },
        teamAvatar(side = 'team1') {
            const suffix = side === 'team2' ? '2' : '1';
            return this.summary?.[`team${suffix}_avatar`]
                || this.summary?.[`team${suffix}Avatar`]
                || this.summary?.[`t${suffix}_avatar`]
                || this.summary?.[`t${suffix}Avatar`]
                || this.matchDetails?.[`team${suffix}_avatar`]
                || this.matchDetails?.[`team${suffix}Avatar`]
                || this.matchDetails?.[`t${suffix}_avatar`]
                || this.matchDetails?.[`t${suffix}Avatar`]
                || this.summary?.[side]?.avatar
                || this.summary?.[side]?.logo
                || this.matchDetails?.[side]?.avatar
                || this.matchDetails?.[side]?.logo
                || null;
        },
        avatarUrl(src) {
            const fallback = window.PAPPALIIGA_DEFAULT_LOGO;
            if (!src) return fallback;
            try {
                const proxied = window.apiClient && typeof window.apiClient.proxyAvatar === 'function'
                    ? window.apiClient.proxyAvatar(src)
                    : src;
                return proxied || fallback;
            } catch (error) {
                return fallback;
            }
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
        mapRoundDiffAbs(map) {
            const left = Number(this.mapScoreFor(map) ?? 0);
            const right = Number(this.mapScoreAgainst(map) ?? 0);
            if (!Number.isFinite(left) || !Number.isFinite(right)) return 0;
            return Math.abs(Math.round(left - right));
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
        vetoStepForMap(map) {
            return this.vetoLookup[this.mapKey(this.mapName(map))] || null;
        },
        vetoChipLabel(map) {
            const step = this.vetoStepForMap(map);
            if (!step) return 'Played';
            const action = String(step.action || '').trim();
            return action ? action.toUpperCase() : 'Played';
        },
        vetoChipSubline(map) {
            const step = this.vetoStepForMap(map);
            if (!step?.teamName) return 'Ottelukartta';
            return step.teamName;
        },
        mapOutcomeLabel(map) {
            const left = Number(this.mapScoreFor(map) || 0);
            const right = Number(this.mapScoreAgainst(map) || 0);
            if (!Number.isFinite(left) || !Number.isFinite(right) || left === right) return 'Tasainen';
            return left > right ? 'Voitto' : 'Tappio';
        },
        mapOutcomeClass(map) {
            const left = Number(this.mapScoreFor(map) || 0);
            const right = Number(this.mapScoreAgainst(map) || 0);
            if (!Number.isFinite(left) || !Number.isFinite(right) || left === right) return 'match-map-card--neutral';
            return left > right ? 'match-map-card--win' : 'match-map-card--loss';
        },
        mapSequenceLabel(map, index) {
            const roundIndex = Number(this.mapRoundIndex(map));
            if (Number.isFinite(roundIndex) && roundIndex > 0) {
                return `Kartta ${roundIndex}`;
            }
            return `Kartta ${index + 1}`;
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
            <section class="match-overview glass-card">
                <div class="match-overview__header">
                    <div class="match-overview__copy">
                        <h4 class="match-overview__title titleUnderlineSection title-duration-fast">Ottelun kulku</h4>
                    </div>
                </div>

                <div class="match-overview__scoreboard">
                    <div class="match-overview__team match-overview__team--left">
                        <img class="match-overview__team-logo" :src="avatarUrl(teamAvatar('team1'))" :alt="teamName + ' logo'" loading="lazy" />
                        <router-link v-if="teamRoute(teamId)" :to="teamRoute(teamId)" class="match-overview__team-name team-link">{{ teamName }}</router-link>
                        <span v-else class="match-overview__team-name">{{ teamName }}</span>
                    </div>

                    <div class="match-overview__score-shell">
                        <div class="match-overview__score mono-num">{{ scoreLabel || (mapsWon() + '-' + mapsLost()) }}</div>
                        <div v-if="dateLabel" class="match-overview__date">{{ dateLabel }}</div>
                    </div>

                    <div class="match-overview__team match-overview__team--right">
                        <router-link v-if="teamRoute(opponentId)" :to="teamRoute(opponentId)" class="match-overview__team-name team-link">{{ opponentName }}</router-link>
                        <span v-else class="match-overview__team-name">{{ opponentName }}</span>
                        <img class="match-overview__team-logo" :src="avatarUrl(teamAvatar('team2'))" :alt="opponentName + ' logo'" loading="lazy" />
                    </div>
                </div>
            </section>

            <pick-ban-flow :entry="vetoEntry" :map-catalog="mapCatalog" :match-maps="mapEntries"></pick-ban-flow>

            <div class="match-maps">
                <div class="section-heading section-heading--split match-maps__heading">
                    <div class="section-heading__main">
                        <h4 class="titleUnderlineCard">Kartat</h4>
                        <span class="section-sub section-subtext">Ottelukarttakohtaiset tilastot ja pelaajat</span>
                    </div>
                </div>

                <div v-if="loading" class="match-details-skeleton">
                    <div class="skeleton-line"></div>
                    <div class="skeleton-line"></div>
                </div>

                <div v-if="mapEntries.length" class="match-maps-grid">
                    <article
                        v-for="(map, index) in mapEntries"
                        :key="mapRoundIndex(map) || mapName(map)"
                        class="match-map-card glass-card"
                        :class="mapOutcomeClass(map)"
                    >
                        <div class="match-map-card__hero">
                            <div class="match-map-card__topline">
                                <span class="match-map-card__index">{{ mapSequenceLabel(map, index) }}</span>
                                <div class="match-map-card__badges">
                                    <span
                                        v-if="vetoStepForMap(map)?.action && !['played', 'pick'].includes(String(vetoStepForMap(map).action).toLowerCase())"
                                        class="match-map-card__badge"
                                        :class="'match-map-card__badge--' + (vetoStepForMap(map)?.action || 'muted')"
                                    >{{ vetoChipLabel(map) }}</span>
                                </div>
                            </div>

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
                                        <span class="match-map-score__team-wrap">
                                            <img class="match-map-score__logo" :src="avatarUrl(teamAvatar('team1'))" :alt="teamName + ' logo'" loading="lazy" />
                                            <router-link v-if="teamRoute(teamId)" :to="teamRoute(teamId)" class="match-map-score__team team-link">{{ teamName }}</router-link>
                                            <span v-else class="match-map-score__team">{{ teamName }}</span>
                                        </span>
                                        <span class="match-map-score__rounds mono-num">
                                            <span class="match-map-score__line">
                                                <span :class="['match-map-score__value', mapScoreClass(map, 'left')]">{{ mapScoreFor(map) }}</span>
                                                <span class="match-map-score__sep"> - </span>
                                                <span :class="['match-map-score__value', mapScoreClass(map, 'right')]">{{ mapScoreAgainst(map) }}</span>
                                            </span>
                                            <span class="match-map-score__rd">RD {{ mapRoundDiffAbs(map) }}</span>
                                        </span>
                                        <span class="match-map-score__team-wrap">
                                            <router-link v-if="teamRoute(opponentId)" :to="teamRoute(opponentId)" class="match-map-score__team team-link">{{ opponentName }}</router-link>
                                            <span v-else class="match-map-score__team">{{ opponentName }}</span>
                                            <img class="match-map-score__logo" :src="avatarUrl(teamAvatar('team2'))" :alt="opponentName + ' logo'" loading="lazy" />
                                        </span>
                                    </div>

                                    <div class="match-map-metrics match-map-metrics--inline">
                                        <div class="metric-grid metric-grid--compare">
                                            <span class="metric-value metric-value--left mono-num">{{ formatNumber(map.left?.adr, 1) }}</span>
                                            <span class="metric-label metric-label--center">ADR</span>
                                            <span class="metric-value metric-value--right mono-num">{{ formatNumber(map.right?.adr, 1) }}</span>

                                            <span class="metric-value metric-value--left mono-num">{{ formatNumber(map.left?.kd, 2) }}</span>
                                            <span class="metric-label metric-label--center">avgKD</span>
                                            <span class="metric-value metric-value--right mono-num">{{ formatNumber(map.right?.kd, 2) }}</span>

                                            <span class="metric-value metric-value--left mono-num">{{ formatNumber(map.left?.dmg, 0) }}</span>
                                            <span class="metric-label metric-label--center">Damage</span>
                                            <span class="metric-value metric-value--right mono-num">{{ formatNumber(map.right?.dmg, 0) }}</span>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>

                        <div class="match-map-players">
                            <div v-if="playersAvailable(mapRoundIndex(map))" class="map-players-stats">
                                <div class="match-map-players__heading">Pelaajatilastot</div>
                                <section class="map-players-stats__team match-team-panel">
                                    <player-stats-table
                                        :team-players="playersForTeam(mapRoundIndex(map), teamId)"
                                        :team-id="teamId"
                                        :player-stats="playerStats"
                                        :map-index="mapRoundIndex(map)"
                                        :team-name="teamName"
                                    ></player-stats-table>
                                </section>

                                <section class="map-players-stats__team match-team-panel">
                                    <player-stats-table
                                        :team-players="playersForTeam(mapRoundIndex(map), opponentId)"
                                        :team-id="opponentId"
                                        :player-stats="playerStats"
                                        :map-index="mapRoundIndex(map)"
                                        :team-name="opponentName"
                                    ></player-stats-table>
                                </section>
                            </div>
                            <div v-else-if="!loading" class="match-map-empty">No player stats available</div>
                        </div>
                    </article>
                </div>
                <div v-else class="match-map-empty">No map stats available</div>
            </div>
        </div>
    `
};
