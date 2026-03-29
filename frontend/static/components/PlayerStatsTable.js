window.PlayerStatsTable = {
    name: 'PlayerStatsTable',
    props: {
        teamPlayers: {
            type: Array,
            default: () => []
        },
        teamId: {
            type: [String, Number],
            required: true
        },
        playerStats: {
            type: Array,
            default: () => []
        },
        mapIndex: {
            type: Number,
            default: 0
        },
        teamName: {
            type: String,
            default: 'Team'
        }
    },
    computed: {
        statsForTeam() {
            return this.playerStats.filter(entry =>
                String(entry?.teamId || entry?.team_id || '') === String(this.teamId)
                && Number(entry?.roundIndex ?? entry?.round_index ?? -1) === Number(this.mapIndex ?? -1)
            );
        },
        playersWithStats() {
            return this.teamPlayers.map(player => {
                const playerEntry = this.statsForTeam.find(s =>
                    String(s?.playerId || s?.player_id || '') === String(player?.playerId || player?.player_id || '')
                );
                return {
                    ...player,
                    stats: playerEntry?.stats || player?.stats || {}
                };
            });
        },
        mapAllPlayers() {
            return this.playerStats.filter(entry =>
                Number(entry?.roundIndex ?? entry?.round_index ?? -1) === Number(this.mapIndex ?? -1)
                && !entry?.is_forfeit_map
            );
        },
        mapAverages() {
            const players = this.mapAllPlayers;
            if (players.length < 2) return {};
            const compute = (values) => {
                const nums = values.filter(v => Number.isFinite(v) && v >= 0);
                if (nums.length < 2) return null;
                const mean = nums.reduce((a, b) => a + b, 0) / nums.length;
                const stddev = Math.sqrt(nums.reduce((a, b) => a + (b - mean) ** 2, 0) / nums.length);
                return { mean, stddev };
            };
            const result = {};
            for (const key of ['K/D Ratio', 'ADR', 'Utility Damage', 'Headshots %']) {
                result[key] = compute(players.map(p => Number(p?.stats?.[key] ?? 0)));
            }
            result['__entry_pct'] = compute(
                players
                    .map(p => { const c = Number(p?.stats?.['Entry Count'] ?? 0); return c > 0 ? Number(p?.stats?.['Entry Wins'] ?? 0) / c : null; })
                    .filter(v => v !== null)
            );
            result['__flash_pct'] = compute(
                players
                    .map(p => { const c = Number(p?.stats?.['Flash Count'] ?? 0); return c > 0 ? Number(p?.stats?.['Flash Successes'] ?? 0) / c : null; })
                    .filter(v => v !== null)
            );
            return result;
        },
        hasPlayers() {
            return this.playersWithStats.length > 0;
        }
    },
    methods: {
        getStatValue(stat, key, defaultVal = 0) {
            const source = stat?.stats && typeof stat.stats === 'object' ? stat.stats : stat;
            return source?.[key] ?? defaultVal;
        },
        calculatePercentage(numerator, denominator) {
            if (!denominator || denominator === 0) return 0;
            return ((numerator / denominator) * 100).toFixed(0);
        },
        formatValue(value, decimals = 0) {
            const num = Number(value);
            if (!Number.isFinite(num)) return '-';
            return decimals > 0 ? num.toFixed(decimals) : Math.round(num);
        },
        rawPct(wins, total) {
            const t = Number(total);
            return t > 0 ? Number(wins) / t : -1;
        },
        getStatColorClass(value, statKey) {
            const avg = this.mapAverages[statKey];
            if (!avg) return '';
            const num = Number(value);
            if (!Number.isFinite(num) || num < 0) return '';
            const threshold = avg.stddev * 0.75;
            if (num >= avg.mean + threshold) return 'stat-positive';
            if (num <= avg.mean - threshold) return 'stat-negative';
            return '';
        },
        playerRoute(player) {
            const playerId = player?.playerId || player?.player_id || null;
            if (!playerId) return null;
            const championshipId = this.$root?.$route?.params?.championshipId || this.$root?.$route?.query?.championship || null;
            return {
                name: 'player',
                params: { playerId: String(playerId) },
                query: championshipId ? { championship: String(championshipId) } : {}
            };
        }
    },
    template: `
        <div class="player-stats-table-wrapper">
            <div v-if="hasPlayers" class="player-stats-table-container">
                <table class="player-stats-table">
                    <thead>
                        <tr class="player-stats-table__header">
                            <th class="player-stats-table__th player-stats-table__th--nickname">Pelaaja</th>
                            <th class="player-stats-table__th player-stats-table__th--kda">K-D-A (K/D)</th>
                            <th class="player-stats-table__th player-stats-table__th--adr">ADR</th>
                            <th class="player-stats-table__th player-stats-table__th--utility-damage player-stats-table__th--desktop">UtilDmg</th>
                            <th class="player-stats-table__th player-stats-table__th--entries player-stats-table__th--desktop">Entries</th>
                            <th class="player-stats-table__th player-stats-table__th--flashes player-stats-table__th--desktop">Flashes</th>
                            <th class="player-stats-table__th player-stats-table__th--hs-pct player-stats-table__th--desktop">HS%</th>
                            <th class="player-stats-table__th player-stats-table__th--first-kills player-stats-table__th--hidden-mobile player-stats-table__th--hidden-tablet">FK</th>
                            <th class="player-stats-table__th player-stats-table__th--multikill player-stats-table__th--hidden-mobile player-stats-table__th--hidden-tablet">2K</th>
                            <th class="player-stats-table__th player-stats-table__th--multikill player-stats-table__th--hidden-mobile player-stats-table__th--hidden-tablet">3K</th>
                            <th class="player-stats-table__th player-stats-table__th--multikill player-stats-table__th--hidden-mobile player-stats-table__th--hidden-tablet">4K</th>
                            <th class="player-stats-table__th player-stats-table__th--multikill player-stats-table__th--hidden-mobile player-stats-table__th--hidden-tablet">5K</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr v-for="player in playersWithStats" :key="player.playerId || player.player_id" class="player-stats-table__row">
                            <td class="player-stats-table__td player-stats-table__td--nickname">
                                <router-link v-if="playerRoute(player)" :to="playerRoute(player)" class="player-link player-stats-table__name-content">
                                    {{ player.nickname || player.playerId || player.player_id }}
                                </router-link>
                                <span v-else class="player-stats-table__name-content">{{ player.nickname || player.playerId || player.player_id }}</span>
                            </td>
                            <td class="player-stats-table__td player-stats-table__td--kda">
                                <span class="stat-fraction">
                                    {{ formatValue(getStatValue(player.stats, 'Kills')) }}-{{ formatValue(getStatValue(player.stats, 'Deaths')) }}-{{ formatValue(getStatValue(player.stats, 'Assists')) }}
                                </span>
                                <span class="stat-percentage" :class="getStatColorClass(getStatValue(player.stats, 'K/D Ratio'), 'K/D Ratio')">({{ formatValue(getStatValue(player.stats, 'K/D Ratio'), 2) }})</span>
                            </td>
                            <td class="player-stats-table__td player-stats-table__td--adr" :class="getStatColorClass(getStatValue(player.stats, 'ADR'), 'ADR')">{{ formatValue(getStatValue(player.stats, 'ADR'), 1) }}</td>
                            <td class="player-stats-table__td player-stats-table__td--utility-damage player-stats-table__td--desktop" :class="getStatColorClass(getStatValue(player.stats, 'Utility Damage'), 'Utility Damage')">{{ formatValue(getStatValue(player.stats, 'Utility Damage')) }}</td>
                            <td class="player-stats-table__td player-stats-table__td--entries player-stats-table__td--desktop">
                                <span class="stat-fraction">
                                    {{ formatValue(getStatValue(player.stats, 'Entry Wins')) }}/{{ formatValue(getStatValue(player.stats, 'Entry Count')) }}
                                </span>
                                <span class="stat-percentage" :class="getStatColorClass(rawPct(getStatValue(player.stats, 'Entry Wins'), getStatValue(player.stats, 'Entry Count')), '__entry_pct')">({{ calculatePercentage(getStatValue(player.stats, 'Entry Wins'), getStatValue(player.stats, 'Entry Count')) }}%)</span>
                            </td>
                            <td class="player-stats-table__td player-stats-table__td--flashes player-stats-table__td--desktop">
                                <span class="stat-fraction">
                                    {{ formatValue(getStatValue(player.stats, 'Flash Successes')) }}/{{ formatValue(getStatValue(player.stats, 'Flash Count')) }}
                                </span>
                                <span class="stat-percentage" :class="getStatColorClass(rawPct(getStatValue(player.stats, 'Flash Successes'), getStatValue(player.stats, 'Flash Count')), '__flash_pct')">({{ calculatePercentage(getStatValue(player.stats, 'Flash Successes'), getStatValue(player.stats, 'Flash Count')) }}%)</span>
                            </td>
                            <td class="player-stats-table__td player-stats-table__td--hs-pct player-stats-table__td--desktop" :class="getStatColorClass(getStatValue(player.stats, 'Headshots %'), 'Headshots %')">{{ formatValue(getStatValue(player.stats, 'Headshots %')) }}%</td>
                            <td class="player-stats-table__td player-stats-table__td--first-kills player-stats-table__td--hidden-mobile player-stats-table__td--hidden-tablet">{{ formatValue(getStatValue(player.stats, 'First Kills')) }}</td>
                            <td class="player-stats-table__td player-stats-table__td--multikill player-stats-table__td--hidden-mobile player-stats-table__td--hidden-tablet">{{ formatValue(getStatValue(player.stats, 'Double Kills')) }}</td>
                            <td class="player-stats-table__td player-stats-table__td--multikill player-stats-table__td--hidden-mobile player-stats-table__td--hidden-tablet">{{ formatValue(getStatValue(player.stats, 'Triple Kills')) }}</td>
                            <td class="player-stats-table__td player-stats-table__td--multikill player-stats-table__td--hidden-mobile player-stats-table__td--hidden-tablet">{{ formatValue(getStatValue(player.stats, 'Quadro Kills')) }}</td>
                            <td class="player-stats-table__td player-stats-table__td--multikill player-stats-table__td--hidden-mobile player-stats-table__td--hidden-tablet">{{ formatValue(getStatValue(player.stats, 'Penta Kills')) }}</td>
                        </tr>
                    </tbody>
                </table>
            </div>
            <div v-else class="player-stats-table__empty">Ei pelaajatilastoja</div>
        </div>
    `
};
