// StandingsTable - wrapper around SortableTable to render team standings
window.StandingsTable = {
    name: 'StandingsTable',
    props: {
        standings: { type: Array, required: true },
        championshipId: { type: String, required: false }
    },
    components: {
        get SortableTable() { return window.SortableTable; }
    },
    data() {
        return {
            columns: [
                { key: 'team_name', label: 'Team', sortable: true },
                { key: 'record', label: 'Record', sortable: false },
                { key: 'wins', label: 'Wins', sortable: true, numeric: true },
                { key: 'losses', label: 'Losses', sortable: true, numeric: true },
                { key: 'maps_played', label: 'Maps', sortable: true, numeric: true },
                { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2 },
                { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1 }
            ]
        };
    },
    computed: {
        tableData() {
            return this.standings.map(s => ({
                team_id: s.team_id,
                team_name: s.team_name,
                record: `${s.wins || 0}-${s.losses || 0}`,
                wins: s.wins || 0,
                losses: s.losses || 0,
                maps_played: s.maps_played || 0,
                kd: s.kd || 0.0,
                adr: s.adr || 0.0
            }));
        }
    },
    template: `
        <div class="standings-table">
            <sortable-table :columns="columns" :data="tableData" :defaultSort="{ column: 'wins', order: 'desc' }" :colorizeColumns="['kd','adr']"></sortable-table>
        </div>
    `
};
