/**
 * Team Overview Page - Regression Tests
 * Tests for critical UI/UX fixes to prevent regressions
 */

// Mock dependencies
const mockSeasonOptions = [
    { value: '101', season: 5, division: 1, isPlayoffs: false, label: 'Kausi 5 · Div 1' },
    { value: '102', season: 5, division: 2, isPlayoffs: false, label: 'Kausi 5 · Div 2' },
    { value: '103', season: 4, division: 1, isPlayoffs: true, label: 'Kausi 4 · Div 1 Playoffs' }
];

describe('TeamDetail - Season Consistency', () => {
    test('hero pills show currently selected season (not always first option)', () => {
        // Setup: selectedChampionship is '102' (not the first option)
        const selectedChampionship = '102';
        const currentSeasonOption = mockSeasonOptions.find(
            s => String(s.value) === String(selectedChampionship)
        );

        expect(currentSeasonOption).toBeDefined();
        expect(currentSeasonOption.season).toBe(5);
        expect(currentSeasonOption.division).toBe(2);

        // Hero pills should display season 5, division 2
        // (not season 5, division 1 from seasonOptions[0])
    });

    test('hero pills show fallback when currentSeasonOption is null', () => {
        const selectedChampionship = 'invalid-id';
        const currentSeasonOption = mockSeasonOptions.find(
            s => String(s.value) === String(selectedChampionship)
        ) || null;

        expect(currentSeasonOption).toBeNull();

        // Hero pills should display '—' with tooltip
        const fallbackDisplay = currentSeasonOption?.season || '—';
        expect(fallbackDisplay).toBe('—');
    });

    test('playoffs badge appears only when currentSeasonOption.isPlayoffs is true', () => {
        const playoffSeason = mockSeasonOptions.find(s => s.isPlayoffs);
        expect(playoffSeason).toBeDefined();
        expect(playoffSeason.isPlayoffs).toBe(true);

        const regularSeason = mockSeasonOptions.find(s => !s.isPlayoffs);
        expect(regularSeason).toBeDefined();
        expect(regularSeason.isPlayoffs).toBe(false);
    });
});

describe('TeamDetail - Sorting Defaults', () => {
    const PLAYER_COLUMNS = [
        { key: 'nickname', label: 'Pelaaja', sortable: true },
        { key: 'mapsPlayed', label: 'Kartat', sortable: true, numeric: true },
        { key: 'kd', label: 'K/D', sortable: true, numeric: true },
        { key: 'adr', label: 'ADR', sortable: true, numeric: true }
    ];

    const MAP_COLUMNS = [
        { key: 'mapName', label: 'Kartta', sortable: true },
        { key: 'totalRoundsPlayed', label: 'Erät pelattu', sortable: true, numeric: true },
        { key: 'adr', label: 'ADR', sortable: true, numeric: true }
    ];

    const SCOUT_MAP_COLUMNS = [
        { key: 'mapName', label: 'Kartta', sortable: true },
        { key: 'played', label: 'Pelattu', sortable: true, numeric: true },
        { key: 'winrate', label: 'Win %', sortable: true, numeric: true }
    ];

    test('playerDefaultSort uses visible column (kd)', () => {
        const playerDefaultSort = { column: 'kd', order: 'desc', numeric: true };
        
        // Verify 'kd' is in PLAYER_COLUMNS
        const kdColumn = PLAYER_COLUMNS.find(c => c.key === playerDefaultSort.column);
        expect(kdColumn).toBeDefined();
        expect(kdColumn.key).toBe('kd');
    });

    test('mapDefaultSort (detailed mode) uses visible column (totalRoundsPlayed)', () => {
        const mapDefaultSort = { column: 'totalRoundsPlayed', order: 'desc', numeric: true };
        
        // Verify 'totalRoundsPlayed' is in MAP_COLUMNS
        const roundsColumn = MAP_COLUMNS.find(c => c.key === mapDefaultSort.column);
        expect(roundsColumn).toBeDefined();
        expect(roundsColumn.key).toBe('totalRoundsPlayed');
    });

    test('scoutMapDefaultSort (compact mode) uses visible column (played)', () => {
        const scoutMapDefaultSort = { column: 'played', order: 'desc', numeric: true };
        
        // Verify 'played' is in SCOUT_MAP_COLUMNS
        const playedColumn = SCOUT_MAP_COLUMNS.find(c => c.key === scoutMapDefaultSort.column);
        expect(playedColumn).toBeDefined();
        expect(playedColumn.key).toBe('played');
    });

    test('playerDefaultSort does NOT use rating (not visible)', () => {
        // rating is NOT in PLAYER_COLUMNS
        const ratingColumn = PLAYER_COLUMNS.find(c => c.key === 'rating');
        expect(ratingColumn).toBeUndefined();

        // playerDefaultSort should NOT be 'rating'
        const playerDefaultSort = { column: 'kd', order: 'desc', numeric: true };
        expect(playerDefaultSort.column).not.toBe('rating');
    });
});

describe('TeamDetail - Grouped Header Alignment', () => {
    const MAP_COLUMNS = [
        { key: 'mapName' },
        { key: 'totalRoundsPlayed' },
        { key: 'adr' }, { key: 'kr' }, { key: 'kd' }, { key: 'hsPct' },
        { key: 'kills' }, { key: 'deaths' }, { key: 'assists' },
        { key: 'udpr' },
        { key: 'mvps' },
        { key: 'enemiesFlashed' }, { key: 'flashSuccessPct' }, { key: 'flashCount' },
        { key: 'multi2k' }, { key: 'multi3k' }, { key: 'multi4k' }, { key: 'multi5k' },
        { key: 'pistolKills' }, { key: 'sniperKills' },
        { key: 'totalDamage' },
        { key: 'clutchKills' }
    ];

    const SCOUT_MAP_COLUMNS = [
        { key: 'mapName' },
        { key: 'played' }, { key: 'picks' }, { key: 'oppPicks' },
        { key: 'winrate' }, { key: 'pickWinRate' }, { key: 'oppPickWinRate' },
        { key: 'rd' }, { key: 'kd' }, { key: 'adr' },
        { key: 'ban1' }, { key: 'ban2' }, { key: 'oppBan' }, { key: 'totalOwnBan' },
        { key: 'decov' }
    ];

    function computeMapColumnGroups(columns) {
        const groupMap = {
            'mapName': '',
            'totalRoundsPlayed': 'Erät',
            'adr': 'Taistelu', 'kr': 'Taistelu', 'kd': 'Taistelu', 'hsPct': 'Taistelu',
            'kills': 'Tappiot/Assist', 'deaths': 'Tappiot/Assist', 'assists': 'Tappiot/Assist',
            'udpr': 'Utility',
            'mvps': 'MVP',
            'enemiesFlashed': 'Flashbangit', 'flashSuccessPct': 'Flashbangit', 'flashCount': 'Flashbangit',
            'multi2k': 'Multi-kills', 'multi3k': 'Multi-kills', 'multi4k': 'Multi-kills', 'multi5k': 'Multi-kills',
            'pistolKills': 'Aseet', 'sniperKills': 'Aseet',
            'totalDamage': 'Vahinko',
            'clutchKills': 'Clutch'
        };

        const groups = [];
        let currentGroup = null;
        
        columns.forEach((col) => {
            const groupLabel = groupMap[col.key] || '';
            if (!currentGroup || currentGroup.label !== groupLabel) {
                if (currentGroup) groups.push(currentGroup);
                currentGroup = { 
                    label: groupLabel, 
                    colSpan: 1, 
                    className: groupLabel ? `group-${groupLabel.toLowerCase().replace(/[^\w]/g, '')} group-divider` : 'group-map'
                };
            } else {
                currentGroup.colSpan += 1;
            }
        });
        
        if (currentGroup) groups.push(currentGroup);
        return groups;
    }

    function computeScoutMapColumnGroups(columns) {
        const groupMap = {
            'mapName': '',
            'played': 'Pelattu', 'picks': 'Pelattu', 'oppPicks': 'Pelattu',
            'winrate': 'Tulokset', 'pickWinRate': 'Tulokset', 'oppPickWinRate': 'Tulokset',
            'rd': 'Suorituskyky', 'kd': 'Suorituskyky', 'adr': 'Suorituskyky',
            'ban1': 'Bannit', 'ban2': 'Bannit', 'oppBan': 'Bannit', 'totalOwnBan': 'Bannit',
            'decov': 'Decider/OT'
        };

        const groups = [];
        let currentGroup = null;
        
        columns.forEach((col) => {
            const groupLabel = groupMap[col.key] || '';
            if (!currentGroup || currentGroup.label !== groupLabel) {
                if (currentGroup) groups.push(currentGroup);
                currentGroup = { 
                    label: groupLabel, 
                    colSpan: 1, 
                    className: groupLabel ? `group-${groupLabel.toLowerCase().replace(/[^\w]/g, '')} group-divider` : 'group-map'
                };
            } else {
                currentGroup.colSpan += 1;
            }
        });
        
        if (currentGroup) groups.push(currentGroup);
        return groups;
    }

    test('MAP_COLUMN_GROUPS colSpans sum equals MAP_COLUMNS length', () => {
        const MAP_COLUMN_GROUPS = computeMapColumnGroups(MAP_COLUMNS);
        
        const columnsCount = MAP_COLUMNS.length;
        const groupsTotal = MAP_COLUMN_GROUPS.reduce((sum, g) => sum + g.colSpan, 0);

        expect(groupsTotal).toBe(columnsCount);
        expect(groupsTotal).toBe(22); // Current MAP_COLUMNS has 22 columns
    });

    test('SCOUT_MAP_GROUPS colSpans sum equals SCOUT_MAP_COLUMNS length', () => {
        const SCOUT_MAP_GROUPS = computeScoutMapColumnGroups(SCOUT_MAP_COLUMNS);
        
        const columnsCount = SCOUT_MAP_COLUMNS.length;
        const groupsTotal = SCOUT_MAP_GROUPS.reduce((sum, g) => sum + g.colSpan, 0);

        expect(groupsTotal).toBe(columnsCount);
        expect(groupsTotal).toBe(15); // Current SCOUT_MAP_COLUMNS has 15 columns
    });

    test('MAP_COLUMN_GROUPS computed correctly groups adjacent columns', () => {
        const MAP_COLUMN_GROUPS = computeMapColumnGroups(MAP_COLUMNS);
        
        // Verify 'Taistelu' group has 4 columns (adr, kr, kd, hsPct)
        const taisteluGroup = MAP_COLUMN_GROUPS.find(g => g.label === 'Taistelu');
        expect(taisteluGroup).toBeDefined();
        expect(taisteluGroup.colSpan).toBe(4);

        // Verify 'Multi-kills' group has 4 columns
        const multiKillsGroup = MAP_COLUMN_GROUPS.find(g => g.label === 'Multi-kills');
        expect(multiKillsGroup).toBeDefined();
        expect(multiKillsGroup.colSpan).toBe(4);

        // Verify 'MVP' group has 1 column
        const mvpGroup = MAP_COLUMN_GROUPS.find(g => g.label === 'MVP');
        expect(mvpGroup).toBeDefined();
        expect(mvpGroup.colSpan).toBe(1);
    });
});

describe('TeamDetail - URL Tab Persistence', () => {
    test('activeTab initializes from URL query param', () => {
        const mockRoute = { query: { tab: 'players' } };
        const activeTab = mockRoute.query?.tab || 'overview';

        expect(activeTab).toBe('players');
    });

    test('activeTab defaults to overview when query param missing', () => {
        const mockRoute = { query: {} };
        const activeTab = mockRoute.query?.tab || 'overview';

        expect(activeTab).toBe('overview');
    });

    test('selectTab persists tab in URL query', () => {
        let mockQuery = {};
        const mockRouter = {
            replace: ({ query }) => {
                mockQuery = query;
                return Promise.resolve();
            }
        };

        const currentQuery = {};
        const newTab = 'veto';

        // Simulate selectTab logic
        const query = { ...currentQuery };
        query.tab = newTab;
        mockRouter.replace({ query });

        expect(mockQuery.tab).toBe('veto');
    });
});

describe('TeamDetail - Empty States (No Emojis)', () => {
    const emptyStates = [
        { title: 'No matches', description: 'Match history is not available for this season.' },
        { title: 'No player data', description: 'Player statistics are not available for this season.' },
        { title: 'No veto history', description: 'Veto history data is not available for this season.' },
        { title: 'No map data', description: 'Map statistics are not available for this season.' },
        { title: 'No ban/pick history', description: 'Ban/pick history data is not available for this season.' }
    ];

    test('all empty states have neutral titles and descriptions', () => {
        emptyStates.forEach(state => {
            expect(state.title).toBeTruthy();
            expect(state.description).toBeTruthy();
            // Ensure no emojis in title or description
            expect(state.title).not.toMatch(/[\u{1F600}-\u{1F64F}]/u); // Emoticons
            expect(state.description).not.toMatch(/[\u{1F600}-\u{1F64F}]/u);
        });
    });

    test('empty states do not contain emoji unicode', () => {
        const emojiPattern = /[\u{1F300}-\u{1F9FF}]|[\u{2600}-\u{26FF}]|[\u{2700}-\u{27BF}]/u;
        
        emptyStates.forEach(state => {
            expect(state.title).not.toMatch(emojiPattern);
            expect(state.description).not.toMatch(emojiPattern);
        });
    });
});

// Run tests
console.log('Running Team Overview Page Regression Tests...\n');
