(function () {
    if (typeof window === 'undefined') {
        return;
    }
    window.__PAPPALIIGA_DEV_DIVISIONS__ = {
        timestamp: '2025-02-01T12:00:00Z',
        divisions: [
            {
                division_id: 1,
                name: 'DEV · Alpha',
                tier: 1,
                season_number: 11,
                season: {
                    teams: 10,
                    matches_played: 32,
                    matches_total: 45,
                    status: 'active',
                    winner: null
                },
                playoffs: {
                    teams: 8,
                    matches_played: 2,
                    matches_total: 7,
                    status: 'waiting',
                    winner: null
                },
                slug: 'dev-alpha'
            },
            {
                division_id: 6,
                name: 'DEV · Bravo',
                tier: 2,
                season_number: 11,
                season: {
                    teams: 12,
                    matches_played: 48,
                    matches_total: 60,
                    status: 'active',
                    winner: null
                },
                playoffs: {
                    teams: 8,
                    matches_played: 6,
                    matches_total: 7,
                    status: 'active',
                    winner: null
                },
                slug: 'dev-bravo'
            },
            {
                division_id: 11,
                name: 'DEV · Charlie',
                tier: 3,
                season_number: 10,
                season: {
                    teams: 14,
                    matches_played: 70,
                    matches_total: 70,
                    status: 'finished',
                    winner: 'Dev Legends'
                },
                playoffs: {
                    teams: 8,
                    matches_played: 7,
                    matches_total: 7,
                    status: 'finished',
                    winner: 'Dev Legends'
                },
                slug: 'dev-charlie'
            }
        ]
    };
})();
