// Shared frontend constants
window.PAPPALIIGA_DEFAULT_LOGO =
    window.PAPPALIIGA_DEFAULT_LOGO ||
    'https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png';

(function registerDivisionStatusMeta() {
    if (typeof window === 'undefined') {
        return;
    }
    if (window.PAPPALIIGA_DIVISION_STATUS_META) {
        return;
    }

    function icon(viewBox, paths) {
        return Object.freeze({
            viewBox,
            paths: paths.map(path => Object.freeze({ d: path }))
        });
    }

    const STATUS_META = Object.freeze({
        'ei-alkanut': {
            id: 'ei-alkanut',
            label: 'Ei alkanut',
            tone: 'muted',
            icon: icon('0 0 24 24', [
                'M12 3.25a8.75 8.75 0 1 1 0 17.5 8.75 8.75 0 0 1 0-17.5Zm0 1.5a7.25 7.25 0 1 0 0 14.5 7.25 7.25 0 0 0 0-14.5Z',
                'M12.75 6.5h-1.5v4.54l3.2 1.86.75-1.3-2.45-1.42V6.5Z'
            ])
        },
        'runkosarja-kaynnissa': {
            id: 'runkosarja-kaynnissa',
            label: 'Runkosarja käynnissä',
            tone: 'blue',
            icon: icon('0 0 24 24', [
                'M5 13.25h3.8v3.5H5z',
                'M10.1 9.25h3.8v7.5h-3.8z',
                'M15.25 5.25h3.8v11.5h-3.8z'
            ])
        },
        'playoffit-kaynnissa': {
            id: 'playoffit-kaynnissa',
            label: 'Playoffit käynnissä',
            tone: 'purple',
            icon: icon('0 0 24 24', [
                'M7 4.25h10a2.75 2.75 0 0 1 2.75 2.75v1.72a5.25 5.25 0 0 1-4 5.09l-.7 2a2.5 2.5 0 0 1-2.37 1.64H11.3a2.5 2.5 0 0 1-2.37-1.63l-.71-2.01a5.25 5.25 0 0 1-4-5.09V7A2.75 2.75 0 0 1 7 4.25Zm-.5 2.75V7c0 .99.65 1.85 1.6 2.12l.7.2 1.06 3.02c.16.46.6.77 1.1.77h1.38c.5 0 .94-.31 1.1-.77l1.05-3.02.7-.2A2.25 2.25 0 0 0 17.5 7V7A1.25 1.25 0 0 0 16.25 5.75H7A.5.5 0 0 0 6.5 7Z',
                'M9.5 18.25h5v1.5h-5z'
            ])
        },
        'taputeltu-loppuun': {
            id: 'taputeltu-loppuun',
            label: 'Taputeltu loppuun',
            tone: 'green',
            icon: icon('0 0 24 24', [
                'M12 3.5a8.5 8.5 0 1 1 0 17 8.5 8.5 0 0 1 0-17Zm0 1.5a7 7 0 1 0 0 14 7 7 0 0 0 0-14Z',
                'm16.02 9.74-1.13-1.12-3.43 3.42-1.31-1.3-1.12 1.12 2.43 2.43z'
            ])
        }
    });

    window.PAPPALIIGA_DIVISION_STATUS_META = STATUS_META;
    window.PAPPALIIGA_DIVISION_STATUS_ORDER = Object.freeze([
        'ei-alkanut',
        'runkosarja-kaynnissa',
        'playoffit-kaynnissa',
        'taputeltu-loppuun'
    ]);
})();

// ============================================================
// TOOLTIP & COMPONENT HELPERS
// ============================================================

// Tooltip definitions for stats abbreviations
window.STATS_TOOLTIPS = {
    // Win Rate & Performance
    'WR': 'Win Rate - Voittoprosentti',
    'Win Rate': 'Win Rate - Voittoprosentti',
    
    // Kill/Death Stats
    'K/D': 'Kills / Deaths - Tapot per kuolema',
    'KD': 'Kills / Deaths - Tapot per kuolema',
    'K/R': 'Kills per Round - Tapot per kierros',
    'KR': 'Kills per Round - Tapot per kierros',
    'ADR': 'Average Damage per Round - Keskivahingon per kierros',
    'HS%': 'Headshot Percentage - Päähän osumien prosentti',
    
    // Multi-kills
    '2K': '2 Kills - Kaksi tappoa samalla kierroksella',
    '3K': '3 Kills - Kolme tappoa samalla kierroksella',
    '4K': '4 Kills - Neljä tappoa samalla kierroksella',
    'Ace': '5 Kills - Kaikki viisi tappoa samalla kierroksella',
    
    // Utility & Support
    'UDPR': 'Utility Damage per Round - Kranaattivahingon per kierros',
    'Flash': 'Flashbang - Sokeauttavat kranaatit',
    'MVP': 'Most Valuable Player - Arvokkain pelaaja',
    
    // Clutch & Survival
    'Clutch': 'Clutch - Tilanne jossa pelaaja on yksin vastaan useita',
    'Survival': 'Survival Rate - Selviytymisprosentti',
    
    // General
    'Played': 'Pelattu - Pelien määrä',
    'Maps': 'Kartat - Karttojen määrä',
    'Rounds': 'Kierrokset - Kierrosten määrä',
    'Assists': 'Assists - Avustukset',
    'Deaths': 'Deaths - Kuolemat',
    'Rating': 'Rating 1.0 - Yleinen suorituskykyarvo'
};

window.getTooltip = function(abbreviation) {
    return window.STATS_TOOLTIPS[abbreviation] || null;
};

window.wrapStatsWithTooltips = function(text) {
    if (!text || typeof text !== 'string') return text;
    
    let result = text;
    const tooltips = window.STATS_TOOLTIPS;
    
    const abbreviations = Object.keys(tooltips).sort((a, b) => b.length - a.length);
    
    abbreviations.forEach(abbr => {
        const regex = new RegExp(`\\b${abbr}\\b(?=[%/\\s:]|$)`, 'g');
        result = result.replace(regex, match => {
            const tooltip = tooltips[abbr];
            return `<span class="tooltip-wrapper" data-tooltip="${tooltip}">${match}</span>`;
        });
    });
    
    return result;
};

// TooltipWrapper component
window.TooltipWrapper = {
    name: 'TooltipWrapper',
    props: {
        text: {
            type: String,
            required: true
        },
        position: {
            type: String,
            default: 'top',
            validator: (value) => ['top', 'bottom', 'left', 'right'].includes(value)
        }
    },
    template: `
        <span class="tooltip-wrapper" @mouseenter="show = true" @mouseleave="show = false">
            <slot></slot>
            <Transition name="tooltip-fade">
                <span v-if="show" class="tooltip-content" :class="'tooltip-content--' + position">
                    {{ text }}
                </span>
            </Transition>
        </span>
    `,
    data() {
        return {
            show: false
        };
    }
};
