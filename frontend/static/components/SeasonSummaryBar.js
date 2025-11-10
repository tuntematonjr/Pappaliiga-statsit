/**
 * SeasonSummaryBar - Displays aggregated season statistics in card format
 * Shows: Teams, Players, Matches, Rounds, Kills, Deaths, ADR, KD, Win%, Finished%
 */
export function SeasonSummaryBar({ summary }) {
    if (!summary) return null;

    const stats = [
        {
            id: 'teams',
            label: 'Teams',
            value: summary.teams || 0,
            icon: '👥',
            format: 'number'
        },
        {
            id: 'players',
            label: 'Players',
            value: summary.players || 0,
            icon: '🎮',
            format: 'number'
        },
        {
            id: 'matches',
            label: 'Matches',
            value: summary.matches || 0,
            icon: '🎯',
            format: 'number'
        },
        {
            id: 'rounds',
            label: 'Rounds',
            value: summary.rounds || 0,
            icon: '🔄',
            format: 'number'
        },
        {
            id: 'kills',
            label: 'Kills',
            value: summary.kills || 0,
            icon: '💀',
            format: 'number'
        },
        {
            id: 'deaths',
            label: 'Deaths',
            value: summary.deaths || 0,
            icon: '☠️',
            format: 'number'
        },
        {
            id: 'adr',
            label: 'ADR',
            value: summary.adrAvg || 0,
            icon: '📊',
            format: 'decimal',
            tooltip: 'Average Damage per Round'
        },
        {
            id: 'kd',
            label: 'K/D',
            value: summary.kdRatio || 0,
            icon: '⚔️',
            format: 'decimal',
            tooltip: 'Kill/Death Ratio'
        },
        {
            id: 'winrate',
            label: 'Win%',
            value: (summary.winRate || 0) * 100,
            icon: '🏆',
            format: 'percent',
            tooltip: 'Overall Win Rate'
        },
        {
            id: 'finished',
            label: 'Finished',
            value: summary.finishedPercent || 0,
            icon: '✓',
            format: 'percent',
            tooltip: `${summary.progress?.divisionsFinished || 0} / ${summary.progress?.divisionsTotal || 0} divisions completed`
        }
    ];

    const container = document.createElement('div');
    container.className = 'season-summary-bar';
    container.setAttribute('role', 'region');
    container.setAttribute('aria-label', 'Season Summary Statistics');

    stats.forEach((stat, index) => {
        const card = createStatCard(stat, index);
        container.appendChild(card);
    });

    return container;
}

function createStatCard(stat, index) {
    const card = document.createElement('div');
    card.className = 'season-stat-card';
    card.style.animationDelay = `${index * 50}ms`;
    
    if (stat.tooltip) {
        card.title = stat.tooltip;
    }

    const icon = document.createElement('div');
    icon.className = 'season-stat-card__icon';
    icon.textContent = stat.icon;

    const content = document.createElement('div');
    content.className = 'season-stat-card__content';

    const value = document.createElement('div');
    value.className = 'season-stat-card__value';
    value.textContent = formatValue(stat.value, stat.format);

    const label = document.createElement('div');
    label.className = 'season-stat-card__label';
    label.textContent = stat.label;

    content.appendChild(value);
    content.appendChild(label);

    card.appendChild(icon);
    card.appendChild(content);

    return card;
}

function formatValue(value, format) {
    const num = Number(value) || 0;

    switch (format) {
        case 'number':
            return num.toLocaleString('fi-FI', { maximumFractionDigits: 0 });
        case 'decimal':
            return num.toLocaleString('fi-FI', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        case 'percent':
            return `${num.toFixed(1)}%`;
        default:
            return String(num);
    }
}

// Add CSS styles
const styles = `
.season-summary-bar {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: 1rem;
    margin: 2rem 0;
    padding: 1rem;
    background: linear-gradient(135deg, rgba(255,255,255,0.05) 0%, rgba(255,255,255,0.02) 100%);
    border-radius: 12px;
    backdrop-filter: blur(10px);
}

.season-stat-card {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 1rem;
    background: rgba(255, 255, 255, 0.05);
    border: 1px solid rgba(255, 255, 255, 0.1);
    border-radius: 8px;
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    cursor: default;
    animation: slideInUp 0.4s ease-out backwards;
}

.season-stat-card:hover {
    background: rgba(255, 255, 255, 0.08);
    border-color: rgba(255, 255, 255, 0.2);
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
}

.season-stat-card__icon {
    font-size: 1.75rem;
    line-height: 1;
    opacity: 0.9;
}

.season-stat-card__content {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
}

.season-stat-card__value {
    font-size: 1.5rem;
    font-weight: 700;
    line-height: 1;
    color: var(--color-text-primary, #fff);
}

.season-stat-card__label {
    font-size: 0.75rem;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--color-text-secondary, rgba(255, 255, 255, 0.7));
}

@keyframes slideInUp {
    from {
        opacity: 0;
        transform: translateY(20px);
    }
    to {
        opacity: 1;
        transform: translateY(0);
    }
}

@media (max-width: 768px) {
    .season-summary-bar {
        grid-template-columns: repeat(auto-fit, minmax(110px, 1fr));
        gap: 0.75rem;
        padding: 0.75rem;
    }

    .season-stat-card {
        padding: 0.75rem;
        gap: 0.5rem;
    }

    .season-stat-card__icon {
        font-size: 1.5rem;
    }

    .season-stat-card__value {
        font-size: 1.25rem;
    }

    .season-stat-card__label {
        font-size: 0.7rem;
    }
}
`;

// Inject styles if not already present
if (!document.getElementById('season-summary-bar-styles')) {
    const styleEl = document.createElement('style');
    styleEl.id = 'season-summary-bar-styles';
    styleEl.textContent = styles;
    document.head.appendChild(styleEl);
}
