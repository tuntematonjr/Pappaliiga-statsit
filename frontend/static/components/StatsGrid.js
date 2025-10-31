// StatsGrid - displays stat cards in a grid
window.StatsGrid = {
    name: 'StatsGrid',
    props: {
        stats: { type: Array, required: true }
        // Format: [{ icon, label, value, subtitle }]
    },
    template: `
        <div class="stats-grid">
            <div v-for="(stat, idx) in stats" :key="idx" class="stat-card">
                <div v-if="stat.icon" class="stat-icon">{{ stat.icon }}</div>
                <div class="stat-value">{{ stat.value }}</div>
                <div class="stat-label">{{ stat.label }}</div>
                <div v-if="stat.subtitle" class="stat-subtitle">{{ stat.subtitle }}</div>
            </div>
        </div>
    `
};
