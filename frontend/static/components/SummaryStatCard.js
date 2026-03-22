(function () {
    'use strict';

    window.SummaryStatCard = {
        name: 'SummaryStatCard',
        props: {
            icon: {
                type: String,
                default: ''
            },
            label: {
                type: String,
                required: true
            },
            value: {
                type: [String, Number],
                required: true
            },
            subtitle: {
                type: String,
                default: ''
            }
        },
        template: `
            <div class="kausikooste-card" role="listitem">
                <div v-if="icon" class="kausikooste-card__icon">{{ icon }}</div>
                <div class="kausikooste-card__value">{{ value }}</div>
                <div class="kausikooste-card__label">{{ label }}</div>
                <p v-if="subtitle" class="kausikooste-card__subtitle">{{ subtitle }}</p>
            </div>
        `
    };
})();
