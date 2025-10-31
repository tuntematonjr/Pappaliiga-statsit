// HeroCards - display hero cards (AFI + Pappaliiga)
window.HeroCards = {
    name: 'HeroCards',
    props: {
        cards: { type: Array, required: true }
        // Format: [{ title, subtitle, logoUrl, logoAlt, primaryText, primaryUrl, secondaryText, secondaryUrl, target }]
    },
    template: `
        <div class="hero-cards-container">
            <div v-for="(card, idx) in cards" :key="idx" :class="['hero-card', card.variant ? 'hero-card--' + card.variant : '']">
                <div class="hero-card-inner">
                    <div class="hero-body">
                        <h2 class="hero-title">{{ card.title }}</h2>
                        <p v-if="card.subtitle" class="hero-subtitle">{{ card.subtitle }}</p>
                        <div class="hero-cta">
                            <a v-if="card.primaryUrl || card.primaryText || card.ctaUrl" 
                                :href="card.primaryUrl || card.ctaUrl" 
                                :target="card.target || '_self'" 
                                class="btn btn-primary">{{ card.primaryText || card.ctaText || 'Open' }}</a>

                            <a v-if="card.secondaryUrl || card.secondaryText" 
                                :href="card.secondaryUrl || '#'" 
                                :target="card.target || '_self'" 
                                class="btn btn-ghost">{{ card.secondaryText || 'Lisätietoja' }}</a>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    `
};
