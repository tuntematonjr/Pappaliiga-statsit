(function () {
    const DivisionCard = {
        name: 'DivisionCard',
        props: {
            division: {
                type: Object,
                required: true
            }
        },
        computed: {
            hasLink() {
                return Boolean(this.division.route);
            },
            linkTarget() {
                return this.hasLink ? this.division.route : '#';
            },
            recordLabel() {
                const wins = this.division.wins ?? 0;
                const losses = this.division.losses ?? 0;
                const draws = this.division.draws ?? 0;
                const parts = [`${wins}–${losses}`];
                if (draws) {
                    parts.push(`(${draws} tasapeliä)`);
                }
                return parts.join(' ');
            },
            progressValue() {
                const percent = Number(this.division.progressPercent);
                if (!Number.isFinite(percent)) {
                    return 0;
                }
                return Math.max(0, Math.min(100, Math.round(percent)));
            },
            ariaLabel() {
                const name = this.division.name || 'Division';
                const progress = `${this.division.matchesPlayed || 0}/${this.division.matchesTotal || 0}`;
                return `${name}. Ottelut ${progress}. Voitot ${this.division.wins || 0}.`;
            }
        },
        template: `
            <article class="division-card glass-card" :aria-label="ariaLabel" tabindex="0">
                <header class="division-card__header">
                    <div class="division-card__heading">
                        <span v-if="division.phase" class="division-card__eyebrow">{{ division.phase }}</span>
                        <h3 class="division-card__title">{{ division.name }}</h3>
                        <p v-if="division.subtitle" class="division-card__subtitle">{{ division.subtitle }}</p>
                    </div>
                    <span v-if="division.teamCount" class="division-card__meta">{{ division.teamCount }} joukkuetta</span>
                </header>

                <div class="division-card__body">
                    <div class="division-card__stat">
                        <span class="division-card__stat-label">Ottelut</span>
                        <span class="division-card__stat-value">{{ division.matchesPlayed }} / {{ division.matchesTotal }}</span>
                    </div>
                    <div class="division-card__stat">
                        <span class="division-card__stat-label">Voitot</span>
                        <span class="division-card__stat-value">{{ recordLabel }}</span>
                    </div>
                    <div class="division-card__stat">
                        <span class="division-card__stat-label">Round-diff</span>
                        <span
                            class="division-card__stat-value"
                            :class="{
                                'text-ok': division.roundDiff > 0,
                                'text-err': division.roundDiff < 0
                            }"
                        >
                            {{ division.roundDiff > 0 ? '+' : '' }}{{ division.roundDiff }}
                        </span>
                    </div>

                    <div class="division-card__progress" role="group" aria-label="Kausi etenee">
                        <div class="division-card__progress-bar">
                            <span class="division-card__progress-fill" :style="{ width: progressValue + '%' }"></span>
                        </div>
                        <span class="division-card__progress-value">{{ progressValue }} %</span>
                    </div>

                    <div v-if="division.topTeam" class="division-card__leader" aria-label="Johtava joukkue">
                        <img
                            v-if="division.topTeam.logo"
                            class="division-card__leader-logo"
                            :src="division.topTeam.logo"
                            :alt="division.topTeam.name + ' logo'"
                        >
                        <div class="division-card__leader-meta">
                            <span class="division-card__leader-label">Kärkijoukkue</span>
                            <span class="division-card__leader-name">{{ division.topTeam.name }}</span>
                            <span class="division-card__leader-record">
                                {{ division.topTeam.record.wins }}–{{ division.topTeam.record.losses }}
                            </span>
                        </div>
                    </div>
                </div>

                <footer class="division-card__footer">
                    <router-link
                        v-if="hasLink"
                        class="division-card__cta"
                        :to="linkTarget"
                    >
                        Näytä divisioona
                    </router-link>
                    <span v-else class="division-card__cta division-card__cta--disabled">Ei linkkiä</span>
                </footer>
            </article>
        `
    };

    window.DivisionCard = DivisionCard;

    window.DivisionCardList = {
        name: 'DivisionCardList',
        components: {
            DivisionCard
        },
        props: {
            divisions: {
                type: Array,
                default: () => []
            },
            seasonLabel: {
                type: String,
                default: ''
            },
            emptyMessage: {
                type: String,
                default: 'Ei divisioonia'
            }
        },
        computed: {
            hasDivisions() {
                return Array.isArray(this.divisions) && this.divisions.length > 0;
            },
            orderedDivisions() {
                if (!this.hasDivisions) {
                    return [];
                }
                return [...this.divisions].sort((a, b) => {
                    const aName = a?.name || '';
                    const bName = b?.name || '';
                    return aName.localeCompare(bName, 'fi');
                });
            }
        },
        template: `
            <div class="division-card-list" role="list" :aria-label="'Divisioonat ' + seasonLabel">
                <division-card
                    v-for="division in orderedDivisions"
                    :key="division.key"
                    :division="division"
                    role="listitem"
                ></division-card>
                <p v-if="!hasDivisions" class="division-card-list__empty">
                    {{ emptyMessage }}
                </p>
            </div>
        `
    };
})();

