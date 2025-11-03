window.PlayerRow = {
    name: 'PlayerRow',
    props: {
        player: {
            type: Object,
            required: true
        },
        subtitle: {
            type: String,
            default: ''
        }
    },
    computed: {
        displayName() {
            return this.player.nickname || this.player.name || this.player.player_name || 'Tuntematon pelaaja';
        },
        secondaryLine() {
            if (this.subtitle) {
                return this.subtitle;
            }
            const role = this.player.role || this.player.position;
            const maps = this.player.maps ?? this.player.maps_played;
            if (role && maps) {
                return `${role} · ${maps} karttaa`;
            }
            if (role) {
                return role;
            }
            if (maps) {
                return `${maps} karttaa`;
            }
            return '';
        }
    },
    template: `
        <div class="player-row">
            <div class="player-row__avatar" aria-hidden="true">
                <span class="player-row__initials">{{ displayName.charAt(0).toUpperCase() }}</span>
            </div>
            <div class="player-row__meta">
                <span class="player-row__name">{{ displayName }}</span>
                <span v-if="secondaryLine" class="player-row__subtitle">{{ secondaryLine }}</span>
            </div>
        </div>
    `
};

