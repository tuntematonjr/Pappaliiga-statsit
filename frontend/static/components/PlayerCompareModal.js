window.PlayerCompareModal = {
    name: 'PlayerCompareModal',
    props: {
        visible: { type: Boolean, default: false },
        basePlayer: { type: Object, default: null },
        basePlayerId: { type: [String, Number], default: null },
        season: { type: [String, Number], default: null },
        division: { type: [String, Number], default: null },
        comparePlayer: { type: Object, default: null },
        metrics: { type: Array, default: () => [] },
        loading: { type: Boolean, default: false },
        error: { type: String, default: null }
    },
    emits: ['close', 'submit'],
    data() {
        return {
            searchQuery: '',
            selectedCandidateId: '',
            candidates: [],
            loadingCandidates: false,
            candidatesError: null,
            debounceTimer: null
        };
    },
    computed: {
        normalizedBasePlayerId() {
            const value = this.basePlayerId ?? this.basePlayer?.player_id ?? this.basePlayer?.playerId ?? null;
            return value == null ? '' : String(value);
        },
        normalizedCandidates() {
            const baseId = this.normalizedBasePlayerId;
            return (Array.isArray(this.candidates) ? this.candidates : [])
                .filter(item => {
                    const pid = this.playerId(item);
                    if (!pid) return false;
                    return String(pid) !== String(baseId);
                })
                .map(item => ({
                    player_id: String(this.playerId(item)),
                    nickname: this.playerName(item),
                    avatar: this.proxyAvatar(this.playerAvatar(item))
                }));
        },
        filteredCandidates() {
            const list = this.normalizedCandidates;
            const needle = String(this.searchQuery || '').trim().toLowerCase();
            if (!needle) return list.slice(0, 50);
            return list
                .filter(item => {
                    const name = String(item.nickname || '').toLowerCase();
                    return name.includes(needle);
                })
                .slice(0, 100);
        },
        selectedCandidate() {
            if (!this.selectedCandidateId) return null;
            return this.normalizedCandidates.find(item => String(item.player_id) === String(this.selectedCandidateId)) || null;
        },
        submitDisabled() {
            if (this.loading || this.loadingCandidates) return true;
            if (!this.selectedCandidateId) return true;
            return String(this.selectedCandidateId) === String(this.normalizedBasePlayerId);
        }
    },
    watch: {
        visible(newVal) {
            if (newVal) {
                this.searchQuery = '';
                this.selectedCandidateId = '';
                this.candidatesError = null;
                this.loadCandidates();
            } else {
                this.clearDebounce();
            }
        },
        season() {
            if (this.visible) this.debouncedLoadCandidates();
        },
        division() {
            if (this.visible) this.debouncedLoadCandidates();
        }
    },
    methods: {
        clearDebounce() {
            if (this.debounceTimer) {
                clearTimeout(this.debounceTimer);
                this.debounceTimer = null;
            }
        },
        debouncedLoadCandidates() {
            this.clearDebounce();
            this.debounceTimer = setTimeout(() => {
                this.loadCandidates();
            }, 180);
        },
        async loadCandidates() {
            if (!window.apiClient || typeof window.apiClient.getPlayers !== 'function') {
                this.candidatesError = 'Pelaajahaku ei ole käytettävissä';
                this.candidates = [];
                return;
            }
            this.loadingCandidates = true;
            this.candidatesError = null;
            try {
                const params = {
                    season: null,
                    division: null,
                    limit: 10000
                };
                const rows = await window.apiClient.getPlayers(params);
                this.candidates = Array.isArray(rows)
                    ? [...rows].sort((a, b) => this.playerName(a).localeCompare(this.playerName(b), 'fi'))
                    : [];
            } catch (error) {
                this.candidatesError = error?.message || 'Pelaajalistan lataus epäonnistui';
                this.candidates = [];
            } finally {
                this.loadingCandidates = false;
            }
        },
        playerId(player) {
            return player?.player_id || player?.playerId || null;
        },
        playerName(player) {
            return player?.nickname || player?.name || 'Tuntematon pelaaja';
        },
        playerAvatar(player) {
            return player?.avatar || null;
        },
        proxyAvatar(url) {
            const fallback = window.PAPPALIIGA_DEFAULT_LOGO || '/static/pappaliiga-logo-white-bg.png';
            const src = String(url || '').trim();
            if (!src) return fallback;
            try {
                if (window.apiClient && typeof window.apiClient.proxyAvatar === 'function') {
                    return window.apiClient.proxyAvatar(src) || fallback;
                }
            } catch (_) {
                return fallback;
            }
            return src;
        },
        selectCandidate(player) {
            const pid = this.playerId(player);
            if (!pid) return;
            this.selectedCandidateId = String(pid);
            this.searchQuery = this.playerName(player);
        },
        handleClose() {
            this.$emit('close');
        },
        handleSubmit() {
            if (this.submitDisabled) return;
            this.$emit('submit', String(this.selectedCandidateId).trim());
        },
        delta(metric) {
            const base = Number(metric?.base);
            const compare = Number(metric?.compare);
            if (!Number.isFinite(base) || !Number.isFinite(compare)) return null;
            return compare - base;
        },
        deltaClass(metric) {
            const value = this.delta(metric);
            if (value == null) return 'is-neutral';
            if (value > 0) return 'is-pos';
            if (value < 0) return 'is-neg';
            return 'is-neutral';
        },
        deltaLabel(metric) {
            const value = this.delta(metric);
            if (value == null) return '–';
            const sign = value > 0 ? '+' : value < 0 ? '-' : '±';
            const abs = Math.abs(value);
            const decimals = metric?.decimals ?? 1;
            if (metric?.percent) {
                return `${sign}${abs.toFixed(decimals)} %`;
            }
            return `${sign}${abs.toFixed(decimals)}`;
        },
        displayValue(metric, side) {
            const value = side === 'base' ? metric.base : metric.compare;
            if (value == null || Number.isNaN(value)) {
                return '–';
            }
            if (typeof metric.format === 'function') {
                return metric.format(value);
            }
            if (metric.percent) {
                return `${Number(value).toFixed(metric.decimals ?? 1)} %`;
            }
            const decimals = metric.decimals ?? (Number(value) >= 100 ? 0 : 1);
            return Number(value).toFixed(decimals);
        }
    },
    template: `
        <transition name="fade">
            <div v-if="visible" class="compare-modal" role="dialog" aria-modal="true">
                <div class="compare-modal__backdrop" @click="handleClose"></div>
                <div class="compare-modal__content glass-card">
                    <header class="compare-modal__header">
                        <h3 class="title-accent titleUnderlineSection">Vertaa pelaajaa</h3>
                        <button type="button" class="compare-modal__close" @click="handleClose">×</button>
                    </header>

                    <div class="compare-modal__body">

                        <form class="compare-modal__form" @submit.prevent="handleSubmit">
                            <label class="compare-modal__label">
                                Hae pelaajaa
                                <input
                                    v-model="searchQuery"
                                    type="text"
                                    placeholder="Kirjoita pelaajan nimi"
                                />
                            </label>
                            <button type="submit" class="btn-primary" :disabled="submitDisabled">
                                Hae vertailu
                            </button>
                        </form>

                        <p v-if="selectedCandidate" class="compare-modal__selected">
                            Valittu: <strong>{{ selectedCandidate.nickname }}</strong>
                        </p>

                        <div v-if="loadingCandidates" class="compare-modal__hint">Ladataan pelaajia...</div>
                        <error-message v-else-if="candidatesError" :message="candidatesError"></error-message>
                        <div v-else class="compare-modal__candidates">
                            <button
                                v-for="candidate in filteredCandidates"
                                :key="candidate.player_id"
                                type="button"
                                class="compare-modal__candidate"
                                :class="{ 'is-active': String(selectedCandidateId) === String(candidate.player_id) }"
                                @click="selectCandidate(candidate)"
                            >
                                <img v-if="candidate.avatar" :src="candidate.avatar" :alt="candidate.nickname" loading="lazy" />
                                <span class="compare-modal__candidate-name">{{ candidate.nickname }}</span>
                            </button>
                            <p v-if="!filteredCandidates.length" class="compare-modal__hint">Ei osumia haulla.</p>
                        </div>

                        <loading-spinner v-if="loading" message="Pelaajaa verrataan..."></loading-spinner>
                        <error-message v-else-if="error" :message="error"></error-message>
                        <p v-else class="compare-modal__empty">Vertailu näytetään pelaajasivulla valinnan jälkeen.</p>
                    </div>
                </div>
            </div>
        </transition>
    `
};

