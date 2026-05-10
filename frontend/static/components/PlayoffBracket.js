/* PlayoffBracket — Faceit-style bracket columns for a playoff championship. */
window.PlayoffBracket = {
    name: 'PlayoffBracket',
    components: {
        get MatchExpandedDetails() { return window.MatchExpandedDetails; }
    },
    props: {
        bracket: { type: Object, default: null },
        mapCatalog: { type: Array, default: () => [] },
        isExpandedFn: { type: Function, default: () => false },
        toggleExpandFn: { type: Function, default: () => {} },
        matchSummaryFn: { type: Function, default: () => ({}) },
        matchDetailsFn: { type: Function, default: () => ({}) },
        matchVetoFn: { type: Function, default: () => null },
        matchPlayerStatsFn: { type: Function, default: () => [] },
        matchBundleBusyFn: { type: Function, default: () => false },
        resolveAvatarFn: { type: Function, default: (u) => u },
        teamRouteFn: { type: Function, default: () => null },
        faceitUrlFn: { type: Function, default: () => null },
        replay2LinksFn: { type: Function, default: () => [] },
        replay2PlayerUrlFn: { type: Function, default: () => null },
        demoAvailabilityLoadingFn: { type: Function, default: () => false },
    },
    data() {
        return {
            openMatchId: null,
            openMatch: null,
        };
    },
    computed: {
        rounds() {
            const rawRounds = Array.isArray(this.bracket?.rounds) ? this.bracket.rounds : [];
            return this._normalizeRounds(rawRounds);
        },
    },
    mounted() {
        this.$nextTick(() => { this._layout(); });
    },
    updated() {
        this.$nextTick(() => { this._layout(); });
    },
    beforeUnmount() {
        if (this._ro) { this._ro.disconnect(); this._ro = null; }
    },
    methods: {
        matchId(m) { return String(m?.match_id || m?.matchId || ''); },
        team1Name(m) { return m?.team1_name || m?.team1Name || null; },
        team2Name(m) { return m?.team2_name || m?.team2Name || null; },
        team1Id(m) { return m?.team1_id || m?.team1Id || null; },
        team2Id(m) { return m?.team2_id || m?.team2Id || null; },
        team1Avatar(m) { return m?.team1_avatar || m?.team1Avatar || null; },
        team2Avatar(m) { return m?.team2_avatar || m?.team2Avatar || null; },
        winnerId(m) { return m?.winner_team_id || m?.winnerTeamId || null; },
        team1Score(m) { return Number(m?.team1_score ?? m?.team1Score ?? 0); },
        team2Score(m) { return Number(m?.team2_score ?? m?.team2Score ?? 0); },
        isFinished(m) {
            const fa = Number(m?.finished_at ?? m?.finishedAt ?? 0);
            return fa > 0 || !!m?.is_forfeit || (this.team1Score(m) + this.team2Score(m)) > 0;
        },
        isOngoing(m) {
            const s = String(m?.status || '').toLowerCase();
            return s === 'ongoing' || s === 'voting' || s === 'running';
        },
        scheduledDate(m) {
            const ts = Number(m?.scheduled_at ?? m?.scheduledAt ?? m?.started_at ?? m?.startedAt ?? 0);
            if (!ts) return null;
            const d = new Date(ts * 1000);
            return d.toLocaleDateString('fi-FI', { day: 'numeric', month: 'numeric' }) +
                ' · ' + d.toLocaleTimeString('fi-FI', { hour: '2-digit', minute: '2-digit' });
        },
        cardStateClass(m) {
            if (this.isOngoing(m)) return 'bk-card--ongoing';
            if (this.isFinished(m)) return 'bk-card--finished';
            return 'bk-card--scheduled';
        },
        teamWon(m, side) {
            const wid = this.winnerId(m);
            if (!wid) return false;
            return wid === (side === 'team1' ? this.team1Id(m) : this.team2Id(m));
        },
        teamLost(m, side) {
            return this.isFinished(m) && !!this.winnerId(m) && !this.teamWon(m, side);
        },
        faceitUrl(m) { return this.faceitUrlFn(m); },
        replay2Links(m) { return m ? this.replay2LinksFn(m) : []; },
        replay2PlayerUrl(mid, mapId) { return this.replay2PlayerUrlFn(mid, mapId); },
        globalMatchNum(roundIdx, matchIdx) {
            let base = 0;
            for (let ri = 0; ri < roundIdx; ri++) {
                base += (this.rounds[ri]?.match_count_expected || 1);
            }
            return base + matchIdx + 1;
        },
        tbdCount(round) {
            const exp = round.match_count_expected || 1;
            return Math.max(0, exp - (round.matches?.length || 0));
        },
        openPanel(match) {
            const mid = this.matchId(match);
            if (this.openMatchId === mid) {
                this.openMatchId = null;
                this.openMatch = null;
                return;
            }
            this.openMatchId = mid;
            this.openMatch = match;
            // Trigger data loading via parent delegate
            this.toggleExpandFn(match);
            this.$nextTick(() => {
                const panel = this.$refs.detailPanel;
                if (panel) panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
            });
        },
        closePanel() {
            this.openMatchId = null;
            this.openMatch = null;
        },
        openMatchLabel() {
            if (!this.openMatch) return '';
            const n1 = this.team1Name(this.openMatch) || 'TBD';
            const n2 = this.team2Name(this.openMatch) || 'TBD';
            return `${n1} vs ${n2}`;
        },
        _normalizeRounds(rawRounds) {
            if (!Array.isArray(rawRounds) || !rawRounds.length) return [];
            const normalized = rawRounds.map((round) => ({
                ...round,
                matches: Array.isArray(round?.matches) ? [...round.matches] : [],
            }));
            for (let ri = normalized.length - 2; ri >= 0; ri--) {
                const srcMatches = normalized[ri]?.matches || [];
                const dstMatches = normalized[ri + 1]?.matches || [];
                if (srcMatches.length < 2 || !dstMatches.length) continue;
                const decorated = srcMatches.map((match, srcIdx) => {
                    const candidates = [
                        this.winnerId(match),
                        this.team1Id(match),
                        this.team2Id(match),
                    ].filter(Boolean).map(String);
                    let targetRoundIdx = Number.POSITIVE_INFINITY;
                    let targetSide = Number.POSITIVE_INFINITY;
                    for (let dIdx = 0; dIdx < dstMatches.length; dIdx++) {
                        const dst = dstMatches[dIdx];
                        const dstT1 = String(this.team1Id(dst) || '');
                        const dstT2 = String(this.team2Id(dst) || '');
                        if (dstT1 && candidates.includes(dstT1)) {
                            targetRoundIdx = dIdx;
                            targetSide = 0;
                            break;
                        }
                        if (dstT2 && candidates.includes(dstT2)) {
                            targetRoundIdx = dIdx;
                            targetSide = 1;
                            break;
                        }
                    }
                    return { match, srcIdx, targetRoundIdx, targetSide };
                });
                decorated.sort((a, b) =>
                    (a.targetRoundIdx - b.targetRoundIdx)
                    || (a.targetSide - b.targetSide)
                    || (a.srcIdx - b.srcIdx)
                );
                normalized[ri].matches = decorated.map((x) => x.match);
            }
            return normalized;
        },
        _layout() {
            const el = this.$refs.bracketEl;
            if (!el) return;
            // Measure the tallest card in round 1 to set SLOT_H
            const cols = Array.from(el.querySelectorAll(':scope > .bracket-col'));
            if (!cols.length) return;
            const firstMc = cols[0].querySelector('.bk-matches');
            if (!firstMc) return;
            const totalSlots = parseInt(firstMc.dataset.slotCount) || 1;
            // Measure max card height in first column + padding
            const firstCards = Array.from(firstMc.querySelectorAll(':scope > .bk-card'));
            let maxH = 0;
            firstCards.forEach(c => { const h = c.getBoundingClientRect().height; if (h > maxH) maxH = h; });
            const SLOT_H = Math.max(140, maxH + 24); // 24px gap between cards
            const totalH = totalSlots * SLOT_H;
            cols.forEach((col) => {
                const mc = col.querySelector('.bk-matches');
                if (!mc) return;
                const slotCount = parseInt(mc.dataset.slotCount) || 1;
                const slotsPerCard = totalSlots / slotCount;
                mc.style.position = 'relative';
                mc.style.height = totalH + 'px';
                const cards = Array.from(mc.querySelectorAll(':scope > .bk-card'));
                cards.forEach((card, j) => {
                    const cardH = card.getBoundingClientRect().height || 100;
                    const centerY = (j + 0.5) * slotsPerCard * SLOT_H;
                    card.style.position = 'absolute';
                    card.style.top = Math.round(centerY - cardH / 2) + 'px';
                    card.style.left = '0';
                    card.style.right = '0';
                    card.style.width = '100%';
                });
            });
            this._drawConnectors();
        },
        _drawConnectors() {
            const el = this.$refs.bracketEl;
            if (!el) return;
            el.querySelectorAll('.bk-connectors').forEach(s => s.remove());
            const cols = Array.from(el.querySelectorAll(':scope > .bracket-col'));
            if (cols.length < 2) return;
            const cRect = el.getBoundingClientRect();
            const scrollL = el.scrollLeft;
            const scrollT = el.scrollTop;
            const absRect = (domEl) => {
                const r = domEl.getBoundingClientRect();
                return {
                    left:  r.left  - cRect.left + scrollL,
                    right: r.right - cRect.left + scrollL,
                    midY:  (r.top + r.bottom) / 2 - cRect.top + scrollT,
                };
            };
            const svgNS = 'http://www.w3.org/2000/svg';
            const svg = document.createElementNS(svgNS, 'svg');
            svg.classList.add('bk-connectors');
            svg.setAttribute('width', el.scrollWidth);
            svg.setAttribute('height', el.scrollHeight);
            svg.style.cssText = 'position:absolute;left:0;top:0;pointer-events:none;overflow:visible;z-index:0;';
            el.insertBefore(svg, el.firstChild);
            const line = (d) => {
                const p = document.createElementNS(svgNS, 'path');
                p.setAttribute('d', d);
                p.setAttribute('stroke', 'rgba(255,255,255,0.13)');
                p.setAttribute('stroke-width', '1.5');
                p.setAttribute('fill', 'none');
                p.setAttribute('stroke-linecap', 'round');
                svg.appendChild(p);
            };
            // Y of the winner row in a source card (or card center if match not finished)
            const srcY = (card) => {
                const won = card.querySelector('.bk-team--won');
                return won ? absRect(won).midY : absRect(card).midY;
            };
            for (let ri = 0; ri < cols.length - 1; ri++) {
                const curCards  = Array.from(cols[ri].querySelectorAll('.bk-card'));
                const nextCards = Array.from(cols[ri + 1].querySelectorAll('.bk-card'));
                if (!curCards.length || !nextCards.length) continue;
                for (let ci = 0; ci < nextCards.length; ci++) {
                    const srcA = curCards[ci * 2];
                    const srcB = curCards[ci * 2 + 1];
                    const dst  = nextCards[ci];
                    if (!srcA || !dst) continue;
                    const xA  = absRect(srcA).right;
                    const yA  = srcY(srcA);
                    const tX  = absRect(dst).left;
                    // Land on each specific team row in the destination card
                    const dstTeams = Array.from(dst.querySelectorAll('.bk-team'));
                    const usedDstIndexes = new Set();
                    const resolveDstIndex = (srcCard, fallbackIndex) => {
                        const wid = String(srcCard?.dataset?.winnerId || '');
                        if (wid) {
                            const matched = dstTeams.findIndex((teamRow) => String(teamRow?.dataset?.teamId || '') === wid);
                            if (matched >= 0 && !usedDstIndexes.has(matched)) {
                                usedDstIndexes.add(matched);
                                return matched;
                            }
                        }
                        const safeFallback = (fallbackIndex === 0 || fallbackIndex === 1) ? fallbackIndex : 0;
                        if (!usedDstIndexes.has(safeFallback)) {
                            usedDstIndexes.add(safeFallback);
                            return safeFallback;
                        }
                        const alt = safeFallback === 0 ? 1 : 0;
                        if (!usedDstIndexes.has(alt)) {
                            usedDstIndexes.add(alt);
                            return alt;
                        }
                        return safeFallback;
                    };
                    const dstAIndex = resolveDstIndex(srcA, 0);
                    const dstYA = dstTeams[dstAIndex] ? absRect(dstTeams[dstAIndex]).midY : absRect(dst).midY;
                    const cpX = (xA + tX) / 2;
                    // Bezier from source winner row → destination team1 row
                    line(`M ${xA} ${yA} C ${cpX} ${yA} ${cpX} ${dstYA} ${tX} ${dstYA}`);
                    if (srcB) {
                        const xB  = absRect(srcB).right;
                        const yB  = srcY(srcB);
                        const dstBIndex = resolveDstIndex(srcB, 1);
                        const dstYB = dstTeams[dstBIndex] ? absRect(dstTeams[dstBIndex]).midY : absRect(dst).midY;
                        const cpX2 = (xB + tX) / 2;
                        // Bezier from source winner row → destination team2 row
                        line(`M ${xB} ${yB} C ${cpX2} ${yB} ${cpX2} ${dstYB} ${tX} ${dstYB}`);
                    }
                }
            }
        },
    },
    template: `
        <div v-if="rounds.length" class="playoff-bracket-wrapper">

            <!-- ── Bracket ── -->
            <div class="playoff-bracket" ref="bracketEl">
                <div
                    v-for="(round, ri) in rounds"
                    :key="round.round_number ?? 'r'+ri"
                    class="bracket-col"
                >
                    <div class="bk-round-hdr">
                        <span class="bk-round-hdr__title">{{ round.label }}</span>
                        <span class="bk-round-hdr__meta">
                            {{ round.match_count_expected || round.matches.length }} {{ (round.match_count_expected || round.matches.length) === 1 ? 'ottelu' : 'ottelua' }}<template v-if="round.best_of">&nbsp;&middot; Best of {{ round.best_of }}</template>
                        </span>
                    </div>

                    <div class="bk-matches" :data-slot-count="round.match_count_expected || round.matches.length">

                        <!-- Real match cards -->
                        <div
                            v-for="(match, mi) in round.matches"
                            :key="matchId(match)"
                            class="bk-card"
                            :data-winner-id="winnerId(match) || ''"
                            :class="[cardStateClass(match), { 'bk-card--open': openMatchId === matchId(match) }]"
                        >
                            <div class="bk-card__hdr">
                                <span class="bk-card__num">MATCH {{ globalMatchNum(ri, mi) }}</span>
                                <span v-if="isOngoing(match)" class="bk-badge bk-badge--live">KÄYNNISSÄ</span>
                                <span v-else-if="scheduledDate(match)" class="bk-card__date">{{ scheduledDate(match) }}</span>
                            </div>

                            <div class="bk-team" :data-team-id="team1Id(match) || ''" :class="{ 'bk-team--won': teamWon(match,'team1'), 'bk-team--lost': teamLost(match,'team1') }">
                                <img v-if="resolveAvatarFn(team1Avatar(match))" :src="resolveAvatarFn(team1Avatar(match))" :alt="team1Name(match)" class="bk-team__logo" loading="lazy" decoding="async" />
                                <span v-else class="bk-team__logo bk-team__logo--ph"></span>
                                <router-link v-if="team1Name(match) && teamRouteFn(team1Id(match))" :to="teamRouteFn(team1Id(match))" class="bk-team__name">{{ team1Name(match) }}</router-link>
                                <span v-else class="bk-team__name" :class="{ 'bk-team__name--tbd': !team1Name(match) }">{{ team1Name(match) || 'TBD' }}</span>
                                <span class="bk-team__score" :class="{ 'bk-team__score--win': teamWon(match,'team1') }">{{ isFinished(match) ? team1Score(match) : '' }}</span>
                                <span v-if="teamWon(match,'team1')" class="bk-win-bar"></span>
                            </div>

                            <div class="bk-team-divider"></div>

                            <div class="bk-team" :data-team-id="team2Id(match) || ''" :class="{ 'bk-team--won': teamWon(match,'team2'), 'bk-team--lost': teamLost(match,'team2') }">
                                <img v-if="resolveAvatarFn(team2Avatar(match))" :src="resolveAvatarFn(team2Avatar(match))" :alt="team2Name(match)" class="bk-team__logo" loading="lazy" decoding="async" />
                                <span v-else class="bk-team__logo bk-team__logo--ph"></span>
                                <router-link v-if="team2Name(match) && teamRouteFn(team2Id(match))" :to="teamRouteFn(team2Id(match))" class="bk-team__name">{{ team2Name(match) }}</router-link>
                                <span v-else class="bk-team__name" :class="{ 'bk-team__name--tbd': !team2Name(match) }">{{ team2Name(match) || 'TBD' }}</span>
                                <span class="bk-team__score" :class="{ 'bk-team__score--win': teamWon(match,'team2') }">{{ isFinished(match) ? team2Score(match) : '' }}</span>
                                <span v-if="teamWon(match,'team2')" class="bk-win-bar"></span>
                            </div>

                            <div class="bk-card__footer">
                                <div class="bk-card__links">
                                    <a v-if="faceitUrl(match)" :href="faceitUrl(match)" target="_blank" rel="noopener" class="chip chip--link chip--sm">Faceit</a>
                                    <a v-for="link in replay2Links(match)" :key="'r2-'+matchId(match)+'-'+link.mapId" :href="replay2PlayerUrl(link.matchId, link.mapId)" target="_blank" rel="noopener" :class="['chip','chip--link','chip--sm',['queued','parsing'].includes(link.status)?'chip--warn':'']">2D {{ link.mapId }}</a>
                                    <span v-if="demoAvailabilityLoadingFn(match)" class="cell-muted" style="font-size:.72rem">Tark…</span>
                                </div>
                                <button v-if="isFinished(match)" type="button" class="expand-button bk-expand-btn" :class="{ 'expand-button--open': openMatchId === matchId(match) }" aria-label="Tilastot" @click.stop="openPanel(match)"><span class="chevron">›</span></button>
                            </div>
                        </div>

                        <!-- TBD placeholder slots -->
                        <div v-for="n in tbdCount(round)" :key="'tbd-'+ri+'-'+n" class="bk-card bk-card--tbd">
                            <div class="bk-card__hdr">
                                <span class="bk-card__num">MATCH {{ globalMatchNum(ri, round.matches.length + n - 1) }}</span>
                            </div>
                            <div class="bk-team bk-team--tbd">
                                <span class="bk-team__logo bk-team__logo--ph"></span>
                                <span class="bk-team__name bk-team__name--tbd">TBD</span>
                            </div>
                            <div class="bk-team-divider"></div>
                            <div class="bk-team bk-team--tbd">
                                <span class="bk-team__logo bk-team__logo--ph"></span>
                                <span class="bk-team__name bk-team__name--tbd">TBD</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- ── Detail panel below bracket ── -->
            <div v-if="openMatchId && openMatch" class="bk-detail-panel" ref="detailPanel">
                <div class="bk-detail-panel__hdr">
                    <div class="bk-detail-panel__title">
                        <span class="bk-detail-panel__vs">{{ openMatchLabel() }}</span>
                        <span v-if="scheduledDate(openMatch)" class="bk-detail-panel__date">{{ scheduledDate(openMatch) }}</span>
                    </div>
                    <div class="bk-detail-panel__links">
                        <a v-if="faceitUrl(openMatch)" :href="faceitUrl(openMatch)" target="_blank" rel="noopener" class="chip chip--link chip--sm">Faceit</a>
                        <a v-for="link in replay2Links(openMatch)" :key="'dp-r2-'+link.mapId" :href="replay2PlayerUrl(link.matchId, link.mapId)" target="_blank" rel="noopener" :class="['chip','chip--link','chip--sm',['queued','parsing'].includes(link.status)?'chip--warn':'']">2D {{ link.mapId }}</a>
                        <span v-if="demoAvailabilityLoadingFn(openMatch)" class="cell-muted" style="font-size:.75rem">Tark…</span>
                    </div>
                    <button type="button" class="bk-detail-panel__close" aria-label="Sulje" @click="closePanel()">✕</button>
                </div>
                <div class="match-expand-content">
                    <match-expanded-details
                        :summary="matchSummaryFn(openMatch)"
                        :details="matchDetailsFn(openMatch)"
                        :veto-entry="matchVetoFn(openMatch)"
                        :player-stats="matchPlayerStatsFn(openMatch)"
                        :map-catalog="mapCatalog"
                        :loading="matchBundleBusyFn(openMatchId)"
                    ></match-expanded-details>
                </div>
            </div>

        </div>
        <p v-else class="division-section__empty">Bracket-tietoja ei saatavilla.</p>
    `
};
