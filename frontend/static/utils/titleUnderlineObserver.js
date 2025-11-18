(function () {
    'use strict';

    const SELECTOR = '.titleUnderlineMain, .titleUnderlineCard';
    const SCALE_FACTORS = {
        main: 1.28,
        card: 1.08
    };
    const SAFE_MARGIN = 12;
    const RESIZE_SUPPORTED = typeof ResizeObserver !== 'undefined';
    const MUTATION_SUPPORTED = typeof MutationObserver !== 'undefined';

    function TitleUnderlineManager() {
        this._tracked = new WeakSet();
        this._state = new WeakMap();
        this._resizeObserver = RESIZE_SUPPORTED
            ? new ResizeObserver(entries => {
                  entries.forEach(entry => this._update(entry.target));
              })
            : null;
        this._mutationObserver = MUTATION_SUPPORTED
            ? new MutationObserver(mutations => {
                  mutations.forEach(mutation => {
                      mutation.addedNodes.forEach(node => {
                          if (node.nodeType !== 1) {
                              return;
                          }
                          if (node.matches && node.matches(SELECTOR)) {
                              this._register(node);
                          }
                          if (node.querySelectorAll) {
                              node.querySelectorAll(SELECTOR).forEach(el => this._register(el));
                          }
                      });
                  });
              })
            : null;
    }

    TitleUnderlineManager.prototype.init = function init() {
        const ready = () => {
            this._scan();
            if (this._mutationObserver) {
                this._mutationObserver.observe(document.body, {
                    childList: true,
                    subtree: true
                });
            }
            window.addEventListener('resize', () => this._scan(), { passive: true });
            document.addEventListener('visibilitychange', () => {
                if (!document.hidden) {
                    this._scan();
                }
            });
        };

        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', ready, { once: true });
        } else {
            ready();
        }
    };

    TitleUnderlineManager.prototype._scan = function _scan() {
        document.querySelectorAll(SELECTOR).forEach(el => {
            this._register(el);
            this._update(el);
        });
    };

    TitleUnderlineManager.prototype._register = function _register(el) {
        if (!el || this._tracked.has(el)) {
            return;
        }
        this._tracked.add(el);
        this._assignDelay(el);
        if (this._resizeObserver) {
            this._resizeObserver.observe(el);
        }
        this._update(el);
    };

    TitleUnderlineManager.prototype._assignDelay = function _assignDelay(el) {
        const delay = this._computeDelay(el);
        const state = this._stateFor(el);
        el.style.setProperty('--title-underline-delay', delay + 's');
        state.delay = delay;
        this._restartAnimation(el);
    };

    TitleUnderlineManager.prototype._computeDelay = function _computeDelay(el) {
        const key = (el.getAttribute('data-title-key') || el.textContent || el.id || 'title').trim();
        let hash = 0;
        for (let i = 0; i < key.length; i += 1) {
            hash = (hash * 31 + key.charCodeAt(i)) >>> 0;
        }
        if (!hash) {
            hash = Math.floor(Math.random() * 1e6);
        }
        const normalized = (hash % 900) / 1000; // 0 -> 0.899
        const minDelay = 0.12;
        const maxDelay = 1.4;
        return Number((minDelay + normalized * (maxDelay - minDelay)).toFixed(2));
    };

    TitleUnderlineManager.prototype._update = function _update(el) {
        if (!el || !el.isConnected) {
            return;
        }
        const type = el.classList.contains('titleUnderlineCard') ? 'card' : 'main';
        const scale = SCALE_FACTORS[type] || SCALE_FACTORS.main;
        const measure = () => {
            const rect = el.getBoundingClientRect();
            const textWidth = rect.width || el.scrollWidth || el.offsetWidth;
            if (!textWidth) {
                return;
            }
            const parentWidth = el.parentElement
                ? el.parentElement.getBoundingClientRect().width || el.parentElement.offsetWidth || textWidth
                : textWidth;
            const available = Math.max(textWidth, parentWidth - SAFE_MARGIN);
            const desiredWidth = textWidth * scale;
            const boundedWidth = Number.isFinite(available) ? Math.min(desiredWidth, available) : desiredWidth;
            const computedWidth = Math.max(textWidth, boundedWidth);
            const state = this._stateFor(el);
            const previousWidth = state.width;
            const widthChanged =
                !previousWidth || Math.abs(previousWidth - computedWidth) > 0.5;
            if (widthChanged) {
                state.width = computedWidth;
            }
            el.style.setProperty('--title-underline-absolute', computedWidth + 'px');
            if (widthChanged) {
                this._restartAnimation(el);
            }
        };
        if (typeof window.requestAnimationFrame === 'function') {
            window.requestAnimationFrame(measure);
        } else {
            measure();
        }
    };

    TitleUnderlineManager.prototype._stateFor = function _stateFor(el) {
        let state = this._state.get(el);
        if (!state) {
            state = { width: undefined, delay: undefined };
            this._state.set(el, state);
        }
        return state;
    };

    TitleUnderlineManager.prototype._restartAnimation = function _restartAnimation(el) {
        if (!el || !el.style || typeof el.style.setProperty !== 'function') {
            return;
        }
        const previousInline = el.style.getPropertyValue('--title-underline-animation-name');
        el.style.setProperty('--title-underline-animation-name', 'none');
        // Force reflow so the browser picks up the reset before we restore the animation name.
        void el.offsetWidth;
        if (previousInline) {
            el.style.setProperty('--title-underline-animation-name', previousInline);
        } else {
            el.style.removeProperty('--title-underline-animation-name');
        }
    };

    if (typeof window !== 'undefined') {
        window.PappaliigaTitleUnderlineManager = window.PappaliigaTitleUnderlineManager || new TitleUnderlineManager();
        window.PappaliigaTitleUnderlineManager.init();
    }
})();
