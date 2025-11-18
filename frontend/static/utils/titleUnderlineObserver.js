(function () {
    'use strict';

    const SELECTOR = '.titleUnderlineMain, .titleUnderlineCard';
    const SCALE_PRESETS = {
        main: { base: 1.22, min: 1.1, max: 1.32 },
        card: { base: 1.15, min: 1.08, max: 1.22 }
    };
    const RESIZE_SUPPORTED = typeof ResizeObserver !== 'undefined';
    const MUTATION_SUPPORTED = typeof MutationObserver !== 'undefined';

    function TitleUnderlineManager() {
        this._tracked = new WeakSet();
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
        el.style.setProperty('--title-underline-delay', delay + 's');
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
        const preset = SCALE_PRESETS[type];
        if (!preset) {
            return;
        }
        const measure = () => {
            const rect = el.getBoundingClientRect();
            const textWidth = rect.width || el.scrollWidth || el.offsetWidth;
            if (!textWidth) {
                return;
            }
            const containerWidth = el.parentElement
                ? el.parentElement.getBoundingClientRect().width || textWidth
                : textWidth;
            const available = Math.max(containerWidth, textWidth);
            const desiredScale = Math.min(Math.max(preset.base, preset.min), preset.max);
            const targetWidth = textWidth * desiredScale;
            const maxAllowed = Math.max(Math.min(targetWidth, available - 16), textWidth);
            const computed = isFinite(maxAllowed) ? maxAllowed : targetWidth;
            el.style.setProperty('--title-underline-absolute', computed + 'px');
            el.style.setProperty('--title-container-width', available + 'px');
        };
        if (typeof window.requestAnimationFrame === 'function') {
            window.requestAnimationFrame(measure);
        } else {
            measure();
        }
    };

    if (typeof window !== 'undefined') {
        window.PappaliigaTitleUnderlineManager = window.PappaliigaTitleUnderlineManager || new TitleUnderlineManager();
        window.PappaliigaTitleUnderlineManager.init();
    }
})();
