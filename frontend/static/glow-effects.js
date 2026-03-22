(() => {
    function randomBetween(min, max) {
        return Math.random() * (max - min) + min;
    }

    document.addEventListener('DOMContentLoaded', () => {
        const logoCards = document.querySelectorAll('.logo-card');

        logoCards.forEach(card => {
            const idleDuration = randomBetween(7, 11);
            const hoverDuration = randomBetween(5.5, 7.5);
            const delay = randomBetween(-4, 1);
            const scaleJitter = randomBetween(0.96, 1.05);

            card.style.setProperty('--idle-duration', `${idleDuration.toFixed(2)}s`);
            card.style.setProperty('--hover-duration', `${hoverDuration.toFixed(2)}s`);
            card.style.setProperty('--pulse-delay', `${delay.toFixed(2)}s`);
            card.style.setProperty('--scale-jitter', scaleJitter.toFixed(3));
        });

        const setWidth = wrap => {
            const width = wrap.getBoundingClientRect().width || wrap.clientWidth;
            if (width > 0) {
                wrap.style.setProperty('--logo-width', `${width}px`);
            }
        };

        logoCards.forEach(wrap => setWidth(wrap));

        if (typeof ResizeObserver === 'function') {
            const resizeObserver = new ResizeObserver(entries => {
                entries.forEach(entry => {
                    const target = entry.target;
                    const width =
                        entry.contentBoxSize?.[0]?.inlineSize ||
                        entry.contentBoxSize?.inlineSize ||
                        entry.contentRect?.width ||
                        target.clientWidth;
                    if (width > 0) {
                        target.style.setProperty('--logo-width', `${width}px`);
                    }
                });
            });

            logoCards.forEach(wrap => resizeObserver.observe(wrap));
        } else {
            window.addEventListener('resize', () => {
                logoCards.forEach(wrap => setWidth(wrap));
            });
        }
    });
})();
