/**
 * Mobile utilities for Pappaliiga Stats
 * Handles scroll indicators, touch interactions, and responsive behavior
 */

window.MobileUtils = {
    /**
     * Detect if device is mobile/touch
     */
    isMobile() {
        return window.innerWidth <= 768;
    },

    isTouchDevice() {
        return ('ontouchstart' in window) || 
               (navigator.maxTouchPoints > 0) ||
               (navigator.msMaxTouchPoints > 0);
    },

    /**
     * Setup horizontal scroll indicators for table wrappers
     */
    setupScrollIndicators(container = document) {
        const wrappers = container.querySelectorAll('.table-wrapper');
        
        wrappers.forEach(wrapper => {
            // Check if table is scrollable
            const isScrollable = wrapper.scrollWidth > wrapper.clientWidth;
            
            if (isScrollable) {
                wrapper.setAttribute('data-scrollable', 'true');
                
                // Update shadow classes on scroll
                const updateScrollState = () => {
                    const scrollLeft = wrapper.scrollLeft;
                    const maxScroll = wrapper.scrollWidth - wrapper.clientWidth;
                    
                    // Remove scroll hint after first scroll
                    if (scrollLeft > 10) {
                        wrapper.classList.add('scrolled');
                    }
                    
                    // Update left/right shadow indicators
                    if (scrollLeft > 10) {
                        wrapper.classList.add('scrolled-left');
                    } else {
                        wrapper.classList.remove('scrolled-left');
                    }
                    
                    if (scrollLeft < maxScroll - 10) {
                        wrapper.classList.add('scrolled-right');
                    } else {
                        wrapper.classList.remove('scrolled-right');
                    }
                };
                
                // Initial state
                updateScrollState();
                
                // Listen to scroll events (throttled)
                let scrollTimeout;
                wrapper.addEventListener('scroll', () => {
                    if (scrollTimeout) {
                        window.cancelAnimationFrame(scrollTimeout);
                    }
                    scrollTimeout = window.requestAnimationFrame(updateScrollState);
                }, { passive: true });
                
                // Update on resize
                const resizeObserver = new ResizeObserver(updateScrollState);
                resizeObserver.observe(wrapper);
            } else {
                wrapper.removeAttribute('data-scrollable');
            }
        });
    },

    /**
     * Get visible columns based on screen size
     * Used by Vue components to filter columns
     */
    getVisibleColumns(allColumns, priority = 'default') {
        const width = window.innerWidth;
        
        if (width >= 1025) {
            // Desktop: show all
            return allColumns;
        } else if (width >= 641) {
            // Tablet: show important columns
            const importantGroups = ['map', 'combat', 'kills', 'rounds', 'usage', 'results', 'performance'];
            return allColumns.filter(col => 
                importantGroups.includes(col.group) || 
                col.key === 'mapName' ||
                col.key === 'nickname'
            );
        } else if (width >= 481) {
            // Small mobile: show essential columns
            const essentialKeys = ['mapName', 'nickname', 'kd', 'adr', 'kills', 'deaths', 'played', 'winrate'];
            return allColumns.filter(col => essentialKeys.includes(col.key));
        } else {
            // Extra small: minimal columns
            const minimalKeys = ['mapName', 'nickname', 'kd', 'adr', 'kills'];
            return allColumns.filter(col => minimalKeys.includes(col.key));
        }
    },

    /**
     * Setup responsive table behavior
     */
    makeTableResponsive(tableElement) {
        if (!tableElement) return;
        
        const wrapper = tableElement.closest('.table-wrapper');
        if (!wrapper) return;
        
        // Add data attributes to cells for mobile card view (future enhancement)
        const headers = Array.from(tableElement.querySelectorAll('thead th'));
        const rows = tableElement.querySelectorAll('tbody tr');
        
        rows.forEach(row => {
            const cells = row.querySelectorAll('td');
            cells.forEach((cell, index) => {
                if (headers[index]) {
                    const label = headers[index].textContent.trim();
                    cell.setAttribute('data-label', label);
                }
            });
        });
    },

    /**
     * Lazy load images with Intersection Observer
     */
    setupLazyLoading(container = document) {
        if (!('IntersectionObserver' in window)) {
            // Fallback: load all images immediately
            const images = container.querySelectorAll('img[data-src]');
            images.forEach(img => {
                img.src = img.dataset.src;
                img.removeAttribute('data-src');
            });
            return;
        }

        const imageObserver = new IntersectionObserver((entries, observer) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    const img = entry.target;
                    if (img.dataset.src) {
                        img.src = img.dataset.src;
                        img.removeAttribute('data-src');
                    }
                    observer.unobserve(img);
                }
            });
        }, {
            rootMargin: '50px 0px',
            threshold: 0.01
        });

        const images = container.querySelectorAll('img[data-src]');
        images.forEach(img => imageObserver.observe(img));
    },

    /**
     * Add touch feedback to interactive elements
     */
    enhanceTouchFeedback(container = document) {
        const interactiveElements = container.querySelectorAll(
            '.clickable-row, .table-row, .btn-primary, .btn-secondary, .team-tab'
        );

        interactiveElements.forEach(element => {
            element.addEventListener('touchstart', function() {
                this.classList.add('touch-active');
            }, { passive: true });

            element.addEventListener('touchend', function() {
                this.classList.remove('touch-active');
            }, { passive: true });

            element.addEventListener('touchcancel', function() {
                this.classList.remove('touch-active');
            }, { passive: true });
        });
    },

    /**
     * Initialize all mobile utilities
     */
    init(container = document) {
        this.setupScrollIndicators(container);
        this.setupLazyLoading(container);
        
        if (this.isTouchDevice()) {
            this.enhanceTouchFeedback(container);
        }

        // Re-initialize on dynamic content changes
        const observer = new MutationObserver((mutations) => {
            let shouldReinit = false;
            mutations.forEach(mutation => {
                if (mutation.addedNodes.length > 0) {
                    shouldReinit = true;
                }
            });
            
            if (shouldReinit) {
                // Debounce reinit
                clearTimeout(this._reinitTimeout);
                this._reinitTimeout = setTimeout(() => {
                    this.setupScrollIndicators(container);
                }, 100);
            }
        });

        observer.observe(container, {
            childList: true,
            subtree: true
        });
    },

    /**
     * Get responsive grid columns count
     */
    getGridColumns(defaultCols) {
        const width = window.innerWidth;
        if (width < 481) return 1;
        if (width < 641) return Math.min(defaultCols, 2);
        if (width < 1025) return Math.min(defaultCols, 3);
        return defaultCols;
    }
};

// Auto-initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        window.MobileUtils.init();
    });
} else {
    window.MobileUtils.init();
}
