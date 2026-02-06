// Main Vue App with Router
const { createApp } = Vue;
const { createRouter, createWebHistory } = VueRouter;
const { createPinia } = Pinia;

// Create router
const router = createRouter({
    // Use HTML5 history mode (no # in URLs). Ensure your server is
    // configured to return the SPA entrypoint (`index.html`) for
    // unknown paths so refreshing deep-links won't 404. The repo already
    // includes `serve_spa.py` and `api/main.py` provides a fallback.
    history: createWebHistory(),
    routes: [
        {
            path: '/',
            name: 'home',
            component: window.HomeView
        },
        {
            path: '/seasons',
            name: 'seasons',
            component: window.SeasonsView
        },
        {
            path: '/season/current/upcoming',
            name: 'season-upcoming',
            component: window.UpcomingMatchesView
        },
        {
            path: '/division/:championshipId',
            name: 'division',
            component: window.DivisionView
        },
        {
            path: '/division/:championshipId/playoffs',
            name: 'division-playoffs',
            component: window.DivisionView
        },
        {
            path: '/team/:teamId',
            name: 'team',
            component: window.TeamDetailView
        },
        {
            path: '/team/:championshipId/:teamId',
            name: 'team-detail',
            component: window.TeamDetailView
        },
        {
            path: '/player/:playerId',
            name: 'player',
            component: window.PlayerView
        }
    ],
    scrollBehavior(to, from, savedPosition) {
        if (savedPosition) {
            return savedPosition;
        }
        // Keep scroll position when only query params change (e.g., swapping seasons)
        if (to.path === from.path && to.hash === from.hash) {
            return false;
        }
        return { top: 0 };
    }
});

// Create app
const app = createApp({
    name: 'PappaliigaStats',
    components: {
        GlobalNav: window.GlobalNav
    },
    template: `
        <GlobalNav />
        <main class="main-content">
            <div class="layout-boundary main-surface">
                <router-view v-slot="{ Component }">
                    <transition name="fade" mode="out-in">
                        <component :is="Component"></component>
                    </transition>
                </router-view>
            </div>
        </main>
    `
});

// Use plugins
app.use(createPinia());
app.use(router);

// Mount app
app.mount('#app');

console.log('✓ Pappaliiga Stats App initialized');
