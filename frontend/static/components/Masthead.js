// Reusable Masthead component used across pages
window.Masthead = {
    name: 'Masthead',
    template: `
        <header class="page-masthead">
            <div class="masthead-inner">
                <a href="https://armafinland.fi/" target="_blank" rel="noopener noreferrer" class="masthead-logo-link masthead-logo-link--afi" aria-label="Armafinland">
                    <img class="masthead-logo masthead-logo--left" src="https://armafinland.fi/logot/images/armafin-logo-200px.png" alt="AFI logo" loading="lazy" />
                </a>
                <h1 class="page-title">AFI - Unofficial Pappaliiga CS Stats</h1>
                <a href="https://pappaliiga.fi/" target="_blank" rel="noopener noreferrer" class="masthead-logo-link masthead-logo-link--pappaliiga" aria-label="Pappaliiga">
                    <img class="masthead-logo masthead-logo--right" src="/static/pappaliiga-logo-white-bg.png" alt="Pappaliiga logo" loading="lazy" />
                </a>
            </div>
        </header>
    `
};
