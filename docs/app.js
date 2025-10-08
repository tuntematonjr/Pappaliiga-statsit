// Extracted from html_gen.py UNIFIED_HEAD <script>
function sortTable(tableId,n,numeric){
	const table=document.getElementById(tableId);
	const dirAttr=table.getAttribute('data-sort-dir')||'asc';
	const colAttr=table.getAttribute('data-sort-col')||'0';
	const dir=dirAttr==='asc'?1:-1;
	
	// Clear previous sort indicators
	const headers = table.querySelectorAll('th[data-sortable]');
	headers.forEach(th => th.removeAttribute('data-sort-dir'));
	
	let rows=Array.from(table.tBodies[0].rows);
	rows.sort((a,b)=>{
		const x=a.cells[n].textContent.trim(); const y=b.cells[n].textContent.trim();
		if(numeric){
			const nx=parseFloat(x.replace(',','.'))||0; const ny=parseFloat(y.replace(',','.'))||0;
			return (nx-ny)*dir;
		}
		return x.localeCompare(y)*dir;
	});
	table.tBodies[0].append(...rows);
	
	// Set new sort direction and column
	const newDir = dirAttr==='asc'?'desc':'asc';
	table.setAttribute('data-sort-dir', newDir);
	table.setAttribute('data-sort-col', n.toString());
	
	// Add sort indicator to current column
	if(headers[n]) {
		headers[n].setAttribute('data-sort-dir', dirAttr);
	}
}
function applyDefaultSort(tableId) {
	const t = document.getElementById(tableId);
	if (!t) return;
	const col = parseInt(t.getAttribute('data-sort-col') || '0', 10);
	const dir = t.getAttribute('data-sort-dir') || 'asc';
	
	// Set the direction first, then sort once
	t.setAttribute('data-sort-dir', dir === 'asc' ? 'desc' : 'asc');
	sortTable(tableId, col, true);
}
// Helper function for shared bar styling
function createBarElement(td, template) {
	td.innerHTML = template;
	return {
		span: td.querySelector('.bar > span, .win, .loss'),
		val: td.querySelector('.val')
	};
}

function setBarWidth(element, percentage) {
	const width = Math.max(0, Math.min(100, percentage));
	element.style.width = width + '%';
	return width;
}

function getProgressColor(percentage) {
	const g = Math.round(120 * (percentage / 100));
	const r = Math.round(180 * (1 - percentage / 100)) + 60;
	return `rgb(${r},${g + 80},100)`;
}

function getWRColor(percentage) {
	const gcol = Math.round(180 * (percentage / 100));
	const rcol = Math.round(200 * (1 - percentage / 100));
	return `rgb(${rcol},${gcol},100)`;
}

function renderBar(cell, value) {
	const {span, val} = createBarElement(cell, '<div class="bar"><span></span><div class="val"></div></div>');
	val.textContent = (isFinite(value) ? value.toFixed(1) : 0) + '%';
	const width = setBarWidth(span, value);
	span.style.background = getProgressColor(width);
}
function bindPlayedOnly(tableId,chkId){
	const chk=document.getElementById(chkId); const t=document.getElementById(tableId);
	if(!chk||!t) return; const colPlayed=1;
	chk.addEventListener('change',()=>{ for(const tr of t.tBodies[0].rows){ const played=parseInt(tr.cells[colPlayed].textContent||'0',10); tr.style.display=(chk.checked&&!played)?'none':''; }});
}
function colorizeContinuous(tableId, colIdx, p25, p50, p75, inverse = false) {
	// Add small delay to ensure DOM is ready
	setTimeout(() => {
		const t = document.getElementById(tableId);
		if (!t || !t.tBodies.length) return;
		const rows = t.tBodies[0].rows;
		for (let i = 0; i < rows.length; i++) {
			const tr = rows[i];
			const td = tr.cells[colIdx];
			if (!td) continue;
			let v = parseFloat((td.textContent || '').replace(',', '.'));
			if (!isFinite(v)) {
				td.classList.add('cell-muted');
				continue;
			}
			let ratio;
			if (v <= p25) ratio = 0;
			else if (v >= p75) ratio = 1;
			else ratio = (v - p25) / (p75 - p25 || 1);
			if (inverse) ratio = 1 - ratio;
			const r = Math.round(240 * (1 - ratio));
			const g = Math.round(220 * ratio);
			td.style.setProperty('background-color', `rgba(${r},${g},0,0.5)`, 'important');
		}
	}, 10);
}
function processBars(tableId, barColumns) {
	const t = document.getElementById(tableId);
	if (!t || !t.tBodies.length) return;
	const rows = t.tBodies[0].rows;
	
	for (const tr of rows) {
		const played = parseInt(tr.cells[1].textContent || '0', 10);
		barColumns.forEach(i => {
			const num = parseFloat((tr.cells[i].textContent || '').replace(',', '.'));
			renderBar(tr.cells[i], isFinite(num) ? num : 0);
			const span = tr.cells[i].querySelector('.bar > span');
			if (span) span.style.opacity = Math.max(.35, Math.min(1, Math.sqrt(played) / 2));
		});
	}
}

function processWRBars(tableId, wrBarColumns) {
	const t = document.getElementById(tableId);
	if (!t || !t.tBodies.length) return;
	const rows = t.tBodies[0].rows;
	
	for (const tr of rows) {
		wrBarColumns.forEach(i => {
			const td = tr.cells[i];
			if (!td || !td.classList.contains('wr')) return;
			const g = parseInt(td.dataset.g || '0', 10);
			const w = parseInt(td.dataset.w || '0', 10);
			const pctAttr = parseFloat((td.dataset.pct || '').replace(',', '.'));
			const pct = isFinite(pctAttr) ? pctAttr : (g ? (100 * w / g) : 0);
			
			if (!g) {
				if (td.dataset.zero === 'show') {
					td.innerHTML = '<div class="bar-split"><span class="win"></span><span class="loss"></span><div class="val"></div></div>';
					const val = td.querySelector('.val');
					val.textContent = '0–0 (0%)';
					td.querySelector('.win').style.width = '0%';
					td.querySelector('.loss').style.left = '0%';
					td.querySelector('.loss').style.width = '100%';
					td.querySelector('.win').style.background = '#555';
					td.querySelector('.loss').style.background = '#555';
					td.classList.add('cell-muted');
					td.title = 'No attempts';
				} else {
					td.textContent = 'not played';
					td.classList.add('cell-muted');
					td.title = 'No games';
				}
				return;
			}
			renderSplitWR(td, g, pct);
		});
	}
}

function processColorization(tableId, colorOptions) {
	colorOptions.forEach(c => colorizeContinuous(tableId, c.col, c.p[0], c.p[1], c.p[2], c.inverse || false));
}

function processDefaultSort(tableId, sortOptions) {
	sortTable(tableId, sortOptions.col, sortOptions.dir === 'asc');
}

function postProcessTable(tableId, opts) {
	if (opts.bars) processBars(tableId, opts.bars);
	if (opts.wrbars) processWRBars(tableId, opts.wrbars);
	if (opts.color) processColorization(tableId, opts.color);
	if (opts.defaultSort) processDefaultSort(tableId, opts.defaultSort);
}
function initTabsAutoSort(rootId) {
	const root = document.getElementById(rootId);
	if (!root) return;
	const activePanel = root.querySelector('.tab-panel.active');
	if (!activePanel) return;
	const table = activePanel.querySelector('table');
	if (!table) return;
	
	// Set default sort and apply it once
	table.setAttribute('data-sort-col', '0');
	table.setAttribute('data-sort-dir', 'desc');
	applyDefaultSort(table.id);
}
function renderWRCell(td) {
	const w = parseInt(td.dataset.w || '0', 10);
	const g = parseInt(td.dataset.g || '0', 10);
	const l = Math.max(0, g - w);
	const pctAttr = parseFloat((td.dataset.pct || '').replace(',', '.'));
	const pct = isFinite(pctAttr) ? pctAttr : (g ? (100 * w / g) : 0);
	
	const {span, val} = createBarElement(td, '<div class="bar"><span></span><div class="val"></div></div>');
	const wPct = setBarWidth(span, pct);
	val.textContent = `${w}–${l} (${Math.round(pct)}%)`;
	span.style.background = getWRColor(wPct);
	td.title = g ? `Wins: ${w}, Losses: ${l}, WR: ${pct.toFixed(1)}%` : 'No games';
}
function renderSplitWR(td, played, wrPct) {
	const g = Math.max(0, parseInt(played || 0, 10));
	const pct = Math.max(0, Math.min(100, parseFloat((wrPct || 0))));
	const wins = Math.round(g * pct / 100);
	const losses = Math.max(0, g - wins);

	td.innerHTML = '<div class="bar-split"><span class="win"></span><span class="loss"></span><div class="val"></div></div>';
	const win = td.querySelector('.win');
	const loss = td.querySelector('.loss');
	const val = td.querySelector('.val');

	setBarWidth(win, pct);
	loss.style.left = pct + '%';
	setBarWidth(loss, 100 - pct);

	if (td.dataset.mode === 'ratio') {
		val.textContent = g ? `${wins}/${g} (${Math.round(pct)}%)` : '0/0 (0%)';
		td.title = g ? `Successes: ${wins}, Throws: ${g}, Rate: ${pct.toFixed(1)}%` : 'No attempts';
	} else {
		val.textContent = g ? `${wins}–${losses} (${Math.round(pct)}%)` : '0–0 (0%)';
		td.title = g ? `Wins: ${wins}, Losses: ${losses}, WR: ${pct.toFixed(1)}%` : 'No games';
	}
}
// Initialize all app functionality once DOM is ready
document.addEventListener('DOMContentLoaded', () => {
	initTabs();
	initWRTables();
	initPlayedOnlyCheckboxes();
	initPreventCheckboxToggle();
	initCollapseDetails();
	initTeamLinks();
	initSmoothAnimations();
	initSeasonSelector();
});

function initTabs() {
	document.querySelectorAll('.tabs[id]').forEach(root => initTabsAutoSort(root.id));
}

function initWRTables() {
	document.querySelectorAll('table').forEach(table => {
		const wrCells = table.querySelectorAll('td.wr');
		if (wrCells.length && !table.dataset.wrProcessed) {
			wrCells.forEach(td => {
				if (td.dataset.mode === 'ratio') {
					renderSplitWR(td, td.dataset.g, td.dataset.pct);
				} else {
					renderWRCell(td);
				}
			});
			table.dataset.wrProcessed = '1';
		}
	});
}

function initPlayedOnlyCheckboxes() {
	document.querySelectorAll('input[type="checkbox"][id$="-played-only"]').forEach(chk => {
		const tableId = chk.id.replace(/-played-only$/, '');
		bindPlayedOnly(tableId, chk.id);
	});
}
function initPreventCheckboxToggle() {
	// Prevent checkbox clicks inside summary from toggling the <details>
	document.addEventListener('click', function(e){
		const target = e.target;
		if (!target) return;
		if (target.matches('.matches-head input[type="checkbox"], .matches-head label.toggle-played, .matches-head .toggle-played *')) {
			e.stopPropagation();
		}
	}, {capture:true});
}

// Centralized logging function for team section debugging

function initCollapseDetails() {
	// Always keep matches collapsed, regardless of screen size
	document.querySelectorAll('.matches-mirror .match-row').forEach(d => { d.removeAttribute('open'); });
	// Also keep all team sections collapsed
	document.querySelectorAll('.team-section').forEach(d => { 
		d.removeAttribute('open'); 
	});
}

function initTeamLinks() {
	// Listen for clicks on team links
	document.addEventListener('click', function(e) {
		const link = e.target.closest('a[href^="#team-"]');
		if (!link) return;
		
		// Prevent default anchor behavior to control the process
		e.preventDefault();
		
		const teamId = link.getAttribute('href').substring(1); // Remove # from href
		const teamSection = document.getElementById(teamId);
		
		if (teamSection && teamSection.tagName === 'DETAILS') {
			// First scroll to the element
			teamSection.scrollIntoView({ 
				behavior: 'smooth', 
				block: 'start' 
			});
			
			// Then expand it using the custom animation after a small delay
			setTimeout(() => {
				if (!teamSection.classList.contains('custom-expanded')) {
					const summaryEl = teamSection.querySelector('summary');
					if (summaryEl) {
						// Strict: call programmatic toggle only to avoid synthetic events triggering multiple handlers
						if (typeof summaryEl._toggle === 'function') {
							summaryEl._toggle();
						} else {
							// No programmatic toggle available => mark expanded as minimal fallback
							teamSection.classList.add('custom-expanded');
						}
					} else {
						// Fallback: mark expanded to rotate arrow even if summary not found
						teamSection.classList.add('custom-expanded');
					}
				}
			}, 200); // Slightly longer delay to ensure smooth scrolling completes
		}
	});
}

function initSmoothAnimations() {
		document.querySelectorAll('details').forEach(details => {
			const content = details.querySelector('.card-content, .match-details');
			if (!content) return;

			// Always keep content visible and use custom class for state
			// Note: do NOT force the native `open` attribute here — adaptDetails() manages initial collapse.
			if (!details.classList.contains('custom-expanded')) {
				// Start collapsed
				content.style.height = '0px';
				content.style.opacity = '0';
				content.style.overflow = 'hidden';
			}

			// Only animate when summary is clicked
			const summary = details.querySelector('summary');
			if (!summary) return;

			// Prevent multiple listeners from being attached to the same summary
			if (summary.dataset.hasSummaryListener) return;
			summary.dataset.hasSummaryListener = '1';

			// Observer to detect when custom-expanded class is removed unexpectedly
			// Removed MutationObserver log

			// Helper: toggle details with fallback for mobile
			function toggleDetails(e) {
				// Only animate if clicking directly on summary (not on controls inside)
				if (e && e.target !== summary && !summary.contains(e.target)) return;
				if (e && e.target.matches('input, label, a, button, .faceit-link, .toggle-played, .toggle-played *')) {
					return;
				}
				if (e) {
					e.preventDefault();
					e.stopPropagation();
				}

				// Quick guard: avoid double toggles from synthetic events
				if (summary._recentlyToggled) {
					return;
				}
				summary._recentlyToggled = true;
				setTimeout(() => { summary._recentlyToggled = false; }, 300);

				// Always set/remove open attribute for reliable expand/collapse
				if (!details.classList.contains('custom-expanded')) {
					details.open = true;
				} else {
					details.open = false;
				}

				// Logging removed
				if (details.classList.contains('custom-expanded')) {
					// Closing animation
					details.classList.remove('custom-expanded');
					const startHeight = content.scrollHeight;
					content.style.height = startHeight + 'px';
					content.style.overflow = 'hidden';
					requestAnimationFrame(() => {
						// Hide slightly faster than collapse
						content.style.transition = 'height 0.4s cubic-bezier(0.4, 0.0, 0.2, 1), opacity 0.25s ease';
						content.style.height = '0px';
						content.style.opacity = '0';
					});
					setTimeout(() => {
						content.style.transition = '';
					}, 400);
				} else {
					// Opening animation
					details.classList.add('custom-expanded');
					content.style.height = '0px';
					content.style.opacity = '0';
					content.style.overflow = 'hidden';
					// Force reflow to ensure starting state
					void content.offsetHeight;
					const endHeight = content.scrollHeight;
					requestAnimationFrame(() => {
						// Fade a bit faster than expand
						content.style.transition = 'height 0.4s cubic-bezier(0.4, 0.0, 0.2, 1), opacity 0.3s ease';
						content.style.height = endHeight + 'px';
						content.style.opacity = '1';
					});
					setTimeout(() => {
						content.style.height = '';
						content.style.overflow = '';
						content.style.transition = '';
						// Trigger content animations
						const tables = content.querySelectorAll('table');
						const chips = content.querySelectorAll('.chips');
						const mapRows = content.querySelectorAll('.map-row');
						tables.forEach((table, index) => {
							table.style.animation = `fadeIn 0.6s ease-out ${0.1 + index * 0.05}s both`;
						});
						chips.forEach((chip, index) => {
							chip.style.animation = `fadeIn 0.6s ease-out ${0.05 + index * 0.03}s both`;
						});
						mapRows.forEach((row, index) => {
							row.style.animation = `fadeIn 0.6s ease-out ${index * 0.05}s both`;
						});
					}, 400);
				}
			}

			// Expose programmatic toggle to avoid synthetic click events
			try { summary._toggle = () => toggleDetails(null); } catch (err) { /* ignore */ }

			// Touch: only toggle on true tap (not swipe/drag). Mark summary as touch-handled to suppress following click.
			let touchStartY = null, touchStartX = null, touchStartTime = null;
			summary._isTouch = false;
			summary.addEventListener('touchstart', function(e) {
				summary._isTouch = true;
				if (e.touches && e.touches.length === 1) {
					const t = e.touches[0];
					touchStartY = t.clientY;
					touchStartX = t.clientX;
					touchStartTime = Date.now();
				}
			}, {passive: true});
			summary.addEventListener('touchend', function(e) {
				let isTap = true;
				let validBox = true;
				const maxMove = 8;
				const maxTime = 400;
				if (touchStartY !== null && touchStartX !== null && e.changedTouches && e.changedTouches.length === 1) {
					const t = e.changedTouches[0];
					const dy = Math.abs(t.clientY - touchStartY);
					const dx = Math.abs(t.clientX - touchStartX);
					if (dx > maxMove || dy > maxMove) isTap = false;
					if (touchStartTime !== null && (Date.now() - touchStartTime) > maxTime) isTap = false;
					// Check both start and end are inside summary hit box
					const rect = summary.getBoundingClientRect();
					if (!(touchStartX >= rect.left && touchStartX <= rect.right && touchStartY >= rect.top && touchStartY <= rect.bottom)) validBox = false;
					if (!(t.clientX >= rect.left && t.clientX <= rect.right && t.clientY >= rect.top && t.clientY <= rect.bottom)) validBox = false;
				} else {
					isTap = false;
					validBox = false;
				}
				touchStartY = null; touchStartX = null; touchStartTime = null;
				if (isTap && validBox) toggleDetails(e);
				// Suppress click synthesized from this touch for a short duration
				setTimeout(() => { summary._isTouch = false; }, 500);
			}, {passive: false});

			// Click/tap: always use our toggle handler (fixes desktop mouse). Ignore clicks that follow touch interactions.
			summary.addEventListener('click', function(e) {
				if (summary._isTouch) {
					// This click likely came from a recent touch — ignore it because touchend handled toggling
					e.stopPropagation();
					return;
				}
				toggleDetails(e);
			});
		});
}

// Season Selector Functionality
function initSeasonSelector() {
		const seasonTabs = document.querySelectorAll('.season-tab');
		const seasonContents = document.querySelectorAll('.season-content');
		
		if (!seasonTabs.length || !seasonContents.length) return;
		
		// Set up click handlers for season tabs
		seasonTabs.forEach(tab => {
			tab.addEventListener('click', function(e) {
				e.preventDefault();
				const selectedSeason = this.dataset.season;
				switchToSeason(selectedSeason);
			});
		});
		
		// Initialize with current season or first available season
		const currentSeasonTab = document.querySelector('.season-tab.active') || seasonTabs[0];
		if (currentSeasonTab) {
			switchToSeason(currentSeasonTab.dataset.season);
		}
	}
	
	function switchToSeason(seasonId) {
		// Update tab states
		document.querySelectorAll('.season-tab').forEach(tab => {
			tab.classList.remove('active');
		});
		document.querySelector(`[data-season="${seasonId}"]`)?.classList.add('active');
		
		// Update content visibility
		document.querySelectorAll('.season-content').forEach(content => {
			content.classList.remove('active');
		});
		document.querySelector(`[data-season-content="${seasonId}"]`)?.classList.add('active');
		
		// Update division sections visibility
		document.querySelectorAll('.season-divisions').forEach(divisions => {
			divisions.classList.remove('active');
		});
		document.querySelector(`[data-season-divisions="${seasonId}"]`)?.classList.add('active');
		
		// Update main statistics display
		updateMainStats(seasonId);
		
		// Add smooth transition effect for content
		const activeContent = document.querySelector(`[data-season-content="${seasonId}"]`);
		if (activeContent) {
			activeContent.style.opacity = '0';
			activeContent.style.transform = 'translateY(10px)';
			
			setTimeout(() => {
				activeContent.style.transition = 'all 0.3s ease';
				activeContent.style.opacity = '1';
				activeContent.style.transform = 'translateY(0)';
			}, 50);
		}
		
		// Add smooth transition effect for divisions
		const activeDivisions = document.querySelector(`[data-season-divisions="${seasonId}"]`);
		if (activeDivisions) {
			activeDivisions.style.opacity = '0';
			activeDivisions.style.transform = 'translateY(10px)';
			
			setTimeout(() => {
				activeDivisions.style.transition = 'all 0.3s ease';
				activeDivisions.style.opacity = '1';
				activeDivisions.style.transform = 'translateY(0)';
			}, 100);
		}
		
		// Store selected season in localStorage for persistence
		localStorage.setItem('selectedSeason', seasonId);
	}
	
	function updateMainStats(seasonId) {
		// Find season-specific stats from the data
		const seasonContent = document.querySelector(`[data-season-content="${seasonId}"]`);
		if (!seasonContent) return;
		
		// Get stats from the season content data attributes or hidden elements
		const statsData = getSeasonStats(seasonId);
		if (!statsData) return;
		
		// Update the main stats overview section
		updateStatsCards(statsData, seasonId);
	}
	
	function getSeasonStats(seasonId) {
		// Look for season data in data attributes or hidden elements
		const seasonContent = document.querySelector(`[data-season-content="${seasonId}"]`);
		if (!seasonContent) return null;
		
		return {
			divisions: seasonContent.dataset.divisions || '0',
			teams: seasonContent.dataset.teams || '0',
			players: seasonContent.dataset.players || '0',
			matchesPlayed: seasonContent.dataset.matchesPlayed || '0',
			matchesTotal: seasonContent.dataset.matchesTotal || '0',
			mapsPlayed: seasonContent.dataset.mapsPlayed || '0',
			roundsPlayed: seasonContent.dataset.roundsPlayed || '0',
			kills: seasonContent.dataset.kills || '0',
			deaths: seasonContent.dataset.deaths || '0',
			progress: seasonContent.dataset.progress || '0',
			regularProgress: seasonContent.dataset.regularProgress || '0',
			playoffsProgress: seasonContent.dataset.playoffsProgress || '0',
			playoffDivisions: seasonContent.dataset.playoffDivisions || '0',
			playoffsMatchesPlayed: seasonContent.dataset.playoffsMatchesPlayed || '0',
			playoffsMatchesTotal: seasonContent.dataset.playoffsMatchesTotal || '0'
		};
	}
	
	function updateStatsCards(stats, seasonId) {
		// Update dynamic season overview title
		const titleEl = document.getElementById('season-overview-title');
		if (titleEl) {
			titleEl.textContent = `Season ${seasonId} Yleiskatsaus`;
		}
		
		// Update stat values in the dynamic overview
		const overviewDivisions = document.getElementById('overview-divisions');
		const overviewTeams = document.getElementById('overview-teams');
		const overviewPlayers = document.getElementById('overview-players');
		const overviewMatches = document.getElementById('overview-matches');
		const overviewMaps = document.getElementById('overview-maps');
		const overviewRounds = document.getElementById('overview-rounds');
		const overviewKills = document.getElementById('overview-kills');
		const overviewDeaths = document.getElementById('overview-deaths');
		const overviewProgress = document.getElementById('overview-progress');
		const overviewPlayoffs = document.getElementById('overview-playoffs');
		const overviewPlayoffIcon = document.getElementById('overview-playoff-icon');
		
		if (overviewDivisions) overviewDivisions.textContent = stats.divisions;
		if (overviewTeams) overviewTeams.textContent = stats.teams;
		if (overviewPlayers) overviewPlayers.textContent = stats.players;
		if (overviewMatches) overviewMatches.textContent = stats.matchesPlayed;
		if (overviewMaps) overviewMaps.textContent = stats.mapsPlayed;
		if (overviewRounds) overviewRounds.textContent = stats.roundsPlayed;
		if (overviewKills) overviewKills.textContent = stats.kills;
		if (overviewDeaths) overviewDeaths.textContent = stats.deaths;
		if (overviewProgress) {
			const progressPct = parseFloat(stats.progress) || 0;
			overviewProgress.textContent = `${progressPct.toFixed(0)}%`;
		}
		if (overviewPlayoffs) overviewPlayoffs.textContent = stats.playoffDivisions;
		
		// Update playoff icon based on division count (divisions 4+ use different emoji)
		if (overviewPlayoffIcon) {
			const divisionCount = parseInt(stats.divisions) || 0;
			overviewPlayoffIcon.textContent = divisionCount >= 4 ? '🥇' : '🏁';
		}
		
		// Update progress bars if they exist
		updateProgressBars(stats);
	}
	
	function updateProgressBars(stats) {
		// Update regular season progress bar and text
		const regularBar = document.getElementById('overview-regular-bar');
		const regularText = document.getElementById('overview-regular-text');
		const regularPct = parseFloat(stats.regularProgress) || 0;
		if (regularBar) {
			regularBar.style.transition = 'width 0.6s ease, opacity 0.35s ease';
			regularBar.style.width = `${regularPct}%`;
			regularBar.classList.add('progress-glow');
		}
		if (regularText) {
			regularText.textContent = `${stats.matchesPlayed} / ${stats.matchesTotal} ottelua`;
		}

		// Update playoffs progress bar and text
		const playoffsBar = document.getElementById('overview-playoffs-bar');
		const playoffsText = document.getElementById('overview-playoffs-text');
		const playoffsPct = parseFloat(stats.playoffsProgress) || 0;
		if (playoffsBar) {
			playoffsBar.style.transition = 'width 0.6s ease, opacity 0.35s ease';
			playoffsBar.style.width = `${playoffsPct}%`;
			playoffsBar.classList.add('progress-glow');
		}
		if (playoffsText) {
			playoffsText.textContent = `${stats.playoffsMatchesPlayed || 0} / ${stats.playoffsMatchesTotal || 0} ottelua`;
		}
	}
	
	// Restore last selected season from localStorage
	const savedSeason = localStorage.getItem('selectedSeason');
	if (savedSeason && document.querySelector(`[data-season="${savedSeason}"]`)) {
		switchToSeason(savedSeason);
	}
