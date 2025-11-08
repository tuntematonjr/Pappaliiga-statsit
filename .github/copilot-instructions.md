# Copilot Instructions for Pappaliiga-statsit

## Project Overview

This is a **Pappaliiga Statistics System** - a full-stack application that fetches, processes, and displays CS2 (Counter-Strike 2) competitive match data from FACEIT API. The system consists of:

1. **Data Sync Pipeline** - Fetches championship, match, and player statistics from FACEIT API
2. **REST API Backend** - FastAPI-based service providing data endpoints
3. **Frontend SPA** - Vanilla JavaScript single-page application with component-based architecture
4. **Database** - MariaDB/MySQL for persistent storage

**Important**: This project does **NOT** use GitHub Actions, deployment automation, or any CI/CD pipelines. All operations are manual and local.

## Architecture

### Backend (Python)
- **API Framework**: FastAPI with async/await patterns
- **Database**: asyncmy for async MariaDB/MySQL connections
- **HTTP Client**: httpx for FACEIT API calls with retry logic (tenacity)
- **Key Files**:
  - `api/main.py` - FastAPI application entry point
  - `sync_pipeline.py` - Main data synchronization logic
  - `faceit_client_async.py` - FACEIT API client
  - `db_async.py` - Database connection pool
  - `db_ops_async.py` - Database operations

### Frontend (Vanilla JavaScript)
- **Architecture**: Component-based SPA with no framework dependencies
- **Router**: History mode routing (handled by `spa_server.py`)
- **State Management**: Store pattern (`stores/` directory)
- **Components**: Reusable UI components in `frontend/static/components/`
- **Key Files**:
  - `frontend/index.html` - Application entry point
  - `frontend/static/app-main.js` - Router and app initialization
  - `frontend/static/api-client.js` - API communication layer
  - `frontend/spa_server.py` - Development server with SPA fallback

### Database Schema
- Located in `mariadb_schema.sql`
- Tables include: championships, divisions, teams, players, matches, maps, match_stats, player_stats, team_stats, etc.
- Uses snapshot timestamps for data versioning

## Development Workflow

### Environment Setup
1. Python virtual environment with dependencies from `requirements.txt`
2. MariaDB/MySQL database (connection via `.env` file)
3. FACEIT API credentials in environment variables

### Starting Development Servers
Use the PowerShell script for convenience:
```powershell
.\scripts\dev_start.ps1
```

Or manually:
```powershell
# Backend (default port 8000)
python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

# Frontend (default port 8001)
python frontend\spa_server.py 8001
```

### Database Operations
- **Apply schema**: `python tools\apply_schema.py`
- **Check connection**: `python tools\check_db_connection.py`
- **Drop tables**: `python drop_all_tables.py` (use with caution)

### Data Synchronization
Run the sync pipeline to fetch latest data from FACEIT:
```powershell
python sync.py
```

## Coding Standards

### Python
- **Async/await**: All database and API calls use async patterns
- **Type hints**: Use `from __future__ import annotations` for forward references
- **Docstrings**: Include module-level docstrings explaining purpose
- **Error handling**: Use custom exceptions from `api/exceptions.py`
- **Logging**: Use Python's logging module, not print statements (except for startup messages)
- **Database**: Always use connection context managers, never leave connections open
- **Import order**: Standard library → Third-party → Local modules

### JavaScript (Frontend)
- **ES6+ syntax**: Use modern JavaScript (const/let, arrow functions, async/await, destructuring)
- **No framework dependencies**: Pure vanilla JS, no React/Vue/Angular
- **Component pattern**: Self-contained components that return DOM elements
- **Store pattern**: Centralized state management with reactivity
- **API calls**: Use `api-client.js` for all backend communication
- **Error handling**: Always handle promise rejections and display user-friendly errors
- **CSS**: Scoped styles in `styles.css`, use CSS custom properties for theming

### File Naming
- Python: snake_case for files and functions
- JavaScript: PascalCase for component files, camelCase for utilities
- Directories: lowercase with hyphens if needed

## Common Tasks

### Adding a New API Endpoint
1. Create route handler in `api/routers/`
2. Add service logic in `api/services/` if needed
3. Update models in `api/models.py` with Pydantic schemas
4. Register router in `api/main.py`
5. Test endpoint manually (no automated testing framework)

### Adding a New Frontend Component
1. Create component file in `frontend/static/components/`
2. Export function that returns DOM element
3. Import in relevant view (`frontend/static/views/`)
4. Add to component examples if reusable (`component-examples.html`)

### Adding a New Database Table
1. Update `mariadb_schema.sql` with new table definition
2. Add corresponding operations in `db_ops_async.py`
3. Apply schema changes: `python tools\apply_schema.py`
4. Update sync pipeline if data source is FACEIT API

### Debugging
- **Check database schema**: `python check_schema.py`
- **Verify data integrity**: Scripts in `scripts/` directory (e.g., `check_team_totals.py`)
- **Inspect API responses**: Check `[API] match_id=*.json` files in project root
- **Database diagnostics**: `python scripts\db_diag.py`

## Project-Specific Context

### Division Management
- Division overrides stored in `division_overrides.json`
- Registry in `division_registry.py` and `divisions.json`
- Team status managed via `manage_team_status.py`

### Data Flow
1. **Fetch**: FACEIT API → `faceit_client_async.py`
2. **Transform**: `sync_pipeline.py` processes raw data
3. **Store**: `db_ops_async.py` writes to MariaDB
4. **Serve**: FastAPI exposes via REST endpoints
5. **Display**: Frontend fetches and renders data

### Key Configuration Files
- `.env` - Database credentials and API keys (not in git)
- `faceit_config.py` - FACEIT API configuration
- `division_overrides.json` - Manual division/team overrides
- `mariadb_schema.sql` - Complete database schema

## Important Constraints

### What NOT to suggest:
- ❌ GitHub Actions or any CI/CD automation
- ❌ Deployment scripts or production configurations
- ❌ Docker/containerization (unless explicitly requested)
- ❌ Testing frameworks (pytest, jest, etc.) - testing is manual
- ❌ Build tools or bundlers for frontend (it's vanilla JS)
- ❌ TypeScript migration
- ❌ Framework migrations (React, Vue, etc.)

### What to prefer:
- ✅ Simple, direct solutions that work locally
- ✅ PowerShell scripts for Windows automation
- ✅ Manual verification and testing approaches
- ✅ Async patterns for I/O operations
- ✅ Clear, documented code over clever abstractions
- ✅ Performance optimizations for database queries
- ✅ Error handling and logging

## Performance Considerations

- Database queries should use proper indexes (see schema)
- Batch operations for bulk inserts/updates
- Cache frequently accessed data where appropriate (`api/utils/cache.py`)
- Use async HTTP calls with connection pooling
- Minimize frontend re-renders by checking state changes

## Security Notes

- API keys and database credentials via environment variables only
- No sensitive data in git repository
- CORS configured in `api/main.py` for frontend access
- Input validation using Pydantic models
- SQL injection prevention via parameterized queries

## When Helping with Code

1. **Understand context first**: Check related files before suggesting changes
2. **Respect existing patterns**: Follow the established architecture and style
3. **Test implications**: Consider database state, API dependencies, and frontend impact
4. **Provide complete solutions**: Include necessary imports, error handling, and documentation
5. **Explain trade-offs**: Mention any performance or maintenance implications
6. **Stay pragmatic**: Simple working code > complex elegant code

## Questions to Ask When Uncertain

- Does this change require database schema modifications?
- Will this affect existing API contracts that the frontend depends on?
- Should this be an async operation?
- Does this need error handling and logging?
- Is there an existing utility or pattern I should reuse?
- What's the impact on existing data?

---

**Remember**: This is a local development project with manual deployment. Focus on code quality, maintainability, and local development experience.
