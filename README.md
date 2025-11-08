# Pappaliiga Statistics System

A full-stack web application for tracking and displaying CS2 (Counter-Strike 2) competitive match statistics from FACEIT championships. Built with FastAPI backend, vanilla JavaScript frontend, and MariaDB database.

## Features

- 📊 **Real-time Statistics**: Fetch and display player and team performance metrics
- 🏆 **Championship Tracking**: Monitor multiple divisions and seasons
- 📈 **Performance Analytics**: Detailed breakdowns by map, player, and team
- 🎯 **Match History**: Complete match details with player statistics
- 🔄 **Automatic Sync**: Pipeline to refresh data from FACEIT API
- 🎨 **Modern UI**: Responsive SPA with component-based architecture

## Project Structure

```
Pappaliiga-statsit/
├── api/                      # FastAPI backend application
│   ├── main.py              # Application entry point
│   ├── models.py            # Pydantic data models
│   ├── exceptions.py        # Custom exception classes
│   ├── routers/             # API route handlers
│   ├── services/            # Business logic layer
│   └── utils/               # Utilities (cache, etc.)
├── frontend/                 # Vanilla JavaScript SPA
│   ├── index.html           # Application entry point
│   ├── spa_server.py        # Development server
│   └── static/
│       ├── app-main.js      # Router & initialization
│       ├── api-client.js    # API communication
│       ├── components/      # Reusable UI components
│       ├── composables/     # Shared logic
│       ├── stores/          # State management
│       └── views/           # Page components
├── scripts/                  # Development and utility scripts
├── tools/                    # Database and maintenance tools
├── sync_pipeline.py         # Data synchronization logic
├── faceit_client_async.py   # FACEIT API client
├── db_async.py              # Database connection pool
├── db_ops_async.py          # Database operations
├── mariadb_schema.sql       # Complete database schema
└── requirements.txt         # Python dependencies
```

## Prerequisites

- **Python 3.10+** with pip
- **MariaDB/MySQL** database server
- **FACEIT API credentials** (API key)
- **Windows** (scripts are PowerShell-based)

## Installation

### 1. Clone the Repository

```powershell
git clone https://github.com/tuntematonjr/Pappaliiga-statsit.git
cd Pappaliiga-statsit
```

### 2. Create Virtual Environment

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

### 3. Install Dependencies

```powershell
pip install -r requirements.txt
```

### 4. Configure Database

Create a `.env` file in the project root:

```env
# Database configuration
DB_HOST=localhost
DB_PORT=3306
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=pappaliiga_stats

# FACEIT API
FACEIT_API_KEY=your_faceit_api_key
```

### 5. Initialize Database

```powershell
# Apply the schema
python tools\apply_schema.py

# Verify connection
python tools\check_db_connection.py
```

### 6. Initial Data Sync

Fetch initial data from FACEIT API:

```powershell
python sync.py
```

This will populate the database with championship, match, and player data.

## Development

### Start Development Servers

The easiest way is to use the development script:

```powershell
.\scripts\dev_start.ps1
```

This starts both backend and frontend in separate terminal windows.

**Or manually:**

```powershell
# Terminal 1: Backend API (port 8000)
python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

# Terminal 2: Frontend SPA (port 8001)
python frontend\spa_server.py 8001
```

### Access the Application

- **Frontend**: http://localhost:8001
- **API Docs**: http://localhost:8000/docs
- **API ReDoc**: http://localhost:8000/redoc

## Usage

### Synchronizing Data

Run the sync pipeline to update statistics:

```powershell
python sync.py
```

This fetches the latest matches, player stats, and team information from FACEIT.

### Managing Divisions

Edit `division_overrides.json` to configure division settings and team assignments:

```json
{
  "division_id_here": {
    "teams": {
      "team_id": "active"
    }
  }
}
```

Apply changes:

```powershell
python manage_team_status.py
```

### Database Maintenance

```powershell
# Recompute aggregated statistics
python tools\recompute_totals.py

# Check data integrity
python scripts\check_team_totals.py
python scripts\check_player_sums.py

# Database diagnostics
python scripts\db_diag.py
```

## API Endpoints

### Championships & Divisions
- `GET /api/championships` - List all championships
- `GET /api/divisions` - List divisions with statistics
- `GET /api/divisions/{id}/teams` - Teams in a division

### Teams
- `GET /api/teams/{id}` - Team details and roster
- `GET /api/teams/{id}/matches` - Team match history
- `GET /api/teams/{id}/stats` - Team statistics

### Players
- `GET /api/players/{id}` - Player profile
- `GET /api/players/{id}/stats` - Player statistics
- `GET /api/players/search?q=name` - Search players

### Matches
- `GET /api/matches/{id}` - Match details with player stats
- `GET /api/matches` - Recent matches

### Statistics
- `GET /api/stats/leaders` - Top performers across categories
- `GET /api/stats/teams/{id}` - Team performance metrics

See `/docs` endpoint for complete API documentation.

## Architecture

### Backend
- **Framework**: FastAPI with async/await for non-blocking I/O
- **Database**: asyncmy for async MariaDB connections
- **HTTP**: httpx with retry logic (tenacity) for FACEIT API
- **Validation**: Pydantic models for request/response schemas

### Frontend
- **No frameworks**: Pure vanilla JavaScript ES6+
- **Component-based**: Reusable UI components
- **State management**: Store pattern for reactive data
- **Routing**: History mode SPA routing
- **Styling**: CSS with custom properties for theming

### Database
- **Schema**: Normalized relational design
- **Tables**: championships, divisions, teams, players, matches, maps, stats
- **Aggregations**: Pre-computed totals for performance
- **Indexing**: Optimized for common query patterns

## Development Guidelines

### Python Code Style
- Use async/await for all I/O operations
- Type hints with `from __future__ import annotations`
- Docstrings for modules and complex functions
- Logging instead of print statements
- Context managers for database connections

### JavaScript Code Style
- ES6+ modern syntax
- Async/await for API calls
- Component functions return DOM elements
- Centralized state in stores
- Error handling with user-friendly messages

### Database Operations
- Always use parameterized queries
- Batch operations for bulk inserts
- Connection pooling for performance
- Transaction support for data consistency

## Troubleshooting

### Database Connection Issues
```powershell
# Test connection
python tools\check_db_connection.py

# Check schema
python check_schema.py
```

### API Errors
- Check `.env` file for correct credentials
- Verify FACEIT API key is valid
- Review logs in terminal output

### Frontend Not Loading
- Ensure backend is running on port 8000
- Check CORS settings in `api/main.py`
- Verify `spa_server.py` is serving on correct port

### Data Sync Problems
- Check FACEIT API rate limits
- Verify championship IDs in `faceit_config.py`
- Review `division_overrides.json` for team assignments

## Scripts Reference

### Development
- `scripts\dev_start.ps1` - Start both servers
- `scripts\run_in_venv.ps1` - Run commands in venv

### Database
- `tools\apply_schema.py` - Apply SQL schema
- `tools\check_db_connection.py` - Test database connection
- `tools\recompute_totals.py` - Recalculate aggregates
- `drop_all_tables.py` - Drop all tables (⚠️ destructive)

### Data Validation
- `scripts\check_team_totals.py` - Verify team statistics
- `scripts\check_player_sums.py` - Verify player statistics
- `scripts\check_map_totals.py` - Verify map statistics
- `scripts\db_diag.py` - Database diagnostics

### Sync & Management
- `sync.py` - Main data synchronization
- `manage_team_status.py` - Update team division assignments
- `division_overrides.py` - Division configuration helpers

## Configuration Files

- `.env` - Environment variables (database, API keys)
- `faceit_config.py` - FACEIT API configuration
- `division_overrides.json` - Division and team overrides
- `division_registry.py` - Division registry logic
- `divisions.json` - Division metadata
- `mariadb_schema.sql` - Database schema definition

## Performance Tips

- **Database**: Use indexes, batch operations, connection pooling
- **API**: Enable caching for frequently accessed endpoints
- **Frontend**: Minimize DOM updates, use event delegation
- **Sync**: Schedule during off-peak hours, use incremental updates

## Security

- API keys stored in environment variables only
- Database credentials never committed to git
- CORS configured for known frontend origins
- Input validation via Pydantic models
- Parameterized queries prevent SQL injection

## Contributing

This is a personal project without formal contribution guidelines. If you'd like to suggest improvements:

1. Test changes locally first
2. Ensure database schema compatibility
3. Follow existing code patterns
4. Document significant changes

## License

[Specify your license here]

## Support

For issues or questions, please [open an issue on GitHub](https://github.com/tuntematonjr/Pappaliiga-statsit/issues).

## Acknowledgments

- **FACEIT API** for providing match and player data
- **FastAPI** for the excellent async web framework
- **MariaDB** for robust database storage

---

**Note**: This project is designed for local development and manual deployment. No CI/CD or automated deployment pipelines are used.
