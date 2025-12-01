Here's the complete updated README.md for your Flag Dispensations Alert System:

```markdown
# Flag Dispensations Alert System

A modular, production-ready alert system for monitoring flag extension and dispensation jobs and sending automated email notifications. Built with a plugin-based architecture that makes it easy to create new alert types by copying and customizing the project.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Testing](#testing)
- [Creating New Alert Projects](#creating-new-alert-projects)
- [Docker Deployment](#docker-deployment)
- [Development](#development)
- [Troubleshooting](#troubleshooting)
- [Project Structure](#project-structure)

---

## 🎯 Overview

This system monitors a PostgreSQL database for flag extension and dispensation jobs requiring approval and sends automated email notifications to vessel-specific recipients with company-specific CC lists. The modular architecture allows you to easily create new alert types (hot works, certifications, surveys, etc.) by copying this project and customizing the alert logic.

**Current Alert Type**: Flag Dispensations
- Monitors `job_entities` table for flag-extension-dispensation records (type='flag-extension-dispensation') in 'for_approval' status
- Tracks jobs created in the last 24 hours (configurable)
- Sends individual emails to each vessel with clickable links to view full job details
- Automatically determines CC recipients based on vessel email domain
- Tracks sent notifications to prevent duplicates
- Optional reminder system after configurable days

---

## 🏗️ Architecture

### Core Components
```
┌─────────────────────────────────────────────────────────────┐
│                         main.py                             │
│                      (Entry Point)                          │
└──────────────────────┬──────────────────────────────────────┘
                       │
          ┌────────────┴────────────┐
          │                         │
    ┌─────▼──────┐           ┌──────▼──────┐
    │ AlertConfig│           │  Scheduler  │
    │            │           │             │
    └─────┬──────┘           └─────┬───────┘
          │                        │
          │               ┌────────┴──────────┐
          │               │                   │
    ┌─────▼──────┐  ┌─────▼──────┐    ┌───────▼──────┐
    │  Tracker   │  │ BaseAlert  │    │    Alert     │
    │            │  │ (Abstract) │    │  Subclass    │
    └────────────┘  └─────┬──────┘    └──────┬───────┘
                          │                  │
                    ┌─────┴────────┬─────────┴───────┐
                    │              │                 │
            ┌───────▼────┐  ┌──────▼──────┐   ┌──────▼──────┐
            │EmailSender │  │ Formatters  │   │  db_utils   │
            └────────────┘  └─────────────┘   └─────────────┘
```

### Module Breakdown

| Module | Purpose | Reusable? |
|--------|---------|-----------|
| **src/core/** | Core infrastructure (config, tracking, scheduling, base alert class) | ✅ Yes - shared across all alerts |
| **src/notifications/** | Email and Teams notification handlers | ✅ Yes - shared across all alerts |
| **src/formatters/** | HTML and plain text email templates | ✅ Yes - shared across all alerts |
| **src/utils/** | Validation, image loading utilities | ✅ Yes - shared across all alerts |
| **src/alerts/** | Alert-specific implementations | ❌ No - customized per alert type |
| **queries/** | SQL query files | ❌ No - customized per alert type |

---

## ✨ Features

### Current Features
- ✅ **Modular Architecture**: Plugin-based design for easy extensibility
- ✅ **Email Notifications**: Rich HTML emails with company logos and responsive design
- ✅ **Clickable Job Links**: Direct links from emails to job details in your application
- ✅ **Smart Routing**: Automatic CC list selection based on email domain
- ✅ **Duplicate Prevention**: Tracks sent jobs to avoid re-sending notifications
- ✅ **Optional Reminders**: Re-send alerts after configurable days (or never)
- ✅ **Timezone Aware**: All datetime operations respect configured timezone
- ✅ **Dry-Run Mode**: Test without sending emails (redirects to test addresses)
- ✅ **Command-Line Overrides**: `--dry-run` flag overrides `.env` settings
- ✅ **Graceful Shutdown**: SIGTERM/SIGINT handlers for clean termination
- ✅ **Error Recovery**: Continues running after transient failures
- ✅ **Docker Support**: Fully containerized with docker-compose
- ✅ **SSH Tunnel Support**: Secure remote database access
- ✅ **Atomic File Operations**: Prevents data corruption on interruption
- ✅ **Configurable Scheduling**: Run on any frequency (hourly, every 30 minutes, daily, etc.)
- ✅ **Comprehensive Logging**: Rotating logs with detailed execution traces
- ✅ **Responsive Email Design**: Adapts to desktop, tablet, and mobile screens

### Future Features (Planned)
- 🔜 **Microsoft Teams Integration**: Send notifications to Teams channels
- 🔜 **Slack Integration**: Send notifications to Slack channels
- 🔜 **Multiple Alert Types**: Hot works, certifications, surveys, etc.
- 🔜 **Comprehensive Tests**: Update test suite for flag dispensations

---

## 📋 Prerequisites

### Required Software
- **Python 3.13+**
- **Docker & Docker Compose** (recommended for deployment)
- **PostgreSQL** database (remote or local)
- **SSH key** (if using SSH tunnel to database)

### Required Python Packages

See `requirements.txt` for exact versions. Key dependencies:

**Core Dependencies**:
- `python-decouple==3.8` - Environment variable management
- `pandas==2.3.3` - Data manipulation and analysis
- `sqlalchemy==2.0.44` - Database ORM and connection pooling
- `psycopg2-binary==2.9.11` - PostgreSQL adapter
- `sshtunnel>=0.4.0,<1.0.0` - SSH tunnel for remote database access
- `paramiko>=2.12.0,<4.0.0` - SSH protocol implementation (required by sshtunnel)
- `pymsteams==0.2.5` - Microsoft Teams webhook integration *(planned)*

**Testing Dependencies**:
- `pytest==7.4.3` - Testing framework
- `pytest-cov==4.1.0` - Coverage reporting
- `pytest-mock==3.12.0` - Mocking utilities
- `freezegun==1.4.0` - Time/date mocking for tests

**Install all dependencies**:
```bash
pip install -r requirements.txt
```

**Install only production dependencies** (exclude testing):
```bash
grep -v "^#\|pytest\|freezegun" requirements.txt | pip install -r /dev/stdin
```

### Required Accounts/Access
- SMTP server credentials (e.g., Gmail, Office365)
- PostgreSQL database credentials
- SSH access to database server (if using SSH tunnel)

---

## 🚀 Installation

### Docker Deployment (Recommended)

1. **Clone or copy the project**:
```bash
   cd ~/Dev
   git clone <repository> flag-dispensations-alerts
   cd flag-dispensations-alerts
```

2. **Create `.env` file**:
```bash
   cp .env.example .env
   vi .env  # Edit with your settings
```

3. **Build and run**:
```bash
   export UID=$(id -u) GID=$(id -g)
   docker-compose build
   docker-compose up -d
```

4. **Fix directory permissions** (important for Linux servers):
```bash
   # Ensure the container can write to logs and data directories
   # Use your user's UID:GID (check with: id -u and id -g)
   sudo chown -R $(id -u):$(id -g) logs/ data/
   
   # Or if deploying on a server where you know the UID/GID:
   sudo chown -R 1000:1000 logs/ data/
   
   # Alternative (less secure but works):
   chmod -R 777 logs/ data/
```

**Note**: This step is especially important when:
- Deploying to a remote Linux server
- Using this project as a template for a new alert
- The directories were created by a different user (e.g., root)

5. **Verify it's running**:
```bash
   docker-compose logs -f alerts
```

### Local Development Setup

1. **Clone or copy the project**:
```bash
   cd ~/Dev
   git clone <repository> flag-dispensations-alerts
   cd flag-dispensations-alerts
```

2. **Create virtual environment**:
```bash
   python3.13 -m venv venv
   source venv/bin/activate  # On macOS/Linux
   # or
   venv\Scripts\activate  # On Windows
```

3. **Install dependencies**:
```bash
   pip install -r requirements.txt
```

4. **Create `.env` file**:
```bash
   cp .env.example .env
   vi .env  # Edit with your settings
```

5. **Test the configuration**:
```bash
   python -m src.main --dry-run --run-once
```

---

## ⚙️ Configuration

### Environment Variables (`.env`)

Create a `.env` file in the project root with the following variables:
```bash
# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================
DB_HOST=your.database.host.com
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_user
DB_PASS=your_password

# SSH Tunnel (set USE_SSH_TUNNEL=True if database requires SSH tunnel)
USE_SSH_TUNNEL=True
SSH_HOST=your.ssh.host.com
SSH_PORT=22
SSH_USER=your_ssh_user
SSH_KEY_PATH=/app/ssh_ubuntu_key

# ============================================================================
# EMAIL CONFIGURATION
# ============================================================================
SMTP_HOST=smtp.gmail.com
SMTP_PORT=465
SMTP_USER=alerts@yourcompany.com
SMTP_PASS=your_app_password

# Internal recipients (always receive all notifications)
INTERNAL_RECIPIENTS=admin@company.com,manager@company.com

# Company-specific CC recipients (applied based on vessel email domain)
PROMINENCE_EMAIL_CC_RECIPIENTS=user1@prominencemaritime.com,user2@prominencemaritime.com
SEATRADERS_EMAIL_CC_RECIPIENTS=user1@seatraders.com,user2@seatraders.com

# ============================================================================
# DRY-RUN / TESTING CONFIGURATION
# ============================================================================
# Set DRY_RUN=True to redirect ALL emails to test addresses (no real emails sent)
# Command-line flag --dry-run overrides this setting
DRY_RUN=False

# When DRY_RUN=True, all emails are redirected to these addresses (comma-separated)
DRY_RUN_EMAIL=test1@company.com,test2@company.com

# ============================================================================
# FEATURE FLAGS
# ============================================================================
ENABLE_EMAIL_ALERTS=True
ENABLE_TEAMS_ALERTS=False
ENABLE_SPECIAL_TEAMS_EMAIL_ALERT=False

# ============================================================================
# CLICKABLE LINKS CONFIGURATION
# ============================================================================
# Enable clickable links in emails (title becomes clickable)
ENABLE_LINKS=True

# Base URL for your application (e.g., https://prominence.orca.tools)
BASE_URL=https://prominence.orca.tools

# URL path to flag dispensations page (e.g., /jobs/flag-extension-dispensation)
# Full URL will be: {BASE_URL}{URL_PATH}/{job_id}
# Example: https://prominence.orca.tools/jobs/flag-extension-dispensation/12345
URL_PATH=/jobs/flag-extension-dispensation

# ============================================================================
# COMPANY BRANDING
# ============================================================================
PROMINENCE_LOGO=trans_logo_prominence_procreate_small.png
SEATRADERS_LOGO=trans_logo_seatraders_procreate_small.png

# ============================================================================
# SCHEDULING & TRACKING
# ============================================================================
# How often to check for new alerts (in hours)
# Examples: 0.5 = every 30 minutes, 1 = hourly, 24 = daily, 168 = weekly
SCHEDULE_FREQUENCY_HOURS=1.0

# Timezone for all datetime operations
TIMEZONE=Europe/Athens

# Reminder frequency (in days)
# - Set to a number (e.g., 30) to re-send alerts after X days
# - Leave blank or empty to NEVER re-send (track forever, no reminders)
REMINDER_FREQUENCY_DAYS=

# File where sent jobs are tracked (relative to project root)
SENT_EVENTS_FILE=sent_alerts.json

# ============================================================================
# ALERT-SPECIFIC CONFIGURATION
# ============================================================================
# How many days back to look for flag dispensation jobs
# Jobs created within this window will be included
LOOKBACK_DAYS=1

# Job status to filter for (typically 'for_approval')
JOB_STATUS=for_approval

# ============================================================================
# LOGGING
# ============================================================================
LOG_FILE=alerts.log
LOG_MAX_BYTES=10485760
LOG_BACKUP_COUNT=5
```

### Configuration Notes

**SSH Tunnel**:
- Set `USE_SSH_TUNNEL=True` if your database is only accessible via SSH
- `SSH_KEY_PATH` should point to your private SSH key file
- In Docker, mount your SSH key as read-only: `~/.ssh/your_key:/app/ssh_ubuntu_key:ro`

**DRY_RUN Mode**:
- `DRY_RUN=True` in `.env` → All emails redirected to `DRY_RUN_EMAIL` addresses
- `--dry-run` command-line flag → Overrides `.env`, enables dry-run mode
- **Three-layer safety**: Even with `DRY_RUN=False`, code checks prevent accidental sends

**REMINDER_FREQUENCY_DAYS**:
- **Empty/blank** → Never re-send notifications (track jobs forever)
- **Number** (e.g., `30`) → Re-send notifications after X days
- Jobs older than X days are removed from tracking file

**Clickable Links Configuration**:
- **ENABLE_LINKS=True** → Job titles in emails become clickable links
- **BASE_URL** → Your application's base URL (e.g., `https://prominence.orca.tools`)
- **URL_PATH** → Path to flag dispensations page (e.g., `/jobs/flag-extension-dispensation`)
- **Result**: Links like `https://prominence.orca.tools/jobs/flag-extension-dispensation/12345` where `12345` is the job_id
- **When disabled**: Titles appear as plain text (no links)

**Email Routing**:
- System extracts domain from vessel email (e.g., `vessel@vsl.prominencemaritime.com` → `prominencemaritime.com`)
- Matches domain to CC list (e.g., `PROMINENCE_EMAIL_CC_RECIPIENTS`)
- Falls back to `INTERNAL_RECIPIENTS` if no match found

**Flag Dispensations Specific**:
- **LOOKBACK_DAYS**: Set to `1` to check jobs created in the last 24 hours
- **JOB_STATUS**: Set to `for_approval` to only alert on jobs requiring approval
- Monitors `job_entities` table where `type = 'flag-extension-dispensation'`

---

## 🎮 Usage

### Command Line Options
```bash
# Dry-run mode (redirects emails to DRY_RUN_EMAIL addresses)
python -m src.main --dry-run --run-once

# Run once and exit (sends real emails based on .env DRY_RUN setting)
python -m src.main --run-once

# Run continuously with scheduling (production mode)
python -m src.main

# Docker equivalent commands
docker-compose run --rm alerts python -m src.main --dry-run --run-once
docker-compose run --rm alerts python -m src.main --run-once
docker-compose up -d  # Runs continuously
```

### Command-Line Flags

| Flag | Effect | Overrides .env? |
|------|--------|-----------------|
| `--dry-run` | Redirects all emails to `DRY_RUN_EMAIL` | Yes - forces dry-run ON |
| `--run-once` | Executes once and exits (no scheduling) | No |
| (none) | Runs continuously on schedule | No |

### Expected Output (Dry-Run)
```
======================================================================
▶ ALERT SYSTEM STARTING
======================================================================
[OK] Configuration validation passed
======================================================================
🔒 DRY RUN MODE ACTIVATED - EMAILS REDIRECTED TO: test@company.com
======================================================================
[OK] Event tracker initialized
[OK] Email sender initialized (DRY-RUN MODE - emails redirected)
[OK] Formatters initialized
[OK] Registered FlagDispensationsAlert
============================================================
▶ RUN-ONCE MODE: Executing alerts once without scheduling
============================================================
Running 1 alert(s)...
Executing alert 1/1...
============================================================
▶ FlagDispensationsAlert RUN STARTED
============================================================
--> Fetching data from database...
[OK] Fetched 45 record(s)
--> Applying filtering logic...
[OK] Filtered to 8 entries synced in last 1 day(s)
--> Checking for previously sent notifications...
[OK] 8 new record(s) to notify
--> Routing notifications to recipients...
[OK] Created notification job for vessel 'KNOSSOS' (2 job(s))
[OK] Created notification job for vessel 'MINI' (5 job(s))
[OK] Created notification job for vessel 'NONDAS' (1 job(s))
[OK] Created 3 notification job(s)
--> Sending notification 1/3...
[DRY-RUN-EMAIL] Redirecting to: test@company.com
[DRY-RUN-EMAIL] Original recipient: knossos@vsl.prominencemaritime.com
[DRY-RUN-EMAIL] Original CC: user1@prominencemaritime.com, user2@prominencemaritime.com
[DRY-RUN-EMAIL] Subject: AlertDev | KNOSSOS Flag Extensions-Dispensations
[OK] Sent notification 1/3
...
[OK] Marked 8 job(s) as sent
◼ FlagDispensationsAlert RUN COMPLETE
```

### Production Output
```
======================================================================
▶ ALERT SYSTEM STARTING
======================================================================
[OK] Configuration validation passed
[OK] Event tracker initialized
[OK] Email sender initialized
[OK] Formatters initialized
[OK] Registered FlagDispensationsAlert
============================================================
▶ SCHEDULER STARTED
Frequency: Every 1h
Timezone: Europe/Athens
Registered alerts: 1
============================================================
[OK] Next run at: 2025-12-01 14:00:00 EET
Running 1 alert(s)...
...
[OK] Sent notification to knossos@vsl.prominencemaritime.com
[OK] CC: user1@prominencemaritime.com, user2@prominencemaritime.com
[OK] Marked 8 job(s) as sent
◼ FlagDispensationsAlert RUN COMPLETE
[OK] Sleeping for 1h
[OK] Next run scheduled at: 2025-12-01 15:00:00 EET
```

---

## 🧪 Testing

⚠️ **Note**: The test suite has not yet been updated for the Flag Dispensations alert. The current tests reference the old Passage Plan alert implementation.

### Running Tests

**Local (requires pytest installed)**:
```bash
# Run all tests (will have failures due to outdated tests)
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=term --cov-report=html

# Run specific test file
pytest tests/test_config.py -v

# Run specific test
pytest tests/test_tracking.py::test_tracker_marks_events_as_sent -v
```

**Docker (recommended)**:
```bash
# Run all tests
docker-compose run --rm alerts pytest tests/ -v

# Run with coverage
docker-compose run --rm alerts pytest tests/ --cov=src --cov-report=term

# Interactive shell (run multiple test commands)
docker-compose run --rm alerts bash
> pytest tests/ -v
> pytest tests/test_integration.py -v
> exit
```

### Test Status

**Tests need updating** - The following test files reference the old alert:
- `tests/test_passage_plan_alert.py` - Needs renaming and updating for flag dispensations
- `tests/test_integration.py` - May need updates for new data structure
- Other test files should work as-is (they test core infrastructure)

### Test Structure
```
tests/
├── conftest.py                    # Shared fixtures and test configuration
├── test_config.py                 # Configuration loading and validation ✅
├── test_tracking.py               # Event tracking and duplicate prevention ✅
├── test_passage_plan_alert.py     # ⚠️ NEEDS UPDATE for flag dispensations
├── test_formatters.py             # Email HTML/text generation ✅
├── test_email_sender.py           # Email sending functionality ✅
├── test_scheduler.py              # Scheduling and execution ✅
└── test_integration.py            # ⚠️ MAY NEED UPDATE for new workflow
```

### Writing Tests for Flag Dispensations

To update the test suite, you'll need to:

1. **Rename test file**:
```bash
mv tests/test_passage_plan_alert.py tests/test_flag_dispensations_alert.py
```

2. **Update imports and test data**:
```python
# tests/test_flag_dispensations_alert.py
import pytest
from src.alerts.flag_dispensations_alert import FlagDispensationsAlert


@pytest.fixture
def sample_dataframe():
    """Sample flag dispensations data."""
    return pd.DataFrame({
        'vsl_email': ['vessel@prominencemaritime.com'],
        'vessel_id': [123],
        'vessel': ['KNOSSOS'],
        'job_id': [456],
        'importance': ['High'],
        'title': ['Flag Extension Request'],
        'dispensation_type': ['Extension'],
        'department': ['Deck'],
        'due_date': ['2025-12-15'],
        'requested_on': ['2025-12-01'],
        'created_at': ['2025-12-01 10:00:00'],
        'status': ['for_approval']
    })


def test_alert_initializes_correctly(mock_config):
    """Test that alert initializes with correct configuration."""
    alert = FlagDispensationsAlert(mock_config)
    assert alert.sql_query_file == 'FlagDispensations.sql'
    assert alert.lookback_days == 1
    assert alert.job_status == 'for_approval'


def test_alert_filters_data_correctly(mock_config, sample_dataframe):
    """Test filtering logic."""
    alert = FlagDispensationsAlert(mock_config)
    filtered = alert.filter_data(sample_dataframe)
    assert len(filtered) > 0
    assert 'created_at' in filtered.columns
```

---

## 🔄 Creating New Alert Projects

The modular design makes it easy to create new alert types. **Recommended approach**: Copy entire project to new directory (one alert per container).

### Step-by-Step Guide

#### 1. Copy the Project
```bash
cd ~/Dev
cp -r flag-dispensations-alerts hot-works-alerts
cd hot-works-alerts
```

#### 2. Clean Up Old Data
```bash
rm -rf data/*.json logs/*.log
rm -rf .git  # Optional: start fresh git history
git init

# Fix directory permissions for Docker
sudo chown -R $(id -u):$(id -g) logs/ data/
```

**Important**: When copying projects between machines or deploying to servers, always fix directory permissions to match the user that will run Docker. This prevents `PermissionError` on startup.

#### 3. Update Configuration

**Edit `.env`**:
```bash
vi .env
```

Key changes for new alert type:
```bash
# Change schedule (e.g., every 2 hours for hot works)
SCHEDULE_FREQUENCY_HOURS=2.0

# Change reminder frequency (e.g., weekly reminders)
REMINDER_FREQUENCY_DAYS=7

# Update recipients for this alert type
INTERNAL_RECIPIENTS=hotworks-admin@company.com

# Update lookback period
LOOKBACK_DAYS=7  # Look back 7 days instead of 1

# Update job status filter if needed
JOB_STATUS=pending_review

# Update links (if using different URL path)
URL_PATH=/hot-works
```

#### 4. Update Docker Configuration

**Edit `docker-compose.yml`**:
```yaml
services:
  alerts:
    build:
      context: .
      args:
        UID: ${UID}
        GID: ${GID}
    container_name: hot-works-alerts-app  # ← CHANGE THIS
    env_file:
      - .env
    volumes:
      - ./logs:/app/logs
      - ./data:/app/data
      - ./queries:/app/queries
      - ~/.ssh/your_key:/app/ssh_key:ro
    restart: unless-stopped
```

#### 5. Create SQL Query
```bash
rm queries/FlagDispensations.sql
vi queries/HotWorkPermits.sql
```

**Example query**:
```sql
SELECT 
    v.email AS vsl_email,
    jv.vessel_id AS vessel_id,
    v.name AS vessel,
    jv.job_id as job_id,
    ji.name as importance,
    je.title AS title,
    hwt.name as work_type,
    d.name AS department,
    je.due_date AS due_date,
    je.created_at AS created_at,
    js.name AS status
FROM 
    job_entities je 
LEFT JOIN job_importances ji ON ji.id = je.importance_id
LEFT JOIN departments d ON d.id = je.main_department_id
LEFT JOIN job_statuses js ON js.id = je.status_id
LEFT JOIN hot_work_types hwt ON hwt.id = je.work_type_id
LEFT JOIN job_vessels jv ON jv.job_id = je.id
LEFT JOIN vessels v ON v.id = jv.vessel_id
WHERE
    je.type = 'hot-work-permit'
    AND je.deleted_at IS NULL
    AND je.archived_at IS NULL
    AND v.active = 'true'
    AND je.created_at >= NOW() - INTERVAL '1 day' * :lookback_days
    AND js.label = :job_status;
```

#### 6. Create Alert Implementation

Follow the same pattern as `flag_dispensations_alert.py`, updating:
- Class name
- SQL query file name
- Column names in `get_required_columns()`
- Display columns in `route_notifications()`
- Subject line in `get_subject_line()`
- Tracking key format in `get_tracking_key()`

#### 7. Update Module Imports

**Edit `src/alerts/__init__.py`**:
```python
"""Alert implementations."""
from .hot_works_alert import HotWorksAlert  # ← CHANGE THIS

__all__ = ['HotWorksAlert']  # ← CHANGE THIS
```

#### 8. Register the Alert

**Edit `src/main.py`**:
```python
def register_alerts(scheduler: AlertScheduler, config: AlertConfig) -> None:
    """Register all alert implementations with the scheduler."""
    logger = logging.getLogger(__name__)
    
    # Register Hot Works Alert
    from src.alerts.hot_works_alert import HotWorksAlert  # ← CHANGE THIS
    hot_works_alert = HotWorksAlert(config)  # ← CHANGE THIS
    scheduler.register_alert(hot_works_alert.run)
    logger.info("[OK] Registered HotWorksAlert")  # ← CHANGE THIS
```

#### 9. Test the New Alert
```bash
# Test locally (if you have Python setup)
python -m src.main --dry-run --run-once

# Test in Docker
export UID=$(id -u) GID=$(id -g)
docker-compose build --no-cache  # Use --no-cache to avoid module caching issues
docker-compose run --rm alerts python -m src.main --dry-run --run-once
```

**Important**: When creating a new alert project from a template, always use `--no-cache` for the first build to avoid Python module caching issues from the old project.

#### 10. Deploy to Production
```bash
# Start container
docker-compose up -d

# Monitor logs
docker-compose logs -f alerts

# Check status
docker-compose ps

# View tracking file
docker-compose exec alerts cat data/sent_alerts.json | jq '.'
```

---

## 🐳 Docker Deployment

### Building the Container
```bash
# Set user/group IDs for proper file permissions
export UID=$(id -u) GID=$(id -g)

# Build the image
docker-compose build
```

### Running in Production
```bash
# Start in detached mode (background)
docker-compose up -d

# View logs (follow mode)
docker-compose logs -f alerts

# View last 100 lines
docker-compose logs --tail=100 alerts

# Stop the container
docker-compose down

# Restart after config changes
docker-compose restart alerts

# View container status
docker-compose ps
```

### Running Tests in Docker
```bash
# Run all tests (note: some tests need updating)
docker-compose run --rm alerts pytest tests/ -v

# Run with coverage
docker-compose run --rm alerts pytest tests/ --cov=src --cov-report=term

# Interactive shell
docker-compose run --rm alerts bash
```

### Docker Configuration

**`docker-compose.yml`**:
```yaml
services:
  alerts:
    build:
      context: .
      args:
        UID: ${UID:-1000}
        GID: ${GID:-1000}
    container_name: flag-dispensations-app
    env_file:
      - .env
    environment:
      SSH_KEY_PATH: /app/ssh_ubuntu_key
    volumes:
      - ./logs:/app/logs          # Logs persist on host
      - ./data:/app/data          # Tracking data persists on host
      - ./queries:/app/queries    # Mount queries for easy updates
      - ~/.ssh/your_key:/app/ssh_key:ro  # SSH key (read-only)
    restart: unless-stopped        # Auto-restart on failure
```

### Health Monitoring

The Docker container includes a healthcheck that verifies:
- Log file exists
- Log file was updated recently (within schedule frequency + 10 minutes)

**View health status**:
```bash
docker inspect --format='{{.State.Health.Status}}' flag-dispensations-app

# Possible values:
# - healthy: Container is working properly
# - unhealthy: Container has issues
# - starting: Health check hasn't completed yet
```

### Docker Commands Reference
```bash
# Build
export UID=$(id -u) GID=$(id -g)
docker-compose build

# Build with no cache (use after code updates)
docker-compose build --no-cache

# Start
docker-compose up -d

# Stop
docker-compose down

# Restart
docker-compose restart alerts

# Logs (live)
docker-compose logs -f alerts

# Logs (last 100 lines)
docker-compose logs --tail=100 alerts

# Execute command
docker-compose exec alerts python -m src.main --run-once

# Shell access
docker-compose exec alerts bash

# Run tests
docker-compose run --rm alerts pytest tests/ -v

# Remove everything (including volumes)
docker-compose down -v

# Complete cache clear and rebuild
docker-compose down -v && \
docker images | grep flag-dispensations | awk '{print $3}' | xargs -r docker rmi && \
docker builder prune -af && \
docker-compose build --no-cache
```

---

## 🛠️ Development

### Project Structure
```
flag-dispensations-alerts/
├── .env                          # Configuration (not in git)
├── .env.example                  # Configuration template
├── .gitignore                    # Git ignore rules
├── docker-compose.yml            # Docker configuration
├── Dockerfile                    # Container definition
├── requirements.txt              # Python dependencies
├── pytest.ini                    # Pytest configuration
├── README.md                     # This file
│
├── src/                          # Source code
│   ├── __init__.py
│   ├── main.py                   # Entry point
│   ├── db_utils.py               # Database utilities (SSH tunnel, queries)
│   │
│   ├── core/                     # Core infrastructure (reusable)
│   │   ├── __init__.py
│   │   ├── base_alert.py         # Abstract base class for alerts
│   │   ├── config.py             # Configuration management
│   │   ├── tracking.py           # Event tracking system
│   │   └── scheduler.py          # Scheduling logic
│   │
│   ├── notifications/            # Notification handlers (reusable)
│   │   ├── __init__.py
│   │   ├── email_sender.py       # Email sending with SMTP
│   │   └── teams_sender.py       # Teams integration (stub)
│   │
│   ├── formatters/               # Email formatters (reusable)
│   │   ├── __init__.py
│   │   ├── html_formatter.py     # Rich HTML emails with responsive design
│   │   ├── text_formatter.py     # Plain text emails
│   │   └── date_formatter.py     # Duration formatting utility
│   │
│   ├── utils/                    # Utilities (reusable)
│   │   ├── __init__.py
│   │   ├── validation.py         # DataFrame validation
│   │   └── image_utils.py        # Logo loading
│   │
│   └── alerts/                   # Alert implementations (customized)
│       ├── __init__.py
│       └── flag_dispensations_alert.py  # Current alert
│
├── queries/                      # SQL queries (customized)
│   └── FlagDispensations.sql
│
├── media/                        # Company logos
│   ├── trans_logo_prominence_procreate_small.png
│   └── trans_logo_seatraders_procreate_small.png
│
├── data/                         # Runtime data (not in git)
│   └── sent_alerts.json          # Tracking file
│
├── logs/                         # Log files (not in git)
│   └── alerts.log
│
├── docs/                         # Documentation
│   └── AlertDev.docx             # Alert specifications
│
├── scripts/                      # Utility scripts
│   ├── email_checker.py          # Email testing utility
│   └── verify_teams_webhook.py   # Teams webhook testing
│
└── tests/                        # Unit tests (⚠️ NEEDS UPDATE)
    ├── conftest.py               # Shared fixtures
    ├── test_config.py            # Configuration tests ✅
    ├── test_tracking.py          # Tracking tests ✅
    ├── test_passage_plan_alert.py  # ⚠️ Needs renaming/updating
    ├── test_formatters.py        # Formatter tests ✅
    ├── test_email_sender.py      # Email sending tests ✅
    ├── test_scheduler.py         # Scheduler tests ✅
    └── test_integration.py       # End-to-end tests ⚠️
```

### Code Quality Standards

**Before committing**:
```bash
# Run tests (note: some may fail until updated)
pytest tests/ -v

# Check coverage
pytest tests/ --cov=src --cov-report=term

# Format code (if using black)
black src/ tests/

# Lint code (if using flake8)
flake8 src/ tests/
```

---

## 🐛 Troubleshooting

### Common Issues

#### 1. "No module named 'src'"
**Cause**: Running from wrong directory

**Solution**:
```bash
# Always run from project root
cd /path/to/flag-dispensations-alerts
python -m src.main --dry-run --run-once
```

#### 2. Emails not sending in production mode
**Causes**:
- `DRY_RUN=True` in `.env` (check this first!)
- SMTP credentials incorrect
- Gmail blocking "less secure apps"
- Firewall blocking SMTP port

**Solution**:
```bash
# Check DRY_RUN setting
grep DRY_RUN .env

# Check SMTP settings
grep SMTP .env

# For Gmail: Use App Password (not regular password)
# 1. Enable 2FA: https://myaccount.google.com/security
# 2. Generate App Password: https://myaccount.google.com/apppasswords
# 3. Use App Password in SMTP_PASS

# Test SMTP connection
telnet smtp.gmail.com 465
```

#### 3. "SSH key not found" error
**Cause**: SSH key path incorrect or not mounted in Docker

**Solution**:
```bash
# Check SSH key exists locally
ls -la ~/.ssh/your_key

# Update docker-compose.yml volume mount
volumes:
  - ~/.ssh/your_key:/app/ssh_ubuntu_key:ro  # ← Verify this path

# Update .env
SSH_KEY_PATH=/app/ssh_ubuntu_key  # Path inside container
```

#### 4. Database connection fails
**Causes**:
- SSH tunnel not working
- Database credentials incorrect
- Database not accessible from this host

**Solution**:
```bash
# Test SSH connection
ssh -i ~/.ssh/your_key user@host

# Test SSH tunnel manually
ssh -i ~/.ssh/your_key -L 5432:localhost:5432 user@host

# Test database connection (in another terminal)
psql -h localhost -p 5432 -U username -d database_name

# Check .env settings
grep -E "DB_|SSH_" .env
```

#### 5. Links not appearing in emails
**Causes**:
- `ENABLE_LINKS=False` in `.env`
- `BASE_URL` or `URL_PATH` not configured correctly
- `url` column not being added to DataFrame

**Solution**:
```bash
# Check link configuration
grep -E "ENABLE_LINKS|BASE_URL|URL_PATH" .env

# Verify settings
ENABLE_LINKS=True
BASE_URL=https://prominence.orca.tools
URL_PATH=/jobs/flag-extension-dispensation

# Test URL generation
python -c "from src.core.config import AlertConfig; c = AlertConfig.from_env(); print(c.enable_links, c.base_url, c.url_path)"
```

#### 6. Permission denied: '/app/logs/alerts.log'
**Cause**: Docker container doesn't have write permission to mounted volumes

**Solution**:
```bash
# Fix directory permissions
sudo chown -R $(id -u):$(id -g) logs/ data/

# Or use specific UID:GID
sudo chown -R 1000:1000 logs/ data/

# Verify permissions
ls -la logs/ data/

# Restart container
docker-compose down
docker-compose up -d
```

#### 7. Tests fail after git pull / Docker caching old modules
**Cause**: Docker is caching old Python bytecode

**Solution**:
```bash
# Complete Docker cache clear
docker-compose down -v && \
docker-compose build --no-cache && \
docker-compose run --rm alerts pytest tests/ -v
```

#### 8. No jobs found when there should be
**Causes**:
- `JOB_STATUS` doesn't match database values
- `LOOKBACK_DAYS` too short
- Jobs already tracked in `sent_alerts.json`

**Solution**:
```bash
# Check job status values in database
# Connect to database and run:
SELECT DISTINCT js.label 
FROM job_entities je 
LEFT JOIN job_statuses js ON js.id = je.status_id 
WHERE je.type = 'flag-extension-dispensation';

# Update JOB_STATUS in .env to match
# Example: for_approval, pending, submitted, etc.

# Increase lookback if needed
LOOKBACK_DAYS=7  # Check last 7 days instead of 1

# Clear tracking file to re-send (use cautiously!)
rm data/sent_alerts.json
```

### Logging & Debugging
```bash
# View live logs (local)
tail -f logs/alerts.log

# View live logs (Docker)
docker-compose logs -f alerts

# View last 100 lines
tail -n 100 logs/alerts.log

# Search for errors
grep ERROR logs/alerts.log

# Search for specific vessel
grep "KNOSSOS" logs/alerts.log

# Check tracking file
cat data/sent_alerts.json | jq '.'

# Count tracked jobs
cat data/sent_alerts.json | jq '.sent_events | length'

# Find specific job in tracking
cat data/sent_alerts.json | jq '.sent_events[] | select(.tracking_key | contains("job_id_456"))'
```

### Testing Checklist

Before deploying to production:

- [ ] Dry-run completes without errors: `docker-compose run --rm alerts python -m src.main --dry-run --run-once`
- [ ] SQL query returns expected columns
- [ ] Email recipients configured correctly in `.env`
- [ ] CC recipients configured correctly per domain
- [ ] `DRY_RUN=False` in `.env` for production
- [ ] `DRY_RUN_EMAIL` contains valid test addresses
- [ ] Company logos exist in `media/` directory
- [ ] Link generation works (if `ENABLE_LINKS=True`)
- [ ] `BASE_URL` and `URL_PATH` configured correctly
- [ ] `JOB_STATUS` matches database values
- [ ] Tracking file updates after test run: `cat data/sent_alerts.json`
- [ ] No duplicates on second dry-run
- [ ] Docker build succeeds: `docker-compose build`
- [ ] Container starts: `docker-compose up -d`
- [ ] Container stays running: `docker-compose ps`
- [ ] Logs show successful execution: `docker-compose logs -f alerts`
- [ ] Health check passes: `docker inspect --format='{{.State.Health.Status}}' flag-dispensations-app`

---

## 📚 Key Concepts

### Alert Workflow

```
1. Scheduler triggers alert run (every 1 hour)
   ↓
2. fetch_data() - Query database for flag dispensation jobs
   - WHERE type = 'flag-extension-dispensation'
   - AND status = 'for_approval'
   - AND created_at >= NOW() - 1 day
   ↓
3. filter_data() - Apply timezone conversion and date formatting
   ↓
4. Check tracking - Skip already-sent jobs
   ↓
5. route_notifications() - Group by vessel, add URLs, create email jobs
   ↓
6. Send emails - One email per vessel with job details
   ↓
7. Update tracking - Mark jobs as sent
```

### Tracking Key Format

```python
def get_tracking_key(self, row: pd.Series) -> str:
    vessel_id = row['vessel_id']
    job_id = row['job_id']
    
    return f"vessel_id_{vessel_id}__job_id_{job_id}"

# Example: "vessel_id_123__job_id_456"
```

### Email Content

**Subject**: `AlertDev | KNOSSOS Flag Extensions-Dispensations`

**Body** (responsive HTML table):
- **Title** (clickable link to job) - `https://prominence.orca.tools/jobs/flag-extension-dispensation/456`
- **Type** - Dispensation type (Extension, Dispensation, etc.)
- **Department** - Department name
- **Requested On** - Date job was requested
- **Due Date** - Job due date
- **Created At** - When job was created in system

### Configuration Flow
```
.env file
  ↓
python-decouple reads file
  ↓
AlertConfig.from_env() parses values
  ↓
AlertConfig dataclass instance created
  ↓
Passed to all components (alerts, formatters, senders)
  ↓
Accessed via self.config throughout application
```

---

## 📞 Support

For questions or issues:

1. **Check this README** - Most answers are here
2. **Review logs**: `docker-compose logs -f alerts`
3. **Test in dry-run**: `docker-compose run --rm alerts python -m src.main --dry-run --run-once`
4. **Check tracking file**: `cat data/sent_alerts.json | jq '.'`
5. **Verify database query**: Run `queries/FlagDispensations.sql` manually
6. **Contact**: data@prominencemaritime.com

---

## 📄 License

Proprietary - Prominence Maritime / Seatraders

---

## 🎉 Quick Start Summary
```bash
# 1. Copy/clone project
cd ~/Dev
git clone <repository> flag-dispensations-alerts
cd flag-dispensations-alerts

# 2. Configure
cp .env.example .env
vi .env  # Update all settings

# 3. Fix permissions
sudo chown -R $(id -u):$(id -g) logs/ data/

# 4. Test dry-run
export UID=$(id -u) GID=$(id -g)
docker-compose build
docker-compose run --rm alerts python -m src.main --dry-run --run-once

# 5. Deploy
docker-compose up -d

# 6. Monitor
docker-compose logs -f alerts

# 7. Check health
docker inspect --format='{{.State.Health.Status}}' flag-dispensations-app
```

**That's it! You now have a production-ready flag dispensations alert system.** 🚀

---

## 📖 Additional Resources

- **Python decouple docs**: https://pypi.org/project/python-decouple/
- **Pandas documentation**: https://pandas.pydata.org/docs/
- **Docker Compose docs**: https://docs.docker.com/compose/
- **Pytest documentation**: https://docs.pytest.org/
- **SSH tunnel guide**: https://www.ssh.com/academy/ssh/tunneling

---

*Last updated: December 2025*
```

## Summary of Changes

The major updates to the README include:

1. **Project name** changed from "Passage Plan" to "Flag Dispensations"
2. **Alert description** updated to reflect flag extension/dispensation jobs monitoring
3. **Database details** updated: `job_entities` table, `type='flag-extension-dispensation'`, `status='for_approval'`
4. **SQL query reference** changed from `PassagePlan.sql` to `FlagDispensations.sql`
5. **Alert class references** changed from `PassagePlanAlert` to `FlagDispensationsAlert`
6. **URL path** updated to `/jobs/flag-extension-dispensation/`
7. **Schedule frequency** changed from 30 minutes to 1 hour
8. **Job-specific terminology** throughout (jobs instead of events, dispensations instead of passage plans)
9. **Added warning** about tests needing updates
10. **Email body columns** updated to match flag dispensations spec (Title, Type, Department, Requested On, Due Date, Created At)
11. **Configuration section** added `JOB_STATUS` parameter explanation
12. **Troubleshooting section** added issue #8 for job status mismatch
