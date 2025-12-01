# tests/test_integration.py
"""
Integration tests for complete alert workflow.
"""
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime, timedelta


@patch('src.alerts.flag_dispensations_alert.get_db_connection')
@patch('src.alerts.flag_dispensations_alert.pd.read_sql_query')
@patch('src.notifications.email_sender.EmailSender.send')
def test_complete_alert_workflow(mock_send, mock_read_sql, mock_get_db, mock_config, sample_dataframe, mock_event_tracker, temp_dir):
    """Test complete alert workflow from fetch to send."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    from src.formatters.html_formatter import HTMLFormatter
    from src.formatters.text_formatter import TextFormatter
    from src.notifications.email_sender import EmailSender

    # Mock get_db_connection to return a dummy context manager
    mock_conn = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_get_db.return_value.__exit__.return_value = None

    # Mock pd.read_sql_query to return sample data
    mock_read_sql.return_value = sample_dataframe

    # Create SQL query file
    mock_config.queries_dir.mkdir(parents=True, exist_ok=True)
    sql_file = mock_config.queries_dir / 'FlagDispensations.sql'
    sql_file.write_text('SELECT * FROM job_entities;')

    # Initialize components
    mock_config.tracker = mock_event_tracker
    mock_config.email_sender = EmailSender(
        smtp_host='smtp.test.com',
        smtp_port=465,
        smtp_user='test@test.com',
        smtp_pass='password',
        company_logos={},
        dry_run=False
    )
    mock_config.html_formatter = HTMLFormatter()
    mock_config.text_formatter = TextFormatter()

    # Create and run alert
    alert = FlagDispensationsAlert(mock_config)
    alert.run()

    # Verify email was sent (3 vessels = 3 emails: KNOSSOS, MINI, NONDAS)
    assert mock_send.call_count == 3

    # Verify tracking was updated (4 jobs total)
    assert len(mock_event_tracker.sent_events) == 4


@patch('src.alerts.flag_dispensations_alert.get_db_connection')
@patch('src.alerts.flag_dispensations_alert.pd.read_sql_query')
def test_alert_prevents_duplicate_sends(mock_read_sql, mock_get_db, mock_config, sample_dataframe, mock_event_tracker, temp_dir):
    """Test that alert doesn't send duplicates."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert

    # Mock get_db_connection
    mock_conn = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_get_db.return_value.__exit__.return_value = None

    # Mock pd.read_sql_query to return sample data
    mock_read_sql.return_value = sample_dataframe

    # Create SQL query file
    sql_file = mock_config.queries_dir / 'FlagDispensations.sql'
    sql_file.write_text('SELECT * FROM job_entities;')

    # Initialize
    mock_config.tracker = mock_event_tracker
    mock_email_sender = MagicMock()
    mock_config.email_sender = mock_email_sender
    mock_config.html_formatter = MagicMock()
    mock_config.text_formatter = MagicMock()

    # Mock formatters to return dummy content
    mock_config.html_formatter.format.return_value = '<html>Test</html>'
    mock_config.text_formatter.format.return_value = 'Test'

    # First run - should send
    alert = FlagDispensationsAlert(mock_config)
    alert.run()

    first_call_count = mock_email_sender.send.call_count
    assert first_call_count > 0

    # Second run - should not send (duplicates)
    alert2 = FlagDispensationsAlert(mock_config)
    alert2.run()

    # Call count should be same (no new sends)
    assert mock_email_sender.send.call_count == first_call_count


def test_dry_run_email_redirection(mock_config, sample_dataframe, temp_dir):
    """Test that dry-run mode redirects emails correctly."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert

    # Enable dry-run with email redirection
    mock_config.dry_run = True
    mock_config.dry_run_email = 'dryrun@test.com'
    mock_config.enable_email_alerts = True

    # Create alert
    alert = FlagDispensationsAlert(mock_config)

    # Route notifications
    jobs = alert.route_notifications(sample_dataframe)

    # All jobs should have original recipients (redirection happens in EmailSender, not routing)
    for job in jobs:
        assert len(job['recipients']) > 0
        assert 'vsl.prominencemaritime.com' in job['recipients'][0]


@patch('src.alerts.flag_dispensations_alert.get_db_connection')
@patch('src.alerts.flag_dispensations_alert.pd.read_sql_query')
def test_alert_handles_empty_results(mock_read_sql, mock_get_db, mock_config, mock_event_tracker, temp_dir):
    """Test that alert handles empty database results gracefully."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert

    # Mock get_db_connection
    mock_conn = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_get_db.return_value.__exit__.return_value = None

    # Mock pd.read_sql_query to return empty DataFrame
    empty_df = pd.DataFrame(columns=[
        'vsl_email', 'vessel_id', 'vessel', 'job_id', 'importance',
        'title', 'dispensation_type', 'department', 'due_date',
        'requested_on', 'created_at', 'status'
    ])
    mock_read_sql.return_value = empty_df

    # Create SQL query file
    sql_file = mock_config.queries_dir / 'FlagDispensations.sql'
    sql_file.write_text('SELECT * FROM job_entities;')

    # Initialize
    mock_config.tracker = mock_event_tracker
    mock_email_sender = MagicMock()
    mock_config.email_sender = mock_email_sender
    mock_config.html_formatter = MagicMock()
    mock_config.text_formatter = MagicMock()

    # Run alert
    alert = FlagDispensationsAlert(mock_config)
    result = alert.run()

    # Should complete successfully without sending emails
    # Note: run() returns False when no new data to process, which is expected behavior
    assert result is False  # Changed from True to False
    assert mock_email_sender.send.call_count == 0


@patch('src.alerts.flag_dispensations_alert.get_db_connection')
@patch('src.alerts.flag_dispensations_alert.pd.read_sql_query')
def test_alert_with_multiple_jobs_per_vessel(mock_read_sql, mock_get_db, mock_config, mock_event_tracker, temp_dir):
    """Test alert correctly groups multiple jobs for the same vessel."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert

    # Mock get_db_connection
    mock_conn = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_get_db.return_value.__exit__.return_value = None

    # Create DataFrame with multiple jobs for same vessel
    multi_job_df = pd.DataFrame({
        'vessel_id': [101, 101, 101],
        'vessel': ['KNOSSOS', 'KNOSSOS', 'KNOSSOS'],
        'vsl_email': ['knossos@vsl.prominencemaritime.com'] * 3,
        'job_id': [501, 502, 503],
        'importance': ['High', 'Medium', 'Low'],
        'title': ['Job 1', 'Job 2', 'Job 3'],
        'dispensation_type': ['Extension', 'Dispensation', 'Extension'],
        'department': ['Deck', 'Safety', 'Engine'],
        'due_date': ['2025-12-15', '2025-12-20', '2025-12-25'],
        'requested_on': ['2025-11-01', '2025-11-02', '2025-11-03'],
        'created_at': [datetime.now() - timedelta(hours=i) for i in range(1, 4)],
        'status': ['for_approval'] * 3
    })

    mock_read_sql.return_value = multi_job_df

    # Create SQL query file
    sql_file = mock_config.queries_dir / 'FlagDispensations.sql'
    sql_file.write_text('SELECT * FROM job_entities;')

    # Initialize
    mock_config.tracker = mock_event_tracker
    mock_email_sender = MagicMock()
    mock_config.email_sender = mock_email_sender
    mock_config.html_formatter = MagicMock()
    mock_config.text_formatter = MagicMock()

    # Mock formatters
    mock_config.html_formatter.format.return_value = '<html>Test</html>'
    mock_config.text_formatter.format.return_value = 'Test'

    # Run alert
    alert = FlagDispensationsAlert(mock_config)
    alert.run()

    # Should send only 1 email (all jobs for KNOSSOS grouped together)
    assert mock_email_sender.send.call_count == 1

    # Should track all 3 jobs
    assert len(mock_event_tracker.sent_events) == 3


@patch('src.alerts.flag_dispensations_alert.get_db_connection')
@patch('src.alerts.flag_dispensations_alert.pd.read_sql_query')
def test_alert_respects_lookback_days(mock_read_sql, mock_get_db, mock_config, mock_event_tracker, temp_dir):
    """Test that alert correctly filters by lookback_days."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert

    # Mock get_db_connection
    mock_conn = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_get_db.return_value.__exit__.return_value = None

    # Create DataFrame with jobs at different ages
    mixed_age_df = pd.DataFrame({
        'vessel_id': [101, 102, 103],
        'vessel': ['VESSEL1', 'VESSEL2', 'VESSEL3'],
        'vsl_email': ['v1@test.com', 'v2@test.com', 'v3@test.com'],
        'job_id': [501, 502, 503],
        'importance': ['High'] * 3,
        'title': ['Recent Job', 'Old Job', 'Very Old Job'],
        'dispensation_type': ['Extension'] * 3,
        'department': ['Deck'] * 3,
        'due_date': ['2025-12-15'] * 3,
        'requested_on': ['2025-11-01'] * 3,
        'created_at': [
            datetime.now() - timedelta(hours=2),    # Recent (within 1 day)
            datetime.now() - timedelta(days=3),     # Old (outside 1 day)
            datetime.now() - timedelta(days=10)     # Very old (outside 1 day)
        ],
        'status': ['for_approval'] * 3
    })

    mock_read_sql.return_value = mixed_age_df

    # Create SQL query file
    sql_file = mock_config.queries_dir / 'FlagDispensations.sql'
    sql_file.write_text('SELECT * FROM job_entities;')

    # Initialize with lookback_days=1
    mock_config.lookback_days = 1
    mock_config.tracker = mock_event_tracker
    mock_email_sender = MagicMock()
    mock_config.email_sender = mock_email_sender
    mock_config.html_formatter = MagicMock()
    mock_config.text_formatter = MagicMock()

    # Mock formatters
    mock_config.html_formatter.format.return_value = '<html>Test</html>'
    mock_config.text_formatter.format.return_value = 'Test'

    # Run alert
    alert = FlagDispensationsAlert(mock_config)
    alert.run()

    # Should only send 1 email (only the recent job within 1 day)
    assert mock_email_sender.send.call_count == 1

    # Should only track 1 job (the recent one)
    assert len(mock_event_tracker.sent_events) == 1


@patch('src.alerts.flag_dispensations_alert.get_db_connection')
@patch('src.alerts.flag_dispensations_alert.pd.read_sql_query')
def test_alert_includes_urls_when_enabled(mock_read_sql, mock_get_db, mock_config, sample_dataframe, mock_event_tracker, temp_dir):
    """Test that URLs are added to job data when links are enabled."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert

    # Mock get_db_connection
    mock_conn = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_get_db.return_value.__exit__.return_value = None

    # Mock pd.read_sql_query
    mock_read_sql.return_value = sample_dataframe

    # Create SQL query file
    sql_file = mock_config.queries_dir / 'FlagDispensations.sql'
    sql_file.write_text('SELECT * FROM job_entities;')

    # Enable links
    mock_config.enable_links = True
    mock_config.base_url = 'https://prominence.orca.tools'
    mock_config.url_path = '/jobs/flag-extension-dispensation'

    # Initialize
    alert = FlagDispensationsAlert(mock_config)

    # Route notifications (don't need full run)
    jobs = alert.route_notifications(sample_dataframe)

    # Check that all jobs have URL column in data
    for job in jobs:
        assert 'url' in job['data'].columns
        # Verify URLs are properly formatted
        for url in job['data']['url']:
            assert url.startswith('https://prominence.orca.tools/jobs/flag-extension-dispensation/')
            assert url.split('/')[-1].isdigit()  # Should end with job_id


@patch('src.alerts.flag_dispensations_alert.get_db_connection')
@patch('src.alerts.flag_dispensations_alert.pd.read_sql_query')
def test_alert_metadata_includes_vessel_info(mock_read_sql, mock_get_db, mock_config, sample_dataframe, mock_event_tracker, temp_dir):
    """Test that metadata includes correct vessel information."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert

    # Mock get_db_connection
    mock_conn = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_get_db.return_value.__exit__.return_value = None

    # Mock pd.read_sql_query
    mock_read_sql.return_value = sample_dataframe

    # Create SQL query file
    sql_file = mock_config.queries_dir / 'FlagDispensations.sql'
    sql_file.write_text('SELECT * FROM job_entities;')

    # Initialize
    alert = FlagDispensationsAlert(mock_config)

    # Route notifications
    jobs = alert.route_notifications(sample_dataframe)

    # Check metadata for each job
    for job in jobs:
        metadata = job['metadata']
        
        # Should have all required metadata fields
        assert 'vessel_id' in metadata
        assert 'vessel_name' in metadata
        assert 'alert_title' in metadata
        assert 'company_name' in metadata
        assert 'display_columns' in metadata
        
        # Alert title should be correct
        assert metadata['alert_title'] == 'Flag Dispensations'
        
        # Vessel name should match one from sample data
        assert metadata['vessel_name'] in ['KNOSSOS', 'MINI', 'NONDAS']
        
        # Company name should be set (all are Prominence in sample data)
        assert metadata['company_name'] == 'Prominence Maritime S.A.'
