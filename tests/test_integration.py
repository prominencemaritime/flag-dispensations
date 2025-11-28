# tests/test_integration.py
"""
Integration tests for complete alert workflow.
"""
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime, timedelta


@patch('src.alerts.passage_plan_alert.get_db_connection')
@patch('src.alerts.passage_plan_alert.pd.read_sql_query')
@patch('src.notifications.email_sender.EmailSender.send')
def test_complete_alert_workflow(mock_send, mock_read_sql, mock_get_db, mock_config, sample_dataframe, mock_event_tracker, temp_dir):
    """Test complete alert workflow from fetch to send."""
    from src.alerts.passage_plan_alert import PassagePlanAlert
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
    sql_file = mock_config.queries_dir / 'PassagePlan.sql'
    sql_file.write_text('SELECT * FROM events;')

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
    alert = PassagePlanAlert(mock_config)
    alert.run()

    # Verify email was sent (3 vessels = 3 emails)
    assert mock_send.call_count == 3

    # Verify tracking was updated (4 events total)
    assert len(mock_event_tracker.sent_events) == 4


@patch('src.alerts.passage_plan_alert.get_db_connection')
@patch('src.alerts.passage_plan_alert.pd.read_sql_query')
def test_alert_prevents_duplicate_sends(mock_read_sql, mock_get_db, mock_config, sample_dataframe, mock_event_tracker, temp_dir):
    """Test that alert doesn't send duplicates."""
    from src.alerts.passage_plan_alert import PassagePlanAlert

    # Mock get_db_connection
    mock_conn = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_get_db.return_value.__exit__.return_value = None

    # Mock pd.read_sql_query to return sample data
    mock_read_sql.return_value = sample_dataframe

    # Create SQL query file
    sql_file = mock_config.queries_dir / 'PassagePlan.sql'
    sql_file.write_text('SELECT * FROM events;')

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
    alert = PassagePlanAlert(mock_config)
    alert.run()

    first_call_count = mock_email_sender.send.call_count
    assert first_call_count > 0

    # Second run - should not send (duplicates)
    alert2 = PassagePlanAlert(mock_config)
    alert2.run()

    # Call count should be same (no new sends)
    assert mock_email_sender.send.call_count == first_call_count


def test_dry_run_email_redirection(mock_config, sample_dataframe, temp_dir):
    """Test that dry-run mode redirects emails correctly."""
    from src.alerts.passage_plan_alert import PassagePlanAlert

    # Enable dry-run with email redirection
    mock_config.dry_run = True
    mock_config.dry_run_email = 'dryrun@test.com'
    mock_config.enable_email_alerts = True

    # Create alert
    alert = PassagePlanAlert(mock_config)

    # Route notifications
    jobs = alert.route_notifications(sample_dataframe)

    # All jobs should have original recipients
    for job in jobs:
        assert len(job['recipients']) > 0
        assert 'vsl.prominencemaritime.com' in job['recipients'][0]
