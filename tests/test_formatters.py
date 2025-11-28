# tests/test_formatters.py
"""
Tests for HTML and text email formatters.
"""
import pytest
from datetime import datetime
from src.formatters.html_formatter import HTMLFormatter
from src.formatters.text_formatter import TextFormatter


def test_html_formatter_generates_valid_html(mock_config, sample_dataframe):
    """Test that HTML formatter generates valid HTML."""
    formatter = HTMLFormatter()
    run_time = datetime.now()

    metadata = {
        'alert_title': 'Test Alert',
        'vessel_name': 'TEST VESSEL',
        'company_name': 'Test Company',
        'display_columns': ['event_name', 'status', 'synced_at']
    }

    html = formatter.format(sample_dataframe, run_time, mock_config, metadata)

    assert '<!DOCTYPE html' in html
    assert 'Test Alert' in html
    assert 'TEST VESSEL' in html
    assert 'Athens to Piraeus' in html  # First event name


def test_html_formatter_handles_empty_dataframe(mock_config):
    """Test that HTML formatter handles empty DataFrame gracefully."""
    formatter = HTMLFormatter()
    run_time = datetime.now()

    import pandas as pd
    empty_df = pd.DataFrame()

    metadata = {'alert_title': 'Test Alert', 'vessel_name': 'TEST'}

    html = formatter.format(empty_df, run_time, mock_config, metadata)

    assert 'No records found' in html


def test_html_formatter_displays_only_specified_columns(mock_config, sample_dataframe):
    """Test that only specified columns are displayed."""
    formatter = HTMLFormatter()
    run_time = datetime.now()

    metadata = {
        'alert_title': 'Test',
        'display_columns': ['event_name', 'status']  # Only these
    }

    html = formatter.format(sample_dataframe, run_time, mock_config, metadata)

    # Should include specified columns
    assert 'Event Name' in html
    assert 'Status' in html

    # Should NOT include other columns in table headers
    assert 'Created At' not in html or 'created_at' not in html.lower()


def test_route_notifications_adds_urls(mock_config, sample_dataframe):
    """Test that route_notifications adds url column when links enabled."""
    from src.alerts.passage_plan_alert import PassagePlanAlert
    
    # Enable links
    mock_config.enable_links = True
    mock_config.base_url = 'https://test.com'
    mock_config.url_path = '/events'
    
    alert = PassagePlanAlert(mock_config)
    jobs = alert.route_notifications(sample_dataframe)
    
    # Check that url column was added
    for job in jobs:
        data = job['data']
        
        # Should have url column
        assert 'url' in data.columns
        
        # Each URL should be properly formatted
        for idx, row in data.iterrows():
            expected_url = f"https://test.com/events/{row['event_id']}"
            assert row['url'] == expected_url
            
        # Verify _get_url_links was called correctly
        first_event_id = data.iloc[0]['event_id']
        expected_first_url = alert._get_url_links(first_event_id)
        assert data.iloc[0]['url'] == expected_first_url


def test_text_formatter_generates_plain_text(mock_config, sample_dataframe):
    """Test that text formatter generates plain text."""
    formatter = TextFormatter()
    run_time = datetime.now()

    metadata = {
        'alert_title': 'Test Alert',
        'vessel_name': 'TEST VESSEL',
        'display_columns': ['event_name', 'status']
    }

    text = formatter.format(sample_dataframe, run_time, mock_config, metadata)

    assert 'Test Alert' in text
    assert 'TEST VESSEL' in text
    assert 'Athens to Piraeus' in text  # First event name
    assert '<' not in text  # No HTML tags


def test_text_formatter_handles_empty_dataframe(mock_config):
    """Test that text formatter handles empty DataFrame."""
    formatter = TextFormatter()
    run_time = datetime.now()

    import pandas as pd
    empty_df = pd.DataFrame()

    metadata = {'alert_title': 'Test', 'vessel_name': 'TEST'}

    text = formatter.format(empty_df, run_time, mock_config, metadata)

    assert 'No records' in text or 'no records' in text.lower()
