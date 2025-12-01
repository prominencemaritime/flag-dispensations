# tests/test_formatters.py
"""
Tests for HTML and Text formatters.
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
        'display_columns': ['title', 'status', 'dispensation_type']  # Use actual columns
    }
    
    html = formatter.format(sample_dataframe, run_time, mock_config, metadata)
    
    assert '<!DOCTYPE html' in html
    assert 'Test Alert' in html
    assert 'TEST VESSEL' in html
    assert 'Flag Extension Request - Greece' in html  # First job title from sample data


def test_html_formatter_handles_empty_dataframe(mock_config):
    """Test that HTML formatter handles empty dataframes."""
    import pandas as pd
    
    formatter = HTMLFormatter()
    run_time = datetime.now()
    
    empty_df = pd.DataFrame()
    metadata = {
        'alert_title': 'Empty Test',
        'vessel_name': 'TEST VESSEL',
    }
    
    html = formatter.format(empty_df, run_time, mock_config, metadata)
    
    assert '<!DOCTYPE html' in html
    assert 'Empty Test' in html
    assert 'No records found' in html


def test_html_formatter_displays_only_specified_columns(mock_config, sample_dataframe):
    """Test that only specified columns are displayed."""
    formatter = HTMLFormatter()
    run_time = datetime.now()
    
    metadata = {
        'alert_title': 'Test',
        'display_columns': ['title', 'status']  # Only these columns (using actual column names)
    }
    
    html = formatter.format(sample_dataframe, run_time, mock_config, metadata)
    
    # Should include specified columns (column names are title-cased in HTML)
    assert 'Title' in html
    assert 'Status' in html
    
    # Should NOT include other columns
    assert 'Dispensation Type' not in html
    assert 'Department' not in html


def test_route_notifications_adds_urls(mock_config, sample_dataframe):
    """Test that route_notifications adds url column when links enabled."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    # Enable links
    mock_config.enable_links = True
    mock_config.base_url = 'https://test.com'
    mock_config.url_path = '/jobs/flag-extension-dispensation'
    
    alert = FlagDispensationsAlert(mock_config)
    jobs = alert.route_notifications(sample_dataframe)
    
    # Check that url column was added
    for job in jobs:
        data = job['data']
        
        # Should have url column
        assert 'url' in data.columns
        
        # Each URL should be properly formatted
        for idx, row in data.iterrows():
            expected_url = f"https://test.com/jobs/flag-extension-dispensation/{row['job_id']}"  # Use job_id not event_id
            assert row['url'] == expected_url


def test_text_formatter_generates_plain_text(mock_config, sample_dataframe):
    """Test that text formatter generates plain text."""
    formatter = TextFormatter()
    run_time = datetime.now()
    
    metadata = {
        'alert_title': 'Test Alert',
        'vessel_name': 'TEST VESSEL',
        'display_columns': ['title', 'status']  # Use actual column names
    }
    
    text = formatter.format(sample_dataframe, run_time, mock_config, metadata)
    
    assert 'Test Alert' in text
    assert 'TEST VESSEL' in text
    assert 'Flag Extension Request - Greece' in text  # First job title from sample data


def test_text_formatter_handles_empty_dataframe(mock_config):
    """Test that text formatter handles empty dataframes."""
    import pandas as pd
    
    formatter = TextFormatter()
    run_time = datetime.now()
    
    empty_df = pd.DataFrame()
    metadata = {
        'alert_title': 'Empty Test',
        'vessel_name': 'TEST VESSEL',
    }
    
    text = formatter.format(empty_df, run_time, mock_config, metadata)
    
    assert 'Empty Test' in text
    assert 'TEST VESSEL' in text
    assert 'No records found' in text or 'Found 0 record' in text
