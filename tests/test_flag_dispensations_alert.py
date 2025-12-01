# tests/test_flag_dispensations_alert.py
"""
Tests for FlagDispensationsAlert logic.
"""
import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock


def test_alert_initializes_correctly(mock_config):
    """Test that alert initializes with correct configuration."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    alert = FlagDispensationsAlert(mock_config)
    
    assert alert.sql_query_file == 'FlagDispensations.sql'
    assert alert.lookback_days == mock_config.lookback_days
    assert alert.job_status == mock_config.job_status


def test_alert_filters_data_by_lookback_days(mock_config, sample_dataframe):
    """Test that filter_data correctly filters by lookback days."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    alert = FlagDispensationsAlert(mock_config)
    alert.lookback_days = 1  # Last 24 hours
    
    # All sample data is within last 24 hours (created_at is recent)
    filtered = alert.filter_data(sample_dataframe)
    
    assert len(filtered) == 4  # All 4 records should pass


def test_alert_filters_out_old_data(mock_config, sample_dataframe):
    """Test that old data is filtered out."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    # Create old record by copying a row and modifying it
    old_record = sample_dataframe.iloc[[0]].copy()
    old_record['vessel_id'] = 999
    old_record['vessel'] = 'OLD VESSEL'
    old_record['vsl_email'] = 'old@test.com'
    old_record['job_id'] = 999
    old_record['title'] = 'Old Flag Dispensation'
    old_record['created_at'] = datetime.now() - timedelta(days=5)
    
    df_with_old = pd.concat([sample_dataframe, old_record], ignore_index=True)
    
    alert = FlagDispensationsAlert(mock_config)
    alert.lookback_days = 1
    
    filtered = alert.filter_data(df_with_old)
    
    # Should exclude the old record
    assert len(filtered) == 4
    assert 999 not in filtered['job_id'].values


def test_alert_routes_by_vessel(mock_config, sample_dataframe):
    """Test that notifications are routed correctly by vessel."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    alert = FlagDispensationsAlert(mock_config)
    jobs = alert.route_notifications(sample_dataframe)
    
    # Should create 3 jobs (KNOSSOS with 2 jobs, MINI with 1, NONDAS with 1)
    assert len(jobs) == 3
    
    # Check KNOSSOS job
    knossos_job = next(j for j in jobs if j['metadata']['vessel_name'] == 'KNOSSOS')
    assert len(knossos_job['data']) == 2
    assert knossos_job['recipients'] == ['knossos@vsl.prominencemaritime.com']


def test_alert_assigns_correct_cc_recipients(mock_config, sample_dataframe):
    """Test that CC recipients are assigned based on email domain plus internal recipients."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert

    alert = FlagDispensationsAlert(mock_config)
    jobs = alert.route_notifications(sample_dataframe)

    # All vessels are @prominencemaritime.com
    for job in jobs:
        cc_recipients = job['cc_recipients']

        # Should include domain-specific CC recipients
        assert 'prom1@test.com' in cc_recipients
        assert 'prom2@test.com' in cc_recipients

        # Should ALSO include internal recipients (from conftest.py)
        assert 'internal@test.com' in cc_recipients

        # Total: 2 domain + 1 internal = 3 recipients
        assert len(cc_recipients) == 3


def test_alert_prominence_domain_gets_prominence_cc(mock_config):
    """Test that Prominence domain gets Prominence CC recipients."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    # Create test dataframe with Prominence vessel
    test_df = pd.DataFrame({
        'vessel_id': [123],
        'vessel': ['TEST VESSEL'],
        'vsl_email': ['test@prominencemaritime.com'],
        'job_id': [456],
        'importance': ['High'],
        'title': ['Test Dispensation'],
        'dispensation_type': ['Extension'],
        'department': ['Deck'],
        'due_date': ['2025-12-15'],
        'requested_on': ['2025-12-01'],
        'created_at': [datetime.now()],
        'status': ['for_approval']
    })
    
    alert = FlagDispensationsAlert(mock_config)
    jobs = alert.route_notifications(test_df)
    
    assert len(jobs) == 1
    cc_recipients = jobs[0]['cc_recipients']
    
    # Should include Prominence CC recipients
    assert 'prom1@test.com' in cc_recipients
    assert 'prom2@test.com' in cc_recipients


def test_alert_seatraders_domain_gets_seatraders_cc(mock_config):
    """Test that Seatraders domain gets Seatraders CC recipients."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    # Create test dataframe with Seatraders vessel
    test_df = pd.DataFrame({
        'vessel_id': [789],
        'vessel': ['SEA VESSEL'],
        'vsl_email': ['test@seatraders.com'],
        'job_id': [789],
        'importance': ['Medium'],
        'title': ['Sea Test Dispensation'],
        'dispensation_type': ['Dispensation'],
        'department': ['Engine'],
        'due_date': ['2025-12-20'],
        'requested_on': ['2025-12-01'],
        'created_at': [datetime.now()],
        'status': ['for_approval']
    })
    
    alert = FlagDispensationsAlert(mock_config)
    jobs = alert.route_notifications(test_df)
    
    assert len(jobs) == 1
    cc_recipients = jobs[0]['cc_recipients']
    
    # Should include Seatraders CC recipients
    assert 'sea1@test.com' in cc_recipients
    assert 'sea2@test.com' in cc_recipients


def test_alert_generates_correct_subject_lines(mock_config, sample_dataframe):
    """Test subject line generation."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    alert = FlagDispensationsAlert(mock_config)
    
    # Single record
    single_df = sample_dataframe.iloc[:1]
    subject_single = alert.get_subject_line(single_df, {'vessel_name': 'TEST VESSEL'})
    assert subject_single == "AlertDev | TEST VESSEL Flag Extensions-Dispensations"
    
    # Multiple records (same subject format regardless of count)
    multi_df = sample_dataframe.iloc[:3]
    subject_multi = alert.get_subject_line(multi_df, {'vessel_name': 'KNOSSOS'})
    assert subject_multi == "AlertDev | KNOSSOS Flag Extensions-Dispensations"


def test_alert_generates_correct_tracking_keys(mock_config, sample_dataframe):
    """Test that tracking keys are generated correctly."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    alert = FlagDispensationsAlert(mock_config)
    
    row = sample_dataframe.iloc[0]
    key = alert.get_tracking_key(row)
    
    # Format: vessel_id_{X}__job_id_{Y}
    expected_key = f"vessel_id_{row['vessel_id']}__job_id_{row['job_id']}"
    assert key == expected_key
    assert '__' in key  # Double underscore separator


def test_alert_required_columns_validation(mock_config):
    """Test that required columns are correctly defined."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    alert = FlagDispensationsAlert(mock_config)
    required = alert.get_required_columns()
    
    # Flag dispensations schema
    assert 'vsl_email' in required
    assert 'vessel_id' in required
    assert 'vessel' in required
    assert 'job_id' in required
    assert 'importance' in required
    assert 'title' in required
    assert 'dispensation_type' in required
    assert 'department' in required
    assert 'due_date' in required
    assert 'requested_on' in required
    assert 'created_at' in required
    assert 'status' in required


def test_alert_validates_dataframe_columns(mock_config, sample_dataframe):
    """Test that DataFrame validation works correctly."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    alert = FlagDispensationsAlert(mock_config)
    
    # Should not raise exception with valid DataFrame
    alert.validate_required_columns(sample_dataframe)
    
    # Should raise exception with missing column
    invalid_df = sample_dataframe.drop(columns=['vessel_id'])
    with pytest.raises(ValueError, match="Missing required columns"):
        alert.validate_required_columns(invalid_df)


def test_alert_includes_internal_recipients_in_cc(mock_config, sample_dataframe):
    """Test that internal recipients are always included in CC."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert

    # Set up internal recipients
    mock_config.internal_recipients = ['admin@company.com', 'manager@company.com']

    alert = FlagDispensationsAlert(mock_config)
    jobs = alert.route_notifications(sample_dataframe)

    # Check all jobs include internal recipients in CC
    for job in jobs:
        cc_recipients = job['cc_recipients']

        # Internal recipients should ALWAYS be in the CC list
        assert 'admin@company.com' in cc_recipients, \
            f"Internal recipient 'admin@company.com' missing from CC: {cc_recipients}"
        assert 'manager@company.com' in cc_recipients, \
            f"Internal recipient 'manager@company.com' missing from CC: {cc_recipients}"

        # Domain-specific recipients should also be present
        # (all sample vessels are @prominencemaritime.com)
        assert 'prom1@test.com' in cc_recipients
        assert 'prom2@test.com' in cc_recipients

        # Should have 4 total CC recipients (2 domain + 2 internal)
        assert len(cc_recipients) == 4


def test_alert_internal_recipients_when_no_domain_match(mock_config):
    """Test that internal recipients are used when domain doesn't match routing."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    # Create dataframe with unknown domain
    unknown_domain_df = pd.DataFrame({
        'vessel_id': [999],
        'vessel': ['UNKNOWN VESSEL'],
        'vsl_email': ['unknown@unknowndomain.com'],  # Not in routing
        'job_id': [999],
        'importance': ['High'],
        'title': ['Unknown Domain Job'],
        'dispensation_type': ['Extension'],
        'department': ['Deck'],
        'due_date': ['2025-12-15'],
        'requested_on': ['2025-12-01'],
        'created_at': [datetime.now()],
        'status': ['for_approval']
    })

    # Set up internal recipients
    mock_config.internal_recipients = ['admin@company.com', 'manager@company.com']

    alert = FlagDispensationsAlert(mock_config)
    jobs = alert.route_notifications(unknown_domain_df)

    # Should have one job
    assert len(jobs) == 1

    # Should ONLY have internal recipients (no domain match)
    cc_recipients = jobs[0]['cc_recipients']
    assert 'admin@company.com' in cc_recipients
    assert 'manager@company.com' in cc_recipients
    assert len(cc_recipients) == 2  # Only internal, no domain-specific


def test_alert_deduplicates_cc_recipients(mock_config, sample_dataframe):
    """Test that duplicate emails in CC list are removed."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert

    # Set internal recipients to overlap with domain CC (prom1@test.com)
    mock_config.internal_recipients = ['prom1@test.com', 'admin@company.com']

    alert = FlagDispensationsAlert(mock_config)
    jobs = alert.route_notifications(sample_dataframe)

    # Check that duplicates are removed
    for job in jobs:
        cc_recipients = job['cc_recipients']

        # Should not have duplicates
        assert len(cc_recipients) == len(set(cc_recipients)), \
            f"Duplicate emails found in CC list: {cc_recipients}"

        # prom1@test.com should appear only once (even though it's in both lists)
        assert cc_recipients.count('prom1@test.com') == 1
        
        # Should have 3 unique recipients: prom1, prom2, admin (prom1 appears in both lists)
        assert len(cc_recipients) == 3


def test_alert_format_date_column(mock_config):
    """Test that _format_date_column formats dates correctly."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert

    alert = FlagDispensationsAlert(mock_config)

    # Create test dataframe with various date formats
    test_df = pd.DataFrame({
        'test_date': [
            pd.Timestamp('2025-12-01'),  # Use Timestamp instead of string
            pd.Timestamp('2025-12-15 10:30:00'),  # Use Timestamp
            None,
            pd.NaT
        ]
    })

    alert._format_date_column(test_df, 'test_date')

    # Check formatting
    assert test_df['test_date'].iloc[0] == '2025-12-01'
    assert test_df['test_date'].iloc[1] == '2025-12-15'
    assert test_df['test_date'].iloc[2] == ''  # None becomes empty string
    assert test_df['test_date'].iloc[3] == ''  # NaT becomes empty string


def test_alert_get_url_links_when_enabled(mock_config):
    """Test URL generation when links are enabled."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    mock_config.enable_links = True
    mock_config.base_url = 'https://prominence.orca.tools'
    mock_config.url_path = '/jobs/flag-extension-dispensation'
    
    alert = FlagDispensationsAlert(mock_config)
    
    url = alert._get_url_links(12345)
    
    assert url == 'https://prominence.orca.tools/jobs/flag-extension-dispensation/12345'


def test_alert_get_url_links_when_disabled(mock_config):
    """Test URL generation when links are disabled."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    mock_config.enable_links = False
    
    alert = FlagDispensationsAlert(mock_config)
    
    url = alert._get_url_links(12345)
    
    assert url is None


def test_alert_url_links_added_to_dataframe(mock_config, sample_dataframe):
    """Test that URL links are added to dataframe when enabled."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    mock_config.enable_links = True
    mock_config.base_url = 'https://prominence.orca.tools'
    mock_config.url_path = '/jobs/flag-extension-dispensation'
    
    alert = FlagDispensationsAlert(mock_config)
    jobs = alert.route_notifications(sample_dataframe)
    
    # Check that jobs have URL column
    for job in jobs:
        assert 'url' in job['data'].columns
        # Check that URLs are not null
        assert job['data']['url'].notna().all()


def test_alert_display_columns_specified(mock_config, sample_dataframe):
    """Test that display_columns are specified in metadata."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    alert = FlagDispensationsAlert(mock_config)
    jobs = alert.route_notifications(sample_dataframe)
    
    expected_display_columns = [
        'title',
        'dispensation_type',
        'department',
        'requested_on',
        'due_date',
        'created_at'
    ]
    
    for job in jobs:
        assert 'display_columns' in job['metadata']
        assert job['metadata']['display_columns'] == expected_display_columns


def test_alert_get_company_name_prominence(mock_config):
    """Test company name determination for Prominence."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    alert = FlagDispensationsAlert(mock_config)
    
    company = alert._get_company_name('vessel@prominencemaritime.com')
    assert company == 'Prominence Maritime S.A.'
    
    company = alert._get_company_name('vessel@vsl.prominencemaritime.com')
    assert company == 'Prominence Maritime S.A.'


def test_alert_get_company_name_seatraders(mock_config):
    """Test company name determination for Seatraders."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    alert = FlagDispensationsAlert(mock_config)
    
    company = alert._get_company_name('vessel@seatraders.com')
    assert company == 'Sea Traders S.A.'
    
    company = alert._get_company_name('vessel@vsl.seatraders.com')
    assert company == 'Sea Traders S.A.'


def test_alert_get_company_name_default(mock_config):
    """Test company name determination for unknown domain."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    alert = FlagDispensationsAlert(mock_config)
    
    company = alert._get_company_name('vessel@unknown.com')
    assert company == 'Prominence Maritime S.A.'  # Default


def test_alert_filter_replaces_null_values(mock_config):
    """Test that filter_data replaces null values with empty strings."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    # Create dataframe with null values
    test_df = pd.DataFrame({
        'vessel_id': [123],
        'vessel': ['TEST VESSEL'],
        'vsl_email': ['test@test.com'],
        'job_id': [456],
        'importance': [None],  # Null
        'title': ['Test'],
        'dispensation_type': [None],  # Null
        'department': [None],  # Null
        'due_date': ['2025-12-15'],
        'requested_on': ['2025-12-01'],
        'created_at': [datetime.now()],
        'status': ['for_approval']
    })
    
    alert = FlagDispensationsAlert(mock_config)
    filtered = alert.filter_data(test_df)
    
    # Check that nulls are replaced with empty strings
    assert filtered['importance'].iloc[0] == ''
    assert filtered['dispensation_type'].iloc[0] == ''
    assert filtered['department'].iloc[0] == ''


def test_alert_filter_formats_dates_correctly(mock_config, sample_dataframe):
    """Test that filter_data formats dates correctly."""
    from src.alerts.flag_dispensations_alert import FlagDispensationsAlert
    
    alert = FlagDispensationsAlert(mock_config)
    filtered = alert.filter_data(sample_dataframe)
    
    # created_at should be formatted as datetime string
    for created_at in filtered['created_at']:
        # Should match format: YYYY-MM-DD HH:MM:SS
        assert len(created_at) == 19
        assert created_at[4] == '-'
        assert created_at[7] == '-'
        assert created_at[10] == ' '
        assert created_at[13] == ':'
        assert created_at[16] == ':'
    
    # due_date and requested_on should be formatted as date strings
    for due_date in filtered['due_date']:
        # Should match format: YYYY-MM-DD
        assert len(due_date) == 10
        assert due_date[4] == '-'
        assert due_date[7] == '-'
