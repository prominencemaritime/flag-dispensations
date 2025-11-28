# tests/test_passage_plan_alert.py
"""
Tests for PassagePlanAlert logic.
"""
import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock


def test_alert_initializes_correctly(mock_config):
    """Test that alert initializes with correct configuration."""
    from src.alerts.passage_plan_alert import PassagePlanAlert
    
    alert = PassagePlanAlert(mock_config)
    
    assert alert.sql_query_file == 'PassagePlan.sql'
    assert alert.lookback_days == mock_config.lookback_days


def test_alert_filters_data_by_lookback_days(mock_config, sample_dataframe):
    """Test that filter_data correctly filters by lookback days."""
    from src.alerts.passage_plan_alert import PassagePlanAlert
    
    alert = PassagePlanAlert(mock_config)
    alert.lookback_days = 1  # Last 24 hours
    
    # All sample data is within last 24 hours
    filtered = alert.filter_data(sample_dataframe)
    
    assert len(filtered) == 4  # All records


def test_alert_filters_out_old_data(mock_config, sample_dataframe):
    """Test that old data is filtered out."""
    from src.alerts.passage_plan_alert import PassagePlanAlert
    
    # Create old record by copying a row and modifying it
    old_record = sample_dataframe.iloc[[0]].copy()
    old_record['vessel_id'] = 999
    old_record['vessel_name'] = 'OLD VESSEL'
    old_record['vsl_email'] = 'old@test.com'
    old_record['event_id'] = 999
    old_record['event_name'] = 'Old Passage Plan'
    old_record['synced_at'] = datetime.now() - timedelta(days=5)
    old_record['created_at'] = datetime.now() - timedelta(days=6)
    
    df_with_old = pd.concat([sample_dataframe, old_record], ignore_index=True)
    
    alert = PassagePlanAlert(mock_config)
    alert.lookback_days = 1
    
    filtered = alert.filter_data(df_with_old)
    
    # Should exclude the old record
    assert len(filtered) == 4
    assert 999 not in filtered['event_id'].values


def test_alert_routes_by_vessel(mock_config, sample_dataframe):
    """Test that notifications are routed correctly by vessel."""
    from src.alerts.passage_plan_alert import PassagePlanAlert
    
    alert = PassagePlanAlert(mock_config)
    jobs = alert.route_notifications(sample_dataframe)
    
    # Should create 3 jobs (SERIFOS I with 2 events, AGRIA with 1, BALI with 1)
    assert len(jobs) == 3
    
    # Check SERIFOS I job
    serifos_job = next(j for j in jobs if j['metadata']['vessel_name'] == 'SERIFOS I')
    assert len(serifos_job['data']) == 2
    assert serifos_job['recipients'] == ['serifos.i@vsl.prominencemaritime.com']


def test_alert_assigns_correct_cc_recipients(mock_config, sample_dataframe):
    """Test that CC recipients are assigned based on email domain plus internal recipients."""
    from src.alerts.passage_plan_alert import PassagePlanAlert

    alert = PassagePlanAlert(mock_config)
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


def test_alert_generates_correct_subject_lines(mock_config, sample_dataframe):
    """Test subject line generation."""
    from src.alerts.passage_plan_alert import PassagePlanAlert
    
    alert = PassagePlanAlert(mock_config)
    
    # Single record
    single_df = sample_dataframe.iloc[:1]
    subject_single = alert.get_subject_line(single_df, {'vessel_name': 'TEST VESSEL'})
    assert subject_single == "AlertDev | TEST VESSEL Passage Plan"
    
    # Multiple records (same subject for passage plans)
    multi_df = sample_dataframe.iloc[:3]
    subject_multi = alert.get_subject_line(multi_df, {'vessel_name': 'TEST VESSEL'})
    assert subject_multi == "AlertDev | TEST VESSEL Passage Plan"


def test_alert_generates_correct_tracking_keys(mock_config, sample_dataframe):
    """Test that tracking keys are generated correctly."""
    from src.alerts.passage_plan_alert import PassagePlanAlert
    
    alert = PassagePlanAlert(mock_config)
    
    row = sample_dataframe.iloc[0]
    key = alert.get_tracking_key(row)
    
    # New format: vessel_id_{X}__event_type_{Y}__event_id_{Z}
    expected_key = f"vessel_id_{row['vessel_id']}__event_type_{row['event_type_id']}__event_id_{row['event_id']}"
    assert key == expected_key


def test_alert_required_columns_validation(mock_config):
    """Test that required columns are correctly defined."""
    from src.alerts.passage_plan_alert import PassagePlanAlert
    
    alert = PassagePlanAlert(mock_config)
    required = alert.get_required_columns()
    
    # New passage plan schema
    assert 'vessel_id' in required
    assert 'event_id' in required
    assert 'event_type_id' in required
    assert 'vsl_email' in required
    assert 'event_name' in required
    assert 'synced_at' in required
    assert 'status' in required


def test_alert_validates_dataframe_columns(mock_config, sample_dataframe):
    """Test that DataFrame validation works correctly."""
    from src.alerts.passage_plan_alert import PassagePlanAlert
    
    alert = PassagePlanAlert(mock_config)
    
    # Should not raise exception with valid DataFrame
    alert.validate_required_columns(sample_dataframe)
    
    # Should raise exception with missing column
    invalid_df = sample_dataframe.drop(columns=['vessel_id'])
    with pytest.raises(ValueError, match="Missing required columns"):
        alert.validate_required_columns(invalid_df)


def test_alert_includes_internal_recipients_in_cc(mock_config, sample_dataframe):
    """Test that internal recipients are always included in CC."""
    from src.alerts.passage_plan_alert import PassagePlanAlert

    # Set up internal recipients
    mock_config.internal_recipients = ['admin@company.com', 'manager@company.com']

    alert = PassagePlanAlert(mock_config)
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
    from src.alerts.passage_plan_alert import PassagePlanAlert
    import pandas as pd
    from datetime import datetime

    # Create dataframe with unknown domain
    unknown_domain_df = pd.DataFrame({
        'vessel_id': [999],
        'vessel_name': ['UNKNOWN VESSEL'],
        'vsl_email': ['unknown@unknowndomain.com'],  # Not in routing
        'event_type_id': [37],
        'event_type_name': ['Passage Plan'],
        'event_id': [999],
        'event_name': ['Unknown Route'],
        'status_id': [3],
        'status': ['for-review'],
        'created_at': [datetime.now()],
        'synced_at': [datetime.now()]
    })

    # Set up internal recipients
    mock_config.internal_recipients = ['admin@company.com', 'manager@company.com']

    alert = PassagePlanAlert(mock_config)
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
    from src.alerts.passage_plan_alert import PassagePlanAlert

    # Set internal recipients to overlap with domain CC
    mock_config.internal_recipients = ['prom1@test.com', 'admin@company.com']

    alert = PassagePlanAlert(mock_config)
    jobs = alert.route_notifications(sample_dataframe)

    # Check that duplicates are removed
    for job in jobs:
        cc_recipients = job['cc_recipients']

        # Should not have duplicates
        assert len(cc_recipients) == len(set(cc_recipients)), \
            f"Duplicate emails found in CC list: {cc_recipients}"

        # prom1@test.com should appear only once (even though it's in both lists)
        assert cc_recipients.count('prom1@test.com') == 1
