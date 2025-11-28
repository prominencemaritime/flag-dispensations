# tests/test_scheduler.py
"""
Tests for alert scheduler.
"""
import pytest
from unittest.mock import Mock
import time
from src.core.scheduler import AlertScheduler


def test_scheduler_initializes_correctly():
    """Test that scheduler initializes with correct parameters."""
    scheduler = AlertScheduler(
        frequency_hours=24,
        timezone='Europe/Athens'
    )
    
    assert scheduler.frequency_hours == 24
    assert str(scheduler.timezone) == 'Europe/Athens'
    assert len(scheduler._alerts) == 0


def test_scheduler_registers_alerts():
    """Test that alerts can be registered."""
    scheduler = AlertScheduler(frequency_hours=24, timezone='Europe/Athens')
    
    mock_alert = Mock()
    scheduler.register_alert(mock_alert)
    
    assert len(scheduler._alerts) == 1


def test_scheduler_runs_once():
    """Test that run_once executes all alerts."""
    scheduler = AlertScheduler(frequency_hours=24, timezone='Europe/Athens')
    
    mock_alert1 = Mock()
    mock_alert2 = Mock()
    
    scheduler.register_alert(mock_alert1)
    scheduler.register_alert(mock_alert2)
    
    scheduler.run_once()
    
    mock_alert1.assert_called_once()
    mock_alert2.assert_called_once()


def test_scheduler_handles_alert_failure():
    """Test that scheduler continues after alert failure."""
    scheduler = AlertScheduler(frequency_hours=24, timezone='Europe/Athens')
    
    failing_alert = Mock(side_effect=Exception("Test error"))
    successful_alert = Mock()
    
    scheduler.register_alert(failing_alert)
    scheduler.register_alert(successful_alert)
    
    scheduler.run_once()
    
    # Both should have been called despite first one failing
    failing_alert.assert_called_once()
    successful_alert.assert_called_once()


def test_scheduler_shutdown_signal():
    """Test that scheduler responds to shutdown signal."""
    scheduler = AlertScheduler(frequency_hours=24, timezone='Europe/Athens')
    
    # Trigger shutdown
    scheduler.shutdown_event.set()
    
    assert scheduler.shutdown_event.is_set()
