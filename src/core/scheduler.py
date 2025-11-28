#src/core/scheduler.py
"""
Scheduling system for running alerts at regular intervals.

Handles graceful shutdown, error recovery, and interval-based execution.
"""
import signal
import threading
import logging
from datetime import datetime, timedelta
from src.formatters.date_formatter import duration
from zoneinfo import ZoneInfo
from typing import Callable, List
import pandas as pd

logger = logging.getLogger(__name__)


class AlertScheduler:
    """
    Scheduler for running alerts at regular intervals.
    
    Supports graceful shutdown, multiple alerts, and error recovery.
    """
    
    def __init__(self, frequency_hours: float, timezone: str):
        """
        Initialize scheduler.
        
        Args:
            frequency_hours: Hours between alert runs
            timezone: Timezone for scheduling and logging
        """
        self.frequency_hours = frequency_hours
        self.timezone = ZoneInfo(timezone)
        self.shutdown_event = threading.Event()
        self._alerts: List[Callable] = []
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        self.shutdown_event.set()
    
    def register_alert(self, alert_runner: Callable) -> None:
        """
        Register an alert to be run on schedule.
        
        Args:
            alert_runner: Callable that executes the alert (typically alert.run())
        """
        self._alerts.append(alert_runner)
        logger.info(f"Registered alert: {alert_runner.__name__ if hasattr(alert_runner, '__name__') else 'anonymous'}")
    
    def _run_all_alerts(self) -> None:
        """Execute all registered alerts."""
        if not self._alerts:
            logger.warning("No alerts registered. Nothing to run.")
            return
        
        logger.info(f"Running {len(self._alerts)} alert(s)...")
        
        for idx, alert_runner in enumerate(self._alerts, 1):
            if self.shutdown_event.is_set():
                logger.info("Shutdown requested. Stopping alert execution.")
                break
            
            try:
                logger.info(f"Executing alert {idx}/{len(self._alerts)}...")
                alert_runner()
            except Exception as e:
                logger.exception(f"Error executing alert {idx}: {e}")
                # Continue with next alert despite error
    
    def run_once(self) -> None:
        """
        Run all alerts once and exit.
        
        Useful for manual execution or testing.
        """
        logger.info("=" * 60)
        logger.info("▶ RUN-ONCE MODE: Executing alerts once without scheduling")
        logger.info("=" * 60)
        
        self._run_all_alerts()
        
        logger.info("=" * 60)
        logger.info("◼ RUN-ONCE COMPLETE")
        logger.info("=" * 60)
    
    def run_continuous(self) -> None:
        """
        Run alerts continuously at scheduled intervals.
        
        Runs immediately on startup, then repeats every frequency_hours.
        Handles graceful shutdown and error recovery.
        """
        logger.info("=" * 60)
        logger.info(f"▶ SCHEDULER STARTED")
        logger.info(f"Frequency: Every {duration(self.frequency_hours)}")
        logger.info(f"Timezone: {self.timezone}")
        logger.info(f"Registered alerts: {len(self._alerts)}")
        logger.info("=" * 60)
        
        while not self.shutdown_event.is_set():
            try:
                # Run all alerts
                self._run_all_alerts()
                
                # Check if shutdown was requested during execution
                if self.shutdown_event.is_set():
                    break
                
                # Calculate next run time
                sleep_seconds = self.frequency_hours * 3600
                next_run = datetime.now(tz=self.timezone) + timedelta(hours=self.frequency_hours)
                
                logger.info(f"Sleeping for {duration(self.frequency_hours)}")
                logger.info(f"Next run scheduled at: {next_run.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                
                # Use shutdown_event.wait() for interruptible sleep
                if self.shutdown_event.wait(timeout=sleep_seconds):
                    logger.info("Shutdown requested during sleep period")
                    break
            
            except KeyboardInterrupt:
                logger.info("Keyboard interrupt received. Shutting down...")
                break
            
            except Exception as e:
                logger.exception(f"Unhandled exception in scheduler loop: {e}")
                # Wait before retrying to avoid rapid failure loops
                if not self.shutdown_event.is_set():
                    logger.info("Waiting 5 minutes before retry...")
                    if self.shutdown_event.wait(timeout=300):
                        logger.info("Shutdown requested during error recovery wait")
                        break
        
        logger.info("=" * 60)
        logger.info("⏹ SCHEDULER STOPPED")
        logger.info("=" * 60)
