# src/core/base_alert.py
"""
Abstract base class for all alert types.

All alert implementations must inherit from BaseAlert and implement
the abstract methods for data fetching, filtering, and routing.
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import logging

logger = logging.getLogger(__name__)


class BaseAlert(ABC):
    """
    Abstract base class for alert implementations.

    Each alert type (vessel documents, hot works, etc.) should inherit
    from this class and implement the required abstract methods.
    """

    def __init__(self, config: 'AlertConfig'):
        """
        Initialize alert with configuration.

        Args:
            config: AlertConfig instance with all necessary settings
        """
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @abstractmethod
    def fetch_data(self) -> pd.DataFrame:
        """
        Fetch data from database using the alert's SQL query.

        Returns:
            DataFrame with all records matching the query criteria
        """
        pass

    @abstractmethod
    def filter_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply alert-specific filtering logic to the fetched data.

        Args:
            df: Raw DataFrame from database

        Returns:
            Filtered DataFrame ready for notification
        """
        pass

    @abstractmethod
    def route_notifications(self, df: pd.DataFrame) -> List[Dict]:
        """
        Route data to appropriate recipients.

        Returns list of notification jobs, where each job is a dict with:
        - 'recipients': List[str] - primary email addresses
        - 'cc_recipients': List[str] - CC email addresses
        - 'data': pd.DataFrame - data for this specific notification
        - 'metadata': Dict - any additional info (vessel name, etc.)

        Args:
            df: Filtered DataFrame

        Returns:
            List of notification job dictionaries
        """
        pass

    @abstractmethod
    def get_tracking_key(self, row: pd.Series) -> str:
        """
        Generate unique tracking key for a data row.

        This key is used to prevent duplicate notifications.

        Args:
            row: Single row from DataFrame

        Returns:
            Unique string key (e.g., "vessel_123_doc_456")
        """
        pass

    @abstractmethod
    def get_subject_line(self, data: pd.DataFrame, metadata: Dict) -> str:
        """
        Generate email subject line for a notification.

        Args:
            data: DataFrame for this notification
            metadata: Additional context (vessel name, etc.)

        Returns:
            Email subject string
        """
        pass

    def validate_required_columns(self, df: pd.DataFrame) -> None:
        """
        Validate that DataFrame has all required columns.

        Default implementation - can be overridden by subclasses.

        Args:
            df: DataFrame to validate

        Raises:
            ValueError: If required columns are missing
        """
        if df.empty:
            return

        required = self.get_required_columns()
        missing = set(required) - set(df.columns)

        if missing:
            raise ValueError(
                f"{self.__class__.__name__}: Missing required columns: {missing}. "
                f"Available: {list(df.columns)}"
            )

    @abstractmethod
    def get_required_columns(self) -> List[str]:
        """
        Return list of column names required in the DataFrame.

        Returns:
            List of required column names
        """
        pass

    def run(self) -> bool:
        """
        Execute the complete alert workflow.

        Returns:
            True if notifications were sent successfully, False otherwise
        """
        run_time = datetime.now(tz=ZoneInfo(self.config.timezone))
        self.logger.info("=" * 60)
        self.logger.info(f"▶ {self.__class__.__name__} RUN STARTED")
        self.logger.info(f"Current time ({self.config.timezone}): {run_time.isoformat()}")

        try:
            # Step 1: Fetch data
            self.logger.info("--> Fetching data from database: df = self.fetch_data()")
            df = self.fetch_data()
            self.logger.info(f"[OK] Fetched len(df)={len(df)} record{'' if len(df)==1 else 's'}")

            if df.empty:
                self.logger.info("No records found matching query criteria: df.empty == True")
                return False

            # Step 2: Validate columns
            self.validate_required_columns(df)

            # Step 3: Filter data
            self.logger.info("--> Applying filtering logic: df_filtered = self.filter_data(df)")
            df_filtered = self.filter_data(df)
            self.logger.info(f"[OK] len(df_filtered)={len(df_filtered)} record{'' if len(df_filtered)==1 else 's'} after filtering")

            if df_filtered.empty:
                self.logger.info("No records after filtering: df_filtered.empty == True")
                return False

            # Step 4: Filter out already-sent events
            self.logger.info("--> Checking for previously sent notifications...")
            df_unsent = self.config.tracker.filter_unsent_events(
                df_filtered,
                key_func=self.get_tracking_key
            )

            if df_unsent.empty:
                self.logger.info("All records have been sent previously. No new notifications.")
                return False

            self.logger.info(f"[OK] len(df_unsent)={len(df_unsent)} new record{'' if len(df_unsent)==1 else 's'} to notify")

            # Step 5: Route to recipients
            self.logger.info("--> Routing notifications to recipients...")
            notification_jobs = self.route_notifications(df_unsent)
            self.logger.info(f"[OK] Created len(notification_jobs)={len(notification_jobs)} notification job{'' if len(notification_jobs)==1 else 's'}")

            # Step 6: Send notifications
            success = self._send_notifications(notification_jobs, run_time)

            return success

        except Exception as e:
            self.logger.exception(f"Error in {self.__class__.__name__}.run(): {e}")
            return False
        finally:
            self.logger.info(f"◼ {self.__class__.__name__} RUN COMPLETE")
            self.logger.info("=" * 60)


    def _send_notifications(self, jobs: List[Dict], run_time: datetime) -> bool:
        """
        Send all notification jobs and track sent events.
        
        Args:
            jobs: List of notification job dictionaries 
                (e.g. for Vessel Document Updates: jobs=VesselDocumentsAlert().route_notifications())
            run_time: Timestamp of this run
            
        Returns:
            True if any notifications sent successfully
        """
        sent_keys = set()
        any_sent = False
        
        for idx, job in enumerate(jobs, 1):
            self.logger.info(f"--> Sending notification {idx}/{len(jobs)}...")
            
            try:
                # Get notification components (args for format() method)
                original_recipients = job['recipients']
                original_cc_recipients = job.get('cc_recipients', [])
                data = job['data']
                metadata = job.get('metadata', {})
                
                # GENERATE FORMATTED EMAIL CONTENT
                base_subject = self.get_subject_line(data, metadata)
                plain_text = self.config.text_formatter.format(data, run_time, self.config, metadata)
                html_content = self.config.html_formatter.format(data, run_time, self.config, metadata, enable_links=self.config.enable_links)
                
                # Handle dry-run email redirection
                if self.config.dry_run and self.config.dry_run_email:
                    # Redirect to dry-run email address
                    recipients = [self.config.dry_run_email]
                    cc_recipients = []
                    
                    # Modify subject to show original recipients
                    subject = f"[DRY-RUN] {base_subject} (Original: {', '.join(original_recipients)})"
                    
                    self.logger.info(f"[DRY-RUN-EMAIL] Redirecting to: {self.config.dry_run_email}")
                    self.logger.info(f"[DRY-RUN-EMAIL] Original recipients: {', '.join(original_recipients)}")
                    if original_cc_recipients:
                        self.logger.info(f"[DRY-RUN-EMAIL] Original CC: {', '.join(original_cc_recipients)}")
                else:
                    # Normal mode: use actual recipients
                    recipients = original_recipients
                    cc_recipients = original_cc_recipients
                    subject = base_subject
                
                # Check if email alerts are enabled
                if self.config.enable_email_alerts:
                    # Send email
                    self.config.email_sender.send(
                        subject=subject,
                        plain_text=plain_text,
                        html_content=html_content,
                        recipients=recipients,
                        cc_recipients=cc_recipients
                    )
                    self.logger.info(f"[OK] Notification {idx} sent successfully")
                else:
                    self.logger.info(f"[DRY-RUN] Notification {idx} prepared but NOT sent (emails disabled)")
                    self.logger.info(f"[DRY-RUN] Would send to: {', '.join(recipients)}")
                    if cc_recipients:
                        self.logger.info(f"[DRY-RUN] Would CC: {', '.join(cc_recipients)}")
                    self.logger.info(f"[DRY-RUN] Subject: {subject}")
                    self.logger.info(f"[DRY-RUN] Records: {len(data)}")
                
                # Track sent events (even in dry-run for testing tracking logic)
                for _, row in data.iterrows():
                    tracking_key = self.get_tracking_key(row)
                    sent_keys.add(tracking_key)
                
                any_sent = True
                
            except Exception as e:
                self.logger.error(f"Failed to send notification {idx}: {e}")
                continue
        
        # Save tracking data (in dry-run mode, still track to test the logic)
        if sent_keys:
            if self.config.enable_email_alerts:
                self.config.tracker.mark_as_sent(sent_keys, run_time)
                self.logger.info(f"[OK] Marked {len(sent_keys)} event(s) as sent")
            else:
                self.logger.info(f"[DRY-RUN] Would mark {len(sent_keys)} event(s) as sent (tracking disabled in dry-run)")
        
        return any_sent
