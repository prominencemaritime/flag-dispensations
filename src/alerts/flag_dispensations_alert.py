#src/alerts/flag_dispensations_alert.py
"""Flag Dispensations Alert Implementation.""" 
from typing import Dict, List, Optional
import pandas as pd 
from datetime import datetime, timedelta 
from zoneinfo import ZoneInfo
from sqlalchemy import text
import logging
 
from src.core.base_alert import BaseAlert 
from src.core.config import AlertConfig 
from src.db_utils import get_db_connection, validate_query_file 


logger = logging.getLogger(__name__)


class FlagDispensationsAlert(BaseAlert):
    """Alert for Flag Dispensations jobs"""

    def __init__(self, config: AlertConfig):
        """
        Initialise passage plan alert
        
        Args:
            config: AlertConfig instance
        """
        super().__init__(config)

        # Load query + lookback
        self.sql_query_file = 'FlagDispensations.sql'
        self.lookback_days = config.lookback_days
        self.job_status = config.job_status

        # Log instantiation
        self.logger.info(f"[OK] FlagDispensationsAlert instance created")

        
    def fetch_data(self) -> pd.DataFrame:
        """
        Fetch flag dispensations from database

        Returns:
            DataFrame with columns: 
                
                vsl_email, 
                vessel, 
                job_id,
                importance,
                title,
                dispensation_type,
                department,
                due_date,
                requested_on,
                created_at,
                status
        """
        # Load SQL query
        query_path = self.config.queries_dir / self.sql_query_file
        query_sql = validate_query_file(query_path)

        # Bind params to the query
        params = {
                "lookback_days": self.lookback_days,
                "job_status": self.job_status
                }
        query = text(query_sql)

        # Execute Query
        with get_db_connection() as conn:
            df = pd.read_sql_query(query, conn, params=params)

        self.logger.info(f"FlagDispensationsAlert.fetch_data() is returning a df with {len(df)} rows and {len(df.keys())} columns")
        self.logger.debug(f"df Columns: {[key for key in df.keys()]}")
        return df


    def filter_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter for entries synced in the last lookback_days
    
        Args:
            df: Raw pd.DataFrame from database

        Returns:
            Filtered pd.DataFrame with only recently udpated entries

        Note: this filter preserves the number of columns - which columns are going to be displayed is specified in formatter
        """
        if df.empty:
            return df

        # Timezone awareness
        df['created_at'] = pd.to_datetime(df['created_at'])

        # If the datetime is timezone-naive, localise it to UTC first, then convert to timezone specified in .env 
        # I am assuming all times appearing are UTC, and then converting to TIMEZONE='Europe/Athens' will automatically be correct during Winter (UTC+2) and Summer (UTC+3).
        if df['created_at'].dt.tz is None:
            df['created_at'] = df['created_at'].dt.tz_localize('UTC').dt.tz_convert(self.config.timezone)
        else:
            # If already timezone-aware, convert to timezone specified in .env
            df['created_at'] = df['created_at'].dt.tz_convert(self.config.timezone)

        # Calculate cutoff date (timezone-aware)
        cutoff_date = datetime.now(tz=ZoneInfo(self.config.timezone)) - timedelta(days=self.lookback_days)

        # Filter for recent sync (timezone-aware) corresponding to config.lookback_days
        df_filtered = df[df['created_at'] >= cutoff_date].copy()

        # Format dates for display
        df_filtered['created_at'] = df_filtered['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

        self._format_date_column(df_filtered, 'due_date')
        self._format_date_column(df_filtered, 'requested_on')

        # Replace null values by ''
        for col in ['importance', 'dispensation_type', 'department']:
            if col in df_filtered.columns:
                df_filtered[col] = df_filtered[col].fillna('')


        self.logger.info(f"Filtered to {len(df_filtered)} entr{'y' if len(df_filtered)==1 else 'ies'} synced with LOOKBACK={self.lookback_days} day{'' if len(df_filtered)==1 else 's'}")

        return df_filtered


    def _format_date_column(self, df: pd.DataFrame, col: str) -> None:
        """
        Modifies the DataFrame in place
        """
        if col in df.columns:
            df[col] = (
                pd.to_datetime(df[col], errors='coerce')
                .dt.strftime('%Y-%m-%d')
                .fillna('')
            )


    def _get_url_links(self, link_id: int) -> Optional[str]:
        """
        Generate URL if links are enabled.

        Constructs URL by combining:
            - BASE_URL from config (e.g. https://prominence.orca.tools)
            - URL_PATH from config (e.g. /jobs/flag-extension-dispensation/)
            - link_id from database (e.g. 123)
        Result: https://prominence.orca.tools/events/123

        Args:
            link_id: in PassagePlan project, given by event.id = event_id

        Returns:
            Complete URL, or None if links are disabled
        """
        if not self.config.enable_links:
            return None

        # Build URL: BASE_URL + URL_PATH + link_id
        base_url = self.config.base_url.rstrip('/')
        url_path = self.config.url_path.rstrip('/')
        full_url = f"{base_url}{url_path}/{link_id}"

        return full_url


    def route_notifications(self, df:pd.DataFrame) -> List[Dict]:
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
        jobs = []

        # Group by vessel
        grouped = df.groupby(['vsl_email', 'vessel'])

        for (vessel_email, vessel_name), vessel_df in grouped:
            # Determine cc recipients
            cc_recipients = self._get_cc_recipients(vessel_email)

            # Add URLs to dataframe if ENABLE_LINKS
            if self.config.enable_links:
                vessel_df = vessel_df.copy()
                vessel_df['url'] = vessel_df['job_id'].apply(
                        self._get_url_links
                )

            # Keep full data with tracking columns for the job
            # The formatter will handle which columns to display
            full_data = vessel_df.copy()

            # Specify WHICH cols to display in email and in what order here
            display_columns = [
                    #'vessel',
                    #'job_id',
                    #'importance',
                    'title',
                    'dispensation_type',
                    'department',
                    'requested_on',
                    'due_date',
                    'created_at'
                    #'status'
            ]

            # Create notification job
            job = {
                    'recipients': [vessel_email],
                    'cc_recipients': cc_recipients,
                    'data': full_data,
                    'metadata': {
                        'vessel_id': vessel_df['vessel_id'].iloc[0],
                        'vessel_name': vessel_name,
                        'alert_title': 'Flag Dispensations',
                        'company_name': self._get_company_name(vessel_email),
                        'display_columns': display_columns
                    }
            }

            jobs.append(job)

            self.logger.info(
                    f"Created notification for vessel '{vessel_name}' "
                    f"({len(full_data)} document{'' if len(full_data)==1 else 's'}) -> {vessel_email} "
                    f"(CC: {len(cc_recipients)})"
            )

        return jobs


    def _get_cc_recipients(self, vessel_email: str) -> List[str]:
        """
        Determine CC recipients based on vessel email domain.
        Always includes internal recipients.

        Args:
            vessel_email: Vessel's email address

        Returns:
            List of CC email addresses (domain-specific + internal)
        """
        vessel_email_lower = vessel_email.lower()

        # Start with empty list
        cc_list = []

        # Check each configured domain
        entry = 0
        total_entries = len(self.config.email_routing.items())
        for domain, recipients_config in self.config.email_routing.items():
            entry += 1
            if domain.lower() in vessel_email_lower:
                cc_list = recipients_config.get('cc', [])
                break
            else:
                self.logger.info(f"Entry {entry}/{total_entries}: No domain match for vessel_email={vessel_email} (only including internal CC recipients)")

        # Always add internal recipients to CC list
        all_cc_recipients = list(set(cc_list + self.config.internal_recipients))

        return all_cc_recipients


    def _get_company_name(self, vessel_email: str) -> str:
        """
        Determine company name based on vessel email domain.
        
        Args:
            vessel_email: Vessel's email address
            
        Returns:
            Company name string
        """
        vessel_email_lower = vessel_email.lower()
        
        if 'prominence' in vessel_email_lower:
            return 'Prominence Maritime S.A.'
        elif 'seatraders' in vessel_email_lower:
            return 'Sea Traders S.A.'
        else:
            return 'Prominence Maritime S.A.'   # Default company name


    def get_tracking_key(self, row:pd.Series) -> str:
        """
        Generate unique tracking key for a data row.

        This key is used to prevent duplicate notifications.

        Args:
            row: Single row from DataFrame

        Returns:
            Unique string key (e.g., "vessel_123_doc_456")
        """
        try:
            vessel_id = row['vessel_id']
            job_id = row['job_id']

            return f"vessel_id_{vessel_id}__job_id_{job_id}"

        except KeyError as e:
            self.logger.error(f"Missing column in row for tracking key: {e}")
            self.logger.error(f"Available columns: {list(row.index)}")
            raise


    def get_subject_line(self, data: pd.DataFrame, metadata: Dict) -> str:
        """
        Generate email subject line for a notification.

        Args:
            data: DataFrame for this notification
            metadata: Additional context (vessel name, etc.)

        Returns:
            Email subject string
        """
        vessel_name = metadata.get('vessel_name', 'Vessel')
        return f"AlertDev | {vessel_name.upper()} Flag Extensions-Dispensations"


    def get_required_columns(self) -> List[str]:
        """
        Return list of column names required in the DataFrame.

        Returns:
            List of required column names
        """
        return [
            'vsl_email',
            'vessel_id',
            'vessel',
            'job_id',
            'importance',
            'title',
            'dispensation_type',
            'department',
            'due_date',
            'requested_on',
            'created_at',
            'status'
        ]
