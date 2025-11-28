# tests/test_email_sender.py
"""
Tests for email sending functionality.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from src.notifications.email_sender import EmailSender


def test_email_sender_initializes_correctly():
    """Test that EmailSender initializes with correct parameters."""
    sender = EmailSender(
        smtp_host='smtp.test.com',
        smtp_port=465,
        smtp_user='test@test.com',
        smtp_pass='password',
        company_logos={},
        dry_run=False
    )

    assert sender.smtp_host == 'smtp.test.com'
    assert sender.smtp_port == 465
    assert sender.dry_run is False


def test_email_sender_blocks_in_dry_run_mode():
    """Test that EmailSender blocks sends in dry-run mode."""
    sender = EmailSender(
        smtp_host='smtp.test.com',
        smtp_port=465,
        smtp_user='test@test.com',
        smtp_pass='password',
        company_logos={},
        dry_run=True
    )

    with pytest.raises(RuntimeError, match="SAFETY CHECK FAILED"):
        sender.send(
            subject='Test',
            plain_text='Test',
            html_content='<html>Test</html>',
            recipients=['test@test.com']
        )


@patch('smtplib.SMTP_SSL')
def test_email_sender_sends_successfully(mock_smtp):
    """Test that EmailSender sends email successfully."""
    # Mock SMTP server
    mock_server = MagicMock()
    mock_smtp.return_value.__enter__.return_value = mock_server

    sender = EmailSender(
        smtp_host='smtp.test.com',
        smtp_port=465,
        smtp_user='test@test.com',
        smtp_pass='password',
        company_logos={},
        dry_run=False
    )

    sender.send(
        subject='Test Subject',
        plain_text='Test Body',
        html_content='<html>Test</html>',
        recipients=['recipient@test.com'],
        cc_recipients=['cc@test.com']
    )

    # Verify SMTP methods were called
    mock_server.login.assert_called_once()
    mock_server.send_message.assert_called_once()


@patch('smtplib.SMTP_SSL')
def test_email_sender_includes_cc_recipients(mock_smtp):
    """Test that CC recipients are included in email."""
    mock_server = MagicMock()
    mock_smtp.return_value.__enter__.return_value = mock_server

    sender = EmailSender(
        smtp_host='smtp.test.com',
        smtp_port=465,
        smtp_user='test@test.com',
        smtp_pass='password',
        company_logos={},
        dry_run=False
    )

    sender.send(
        subject='Test',
        plain_text='Test',
        html_content='<html>Test</html>',
        recipients=['to@test.com'],
        cc_recipients=['cc1@test.com', 'cc2@test.com']
    )

    # Get the message that was sent
    call_args = mock_server.send_message.call_args
    msg = call_args[0][0]

    assert 'cc1@test.com' in msg['Cc']
    assert 'cc2@test.com' in msg['Cc']
