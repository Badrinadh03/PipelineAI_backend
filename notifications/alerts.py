import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import settings

logger = logging.getLogger(__name__)


async def send_slack_alert(dag_id: str, dag_run_id: str, analysis: str) -> bool:
    """Send a failure alert with the AI analysis to Slack."""
    if not settings.slack_bot_token or not settings.slack_channel_id:
        logger.info("Slack not configured, skipping alert.")
        return False

    try:
        from slack_sdk.web.async_client import AsyncWebClient

        client = AsyncWebClient(token=settings.slack_bot_token)

        short_analysis = analysis[:2900] + "..." if len(analysis) > 2900 else analysis

        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "Airflow Pipeline Failure Detected"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:*\n`{dag_id}`"},
                    {"type": "mrkdwn", "text": f"*Run ID:*\n`{dag_run_id}`"},
                ],
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*AI Diagnosis:*\n{short_analysis}",
                },
            },
        ]

        await client.chat_postMessage(
            channel=settings.slack_channel_id,
            text=f"Airflow failure in {dag_id}",
            blocks=blocks,
        )
        logger.info(f"Slack alert sent for {dag_id}/{dag_run_id}")
        return True

    except Exception as e:
        logger.error(f"Failed to send Slack alert: {e}")
        return False


async def send_email_alert(dag_id: str, dag_run_id: str, analysis: str) -> bool:
    """Send a failure alert email with the AI analysis."""
    if not settings.smtp_username or not settings.alert_email_to:
        logger.info("Email not configured, skipping alert.")
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[Airflow Alert] Pipeline failure: {dag_id}"
        msg["From"] = settings.smtp_username
        msg["To"] = settings.alert_email_to

        html_body = f"""
        <html><body style="font-family: sans-serif; max-width: 700px; margin: auto;">
          <h2 style="color: #dc2626;">Airflow Pipeline Failure</h2>
          <table style="border-collapse: collapse; width: 100%; margin-bottom: 20px;">
            <tr>
              <td style="padding: 8px; background: #f3f4f6; font-weight: bold; width: 120px;">DAG</td>
              <td style="padding: 8px; font-family: monospace;">{dag_id}</td>
            </tr>
            <tr>
              <td style="padding: 8px; background: #f3f4f6; font-weight: bold;">Run ID</td>
              <td style="padding: 8px; font-family: monospace;">{dag_run_id}</td>
            </tr>
          </table>
          <h3>AI Diagnosis</h3>
          <div style="background: #f9fafb; border-left: 4px solid #3b82f6; padding: 16px; white-space: pre-wrap;">{analysis}</div>
        </body></html>
        """

        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as server:
            server.starttls()
            server.login(settings.smtp_username, settings.smtp_password)
            server.sendmail(settings.smtp_username, settings.alert_email_to, msg.as_string())

        logger.info(f"Email alert sent for {dag_id}/{dag_run_id}")
        return True

    except Exception as e:
        logger.error(f"Failed to send email alert: {e}")
        return False
