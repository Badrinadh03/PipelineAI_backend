import asyncio
import logging
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from airflow_client import airflow_client
from agent import analyze_once
from notifications import send_slack_alert, send_email_alert
from config import settings

logger = logging.getLogger(__name__)

# Track which run IDs we've already alerted on to avoid duplicates
_alerted_runs: set[str] = set()

# In-memory alert history (last 100)
alert_history: list[dict] = []
notifications_enabled = {"slack": True, "email": True}
scheduler: AsyncIOScheduler | None = None


async def poll_and_alert():
    """Check Airflow for new failures and trigger the agent."""
    logger.info("Polling Airflow for failed runs...")
    try:
        failed_runs = await airflow_client.get_all_failed_runs()
        new_failures = [r for r in failed_runs if r["dag_run_id"] not in _alerted_runs]

        for run in new_failures[:3]:  # process max 3 at a time
            dag_id = run["dag_id"]
            dag_run_id = run["dag_run_id"]
            _alerted_runs.add(dag_run_id)

            logger.info(f"New failure detected: {dag_id} / {dag_run_id}")

            try:
                result = await analyze_once(dag_id, dag_run_id)
                analysis = result.get("analysis", "Analysis unavailable.")

                entry = {
                    "id": dag_run_id,
                    "dag_id": dag_id,
                    "dag_run_id": dag_run_id,
                    "analysis": analysis,
                    "detected_at": datetime.utcnow().isoformat(),
                    "slack_sent": False,
                    "email_sent": False,
                }

                if notifications_enabled.get("slack"):
                    entry["slack_sent"] = await send_slack_alert(dag_id, dag_run_id, analysis)

                if notifications_enabled.get("email"):
                    entry["email_sent"] = await send_email_alert(dag_id, dag_run_id, analysis)

                alert_history.insert(0, entry)
                if len(alert_history) > 100:
                    alert_history.pop()

            except Exception as e:
                logger.error(f"Failed to analyze {dag_id}/{dag_run_id}: {e}")

    except Exception as e:
        logger.error(f"Polling error: {e}")


def start_scheduler():
    global scheduler
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        poll_and_alert,
        "interval",
        seconds=settings.poll_interval_seconds,
        id="airflow_poll",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"Scheduler started — polling every {settings.poll_interval_seconds}s")


def stop_scheduler():
    global scheduler
    if scheduler:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")


def update_poll_interval(seconds: int):
    global scheduler
    settings.poll_interval_seconds = seconds
    if scheduler:
        scheduler.reschedule_job("airflow_poll", trigger="interval", seconds=seconds)
        logger.info(f"Poll interval updated to {seconds}s")
