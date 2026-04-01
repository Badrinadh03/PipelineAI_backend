from pydantic import BaseModel
from dotenv import load_dotenv
import os
from pathlib import Path

# Always load the backend's own .env, regardless of where `uvicorn` is started from.
load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

class Settings(BaseModel):
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4o")
    airflow_base_url: str = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")
    airflow_username: str = os.getenv("AIRFLOW_USERNAME", "airflow")
    airflow_password: str = os.getenv("AIRFLOW_PASSWORD", "airflow")
    slack_bot_token: str = os.getenv("SLACK_BOT_TOKEN", "")
    slack_channel_id: str = os.getenv("SLACK_CHANNEL_ID", "")
    smtp_host: str = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port: int = int(os.getenv("SMTP_PORT", "587"))
    smtp_username: str = os.getenv("SMTP_USERNAME", "")
    smtp_password: str = os.getenv("SMTP_PASSWORD", "")
    alert_email_to: str = os.getenv("ALERT_EMAIL_TO", "")
    poll_interval_seconds: int = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
    environment: str = os.getenv("ENVIRONMENT", "development")

settings = Settings()
