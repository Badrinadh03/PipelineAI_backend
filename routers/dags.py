from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from airflow_client import airflow_client
from agent import run_agent, analyze_once
from config import settings

router = APIRouter(prefix="/api", tags=["dags"])


def _airflow_502(exc: Exception) -> HTTPException:
    reason = str(exc).strip() or f"{type(exc).__name__}"
    return HTTPException(
        status_code=502,
        detail=(
            f"Cannot reach Airflow at {settings.airflow_base_url}: {reason}. "
            "If using Docker, ensure the stack is healthy (`docker compose ps`); "
            "see airflow-local/docker-compose.override.yml for webserver worker settings. "
            "Confirm AIRFLOW_BASE_URL, AIRFLOW_USERNAME, and AIRFLOW_PASSWORD in backend/.env."
        ),
    )


@router.get("/health")
async def health():
    airflow_ok = await airflow_client.health_check()
    return {"status": "ok", "airflow_connected": airflow_ok}


@router.get("/dags")
async def list_dags():
    try:
        return await airflow_client.get_dags()
    except Exception as e:
        raise _airflow_502(e)


@router.get("/dags/{dag_id}")
async def get_dag(dag_id: str):
    try:
        return await airflow_client.get_dag(dag_id)
    except Exception as e:
        raise _airflow_502(e)


@router.get("/dags/{dag_id}/runs")
async def get_dag_runs(dag_id: str, limit: int = 10):
    try:
        return await airflow_client.get_dag_runs(dag_id, limit=limit)
    except Exception as e:
        raise _airflow_502(e)


@router.get("/dags/{dag_id}/runs/{dag_run_id}/tasks")
async def get_task_instances(dag_id: str, dag_run_id: str):
    try:
        return await airflow_client.get_task_instances(dag_id, dag_run_id)
    except Exception as e:
        raise _airflow_502(e)


@router.get("/dags/{dag_id}/runs/{dag_run_id}/tasks/{task_id}/log")
async def get_task_log(dag_id: str, dag_run_id: str, task_id: str, try_number: int = 1):
    try:
        log = await airflow_client.get_task_log(dag_id, dag_run_id, task_id, try_number)
        return {"log": log}
    except Exception as e:
        raise _airflow_502(e)


@router.post("/analyze/{dag_id}/{dag_run_id}")
async def analyze_stream(dag_id: str, dag_run_id: str):
    """Stream the AI analysis as Server-Sent Events."""
    return StreamingResponse(
        run_agent(dag_id, dag_run_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/analyze/{dag_id}/{dag_run_id}/sync")
async def analyze_sync(dag_id: str, dag_run_id: str):
    """Non-streaming analysis for simple integrations."""
    try:
        return await analyze_once(dag_id, dag_run_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
