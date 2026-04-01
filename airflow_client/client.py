import httpx
from typing import Optional
from config import settings


class AirflowClient:
    def __init__(self):
        self.base_url = settings.airflow_base_url.rstrip("/")
        self.auth = (settings.airflow_username, settings.airflow_password)
        self.headers = {"Content-Type": "application/json"}

    def _url(self, path: str) -> str:
        return f"{self.base_url}/api/v1{path}"

    async def get_dags(self) -> dict:
        async with httpx.AsyncClient(auth=self.auth, timeout=15) as client:
            r = await client.get(self._url("/dags"), headers=self.headers)
            r.raise_for_status()
            return r.json()

    async def get_dag(self, dag_id: str) -> dict:
        async with httpx.AsyncClient(auth=self.auth, timeout=15) as client:
            r = await client.get(self._url(f"/dags/{dag_id}"), headers=self.headers)
            r.raise_for_status()
            return r.json()

    async def get_dag_runs(self, dag_id: str, limit: int = 10) -> dict:
        async with httpx.AsyncClient(auth=self.auth, timeout=15) as client:
            r = await client.get(
                self._url(f"/dags/{dag_id}/dagRuns"),
                params={"limit": limit, "order_by": "-start_date"},
                headers=self.headers,
            )
            r.raise_for_status()
            return r.json()

    async def get_all_failed_runs(self) -> list[dict]:
        """Poll all DAGs for recently failed runs."""
        async with httpx.AsyncClient(auth=self.auth, timeout=30) as client:
            dags_resp = await client.get(
                self._url("/dags"),
                params={"limit": 100},
                headers=self.headers,
            )
            dags_resp.raise_for_status()
            dags = dags_resp.json().get("dags", [])

            failed = []
            for dag in dags:
                dag_id = dag["dag_id"]
                runs_resp = await client.get(
                    self._url(f"/dags/{dag_id}/dagRuns"),
                    params={"state": "failed", "limit": 5, "order_by": "-start_date"},
                    headers=self.headers,
                )
                if runs_resp.status_code == 200:
                    runs = runs_resp.json().get("dag_runs", [])
                    for run in runs:
                        run["dag_id"] = dag_id
                        failed.append(run)
            return failed

    async def get_task_instances(self, dag_id: str, dag_run_id: str) -> dict:
        async with httpx.AsyncClient(auth=self.auth, timeout=15) as client:
            r = await client.get(
                self._url(f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"),
                headers=self.headers,
            )
            r.raise_for_status()
            return r.json()

    async def get_task_log(
        self, dag_id: str, dag_run_id: str, task_id: str, task_try_number: int = 1
    ) -> str:
        async with httpx.AsyncClient(auth=self.auth, timeout=30) as client:
            r = await client.get(
                self._url(
                    f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try_number}"
                ),
                headers={"Accept": "text/plain"},
            )
            if r.status_code == 200:
                return r.text
            return f"[Log unavailable: HTTP {r.status_code}]"

    async def get_dag_run_history(self, dag_id: str, limit: int = 20) -> list[dict]:
        async with httpx.AsyncClient(auth=self.auth, timeout=15) as client:
            r = await client.get(
                self._url(f"/dags/{dag_id}/dagRuns"),
                params={"limit": limit, "order_by": "-start_date"},
                headers=self.headers,
            )
            r.raise_for_status()
            return r.json().get("dag_runs", [])

    async def health_check(self) -> bool:
        try:
            async with httpx.AsyncClient(auth=self.auth, timeout=5) as client:
                r = await client.get(self._url("/health"))
                return r.status_code == 200
        except Exception:
            return False


airflow_client = AirflowClient()
