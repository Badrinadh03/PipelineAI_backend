"""
Tool definitions for the Airflow debugging agent (shared schema).
Each tool maps to a real Airflow API call so the model can autonomously
gather the context it needs to diagnose failures.
"""

TOOLS = [
    {
        "name": "get_task_log",
        "description": (
            "Retrieve the execution log for a specific task instance in a DAG run. "
            "Use this to read the actual error traceback and understand what went wrong. "
            "Returns: raw log text including timestamps, Python tracebacks, and error messages."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string", "description": "The DAG identifier"},
                "dag_run_id": {"type": "string", "description": "The DAG run identifier"},
                "task_id": {"type": "string", "description": "The task identifier"},
                "task_try_number": {
                    "type": "integer",
                    "description": "The attempt number (default 1)",
                    "default": 1,
                },
            },
            "required": ["dag_id", "dag_run_id", "task_id"],
        },
    },
    {
        "name": "get_task_instances",
        "description": (
            "List all task instances for a DAG run with their states. "
            "Use this to see which tasks succeeded, failed, or were skipped "
            "so you can identify the failure point in the pipeline. "
            "Returns: list of task instances with state, start/end time, and duration."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string", "description": "The DAG identifier"},
                "dag_run_id": {"type": "string", "description": "The DAG run identifier"},
            },
            "required": ["dag_id", "dag_run_id"],
        },
    },
    {
        "name": "get_dag_run_history",
        "description": (
            "Retrieve the recent run history for a DAG to identify patterns. "
            "Use this to determine if failures are recurring, intermittent, or new. "
            "Returns: list of dag runs with state, start time, and end time (up to 20 most recent)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string", "description": "The DAG identifier"},
                "limit": {
                    "type": "integer",
                    "description": "Number of runs to retrieve (max 20)",
                    "default": 10,
                },
            },
            "required": ["dag_id"],
        },
    },
]

OPENAI_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": t["name"],
            "description": t["description"],
            "parameters": t["input_schema"],
        },
    }
    for t in TOOLS
]
