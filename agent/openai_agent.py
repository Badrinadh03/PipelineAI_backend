import json
from typing import AsyncGenerator

from openai import AsyncOpenAI

from config import settings
from airflow_client import airflow_client
from agent.tools import OPENAI_TOOLS

SYSTEM_PROMPT = """You are an expert Apache Airflow pipeline debugger and data engineering assistant.

Your job is to investigate failed Airflow DAG runs by:
1. Examining task logs to find the root cause
2. Checking task instance states to understand the failure scope
3. Reviewing run history to identify if failures are recurring or new

When diagnosing a failure, always:
- Read the actual task log to find the specific error
- Look at which tasks failed vs succeeded to understand pipeline context
- Check if this is a recurring or new failure
- Provide a clear, concise root cause explanation
- Give concrete, actionable fix suggestions with code examples when relevant

Format your final diagnosis as:
## Root Cause
[Clear explanation of what went wrong]

## Impact
[Which tasks were affected and why]

## Suggested Fix
[Concrete steps or code to resolve the issue]

## Recurrence Pattern
[Whether this is new, intermittent, or recurring based on run history]
"""


async def run_agent(dag_id: str, dag_run_id: str) -> AsyncGenerator[str, None]:
    """
    Runs the OpenAI tool-use loop for a given failed DAG run.
    Yields SSE-formatted strings so the frontend can stream the response.
    """
    client = AsyncOpenAI(api_key=settings.openai_api_key)

    initial_message = (
        f"Please investigate the failed Airflow DAG run.\n"
        f"DAG ID: {dag_id}\n"
        f"Run ID: {dag_run_id}\n\n"
        f"Start by getting the task instances to see what failed, "
        f"then read the logs for failed tasks to diagnose the root cause."
    )

    messages: list[dict] = [{"role": "user", "content": initial_message}]

    yield f"data: {json.dumps({'type': 'status', 'message': 'Starting investigation...'})}\n\n"

    max_iterations = 6
    iteration = 0
    completed_normally = False

    while iteration < max_iterations:
        iteration += 1

        response = await client.chat.completions.create(
            model=settings.openai_model,
            max_tokens=4096,
            messages=[{"role": "system", "content": SYSTEM_PROMPT}, *messages],
            tools=OPENAI_TOOLS,
            tool_choice="auto",
        )

        choice = response.choices[0]
        msg = choice.message

        if msg.content:
            yield f"data: {json.dumps({'type': 'text', 'content': msg.content})}\n\n"

        messages.append(msg.model_dump(exclude_none=True))

        if msg.tool_calls:
            for tc in msg.tool_calls:
                tool_name = tc.function.name
                try:
                    tool_input = json.loads(tc.function.arguments or "{}")
                except json.JSONDecodeError:
                    tool_input = {}

                tool_use_id = tc.id

                yield f"data: {json.dumps({'type': 'tool_call', 'tool': tool_name, 'input': tool_input})}\n\n"

                try:
                    result = await _execute_tool(tool_name, tool_input)
                    yield f"data: {json.dumps({'type': 'tool_result', 'tool': tool_name, 'preview': str(result)[:200]})}\n\n"
                except Exception as e:
                    error_msg = f"Tool error: {str(e)}"
                    result = error_msg
                    yield f"data: {json.dumps({'type': 'tool_error', 'tool': tool_name, 'error': error_msg})}\n\n"

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_use_id,
                    "content": str(result),
                })
            continue

        yield f"data: {json.dumps({'type': 'done'})}\n\n"
        completed_normally = True
        break

    if not completed_normally and iteration >= max_iterations:
        yield f"data: {json.dumps({'type': 'done', 'note': 'Max iterations reached'})}\n\n"


async def _execute_tool(tool_name: str, tool_input: dict) -> str:
    if tool_name == "get_task_log":
        log = await airflow_client.get_task_log(
            dag_id=tool_input["dag_id"],
            dag_run_id=tool_input["dag_run_id"],
            task_id=tool_input["task_id"],
            task_try_number=tool_input.get("task_try_number", 1),
        )
        return log[:8000] if len(log) > 8000 else log

    elif tool_name == "get_task_instances":
        result = await airflow_client.get_task_instances(
            dag_id=tool_input["dag_id"],
            dag_run_id=tool_input["dag_run_id"],
        )
        instances = result.get("task_instances", [])
        summary = []
        for ti in instances:
            summary.append({
                "task_id": ti.get("task_id"),
                "state": ti.get("state"),
                "start_date": ti.get("start_date"),
                "end_date": ti.get("end_date"),
                "duration": ti.get("duration"),
                "try_number": ti.get("try_number"),
            })
        return json.dumps(summary, indent=2)

    elif tool_name == "get_dag_run_history":
        runs = await airflow_client.get_dag_run_history(
            dag_id=tool_input["dag_id"],
            limit=tool_input.get("limit", 10),
        )
        summary = []
        for run in runs:
            summary.append({
                "dag_run_id": run.get("dag_run_id"),
                "state": run.get("state"),
                "start_date": run.get("start_date"),
                "end_date": run.get("end_date"),
                "run_type": run.get("run_type"),
            })
        return json.dumps(summary, indent=2)

    else:
        return f"Unknown tool: {tool_name}"


async def analyze_once(dag_id: str, dag_run_id: str) -> dict:
    """Non-streaming version that returns the full analysis as a dict."""
    full_text = []
    tool_calls = []

    async for chunk in run_agent(dag_id, dag_run_id):
        if chunk.startswith("data: "):
            payload = json.loads(chunk[6:])
            if payload["type"] == "text":
                full_text.append(payload["content"])
            elif payload["type"] == "tool_call":
                tool_calls.append(payload["tool"])

    return {
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "analysis": "".join(full_text),
        "tools_used": tool_calls,
    }
