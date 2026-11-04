"""LangGraph runner integrating with FencedPostgresSaver."""

from typing import Any, TypedDict

from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, StateGraph

from app.checkpointer import FencedPostgresSaver


class AgentState(TypedDict):
    job_id: str
    step: int
    data: str


async def step_one(state: AgentState) -> dict[str, Any]:
    """Execute initial phase."""
    return {"step": 1, "data": f"{state.get('data', '')}->step1"}


async def step_two(state: AgentState) -> dict[str, Any]:
    """Execute secondary phase."""
    return {"step": 2, "data": f"{state.get('data', '')}->step2"}


def build_agent_graph(checkpointer: FencedPostgresSaver | None = None):
    """Constructs LangGraph with StateGraph and persistence checkpointer."""
    workflow = StateGraph(AgentState)
    workflow.add_node("step_one", step_one)
    workflow.add_node("step_two", step_two)

    workflow.set_entry_point("step_one")
    workflow.add_edge("step_one", "step_two")
    workflow.add_edge("step_two", END)

    return workflow.compile(checkpointer=checkpointer)


async def run_agent_job(
    graph: Any,
    job_id: str,
    attempt_id: int,
    initial_input: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Executes or resumes an agent workflow.
    When attempt_id > 1, LangGraph reads latest checkpoint for thread_id=job_id.
    """
    config: RunnableConfig = {
        "configurable": {
            "thread_id": job_id,
            "attempt_id": attempt_id,
        }
    }

    # If resuming (attempt_id > 1) and no new input, continue from checkpoint
    input_data = initial_input if attempt_id == 1 else None

    result = await graph.ainvoke(input_data, config=config)
    return result
