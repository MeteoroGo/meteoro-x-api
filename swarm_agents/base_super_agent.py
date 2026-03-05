#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║  SUPER AGENT BASE CLASS                                           ║
║  Think-Act-Observe loop inspired by OpenManus/Manus AI            ║
║                                                                    ║
║  Each SuperAgent:                                                  ║
║    1. THINKS (system prompt + context reasoning)                   ║
║    2. ACTS (executes API calls, tools, or computations)            ║
║    3. OBSERVES (extracts signal, evidence, conviction)             ║
║    4. RETURNS SuperAgentResult with structured output              ║
╚══════════════════════════════════════════════════════════════════╝
"""

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable, Coroutine
from enum import Enum


class Signal(Enum):
    """Trading signal from an agent."""
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"
    NEUTRAL = "NEUTRAL"


@dataclass
class SuperAgentResult:
    """
    Structured result from a SuperAgent.

    This is the standard output format for all agents in the agentic system.
    Enables consensus mechanisms and weighted voting across all agents.
    """
    agent_id: int                           # 0-12, where 0 = Commander
    agent_name: str                         # Human readable: "Satellite Recon", etc
    signal: Signal                          # BUY/SELL/HOLD/NEUTRAL
    confidence: int                         # 0-100, conviction level
    reasoning: str                          # Explanation of the signal
    sources_analyzed: int                   # How many sources/data points examined
    evidence_pack: Dict[str, Any] = field(default_factory=dict)  # Raw data supporting decision
    latency_ms: int = 0                     # Execution time in milliseconds
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    error: Optional[str] = None             # If something went wrong
    tools_called: List[str] = field(default_factory=list)  # APIs/tools used
    cost_usd: float = 0.0                   # Estimated cost of this agent's run
    metadata: Dict[str, Any] = field(default_factory=dict)  # Extra data

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self)
        data['signal'] = self.signal.value if isinstance(self.signal, Signal) else self.signal
        return data

    def is_bullish(self) -> bool:
        """Is this result bullish?"""
        return self.signal == Signal.BUY

    def is_bearish(self) -> bool:
        """Is this result bearish?"""
        return self.signal == Signal.SELL

    def is_high_conviction(self) -> bool:
        """Is confidence > 75%?"""
        return self.confidence > 75

    def is_low_conviction(self) -> bool:
        """Is confidence < 40%?"""
        return self.confidence < 40


class SuperAgent:
    """
    Base class for agents in the Meteoro Agentic System.

    Think-Act-Observe Loop (inspired by OpenManus/ReAct):
    1. THINK: Process directive + context with LLM
    2. ACT: Call tools, APIs, execute computations
    3. OBSERVE: Extract signal, confidence, evidence
    4. Return SuperAgentResult

    Each agent is specialized and runs in parallel with others.
    Dynamic scaling — the system deploys as many agents as the task requires.
    """

    MAX_EXEC_TIME_MS = 5000  # 5 seconds per agent max
    DEFAULT_MODEL = "claude-haiku-4-5-20251001"

    def __init__(
        self,
        agent_id: int,
        agent_name: str,
        system_prompt: str,
        model: str = DEFAULT_MODEL,
    ):
        """
        Initialize a SuperAgent.

        Args:
            agent_id: 1-12 (0 is reserved for Commander)
            agent_name: Human-readable name (e.g., "Satellite Recon")
            system_prompt: Specialized instructions for this agent
            model: LLM model to use (will be set by ModelRouter)
        """
        self.agent_id = agent_id
        self.agent_name = agent_name
        self.system_prompt = system_prompt
        self.model = model
        self._session_id = str(uuid.uuid4().hex[:8])

    async def process(
        self,
        commodity: str,
        directive: str,
        context: Optional[Dict[str, Any]] = None,
        timeout_ms: Optional[int] = None,
    ) -> SuperAgentResult:
        """
        Main entry point for a SuperAgent's decision-making loop.

        MUST be implemented by each subclass.

        Args:
            commodity: Trading target (e.g., "COPPER", "OIL", "WHEAT")
            directive: What to analyze
            context: Shared context from previous agents
            timeout_ms: Override the default timeout

        Returns:
            SuperAgentResult with signal, confidence, reasoning, evidence
        """
        raise NotImplementedError(f"{self.agent_name} must implement process()")

    # ─────────────────────────────────────────────────────────────
    # THINK: Reasoning & Synthesis
    # ─────────────────────────────────────────────────────────────

    def think(self, context: str) -> str:
        """
        Step 1: THINK
        Process directive with system prompt to formulate approach.

        In actual LLM calls, this happens automatically.
        This method documents the "thinking" phase.
        """
        return f"Analyzing {context} using {self.agent_name} expertise..."

    # ─────────────────────────────────────────────────────────────
    # ACT: Execute Tools & APIs
    # ─────────────────────────────────────────────────────────────

    async def act(self, action: str, **params) -> Dict[str, Any]:
        """
        Step 2: ACT
        Execute a tool, API call, or computation.

        Must be implemented by subclass to actually call APIs.

        Args:
            action: What to do (e.g., "fetch_satellite_data")
            **params: Parameters for the action

        Returns:
            Raw data from the action
        """
        raise NotImplementedError(f"{self.agent_name} must implement act()")

    # ─────────────────────────────────────────────────────────────
    # OBSERVE: Extract Signal from Evidence
    # ─────────────────────────────────────────────────────────────

    def observe(
        self,
        evidence: Dict[str, Any],
        context: Optional[str] = None,
    ) -> tuple[Signal, int, str]:
        """
        Step 3: OBSERVE
        Extract trading signal and confidence from evidence.

        Args:
            evidence: Raw data gathered from act() phase
            context: Additional context for interpretation

        Returns:
            (Signal, confidence 0-100, reasoning string)
        """
        # Default implementation: look for standard signals
        if not evidence:
            return Signal.NEUTRAL, 0, "No evidence gathered"

        # Override in subclasses for specialized logic
        return Signal.HOLD, 50, "Neutral assessment"

    # ─────────────────────────────────────────────────────────────
    # RESULT BUILDING
    # ─────────────────────────────────────────────────────────────

    def build_result(
        self,
        signal: Signal,
        confidence: int,
        reasoning: str,
        sources_analyzed: int,
        evidence_pack: Dict[str, Any],
        latency_ms: float,
        tools_called: Optional[List[str]] = None,
        error: Optional[str] = None,
        cost_usd: float = 0.0,
    ) -> SuperAgentResult:
        """
        Build a structured SuperAgentResult.

        Args:
            signal: BUY/SELL/HOLD/NEUTRAL
            confidence: 0-100
            reasoning: Explanation
            sources_analyzed: Count of sources
            evidence_pack: Raw evidence dictionary
            latency_ms: Execution time
            tools_called: List of tools used
            error: If any error occurred
            cost_usd: Estimated cost

        Returns:
            SuperAgentResult ready for consensus voting
        """
        # Clamp confidence to 0-100
        confidence = max(0, min(100, confidence))

        return SuperAgentResult(
            agent_id=self.agent_id,
            agent_name=self.agent_name,
            signal=signal,
            confidence=confidence,
            reasoning=reasoning,
            sources_analyzed=sources_analyzed,
            evidence_pack=evidence_pack,
            latency_ms=int(latency_ms),
            timestamp=datetime.utcnow().isoformat(),
            error=error,
            tools_called=tools_called or [],
            cost_usd=cost_usd,
            metadata={"session_id": self._session_id},
        )

    # ─────────────────────────────────────────────────────────────
    # UTILITIES
    # ─────────────────────────────────────────────────────────────

    async def call_with_timeout(
        self,
        coro: Coroutine,
        timeout_ms: int,
    ) -> Any:
        """
        Execute a coroutine with timeout protection.

        Args:
            coro: Async function to execute
            timeout_ms: Timeout in milliseconds

        Returns:
            Result or None if timeout
        """
        try:
            return await asyncio.wait_for(coro, timeout=timeout_ms / 1000.0)
        except asyncio.TimeoutError:
            return None

    def format_json_compact(self, data: Dict[str, Any], max_keys: int = 5) -> str:
        """
        Format JSON compactly for logging.

        Args:
            data: Dictionary to format
            max_keys: Max keys to show before truncating

        Returns:
            Compact JSON string
        """
        if len(data) > max_keys:
            truncated = dict(list(data.items())[:max_keys])
            truncated["..."] = f"+{len(data) - max_keys} more"
            return json.dumps(truncated, default=str)
        return json.dumps(data, default=str)

    def log(self, message: str, level: str = "INFO"):
        """
        Log a message with agent context.

        Args:
            message: Message to log
            level: Log level (INFO, WARN, ERROR)
        """
        timestamp = datetime.utcnow().isoformat()
        print(f"[{timestamp}] {level:6s} [{self.agent_name:20s}] {message}")
