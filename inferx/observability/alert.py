# inferx/observability/alert.py
"""
InferX Alert Manager.

Evaluates metric telemetry snapshots against registered alert rules,
triggering alerts and logging warnings for operators.
"""
from typing import Any, Callable, Dict, List, Optional, Tuple
import threading

from inferx.utils.logging import get_logger

logger = get_logger("observability.alert")


class AlertRule:
    """Represents a single threshold check rule."""
    def __init__(self, name: str, threshold: float, check_fn: Callable[[Dict[str, Any]], Tuple[bool, str]]) -> None:
        self.name = name
        self.threshold = threshold
        self.check_fn = check_fn


class AlertManager:
    """
    Evaluates metric snapshots against rule conditions.
    """
    def __init__(self) -> None:
        self._rules: Dict[str, AlertRule] = {}
        self._triggered_alerts: Dict[str, float] = {}  # Maps alert_name -> last_trigger_timestamp
        self._handlers: List[Callable[[str, str], None]] = []
        self._lock = threading.Lock()

    def add_rule(self, name: str, threshold: float, check_fn: Callable[[Dict[str, Any]], Tuple[bool, str]]) -> None:
        """Registers a threshold rule check."""
        with self._lock:
            self._rules[name] = AlertRule(name, threshold, check_fn)
            logger.info(f"Registered alert rule: {name} (Threshold: {threshold})", component="alert_manager")

    def register_handler(self, handler: Callable[[str, str], None]) -> None:
        """Registers an alert dispatch handler (e.g. pager webhook or slack)."""
        with self._lock:
            self._handlers.append(handler)

    def evaluate_rules(self, metrics_snapshot: Dict[str, Any]) -> List[str]:
        """
        Evaluates metrics snapshot values against registered rules.
        
        Returns:
            A list of triggered alert names.
        """
        import time
        triggered = []
        now = time.time()

        with self._lock:
            rules = list(self._rules.values())

        for rule in rules:
            try:
                is_triggered, detail_msg = rule.check_fn(metrics_snapshot)
                if is_triggered:
                    triggered.append(rule.name)
                    
                    # Log alert (throttled to avoid log flooding: log at most once every 10s per alert)
                    last_trigger = self._triggered_alerts.get(rule.name, 0.0)
                    if now - last_trigger >= 10.0:
                        logger.error(
                            f"ALERT TRIGGERED: {rule.name}. Detail: {detail_msg}",
                            alert_name=rule.name,
                            component="alert_manager"
                        )
                        self._triggered_alerts[rule.name] = now
                        
                        # Execute handlers
                        with self._lock:
                            handlers = list(self._handlers)
                        for handler in handlers:
                            try:
                                handler(rule.name, detail_msg)
                            except Exception as he:
                                logger.error(f"Alert handler failed: {he}", exc_info=True, component="alert_manager")

            except Exception as e:
                logger.error(f"Failed to evaluate alert rule {rule.name}: {e}", exc_info=True, component="alert_manager")

        return triggered
