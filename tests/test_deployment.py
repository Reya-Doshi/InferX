# tests/test_deployment.py
import unittest
import os
import json
from inferx.deployment.config import RuntimeConfigurationManager
from inferx.deployment.controller import DeploymentController


class TestDeployment(unittest.IsolatedAsyncioTestCase):
    """Unit and integration test suite validating configuration loading and rollout strategies."""

    async def asyncSetUp(self) -> None:
        self.config_file = "test_config_temp.json"

        # Write temporary mock configuration file
        with open(self.config_file, "w") as f:
            json.dump({"concurrency_limit": 10, "gpu_memory_fraction": 0.8}, f)

        self.config_mgr = RuntimeConfigurationManager()
        self.controller = DeploymentController(
            initial_replicas=3, initial_version="v1.0"
        )

    async def asyncTearDown(self) -> None:
        if os.path.exists(self.config_file):
            os.remove(self.config_file)

    def test_configuration_hot_reload(self) -> None:
        """Verifies that modifications to JSON config file trigger registered callbacks."""
        self.config_mgr.load_from_file(self.config_file)
        self.assertEqual(self.config_mgr.get("concurrency_limit"), 10)

        triggered_vals = []

        def on_concurrency_change(new_val: int) -> None:
            triggered_vals.append(new_val)

        self.config_mgr.register_callback("concurrency_limit", on_concurrency_change)

        # Update file values to simulate K8s ConfigMap volume update
        with open(self.config_file, "w") as f:
            json.dump({"concurrency_limit": 25, "gpu_memory_fraction": 0.8}, f)

        # Trigger hot reload
        self.config_mgr.load_from_file(self.config_file)

        self.assertEqual(self.config_mgr.get("concurrency_limit"), 25)
        self.assertEqual(triggered_vals, [25])

    def test_secret_rotation(self) -> None:
        """Verifies base64 encoded secrets decryption and rotation updates."""
        secret_calls = []
        self.config_mgr.register_callback(
            "auth_token", lambda val: secret_calls.append(val)
        )

        # Rotate secret
        # Base64 for "new-secret-token" is "bmV3LXNlY3JldC10b2tlbg=="
        self.config_mgr.rotate_secret("auth_token", "bmV3LXNlY3JldC10b2tlbg==")

        self.assertEqual(self.config_mgr.get("auth_token"), "new-secret-token")
        self.assertEqual(secret_calls, ["new-secret-token"])

    async def test_rolling_update_strategy(self) -> None:
        """Verifies incremental pod replacements during rolling updates."""
        status = self.controller.get_status()
        self.assertEqual(status["version"], "v1.0")
        self.assertEqual(status["replicas"], 3)

        # Launch rolling update
        success = await self.controller.start_rolling_update("v1.1", max_surge=1)
        self.assertTrue(success)

        updated_status = self.controller.get_status()
        self.assertEqual(updated_status["version"], "v1.1")
        self.assertEqual(updated_status["state"], "STABLE")
        self.assertEqual(updated_status["replicas"], 3)

        for inst in updated_status["instances"]:
            self.assertEqual(inst["version"], "v1.1")
            self.assertEqual(inst["status"], "Ready")

    async def test_canary_rollout_and_rollback(self) -> None:
        """Verifies canary pod deployment and rollback cleanup loops on error spikes."""
        status = self.controller.get_status()
        self.assertEqual(status["replicas"], 3)

        # Launch Canary (10% traffic routing)
        success = await self.controller.start_canary_deployment(
            "v2.0", canary_weight_percent=10
        )
        self.assertTrue(success)

        canary_status = self.controller.get_status()
        self.assertEqual(canary_status["state"], "CANARY")
        self.assertEqual(canary_status["replicas"], 4)  # 3 stable + 1 canary pod

        # Simulating telemetry error spike -> Trigger rollback
        await self.controller.execute_canary_rollback("v2.0")

        post_rollback_status = self.controller.get_status()
        self.assertEqual(post_rollback_status["state"], "STABLE")
        self.assertEqual(
            post_rollback_status["replicas"], 3
        )  # Back to 3 stable instances

        for inst in post_rollback_status["instances"]:
            self.assertEqual(inst["version"], "v1.0")

    async def test_autoscaling_hpa_triggers(self) -> None:
        """Simulates custom queue depth metrics and verifies horizontal pod scaling."""
        status = self.controller.get_status()
        self.assertEqual(status["replicas"], 3)

        # Simulating autoscaling check rules:
        # If queue depth > 50, scale up to 6 replicas
        simulated_queue_depth = 75

        if simulated_queue_depth > 50:
            await self.controller.scale_replicas(6)

        new_status = self.controller.get_status()
        self.assertEqual(new_status["replicas"], 6)
        self.assertEqual(len(new_status["instances"]), 6)


if __name__ == "__main__":
    unittest.main()
