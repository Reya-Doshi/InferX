# inferx/distributed/election.py
"""
InferX Leader Election.

Implements Raft-inspired LeaderElection states (FOLLOWER, CANDIDATE, LEADER),
randomized campaign timers, lease heartbeats, and majority vote consensus.
"""
import asyncio
import random
import time
from typing import Any, Dict, List, Optional

from inferx.distributed.interfaces import ILeaderElection
from inferx.distributed.rpc import ClusterRpcClient
from inferx.utils.logging import get_logger

logger = get_logger("distributed.election")


class LeaderElection(ILeaderElection):
    """
    Raft-inspired leader coordinator ensuring cluster state consensus.
    """
    def __init__(
        self,
        node_id: str,
        peers: List[Dict[str, Any]],  # peer configurations: [{"node_id": "n1", "host": "127.0.0.1", "port": 9001}, ...]
        rpc_client: Optional[ClusterRpcClient] = None,
        election_min_ms: int = 150,
        election_max_ms: int = 300,
        heartbeat_ms: int = 50
    ) -> None:
        self.node_id = node_id
        self.peers = peers
        self.rpc_client = rpc_client or ClusterRpcClient()
        
        self.election_min_ms = election_min_ms
        self.election_max_ms = election_max_ms
        self.heartbeat_ms = heartbeat_ms
        
        # State variables
        self.state = "FOLLOWER"  # FOLLOWER, CANDIDATE, LEADER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.leader_id: Optional[str] = None
        
        self._last_heartbeat = time.perf_counter()
        self._is_running = False
        self._lock = asyncio.Lock()
        
        self._election_task: Optional[asyncio.Task[None]] = None
        self._heartbeat_task: Optional[asyncio.Task[None]] = None

    def get_leader(self) -> Optional[str]:
        return self.leader_id

    async def start(self) -> None:
        """Starts background election timer loops."""
        async with self._lock:
            if self._is_running:
                return
            self._is_running = True
            self._last_heartbeat = time.perf_counter()
            self._election_task = asyncio.create_task(self._election_loop())

    async def stop(self) -> None:
        """Stops background timer loops and steps down from leadership."""
        async with self._lock:
            self._is_running = False
            
            if self._election_task:
                self._election_task.cancel()
                self._election_task = None
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
                self._heartbeat_task = None
                
            self.state = "FOLLOWER"
            self.leader_id = None

    async def handle_request_vote(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """RPC Endpoint: Evaluates and votes on candidate campaigns."""
        async with self._lock:
            term = params.get("term", 0)
            candidate_id = params.get("candidate_id", "")
            
            # Step down if term is higher
            if term > self.current_term:
                self.current_term = term
                self.state = "FOLLOWER"
                self.voted_for = None
                self.leader_id = None

            vote_granted = False
            if term == self.current_term and (self.voted_for is None or self.voted_for == candidate_id):
                vote_granted = True
                self.voted_for = candidate_id
                self._last_heartbeat = time.perf_counter()  # Reset election timer
                logger.info(f"Node {self.node_id} voted for Candidate {candidate_id} in term {term}", component="election")

            return {
                "term": self.current_term,
                "vote_granted": vote_granted
            }

    async def handle_heartbeat(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """RPC Endpoint: Resets follower campaign timer lease on Leader heartbeats."""
        async with self._lock:
            term = params.get("term", 0)
            leader_id = params.get("leader_id", "")
            
            if term >= self.current_term:
                self.current_term = term
                self.state = "FOLLOWER"
                self.leader_id = leader_id
                self._last_heartbeat = time.perf_counter()  # Reset timer
                
            return {
                "term": self.current_term,
                "success": term >= self.current_term
            }

    async def _election_loop(self) -> None:
        """Background loop driving follower campaign triggers on lease misses."""
        while self._is_running:
            # Generate randomized timeout to prevent split-vote loops
            timeout = random.randint(self.election_min_ms, self.election_max_ms) / 1000.0
            await asyncio.sleep(timeout)
            
            async with self._lock:
                if self.state == "LEADER":
                    continue
                
                # Check if election timeout elapsed
                elapsed = time.perf_counter() - self._last_heartbeat
                if elapsed >= timeout:
                    # Transition to Candidate and trigger campaign
                    await self._start_campaign()

    async def _start_campaign(self) -> None:
        """Campaigns for Leader by requesting votes from all peers."""
        self.state = "CANDIDATE"
        self.current_term += 1
        self.voted_for = self.node_id
        self._last_heartbeat = time.perf_counter()
        
        term = self.current_term
        logger.info(f"Node {self.node_id} campaigning for Leader in term {term}", component="election")

        # Gather votes from peers
        votes = 1  # Self vote
        majority = (len(self.peers) + 1) // 2 + 1
        
        async def ask_peer_vote(peer: Dict[str, Any]) -> bool:
            try:
                res = await self.rpc_client.call(
                    peer["host"], peer["port"],
                    "request_vote",
                    {"term": term, "candidate_id": self.node_id}
                )
                return res.get("vote_granted", False)
            except Exception:
                return False

        # Query all peers in parallel
        tasks = [asyncio.create_task(ask_peer_vote(p)) for p in self.peers]
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, bool) and r:
                    votes += 1

        # Check if won election
        if votes >= majority and self.state == "CANDIDATE" and self.current_term == term:
            self.state = "LEADER"
            self.leader_id = self.node_id
            logger.error(f"Node {self.node_id} ELECTED LEADER for term {term} (Votes: {votes}/{len(self.peers) + 1})", component="election")
            
            # Start sending heartbeats immediately
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def start_election(self) -> None:
        """Triggers candidate campaign loops to win leadership."""
        async with self._lock:
            await self._start_campaign()

    async def _heartbeat_loop(self) -> None:
        """Periodic heartbeat loop executed by the Leader node."""
        interval = self.heartbeat_ms / 1000.0
        
        while self._is_running and self.state == "LEADER":
            term = self.current_term
            
            async def send_peer_heartbeat(peer: Dict[str, Any]) -> None:
                try:
                    await self.rpc_client.call(
                        peer["host"], peer["port"],
                        "heartbeat",
                        {"term": term, "leader_id": self.node_id}
                    )
                except Exception as e:
                    logger.error(f"Heartbeat failed to send to peer {peer.get('node_id')}: {type(e).__name__}: {e}", component="election")

            tasks = [asyncio.create_task(send_peer_heartbeat(p)) for p in self.peers]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            await asyncio.sleep(interval)
