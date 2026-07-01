# inferx/batcher/padding.py
"""
InferX Tensor Padding & Shape Bucketing.

Provides utilities to align sequence inputs of varying lengths into uniform
matrices, reducing padding overhead via shape-based bucketing.
"""
from typing import Dict, List, Tuple
from inferx.scheduler.interfaces import ScheduledRequest


def pad_tensors(requests: List[ScheduledRequest], pad_token: int = 0) -> Tuple[List[List[int]], List[int]]:
    """
    Pads 1D integer lists inside request payloads to form a rectangular 2D array.
    
    Args:
        requests: List of ScheduledRequest objects where payload is a List[int].
        pad_token: Integer to fill the trailing sequence dimensions.
        
    Returns:
        A tuple of (padded_2D_list, shape_dimensions).
    """
    if not requests:
        return [], [0, 0]

    # Assume payload is a list of token integers
    max_len = 0
    for req in requests:
        if isinstance(req.payload, list):
            max_len = max(max_len, len(req.payload))
        else:
            # Fallback if payload is not a list (e.g., raw int or single float)
            max_len = max(max_len, 1)

    padded_list: List[List[int]] = []
    for req in requests:
        payload = req.payload
        if not isinstance(payload, list):
            payload = [payload]

        padded = list(payload)
        if len(padded) < max_len:
            padded.extend([pad_token] * (max_len - len(padded)))
        padded_list.append(padded)

    shape = [len(requests), max_len]
    return padded_list, shape


class ShapeBucketeer:
    """
    Groups requests into discrete bins based on sequence length.
    
    Prevents short sequences from being heavily padded to match extremely long
    sequences in the same execution batch.
    """
    def __init__(self, thresholds: List[int] = [64, 128, 256, 512]) -> None:
        self.thresholds = sorted(thresholds)
        self._buckets: Dict[int, List[ScheduledRequest]] = {t: [] for t in self.thresholds}
        self._overflow_bucket: List[ScheduledRequest] = []

    def add_request(self, request: ScheduledRequest) -> int:
        """
        Assigns the request to the appropriate threshold bucket.
        
        Returns:
            The threshold key (int) of the target bucket, or -1 for overflow.
        """
        payload = request.payload
        seq_len = len(payload) if isinstance(payload, list) else 1

        for threshold in self.thresholds:
            if seq_len <= threshold:
                self._buckets[threshold].append(request)
                return threshold

        self._overflow_bucket.append(request)
        return -1

    def get_bucket(self, threshold: int) -> List[ScheduledRequest]:
        """Returns the contents of a bucket and clears it."""
        if threshold == -1:
            reqs = list(self._overflow_bucket)
            self._overflow_bucket.clear()
            return reqs

        if threshold not in self._buckets:
            raise KeyError(f"Bucket threshold {threshold} not registered.")
        
        reqs = list(self._buckets[threshold])
        self._buckets[threshold].clear()
        return reqs

    def bucket_size(self, threshold: int) -> int:
        """Returns the current number of requests in a bucket."""
        if threshold == -1:
            return len(self._overflow_bucket)
        return len(self._buckets.get(threshold, []))

    def get_active_thresholds(self) -> List[int]:
        """Returns all configured threshold keys, including -1 representing overflow."""
        return self.thresholds + [-1]
