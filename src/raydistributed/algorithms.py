import math
import time
import ray
from enum import Enum
from raydistributed.datamodel import EventContext


class StatusCode(Enum):
    OK = 0
    FAILURE = 1


class Algorithm:

    def __init__(self, name, niters) -> None:
        self._name = name
        self._niters = niters

    def initialize(self) -> StatusCode:
        return StatusCode.OK

    def execute(self, ec) -> StatusCode:
        sum = 0.0
        val = 0.0
        # s = time.time()
        for i in range(self._niters):
            val = (i + 1) / self._niters * 0.7854
            sum += math.tan(math.log(val))
        # e = time.time()
        # print(f"Total time for {self._name} on {ray.runtime_context.get_runtime_context().get_node_id()}: {(e - s) * 1000}ms")
        return StatusCode.OK

    def get_name(self):
        return self._name


class AlgorithmA(Algorithm):

    def __init__(self, niters=1_500_000) -> None:
        super().__init__("AlgorithmA", niters)


class AlgorithmB(Algorithm):

    def __init__(self, niters=1_500_000) -> None:
        super().__init__("AlgorithmB", niters)


class AlgorithmC(Algorithm):

    def __init__(self, niters=1_500_000) -> None:
        super().__init__("AlgorithmC", niters)


class AlgorithmD(Algorithm):

    def __init__(self, niters=1_500_000) -> None:
        super().__init__("AlgorithmD", niters)


class AlgorithmE(Algorithm):

    def __init__(self, niters=1_500_000) -> None:
        super().__init__("AlgorithmE", niters)
