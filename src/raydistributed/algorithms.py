import math
from enum import Enum


class StatusCode(Enum):
    OK = 0
    FAILURE = 1


# Each subclass of Algorithm does the same thing.
# In a realistic scenario they should all be different and we could have multiple instance of the same algo with different names

class Algorithm:

    def __init__(self, name, niters) -> None:
        self._name = name
        self._niters = niters

    def initialize(self) -> StatusCode:
        return StatusCode.OK

    def execute(self) -> StatusCode:
        sum = 0.0
        val = 0.0
        for i in range(self._niters):
            val = (i + 1) / self._niters * 0.7854
            sum += math.tan(math.log(val))
        return StatusCode.OK

    def get_name(self):
        return self._name


class AlgorithmA(Algorithm):

    def __init__(self, niters=200_000) -> None:
        super().__init__("AlgorithmA", niters)


class AlgorithmB(Algorithm):

    def __init__(self, niters=200_000) -> None:
        super().__init__("AlgorithmB", niters)


class AlgorithmC(Algorithm):

    def __init__(self, niters=200_000) -> None:
        super().__init__("AlgorithmC", niters)


class AlgorithmD(Algorithm):

    def __init__(self, niters=200_000) -> None:
        super().__init__("AlgorithmD", niters)


class AlgorithmE(Algorithm):

    def __init__(self, niters=200_000) -> None:
        super().__init__("AlgorithmE", niters)
