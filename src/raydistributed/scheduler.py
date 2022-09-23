
import ray
import time
from raydistributed.datamodel import EventContext
from raydistributed.algorithms import AlgorithmA, AlgorithmB, AlgorithmC, AlgorithmD, AlgorithmE, StatusCode


@ray.remote
def execute(eventContext, algo, *deps):
    for status_code in deps:
        if status_code != StatusCode.OK:
            return
    return algo.execute(eventContext)


@ray.remote
def sink(*deps):
    for status_code in deps:
        if status_code != StatusCode.OK:
            print(f"Execution failed: {status_code}")


def bind_dependencies(algo_name, eventContext, dependencies, algos_map, scheduled_nodes):
    deps = dependencies[algo_name]
    results = []
    for dep in deps:
        if dep not in scheduled_nodes:
            scheduled_nodes[dep] = bind_dependencies(dep, eventContext, dependencies, algos_map, scheduled_nodes)
        results.append(scheduled_nodes[dep])
    return execute.bind(eventContext, algos_map[algo_name], *results)


class Scheduler:

    def __init__(self, algorithms_deps) -> None:
        self._algorithms_deps = algorithms_deps
        algos = [AlgorithmA(), AlgorithmB(), AlgorithmC(), AlgorithmD(), AlgorithmE()]
        algos_map = {}
        for algo in algos:
            algo.initialize()
            algos_map[algo.get_name()] = algo
        self._algos = algos_map

    def schedule_event(self, ec: EventContext, requested):
        requested_nodes = []
        scheduled_nodes = {}
        #s = time.time()
        for algo_name in requested:
            if algo_name not in scheduled_nodes:
                scheduled_nodes[algo_name] = bind_dependencies(algo_name, ec, self._algorithms_deps, self._algos, scheduled_nodes)
            requested_nodes.append(scheduled_nodes[algo_name])
        # combine the requested nodes
        dag = sink.bind(*requested_nodes)
        #e = time.time()
        #print(f"Total time to schedule event dependencies: {(e-s) * 1000}ms")
        return dag.execute()
