
import ray

from raydistributed.algorithms import AlgorithmA, AlgorithmB, AlgorithmC, AlgorithmD, AlgorithmE, StatusCode


@ray.remote
def execute(algo, *deps):
    for status_code in deps:
        if status_code != StatusCode.OK:
            return
    return algo.execute()


@ray.remote
def sink(*deps):
    for status_code in deps:
        if status_code != StatusCode.OK:
            print(f"Execution failed: {status_code}")


def bind_dependencies(algo_name, dependencies, algos_map, scheduled_nodes):
    deps = dependencies[algo_name]
    results = []
    for dep in deps:
        if dep not in scheduled_nodes:
            scheduled_nodes[dep] = bind_dependencies(dep, dependencies, algos_map, scheduled_nodes)
        results.append(scheduled_nodes[dep])
    return execute.bind(algos_map[algo_name], *results)


class Scheduler:

    def __init__(self, algorithms_deps) -> None:
        self._algorithms_deps = algorithms_deps
        algos = [AlgorithmA(), AlgorithmB(), AlgorithmC(), AlgorithmD(), AlgorithmE()]
        algos_map = {}
        for algo in algos:
            algo.initialize()
            algos_map[algo.get_name()] = algo
        self._algos = algos_map
        self._dags = {}

    def schedule_n_event(self, nevents, requested):
        requested_hash = hash(frozenset(requested))
        futures = [None] * nevents
        if requested_hash not in self._dags:
            requested_nodes = []
            scheduled_nodes = {}
            for algo_name in requested:
                if algo_name not in scheduled_nodes:
                    scheduled_nodes[algo_name] = bind_dependencies(algo_name, self._algorithms_deps, self._algos, scheduled_nodes)
                requested_nodes.append(scheduled_nodes[algo_name])
            # combine the requested nodes
            dag = sink.bind(*requested_nodes)
            self._dags[requested_hash] = dag
        else:
            dag = self._dags[requested_hash]

        for i in range(nevents):
            futures[i] = dag.execute()
        return futures
