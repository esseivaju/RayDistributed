
import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
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


@ray.remote
def schedule_work(algo_deps, algos, requested, nevents):
    requested_nodes = []
    scheduled_nodes = {}
    for algo_name in requested:
        if algo_name not in scheduled_nodes:
            scheduled_nodes[algo_name] = bind_dependencies(algo_name, algo_deps, algos, scheduled_nodes)
        requested_nodes.append(scheduled_nodes[algo_name])
    # combine the requested nodes
    dag = sink.bind(*requested_nodes)
    futures = [None] * nevents
    for i in range(nevents):
        futures[i] = dag.execute()
    return futures


class Scheduler:

    def __init__(self, algorithms_deps) -> None:
        self._algorithms_deps = algorithms_deps
        algos = [AlgorithmA(), AlgorithmB(), AlgorithmC(), AlgorithmD(), AlgorithmE()]
        algos_map = {}
        for algo in algos:
            algo.initialize()
            algos_map[algo.get_name()] = algo
        self._algos = algos_map

    def schedule_n_event(self, nevents, requested):
        nthreads = int(ray.nodes()[0]['Resources']['CPU'])
        events_per_thread = nevents // nthreads
        rem = nevents % nthreads
        futures = [None] * nthreads
        this = ray.get_runtime_context().node_id
        for i in range(nthreads):
            extra = 0
            if i < rem:
                extra = 1
            if not extra and not events_per_thread:
                break
            futures[i] = schedule_work.options(
                                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                                        node_id=this,
                                        soft=True)).remote(self._algorithms_deps, self._algos, requested, events_per_thread + extra)
        futures = [e for e in futures if e is not None]
        scheduled_work = ray.get(futures)
        res = []
        for batch in scheduled_work:
            res.extend(batch)
        return res
