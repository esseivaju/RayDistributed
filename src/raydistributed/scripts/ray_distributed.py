import click
import ray
import time
from raydistributed.algorithms import AlgorithmA, AlgorithmB, AlgorithmC, AlgorithmD, AlgorithmE
from raydistributed.scheduler import Scheduler
from raydistributed.datamodel import EventContext


@click.command()
@click.option('-n', '--nevents', required=True, type=int)
@click.option('-n', '--expected-nnodes', required=True, type=int)
def main(nevents, expected_nnodes):
    ray.init(address="auto")
    nodes = ray.nodes()
    assert(len(nodes) == expected_nnodes)
    nthreads = int(ray.nodes()[0]['Resources']['CPU'])
    print(f"Running with {len(nodes)} nodes with {nthreads} threads each ({nthreads * len(nodes)} cores)")
    algo_a_name = AlgorithmA().get_name()
    algo_b_name = AlgorithmB().get_name()
    algo_c_name = AlgorithmC().get_name()
    algo_d_name = AlgorithmD().get_name()
    algo_e_name = AlgorithmE().get_name()
    deps = {
        algo_a_name: [],
        algo_b_name: [algo_a_name],
        algo_c_name: [algo_a_name],
        algo_d_name: [algo_b_name, algo_c_name],
        algo_e_name: [algo_b_name],
    }
    for algo, dependencies in deps.items():
        if dependencies:
            deps_str = ""
            for dep in dependencies:
                deps_str = f"{deps_str} {dep}"
        else:
            deps_str = "none"
        print(f"Dependency: {algo} -> {deps_str}")
    print(f"Processing {nevents} events")
    to_retrieve = [algo_d_name, algo_e_name]
    scheduler = Scheduler(deps)
    start = time.time()
    futures = scheduler.schedule_n_event(nevents, to_retrieve)
    end_scheduling = time.time()
    output = ray.get(futures)
    end = time.time()
    print(f"Total time to schedule: {(end_scheduling - start) * 1000.0}ms")
    print(f"Total time to process: {(end - start) * 1000.0}ms")
    print(f"Time between scheduling end and processing end: {(end - end_scheduling) * 1000.0}ms")
    print(f"Throughput: { nevents / ((end - start) * 1000.0)} events/ms")
