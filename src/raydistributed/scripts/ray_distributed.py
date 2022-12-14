import click
import ray
import time
from raydistributed.algorithms import AlgorithmA, AlgorithmB, AlgorithmC, AlgorithmD, AlgorithmE
from raydistributed.scheduler import Scheduler


@click.command()
@click.option('-n', '--nevents', required=True, type=int)
@click.option('-e', '--expected-nnodes', required=True, type=int)
@click.option('-s', '--single-alg', is_flag=True, default=False)
@click.option('-m', '--max-inflight', default=0, type=int)
def main(nevents, expected_nnodes, single_alg, max_inflight):
    ray.init(address="auto")
    nodes = ray.nodes()
    assert(len(nodes) == expected_nnodes)
    nthreads = int(ray.nodes()[0]['Resources']['CPU'])
    total_workers = nthreads * len(nodes)
    print(f"Running with {len(nodes)} nodes with {nthreads} threads each ({total_workers} cores)")
    algo_a_name = AlgorithmA().get_name()
    if not single_alg:
        algo_b_name = AlgorithmB().get_name()
        algo_c_name = AlgorithmC().get_name()
        algo_d_name = AlgorithmD().get_name()
        algo_e_name = AlgorithmE().get_name()
    if single_alg:
        deps = {
            algo_a_name: []
        }
    else:
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
    if not single_alg:
        to_retrieve = [algo_d_name, algo_e_name]
    else:
        to_retrieve = [algo_a_name]
    scheduler = Scheduler(deps)
    if max_inflight:
        batch_size = max_inflight
    else:
        batch_size = nevents
    total_sent = 0
    start = time.time()
    while total_sent < nevents:
        if total_sent + batch_size > nevents:
            batch_size = nevents - total_sent
        futures = scheduler.schedule_n_event(batch_size, to_retrieve)
        total_sent += batch_size
        _ = ray.get(futures)
    end_scheduling = time.time()
    end = time.time()
    print(f"Total time to schedule: {(end_scheduling - start) * 1000.0:.2f}ms")
    print(f"Total time to process: {(end - start) * 1000.0:.2f}ms")
    print(f"Time between scheduling end and processing end: {(end - end_scheduling) * 1000.0:.2f}ms")
    print(f"Throughput: { nevents / ((end - start) * 1000.0):.2f} events/ms")
