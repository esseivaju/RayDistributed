import click
import ray
import time
import os
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
    to_retrieve = [algo_d_name, algo_e_name]
    scheduler = Scheduler(deps)
    futures = []
    start = time.time()
    for i in range(nevents):
        futures.append(scheduler.schedule_event(EventContext(i), to_retrieve))
    end_scheduling = time.time()
    output = ray.get(futures)
    end = time.time()
    print(f"Total time to schedule: {(end_scheduling - start) * 1000.0}ms")
    print(f"Total time to process: {(end - start) * 1000.0}ms")
    print(f"Time between scheduling end and processing end: {(end - end_scheduling) * 1000.0}ms")
    print(f"Throughput: { nevents / ((end - start) * 1000.0)} events/ms")
