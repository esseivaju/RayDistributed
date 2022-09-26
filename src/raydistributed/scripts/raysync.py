#!/usr/bin/env python

import argparse
import os
import time

import ray


def wait_on_head() -> None:
    """
    Repeatedly tries to connect to the ray cluster by using the same environment variables that raythena uses. Exits
    once it successfully connected to the cluster.

    Returns:
        None
    """
    timeout = 300
    max_time = time.time() + timeout
    while not ray.is_initialized():
        try:
            print("waiting for ray to initialize...")
            ray.init(address="auto")
        except (ConnectionError, ValueError):
            time.sleep(1)
            if time.time() >= max_time:
                exit(1)


def count_nodes():
    """

    Count alive and dead nodes in the ray cluster
    Returns:
        tuple with the number of (alive, dead) nodes in the cluster
    """
    alive = 0
    dead = 0
    for node in ray.nodes():
        if node['alive']:
            alive += 1
        else:
            dead += 1
    return alive, dead


def wait_workers(nwait: int = 1) -> None:
    """
    Blocks until the number of nodes in the ray cluster is greater than nwait. ray.init() must be called
    before calling this function.

    Args:
        nwait: number of workers (cluster size - 1) in the ray cluster

    Returns:
        None
    """
    # wait for all workers to connect
    alive = 0
    timeout = 300
    max_time = time.time() + timeout
    while alive < nwait:
        alive, dead = count_nodes()
        print(f"{alive}/{nwait} nodes connected ({dead} nodes marked as dead)... ")
        time.sleep(1)
        if time.time() > max_time:
            exit(1)
    alive, dead = count_nodes()
    print(f"{alive}/{nwait} nodes connected ({dead} nodes marked as dead)... ")


def main():
    parser = argparse.ArgumentParser(description='Wait on ray head node or workers to connect')
    parser.add_argument('--wait-workers', action='store_true')
    parser.add_argument('--nworkers',
                        dest="nworkers",
                        default=int(os.environ.get("SLURM_NNODES", 1)) - 1,
                        type=int)
    args = parser.parse_args()
    wait_on_head()
    if args.wait_workers:
        wait_workers(args.nworkers)


if __name__ == "__main__":
    main()
