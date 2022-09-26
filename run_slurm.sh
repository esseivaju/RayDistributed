#!/bin/bash
#SBATCH --cpus-per-task=128
#SBATCH --nodes=40
#SBATCH -C cpu
#SBATCH -q regular
#SBATCH --tasks-per-node=1
#SBATCH --time=02:00:00

set -x

OUTPUT_DIR=/global/homes/e/esseivaj/devel/RayDistributed/weak_scaling-128cores-5algs-$SLURM_JOBID

mkdir $OUTPUT_DIR

# Getting the node names
nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
nodes_array=($nodes)

head_node=${nodes_array[0]}
head_node_ip=$(srun --nodes=1 --ntasks=1 -w "$head_node" hostname --ip-address)

# if we detect a space character in the head node IP, we'll
# convert it to an ipv4 address. This step is optional.
if [[ "$head_node_ip" == *" "* ]]; then
IFS=' ' read -ra ADDR <<<"$head_node_ip"
if [[ ${#ADDR[0]} -gt 16 ]]; then
  head_node_ip=${ADDR[1]}
else
  head_node_ip=${ADDR[0]}
fi
echo "IPV6 address detected. We split the IPV4 address as $head_node_ip"
fi

port=6379
ip_head=$head_node_ip:$port
export ip_head
echo "IP Head: $ip_head"

echo "Starting HEAD at $head_node"
srun --nodes=1 --ntasks=1 -w "$head_node" \
    ray start --head --node-ip-address="$head_node_ip" --port=$port \
    --num-cpus "${SLURM_CPUS_PER_TASK}" --block &

# optional, though may be useful in certain versions of Ray < 1.0.
raysync

# number of nodes other than the head node
worker_num=$((SLURM_JOB_NUM_NODES - 1))
EVENTS_PER_CORE=10
# 2 hyperthreads per core
NCORES=$((${SLURM_CPUS_ON_NODE} / 2))
NEVENTS_PER_NODE=$((${EVENTS_PER_CORE} * ${NCORES}))

for ((i = 1; i <= worker_num; i++)); do
    node_i=${nodes_array[$i]}
    echo "Starting WORKER $i at $node_i"
    srun --nodes=1 --ntasks=1 -w "$node_i" \
        ray start --address "$ip_head" \
        --num-cpus "${SLURM_CPUS_PER_TASK}" --block &
    CLUSSZ=$(($i+1))
    NEVENTS=$(($CLUSSZ * ${NEVENTS_PER_NODE}))
    raysync --wait-workers --nworkers $CLUSSZ
    RayDistributed --nevents ${NEVENTS} --expected-nnodes $CLUSSZ > $OUTPUT_DIR/${CLUSSZ}_nodes_${NEVENTS}_events
done
