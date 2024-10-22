# Snakemake executor plugin: pcluster-slurm v_0.0.7_

# Snakemake Executor Plugins (generally)
[Snakemake plugin catalog docs](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor).

## `pcluster-slurm` plugin
### AWS Parallel Cluster, `pcluster` `slurm`
[AWS Parallel Cluster](https://aws.amazon.com/hpc/parallelcluster/) is a framework to deploy and manage dynamically scalable HPC clusters on AWS, running SLURM as the batch system, and `pcluster` manages all of the creating, configuring, and deleting of the cluster compute nodes. Nodes may be spot or dedicated.  **note**, the `AWS Parallel Cluster` port of slurm has a few small, but critical differences from the standard slurm distribution.  This plugin enables using slurm from pcluster head and compute nodes via snakemake `>=v8.*`.

#### [Daylily Bfx Framework](https://github.com/Daylily-Informatics/daylily)
[Daylily](https://github.com/Daylily-Informatics/daylily) is a bioinformatics framework that automates and standardizes all aspects of creating a self-scaling ephemeral cluster which can grow from 1 head node to many thousands of as-needed compute spot instances (modulo your quotas and budget). This is accomplished by using [AWS Parallel Cluster](https://aws.amazon.com/hpc/parallelcluster/) to manage the cluster, and snakemake to manage the bfx workflows. In this context, `slurm` is the intermediary between snakemake and the cluster resource management. The `pcluster` slurm variant does not play nicely with vanilla slurm, and to date, the slurm snakemake executor has not worked with `pcluster` slurm. This plugin is a bridge between snakemake and `pcluster-slurm`.



# Pre-requisites
## Snakemake >=8.*
### Conda
```bash
conda create -n snakemake -c conda-forge -c bioconda snakemake==8.20.6
conda activate snakemake
```

# Installation (pip)
_from an environment with snakemake and pip installed_
```bash
pip install snakemake-executor-plugin-pcluster-slurm
```

# Example Usage [daylily cluster headnode](https://github.com/Daylily-Informatics/daylily)
```bash
mkdir -p /fsx/resources/environments/containers/ubuntu/cache/
export SNAKEMAKE_OUTPUT_CACHE=/fsx/resources/environments/containers/ubuntu/cache/
snakemake --use-conda --use-singularity -j 10  --singularity-prefix /fsx/resources/environments/containers/ubuntu/ip-10-0-0-240/ --singularity-args "  -B /tmp:/tmp -B /fsx:/fsx  -B /home/$USER:/home/$USER -B $PWD/:$PWD" --conda-prefix /fsx/resources/environments/containers/ubuntu/ip-10-0-0-240/ --executor pcluster-slurm --default-resources slurm_partition='i64,i128,i192' --cache  --verbose -k
```



## What Partitions Are Available?
Use `sinfo` to learn about your cluster (note, `sinfo` reports on all potential and active compute nodes. Read the docs to interpret which are active, which are not yet requested s\
pot instances, etc). Below is what the [daylily AWS parallel cluster](https://github.com/Daylily-Informatics/daylily/blob/main/config/day_cluster/prod_cluster.yaml) looks like.

```bash
sinfo
PARTITION AVAIL  TIMELIMIT  NODES  STATE NODELIST
i8*          up   infinite     12  idle~ i8-dy-gb64-[1-12]
i64          up   infinite     16  idle~ i64-dy-gb256-[1-8],i64-dy-gb512-[1-8]
i96          up   infinite     16  idle~ i96-dy-gb384-[1-8],i96-dy-gb768-[1-8]
i128         up   infinite     28  idle~ i128-dy-gb256-[1-8],i128-dy-gb512-[1-10],i128-dy-gb1024-[1-10]
i192         up   infinite     30  idle~ i192-dy-gb384-[1-10],i192-dy-gb768-[1-10],i192-dy-gb1536-[1-10]
a192         up   infinite     30  idle~ a192-dy-gb384-[1-10],a192-dy-gb768-[1-10],a192-dy-gb1536-[1-10]
```
-  As I look at this, it is possible that if unset, the partition will default to `i8` in the output above. Maybe.

  

# Other Cool Stuff
## Real Time Cost Tracking & Use Throttling via Budgets, Tagging ... and the `--comment` sbatch flag.
I etensively make use of  [Cost allocation tags with AWS ParallelCluster](https://github.com/Daylily-Informatics/aws-parallelcluster-cost-allocation-tags) in the [daylily omics analysis framework](https://github.com/Daylily-Informatics/daylily?tab=readme-ov-file#daylily-aws-ephemeral-cluster-setup-0714) [_$3 30x WGS analysis_](https://github.com/Daylily-Informatics/daylily?tab=readme-ov-file#3-30x-fastq-bam-bamdeduplicated-snvvcfsvvcf-add-035-for-a-raft-of-qc-reports)  to track AWS cluster usage costs in realtime, and impose limits where appropriate (by user and project). This makes use of overriding the `--comment` flag to hold `project/budget` tags applied to ephemeral AWS resources, and thus enabling cost tracking/controls.

* To change the	--comment flag in v`0.0.8` of the pcluster-slurm plugin, set the comment flag value in the envvar `SMK_SLURM_COMMENT=RandD` (RandD is the default).
 
 
 
