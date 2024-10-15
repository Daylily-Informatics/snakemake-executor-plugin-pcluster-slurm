[SLURM](https://slurm.schedmd.com/documentation.html) is a widely used
batch system for performance compute clusters.

[AWS Parallel Cluster](https://aws.amazon.com/hpc/parallelcluster/) is a framework to deploy and manage dynamically scalable HPC clusters on AWS, running SLURM as the batch system, and `pcluster` manages all of the creating, configuring, and deleting of the cluster compute nodes. Nodes may be spot or dedicated.  **note**, the `AWS Parallel Cluster` port of slurm has a few small, but critical differences from the standard slurm distribution. Importantly, `sacct` is not enabled, so any reliance on this command will break.


This executor plugin allows to use slurm via AWS Parallel Cluster as an executor for snakemake via a `pcluster` headnode. For example usage of this executor, see: [daylily](https://github.com/DaylilyProject/daylily).
