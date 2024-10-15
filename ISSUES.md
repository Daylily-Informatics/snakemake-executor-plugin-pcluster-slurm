# In lieu of access to GH issues... this


1. Change job names to be meaningful.
2. Make `--comment` easy to pass through from config, ie with --default-resources comment='my-comment'
3. Make random `slurm` flags easier to pass through from config, esp for globals.
4. re-review the core plugin code, consider what can be added to make use of the novel aspects of pcluster which are not in the vanilla slurm plugin.
5. This is optimized to minimally run, this has not been heavily tuned for heavy and large compute environments.  It might be OK, but this is not known to be the case.