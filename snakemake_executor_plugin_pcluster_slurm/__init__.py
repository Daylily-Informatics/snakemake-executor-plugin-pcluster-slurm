__author__ = "John Major & Cal"
__copyright__ = "Copyright 2023, John Major & Cal"
__email__ = "john@daylilyinformatics.com"
__license__ = "MIT"

import csv
from io import StringIO
import os
import re
import shlex
import subprocess
import signal
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Generator, Optional
import uuid
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.settings import (
    ExecutorSettingsBase,
    CommonSettings,
)
from snakemake_interface_executor_plugins.jobs import (
    JobExecutorInterface,
)
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_executor_plugin_slurm_jobstep import get_cpus_per_task

from .utils import delete_slurm_environment


@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    init_seconds_before_status_checks: Optional[int] = field(
        default=40,
        metadata={
            "help": """
                    Defines the time in seconds before the first status
                    check is performed after job submission.
                    """,
            "env_var": False,
            "required": False,
        },
    )
    requeue: bool = field(
        default=False,
        metadata={
            "help": """
                    Allow requeuing preempted of failed jobs,
                    if no cluster default. Results in `sbatch ... --requeue ...`
                    This flag has no effect, if not set.
                    """,
            "env_var": False,
            "required": False,
        },
    )


# Required:
# Specify common settings shared by various executors.
common_settings = CommonSettings(
    # define whether your executor plugin executes locally
    # or remotely. In virtually all cases, it will be remote execution
    # (cluster, cloud, etc.). Only Snakemake's standard execution
    # plugins (snakemake-executor-plugin-dryrun, snakemake-executor-plugin-local)
    # are expected to specify False here.
    non_local_exec=True,
    # Define whether your executor plugin implies that there is no shared
    # filesystem (True) or not (False).
    # This is e.g. the case for cloud execution.
    implies_no_shared_fs=False,
    job_deploy_sources=False,
    pass_default_storage_provider_args=True,
    pass_default_resources_args=True,
    pass_envvar_declarations_to_cmd=False,
    auto_deploy_default_storage_provider=False,
    # wait a bit until slurmdbd has job info available
    init_seconds_before_status_checks=40,
    pass_group_args=True,
)


# Required:
# Implementation of your executor
class Executor(RemoteExecutor):
    def __post_init__(self):
        # run check whether we are running in a SLURM job context
        self.warn_on_jobcontext()
        self.run_uuid = str(uuid.uuid4())
        self.logger.info(f"SLURM run ID: {self.run_uuid}")
        self._fallback_account_arg = None
        self._fallback_partition = None
        self._preemption_warning = False  # no preemption warning has been issued
        # Register signal handlers to ensure jobs are killed on exit
        self.active_jobs = []
        signal.signal(signal.SIGINT, self._handle_exit)
        signal.signal(signal.SIGTERM, self._handle_exit)

    def _handle_exit(self, signum, frame):
        """Handle termination signals to cancel active Slurm jobs."""
        self.logger.info(f"Received signal {signum}. Canceling all active jobs.")
        
        self.cancel_jobs(self.active_jobs)
        self.logger.info("All jobs canceled. Exiting.")
        exit(1)

    def warn_on_jobcontext(self, done=None):
        if not done:
            if "SLURM_JOB_ID" in os.environ:
                self.logger.warning(
                    "You are running snakemake in a SLURM job context. "
                    "This is not recommended, as it may lead to unexpected behavior. "
                    "Please run Snakemake directly on the login node."
                )
                time.sleep(5)
                delete_slurm_environment()
        done = True

    def additional_general_args(self):
        return "--executor slurm-jobstep --jobs 1"

    def run_job(self, job: JobExecutorInterface):
        """Submit a job to SLURM and track it for cancellation if needed."""
        # Determine job group or rule name
        group_or_rule = f"group_{job.name}" if job.is_group() else f"rule_{job.name}"
        wildcard_str = "_".join(job.wildcards) if job.wildcards else ""

        # Prepare log file paths
        log_dir = f".snakemake/slurm_logs/{group_or_rule}/{wildcard_str}"
        os.makedirs(log_dir, exist_ok=True)
        
        slurm_logfile = os.path.abspath(f"{log_dir}/%j.log")
        slurm_errorlogfile = os.path.abspath(f"{log_dir}/%j.err")

        # SLURM job submission command
        comment_str = os.getenv('SMK_SLURM_COMMENT', 'RandD')
        call = (
            f"sbatch --parsable --no-requeue "
            f"--comment '{comment_str}' "
            f"--job-name '{job.name}-{self.run_uuid}' "
            f"--chdir {os.getcwd()} "
            f"--error '{slurm_errorlogfile}' "
            f"--output '{slurm_logfile}' "
        )

        # Add partition and resources if defined
        call += self.get_partition_arg(job)
        call += f" --ntasks={job.resources.get('tasks', 1)} "
        call += f" --cpus-per-task={max(1, get_cpus_per_task(job))} "
        if job.resources.get("mem_mb"):
            call += f" --mem={job.resources['mem_mb']}"

        # Generate the execution command
        exec_job = self.format_job_exec(job)
        call += f" -D {self.workflow.workdir_init} <<EOF\n#!/bin/bash\n{exec_job}\nEOF"

        # Submit the job to SLURM
        try:
            out = subprocess.check_output(call, shell=True, text=True, stderr=subprocess.STDOUT).strip()
            slurm_jobid = out.split(";")[0]
            slurm_logfile = slurm_logfile.replace("%j", slurm_jobid)

            self.logger.info(f"Job {job.jobid} submitted with SLURM jobid {slurm_jobid} (log: {slurm_logfile}).")

            # Track active job for future cancellation
            submitted_job = SubmittedJobInfo(job, external_jobid=slurm_jobid, aux={"slurm_logfile": slurm_logfile})
            self.active_jobs.append(submitted_job)
            self.report_job_submission(submitted_job)

        except subprocess.CalledProcessError as e:
            self.logger.error(f"SLURM job submission failed: {e.output}")
            raise WorkflowError(f"SLURM job submission failed: {e.output}")


            self.logger.debug(f"sbatch call: {call}")
            try:
                out = subprocess.check_output(
                    call, shell=True, text=True, stderr=subprocess.STDOUT
                ).strip()
            except subprocess.CalledProcessError as e:
                raise WorkflowError(
                    f"SLURM job submission failed. The error message was {e.output}"
                )

            # multicluster submissions yield submission infos like
            # "Submitted batch job <id> on cluster <name>" by default, but with the
            # --parsable option it simply yields "<id>;<name>".
            # To extract the job id we split by semicolon and take the first element
            # (this also works if no cluster name was provided)
            slurm_jobid = out.split(";")[0]
            slurm_logfile = slurm_logfile.replace("%j", slurm_jobid)
            self.logger.info(
                f"Job {job.jobid} has been submitted with SLURM jobid {slurm_jobid} "
                f"(log: {slurm_logfile})."
            )
            self.report_job_submission(
                SubmittedJobInfo(
                    job, external_jobid=slurm_jobid, aux={"slurm_logfile": slurm_logfile}
                )
            )
        
    async def check_active_jobs(
        self, active_jobs: List[SubmittedJobInfo]
    ) -> Generator[SubmittedJobInfo, None, None]:
        fail_stati = (
            "BOOT_FAIL",
            "CANCELLED",
            "DEADLINE",
            "FAILED",
            "NODE_FAIL",
            "OUT_OF_MEMORY",
            "TIMEOUT",
            "PREEMPTED",
            "SUSPENDED",
            "STOPPED",            
            "REVOKED",  # slurm docs suggest this should be here too
        )

        for job_info in active_jobs:
            jobid = job_info.external_jobid
            async with self.status_rate_limiter:
                try:
                    # Run scontrol command
                    command = f"scontrol -o show job {jobid}"
                    command_res = subprocess.check_output(
                        command, text=True, shell=True, stderr=subprocess.PIPE
                    )
                    # Parse JobState
                    match = re.search(r'JobState=(\S+)', command_res)
                    if match:
                        status = match.group(1)
                    else:
                        # If JobState is not found, assume unknown status
                        status = "UNKNOWN"

                    self.logger.debug(f"Job {jobid} status: {status}")

                    if status == "COMPLETED":
                        self.report_job_success(job_info)
                    elif status in fail_stati:
                        msg = (
                            f"SLURM job '{jobid}' failed with status '{status}'."
                        )
                        self.report_job_error(job_info, msg=msg, aux_logs=[job_info.aux["slurm_logfile"]])
                    else:
                        # Job is still running or pending
                        yield job_info
                except subprocess.CalledProcessError as e:
                    # Handle errors from scontrol
                    self.logger.error(
                        f"Failed to get status of job {jobid} with scontrol: {e.stderr.strip()}"
                    )
                    # Assume job has failed
                    msg = f"Failed to get status of job {jobid}."
                    self.report_job_error(job_info, msg=msg, aux_logs=[job_info.aux["slurm_logfile"]])
                except Exception as e:
                    # Handle any other exceptions
                    self.logger.error(f"Unexpected error while checking job {jobid}: {e}")
                    # Assume job is still running
                    yield job_info

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        """Cancel all active Slurm jobs."""
        if active_jobs:
            jobids = " ".join([job_info.external_jobid for job_info in active_jobs])
            try:
                scancel_command = f"scancel {jobids}"
                subprocess.check_output(
                    scancel_command,
                    text=True,
                    shell=True,
                    timeout=60,
                    stderr=subprocess.PIPE,
                )
                self.logger.info(f"Successfully canceled jobs: {jobids}")
            except subprocess.TimeoutExpired:
                self.logger.warning("Unable to cancel jobs within the timeout period.")
            except subprocess.CalledProcessError as e:
                msg = e.stderr.strip()
                self.logger.error(f"Failed to cancel jobs: {msg}")


    def get_partition_arg(self, job: JobExecutorInterface):
        """
        checks whether the desired partition is valid,
        returns a default partition, if applicable
        else raises an error - implicetly.
        """
        if job.resources.get("slurm_partition"):
            partition = job.resources.slurm_partition
        else:
            if self._fallback_partition is None:
                self._fallback_partition = self.get_default_partition(job)
            partition = self._fallback_partition
        if partition:
            return f" -p {partition}"
        else:
            return ""
        

    def get_default_partition(self, job):
        """
        if no partition is given, checks whether a fallback onto a default
        partition is possible
        """
        try:
            out = subprocess.check_output(
                r"sinfo -o %P", shell=True, text=True, stderr=subprocess.PIPE
            )
        except subprocess.CalledProcessError as e:
            raise WorkflowError(
                f"Failed to run sinfo for retrieval of cluster partitions: {e.stderr}"
            )
        for partition in out.split():
            # A default partition is marked with an asterisk, but this is not part of
            # the name.
            if "*" in partition:
                # the decode-call is necessary, because the output of sinfo is bytes
                return partition.replace("*", "")
        self.logger.warning(
            f"No partition was given for rule '{job}', and unable to find "
            "a default partition."
            " Trying to submit without partition information."
            " You may want to invoke snakemake with --default-resources "
            "'slurm_partition=<your default partition>'."
        )
        return ""

    def check_slurm_extra(self, job):
        jobname = re.compile(r"--job-name[=?|\s+]|-J\s?")
        if re.search(jobname, job.resources.slurm_extra):
            raise WorkflowError(
                "The --job-name option is not allowed in the 'slurm_extra' "
                "parameter. The job name is set by snakemake and must not be "
                "overwritten. It is internally used to check the stati of the "
                "all submitted jobs by this workflow."
                "Please consult the documentation if you are unsure how to "
                "query the status of your jobs."
            )
