# Changes from ver 0.9.9.2 to ver 2.0.0

## Command line options

Obsolete:

    -L, --logfile [FILE]

New:

    -L, --log, --log-dir [DIRECTORY]
    --report LOGDIR
    --clear-gfarm2fs


## pwrake_conf.yaml or environment variables

Obsolete:

    LOGFILE
    TASKLOG
    PROFILE
    NUM_NOACTION_THREADS
    THREAD_CREATE_INTERVAL
    STEAL_WAIT
    STEAL_WAIT_MAX

New:

    LOG_DIR
    LOG_FILE
    TASK_CSV_FILE
    COMMAND_CSV_FILE
    GC_LOG_FILE
    SHELL_COMMAND
    SHELL_RC
    HEARTBEAT
    FAILURE_TERMINATION
    SHELL_START_INTERVAL
    GFARM2FS_OPTION
    GFARM2FS_DEBUG
    GFARM2FS_DEBUG_WAIT
