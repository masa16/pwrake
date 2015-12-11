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

## The use of Fiber instead of Thread
* Every Rake Task runs in parallel under Fiber context, instead of Thread.
* Fiber context does not switch in task action blocks.
Instead, it switches in "sh" methods, or outside of task action blocks.

Rakefile:
```ruby
T = (1..4).map do |i|
  task "task#{i}" do
    sleep 1             # Ruby's sleep method: no context switch
  end.name
end

task :default => T
```

Result:

    $ time pwrake -j 4
    
    real	0m4.294s
    user	0m0.151s
    sys     0m0.028s

Rakefile:
```ruby
T = (1..4).map do |i|
  task "task#{i}" do
    sh "sleep 1"        # sleep commands run in parallel
  end.name
end

task :default => T
```

Result:

    $ time pwrake -j 4
    sleep 1
    sleep 1
    sleep 1
    sleep 1
    
    real	0m1.299s
    user	0m0.155s
    sys     0m0.030s
