require "pwrake/master/postprocess"

module Pwrake

  module DefaultFileSystemOption

    def option_data_filesystem
      []
    end

    def set_filesystem_option
      @worker_progs = %w[
        pwrake/nbio
        pwrake/branch/fiber_queue
        pwrake/worker/writer
        pwrake/worker/log_executor
        pwrake/worker/executor
        pwrake/worker/invoker
        pwrake/worker/shared_directory
        pwrake/worker/worker_main
      ]
      @worker_option = {
        :base_dir  => "",
        :work_dir  => self['WORK_DIR'],
        :log_dir   => self['LOG_DIR'],
        :gnu_time  => self['GNU_TIME'],
        :pass_env  => self['PASS_ENV'],
        :ssh_option => self['SSH_OPTION'],
        :heartbeat => self['HEARTBEAT'],
        :shared_directory => "SharedDirectory"
      }
      @queue_class = "NonLocalityQueue"
    end

    def max_postprocess_pool
      1
    end

    def postprocess(runner)
      Postprocess.new(runner)
    end
  end

  class Option
    include DefaultFileSystemOption
  end
end
