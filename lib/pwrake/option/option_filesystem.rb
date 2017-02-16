require "parallel"

module Pwrake

  class Option

    attr_reader :worker_option
    attr_reader :worker_progs
    attr_reader :queue_class

    def setup_filesystem

      @worker_progs = %w[
        parallel/processor_count.rb
        pwrake/nbio
        pwrake/branch/fiber_queue
        pwrake/worker/writer
        pwrake/worker/log_executor
        pwrake/worker/executor
        pwrake/worker/invoker
        pwrake/worker/shared_directory
      ]
      @worker_option = {
        :base_dir  => "",
        :work_dir  => self['WORK_DIR'],
        :log_dir   => self['LOG_DIR'],
        :pass_env  => self['PASS_ENV'],
        :ssh_option => self['SSH_OPTION'],
        :heartbeat => self['HEARTBEAT']
      }

      if @filesystem.nil?
        case mount_type
        when /gfarm2fs/
          self['FILESYSTEM'] = @filesystem = 'gfarm'
        end
      end

      case @filesystem
      when 'gfarm'
        require "pwrake/queue/locality_aware_queue"
        require "pwrake/gfarm/gfarm_path"
        GfarmPath.subdir = self['GFARM_SUBDIR']
        @filesystem  = 'gfarm'
        base = self['GFARM_BASEDIR']
        prefix = self['GFARM_PREFIX']
        mntpnt = "#{base}/#{prefix}"
        @worker_option.merge!({
          :shared_directory => "GfarmDirectory",
          :base_dir => mntpnt,
          :work_dir => GfarmPath.pwd.to_s,
          :gfarm2fs_option => self['GFARM2FS_OPTION'],
          :gfarm2fs_debug => self['GFARM2FS_DEBUG'],
          :gfarm2fs_debug_wait => self['GFARM2FS_DEBUG_WAIT'],
          :single_mp => self['GFARM_SINGLE_MP']
        })
        @worker_progs.push "pwrake/worker/gfarm_directory"

	if self['DISABLE_AFFINITY']
	  @queue_class = "TaskQueue"
	else
	  @queue_class = "LocalityAwareQueue"
	end
      else
        @filesystem  = 'nfs'
        @queue_class = "TaskQueue"
        @worker_option[:shared_directory] = "SharedDirectory"
      end
      @worker_progs.push "pwrake/worker/worker_main"
    end

    def max_postprocess_pool
      case @filesystem
      when 'gfarm'
        self['MAX_GFWHERE_WORKER']
      else
        1
      end
    end

    def postprocess(runner)
      case @filesystem
      when 'gfarm'
        require "pwrake/gfarm/gfarm_postprocess"
        GfarmPostprocess.new(runner)
      else
        require "pwrake/master/postprocess"
        Postprocess.new(runner)
      end
    end

    def mount_type(d=nil)
      mtab = '/etc/mtab'
      if File.exist?(mtab)
        d ||= mountpoint_of_cwd
        open(mtab,'r') do |f|
          f.each_line do |l|
            if /#{d} (?:type )?(\S+)/o =~ l
              return $1
            end
          end
        end
      end
      nil
    end

    def mountpoint_of_cwd
      d = Pathname.pwd
      while !d.mountpoint?
        d = d.parent
      end
      d
    end

  end
end
