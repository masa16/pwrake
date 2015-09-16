module Pwrake

  class Option

    attr_reader :worker_option
    attr_reader :worker_progs
    attr_reader :queue_class

    def setup_filesystem

      @worker_progs = %w[ writer log_executor executor invoker shared_directory ]
      @worker_option = {
        :base_dir  => "",
        :work_dir  => self['WORK_DIR'],
        :log_dir   => self['LOG_DIR'],
        :pass_env  => self['PASS_ENV'],
        :ssh_option => self['SSH_OPTION'],
        :shell_command => self['SHELL_COMMAND'],
        :shell_rc  => self['SHELL_RC'],
        :heartbeat_timeout => self['HEARTBEAT_TIMEOUT']
      }

      if @filesystem.nil?
        case mount_type
        when /gfarm2fs/
          self['FILESYSTEM'] = @filesystem = 'gfarm'
        end
      end

      #n_noaction_th = self['NUM_NOACTION_THREADS']

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
          :debug_gfarm2fs => self['DEBUG'],
          :single_mp => self['GFARM_SINGLE_MP']
        })
        @worker_progs << "gfarm_directory"

	if self['DISABLE_AFFINITY']
	  @queue_class = "TaskQueue"
	else
	  @queue_class = "LocalityAwareQueue"
	end
        #@num_noaction_threads = (n_noaction_th || [8,@host_map.num_threads].max).to_i
      else
        @filesystem  = 'nfs'
        @queue_class = "TaskQueue"
        #@num_noaction_threads = (n_noaction_th || 1).to_i
        @worker_option[:shared_directory] = "SharedDirectory"
      end
      @worker_progs << "worker_main"
      Log.debug "@queue_class=#{@queue_class}"
    end

    def pool_postprocess(dispatcher)
      require "pwrake/master/fiber_pool"
      require "pwrake/gfarm/gfwhere_handler"
      case @filesystem
      when 'gfarm'
        max = self['MAX_GFWHERE_WORKER']
        FiberPool.new(GfwhereHandler,max,dispatcher)
      else
        nil
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
