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
        :output_log => self['OUTPUT_WORKER_LOG'],
        :pass_env  => self['PASS_ENV'],
        :ssh_option => self['SSH_OPTION'],
        :shell_command => self['SHELL_COMMAND'],
        :shell_rc  => self['SHELL_RC'],
        :heartbeat => self['HEARTBEAT']
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
          :gfarm2fs_option => self['GFARM2FS_OPTION'],
          :gfarm2fs_debug => self['DEBUG'],
          :gfarm2fs_debug_wait => self['GFARM2FS_DEBUG_WAIT'],
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
