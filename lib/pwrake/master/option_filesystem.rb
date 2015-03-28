module Pwrake

  class Option

    attr_reader :worker_option
    attr_reader :shell_class
    attr_reader :queue_class
    attr_reader :postprocess

    def setup_filesystem

      @worker_option = {
        :work_dir  => self['WORK_DIR'],
        :pass_env  => self['PASS_ENV'],
        :ssh_opt   => self['SSH_OPTION']
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
        require "pwrake/gfarm"
        GfarmPath.subdir = self['GFARM_SUBDIR']
        @filesystem  = 'gfarm'
        @shell_class = Shell
        base = self['GFARM_BASEDIR']
        prefix = self['GFARM_PREFIX']
        mntpnt = "#{base}/#{prefix}_%02d"
        @worker_option.merge!({
          :work_dir  => GfarmPath.pwd.to_s,
          :filesystem => "gfarm:"+mntpnt,
          :single_mp => self['GFARM_SINGLE_MP']
        })

	if self['DISABLE_AFFINITY']
	  @queue_class = TaskQueue
	else
	  @queue_class = LocalityAwareQueue
	end
        #@num_noaction_threads = (n_noaction_th || [8,@host_map.num_threads].max).to_i
        @postprocess = GfarmPostprocess.new
        Log.debug "--- @queue_class=#{@queue_class}"
      else
        @filesystem  = 'nfs'
        @shell_class = Shell
        @queue_class = TaskQueue
        #@queue_class = LocalityAwareQueue
        #@num_noaction_threads = (n_noaction_th || 1).to_i
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
