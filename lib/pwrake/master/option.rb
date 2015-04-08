module Pwrake

  START_TIME = Time.now

  class Option < Hash

    def initialize
      load_pwrake_conf
      init_options
      if self['SHOW_CONF']
        require "yaml"
        YAML.dump(self,$stdout)
        exit
      end
      init_pass_env
    end

    def init
      Log.debug "Options:"
      self.each do |k,v|
	Log.debug " #{k} = #{v.inspect}"
      end
      #@counter = Counter.new
      setup_hosts
      setup_filesystem # require 'option_filesystem.rb'
      #
      if self['GC_PROFILE']
        GC::Profiler.enable
      end
    end

    attr_reader :counter
    #attr_reader :logfile
    attr_reader :logger

    DEFAULT_CONFFILES = ["pwrake_conf.yaml","PwrakeConf.yaml"]

    # ----- init -----

    def load_pwrake_conf
      # Read pwrake_conf
      pwrake_conf = Rake.application.options.pwrake_conf
      if pwrake_conf
        if !File.exist?(pwrake_conf)
          raise "Configuration file not found: #{pwrake_conf}"
        end
      else
        pwrake_conf = DEFAULT_CONFFILES.find{|fn| File.exist?(fn)}
      end
      self['PWRAKE_CONF'] = pwrake_conf
      if pwrake_conf.nil?
        @yaml = {}
      else
        #Log.debug "--- pwrake_conf=#{pwrake_conf}"
        require "yaml"
        @yaml = open(pwrake_conf){|f| YAML.load(f) }
      end
    end

    def init_options
      option_data.each do |a|
        prc = nil
        keys = []
        case a
        when String
          keys << a
        when Array
          a.each do |x|
            case x
            when String
              keys << x
            when Proc
              prc = x
            end
          end
        end
        key = keys[0]
        val = search_opts(keys)
        val = prc.call(val) if prc
        self[key] = val if !val.nil?
        instance_variable_set("@"+key.downcase, val)
      end

      feedback_options

      Rake.verbose(false) if Rake.application.options.silent
    end

    def option_data
      [
        'DRYRUN',
        'IGNORE_SYSTEM',
        'IGNORE_DEPRECATE',
        'LOAD_SYSTEM',
        'NOSEARCH',
        'RAKELIB',
        'SHOW_PREREQS',
        'SILENT',
        'TRACE',
        'BACKTRACE',
        'TRACE_OUTPUT',
        'TRACE_RULES',

        'FILESYSTEM',
        'SSH_OPTION',
        'PASS_ENV',
        'GNU_TIME',
        'DEBUG',
        'PLOT_PARALLELISM',
        'HALT_QUEUE_WHILE_SEARCH',
        'SHOW_CONF',
        'FAILED_TARGET', # rename(default), delete, leave
        'QUEUE_PRIORITY', # RANK(default), FIFO, LIFO, DFS
        #'NOACTION_QUEUE_PRIORITY', # FIFO(default), LIFO, RAND
        #'NUM_NOACTION_THREADS', # default=4 when gfarm, else 1
        'STEAL_WAIT',
        'STEAL_WAIT_MAX',
        'GRAPH_PARTITION',
        'PLOT_PARTITION',

        ['HOSTFILE','HOSTS'],
        ['LOGDIR',
          proc{|v|
            if v
              if v == "" || !v.kind_of?(String)
                v = "log_%Y%m%d-%H%M%S_%$"
              end
              format_time_pid(v)
            end
          }],
        ['LOGFILE','LOG',
          proc{|v|
            if v
              # turn trace option on
              # Rake.application.options.trace = true
              if v == "" || !v.kind_of?(String)
                v = "%Y%m%d-%H%M%S_%$.log"
              end
              format_time_pid(v)
            end
          }],
        ['TASKLOG',
          proc{|v|
            if v
              if v == "" || !v.kind_of?(String)
                v = "%Y%m%d-%H%M%S_%$_task.csv"
              end
              format_time_pid(v)
            end
          }],
        ['PROFILE','CMDLOG',
          proc{|v|
            if v
              if v == "" || !v.kind_of?(String)
                v = "%Y%m%d-%H%M%S_%$_cmd.csv"
              end
              format_time_pid(v)
            end
          }],
        ['GC_PROFILE',
         proc{|v|
            if v
              if v == "" || !v.kind_of?(String)
                v = "%Y%m%d-%H%M%S_%$_gc"
              end
              format_time_pid(v)
            end
         }],
        ['NUM_THREADS', proc{|v| v && v.to_i}],
        ['THREAD_CREATE_INTERVAL', proc{|v| (v || 0.010).to_f}],
        ['DISABLE_AFFINITY', proc{|v| v || ENV['AFFINITY']=='off'}],
        ['DISABLE_STEAL', proc{|v| v || ENV['STEAL']=='off'}],
        ['GFARM_BASEDIR', proc{|v| v || '/tmp'}],
        ['GFARM_PREFIX', proc{|v| v || "pwrake_#{ENV['USER']}"}],
        ['GFARM_SUBDIR', proc{|v| v || '/'}],
        ['MAX_GFWHERE_WORKER', proc{|v| (v || 8).to_i}],
        ['MASTER_HOSTNAME', proc{|v| (v || begin;`hostname -f`;rescue;end || '').chomp}],
        ['WORK_DIR',proc{|v|
            v ||= '%CWD_RELATIVE_TO_HOME'
            v.sub('%CWD_RELATIVE_TO_HOME',cwd_relative_to_home)
          }],
      ]
    end

    def format_time_pid(v)
      START_TIME.strftime(v).sub("%$","%05d"%Process.pid)
    end

    def feedback_options
      opts = Rake.application.options
      ['DRYRUN',
       'IGNORE_SYSTEM',
       'IGNORE_DEPRECATE',
       'LOAD_SYSTEM',
       'NOSEARCH',
       'RAKELIB',
       'SHOW_PREREQS',
       'SILENT',
       'TRACE',
       'BACKTRACE',
       'TRACE_OUTPUT',
       'TRACE_RULES'
      ].each do |k|
        if v=self[k]
          m = (k.downcase+"=").to_sym
          opts.send(m,v)
        end
      end
      case opts.trace_output
      when 'stdout'
        opts.trace_output = $stdout
      when 'stderr', nil
        opts.trace_output = $stderr
      end
    end

    # Priority of Option:
    #  command_option > ENV > pwrake_conf > DEFAULT_OPTIONS
    def search_opts(keys)
      val = Rake.application.options.send(keys[0].downcase.to_sym)
      return parse_opt(val) if !val.nil?
      #
      keys.each do |k|
        val = ENV[k.upcase]
        return parse_opt(val) if !val.nil?
      end
      #
      return nil if !@yaml
      keys.each do |k|
        val = @yaml[k.upcase]
        return val if !val.nil?
      end
      nil
    end

    def parse_opt(s)
      case s
      when /^(false|nil|off)$/i
        false
      when /^(true|on)$/i
        true
      when $stdout
        "stdout"
      when $stderr
        "stderr"
      else
        s
      end
    end

    def cwd_relative_to_home
      Pathname.pwd.relative_path_from(Pathname.new(ENV['HOME'])).to_s
    end

    def cwd_relative_if_under_home
      home = Pathname.new(ENV['HOME']).realpath
      path = pwd = Pathname.pwd.realpath
      while path != home
        if path.root?
          return pwd.to_s
        end
        path = path.parent
      end
      return pwd.relative_path_from(home).to_s
    end

    # ------------------------------------------------------------------------

    def init_pass_env
      if envs = self['PASS_ENV']
        pass_env = {}

        case envs
        when Array
          envs.each do |k|
            k = k.to_s
            if v = ENV[k]
              pass_env[k] = v
            end
          end
        when Hash
          envs.each do |k,v|
            k = k.to_s
            if v = ENV[k] || v
              pass_env[k] = v
            end
          end
        else
          raise "invalid option for PASS_ENV in pwrake_conf.yaml"
        end

        if pass_env.empty?
          self.delete('PASS_ENV')
        else
          self['PASS_ENV'] = pass_env
        end
      end
    end

    # ------------------------------------------------------------------------


    # ----- setup ------------------------------------------------------------

    #def setup_option
    #  set_hosts
    #  set_filesystem
    #end

    def setup_hosts
      if @hostfile && @num_threads
        raise "Cannot set `hostfile' and `num_threads' simultaneously"
      end
      @host_map = HostMap.new(@hostfile || @num_threads)
      #Log.info "num_cores=#{@host_map.size}"
    end
    attr_reader :host_map

    # ----- finish -----

    def finish_option
      Log.close
    end

  end
end
