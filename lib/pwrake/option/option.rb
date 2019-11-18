require "pathname"
require "yaml"
require "socket"
require "pwrake/option/host_map"

module Pwrake

  def self.clock
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  START_TIME = Time.now
  START_CLOCK = Pwrake.clock

  class Option < Hash

    def initialize
      load_pwrake_conf
      init_filesystem
      init_options
      init_pass_env
      if self['SHOW_CONF']
        require "yaml"
        YAML.dump(self,$stdout)
        exit
      elsif self['REPORT_DIR']
        require 'pwrake/report'
        Report.new(self,[]).report_html
        exit
      end
      setup_hosts
      set_filesystem_option
    end

    attr_reader :counter
    attr_accessor :total_cores

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
        require "yaml"
        @yaml = open(pwrake_conf){|f| YAML.load(f) }
      end
    end

    # ----------------------------------------------------------

    def init_filesystem
      @filesystem = Rake.application.options.filesystem || mount_type
      case @filesystem
      when 'gfarm2fs'
        require "pwrake/option/option_gfarm2fs"
      else
        require "pwrake/option/option_default_filesystem"
      end
    end
    attr_reader :worker_progs
    attr_reader :worker_option
    attr_reader :queue_class

    def mount_type(dir=nil)
      mtab = '/etc/mtab'
      if File.exist?(mtab)
        dir ||= mountpoint_of_cwd
        open(mtab,'r') do |f|
          f.each_line do |l|
            a = l.split
            if a[1] == dir
              return a[2].sub(/^fuse\./,'')
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
      d.to_s
    end

    # ----------------------------------------------------------

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

        'SSH_OPTION',
        'PASS_ENV',
        'GNU_TIME',
        'DEBUG',
        'PLOT_PARALLELISM',
        'SHOW_CONF',
        ['SUBDIR','SUBDIRS',
          proc{|v|
            if Array===v
              v.each do |d|
                if !File.directory?(d)
                  raise "directory #{d.inspect} does not exist"
                end
              end
            elsif !v.nil?
              raise "invalid argument for SUBDIR: #{v.inspect}"
            end
          }
        ],
        ['REPORT_DIR','REPORT'],
        'REPORT_IMAGE',
        'FAILED_TARGET', # rename(default), delete, leave
        'FAILURE_TERMINATION', # wait, kill, continue
        'QUEUE_PRIORITY', # LIHR(default), FIFO, LIFO, RANK
        'NOACTION_QUEUE_PRIORITY', # FIFO(default), LIFO, RAND
        'DISABLE_RANK_PRIORITY',
        ['RESERVE_NODE','RESERVE_HOST'],
        'GRAPH_PARTITION',
        'PLOT_PARTITION',

        ['HOSTFILE','HOSTS'],
        ['LOG_DIR',
          proc{|v|
            if v
              if v == "" || !v.kind_of?(String)
                v = "Pwrake%Y%m%d-%H%M%S"
              end
              d = v = format_time_pid(v)
              i = 1
              while File.exist?(d)
                d = "#{v}.#{i}"
                i += 1
              end
              d
            end
          }],
        ['LOG_FILE',
          proc{|v|
            if v.kind_of?(String) && v != ""
              v
            else
              "pwrake.log"
            end
          }],
        ['TASK_CSV_FILE',
          proc{|v|
            if v.kind_of?(String) && v != ""
              v
            else
              "task.csv"
            end
          }],
        ['COMMAND_CSV_FILE',
          proc{|v|
            if v.kind_of?(String) && v != ""
              v
            else
              "command.csv"
            end
          }],
        ['GC_LOG_FILE',
         proc{|v|
           if v
             if v.kind_of?(String) && v != ""
               v
             else
               "gc.log"
             end
           end
         }],
        ['NUM_THREADS', proc{|v| v && v.to_i}],
        ['SHELL_START_INTERVAL', proc{|v| (v || 0.012).to_f}],
        ['HEARTBEAT', proc{|v| v && v.to_i}],
        ['RETRY', proc{|v| (v || 1).to_i}],
        ['HOST_FAILURE', 'HOST_FAIL', proc{|v| (v || 2).to_i}],
        ['MASTER_HOSTNAME', proc{|v| (v || Socket.gethostname).chomp}],
        ['WORK_DIR', proc{|v|
           v ||= '%CWD_RELATIVE_TO_HOME'
           v.sub('%CWD_RELATIVE_TO_HOME',cwd_relative_if_under_home)
         }],
      ].concat(option_data_filesystem)
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
      when /^(false|nil|off|n|no)$/i
        false
      when /^(true|on|y|yes)$/i
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

    # ----------------------------------------------------------

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

    # ----------------------------------------------------------

    def setup_hosts
      if f = ENV['PBS_NODEFILE']
        if @hostfile
          Log.info "HOSTFILE=#{@hostfile} overrides PBS_NODEFILE=#{f}"
        else
          Log.info "use PBS_NODEFILE=#{f}"
          @hostfile = f
        end
      end
      if @hostfile && @num_threads
        raise "Cannot set `hostfile' and `num_threads' simultaneously"
      end
      @host_map = HostMap.new(@hostfile || @num_threads)
    end
    attr_reader :host_map

    # ----------------------------------------------------------

    def put_log
      Log.info "Pwrake::VERSION=#{Pwrake::VERSION}"
      Log.info "Options:"
      self.each do |k,v|
        Log.info " #{k} = #{v.inspect}"
      end
      Log.debug "@queue_class=#{@queue_class}"
      Log.debug "@filesystem=#{@filesystem}"
    end

  end
end
