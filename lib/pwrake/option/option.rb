require "pathname"
require "yaml"
require "pwrake/option/host_map"

module Pwrake

  START_TIME = Time.now

  class Option < Hash

    def initialize
      load_pwrake_conf
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
    end

    def init
      Log.info "Options:"
      self.each do |k,v|
	Log.info " #{k} = #{v.inspect}"
      end
      #@counter = Counter.new
      setup_hosts
      setup_filesystem # require 'option_filesystem.rb'
      #
      if self['LOG_DIR'] && self['GC_LOG_FILE']
        GC::Profiler.enable
      end
    end

    attr_reader :counter
    attr_reader :logger
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
        #Log.debug "load pwrake_conf=#{pwrake_conf}"
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
        'GFARM2FS_OPTION',
        'GFARM2FS_DEBUG',
        ['GFARM2FS_DEBUG_WAIT', proc{|v| v ? v.to_i : 1}],
        'GNU_TIME',
        'DEBUG',
        'PLOT_PARALLELISM',
        'SHOW_CONF',
        ['REPORT_DIR','REPORT'],
        'REPORT_IMAGE',
        'FAILED_TARGET', # rename(default), delete, leave
        'FAILURE_TERMINATION', # wait, kill, continue
        'QUEUE_PRIORITY', # RANK(default), FIFO, LIFO, DFS
        'NOACTION_QUEUE_PRIORITY', # FIFO(default), LIFO, RAND
        #'NUM_NOACTION_THREADS', # default=4 when gfarm, else 1
        'GRAPH_PARTITION',
        'PLOT_PARTITION',

        ['HOSTFILE','HOSTS'],
        ['LOG_DIR','LOG',
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
        ['HEARTBEAT', proc{|v| (v || 240).to_i}],
        ['RETRY', proc{|v| (v || 1).to_i}],
        ['DISABLE_AFFINITY', proc{|v| v || ENV['AFFINITY']=='off'}],
        ['DISABLE_STEAL', proc{|v| v || ENV['STEAL']=='off'}],
        ['GFARM_BASEDIR', proc{|v| v || '/tmp'}],
        ['GFARM_PREFIX', proc{|v| v || "pwrake_#{ENV['USER']}"}],
        ['GFARM_SUBDIR', proc{|v| v || '/'}],
        ['MAX_GFWHERE_WORKER', proc{|v| (v || 8).to_i}],
        ['MASTER_HOSTNAME', proc{|v| (v || begin;`hostname -f`;rescue;end || '').chomp}],
        ['WORK_DIR', proc{|v|
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


    def setup_hosts
      if @hostfile && @num_threads
        raise "Cannot set `hostfile' and `num_threads' simultaneously"
      end
      @host_map = HostMap.new(@hostfile || @num_threads)
    end
    attr_reader :host_map


    def clear_gfarm2fs
      setup_hosts
      d = File.join(self['GFARM_BASEDIR'],self['GFARM_PREFIX'])
      rcmd = "
for i in #{d}*; do
  if [ -d \"$i\" ]; then
    case \"$i\" in
      *_000) ;;
      *) fusermount -u $i; rmdir $i ;;
    esac
  fi
done
sleep 1
for i in #{d}*_000; do
  if [ -d \"$i\" ]; then
    fusermount -u $i; rmdir $i
  fi
done
"
      threads = []
      @host_map.each do |k,hosts|
        hosts.each do |info|
          threads << Thread.new do
            system "ssh #{info.name} '#{rcmd}'"
          end
        end
      end
      threads.each{|t| t.join}
    end

    # ----- finish -----

    def finish_option
      Log.close
    end

  end
end

require "pwrake/option/option_filesystem"
