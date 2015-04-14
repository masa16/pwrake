module Pwrake

  # a mixin for managing Rake application.
  module MasterApplication

    def pwrake_options
      @role.option
    end

    def logger
      @role.logger
    end

    def task_logger
      @role.task_logger
    end

    def task_queue
      @role.task_queue
    end

    def postprocess(t)
      if @postprocess
        @postprocess.postprocess(t)
      end
    end

    # Run the Pwrake application.
    def run
      standard_exception_handling do
        init("pwrake")  # <- parse options here
        @role = @master = Master.new
        load_rakefile
        t = Time.now
        @master.init
        if pospro = @role.option.postprocess
          @postprocess = Pwrake.const_get(pospro).new
        end
        begin
          @master.setup_branches
          Log.debug "init: #{Time.now-t} sec"
          t = Time.now
          top_level
          Log.debug "main: #{Time.now-t} sec"
          t = Time.now
        ensure
          @master.finish
        end
        Log.debug "finish: #{Time.now-t} sec"
      end
    end

    def invoke_task(task_string)
      name, args = parse_task_string(task_string)
      t = self[name]
      @master.invoke(t,args)
    end

    def standard_rake_options
      opts = super
      opts.each_with_index do |a,i|
        if a[0] == '--version'
          a[3] = lambda { |value|
            puts "rake, version #{RAKEVERSION}"
            puts "pwrake, version #{Pwrake::VERSION}"
            exit
          }
        end
      end

      opts.concat(
      [
       ['-F', '--hostfile FILE',
        "[Pw] Read hostnames from FILE",
        lambda { |value|
          options.hostfile = value
        }
       ],
       ['-j', '--jobs [N]',
        "[Pw] Number of threads at localhost (default: # of processors)",
        lambda { |value|
          if value
            value = value.to_i
            if value > 0
              options.num_threads = value
            else
              options.num_threads = x = processor_count + value
              raise "negative/zero number of threads (#{x})" if x <= 0
            end
          else
            options.num_threads = processor_count
          end
        }
       ],
       ['-L', '--logfile [FILE]', "[Pw] Write log to FILE",
        lambda { |value|
          if value.kind_of? String
            options.logfile = value
          else
            options.logfile = ""
          end
        }
       ],
       ['--ssh-opt', '--ssh-option OPTION', "[Pw] Option passed to SSH",
        lambda { |value|
          options.ssh_option = value
        }
       ],
       ['--filesystem FILESYSTEM', "[Pw] Specify FILESYSTEM (nfs|gfarm)",
        lambda { |value|
          options.filesystem = value
        }
       ],
       ['--gfarm', "[Pw] FILESYSTEM=gfarm",
        lambda { |value|
          options.filesystem = "gfarm"
        }
       ],
       ['-A', '--disable-affinity', "[Pw] Turn OFF affinity (AFFINITY=off)",
        lambda { |value|
          options.disable_affinity = true
        }
       ],
       ['-S', '--disable-steal', "[Pw] Turn OFF task steal",
        lambda { |value|
          options.disable_steal = true
        }
       ],
       ['-d', '--debug',
        "[Pw] Output Debug messages",
        lambda { |value|
          options.debug = true
        }
       ],
       ['--pwrake-conf [FILE]',
        "[Pw] Pwrake configuation file in YAML",
        lambda {|value| options.pwrake_conf = value}
       ],
       ['--show-conf','--show-config',
        "[Pw] Show Pwrake configuration options",
        lambda {|value| options.show_conf = true }
       ],
       ['--report LOG', "[Pw] Report profile HTML from LOG and exit.",
         lambda { |value|
           require 'pwrake/report'
           Report.new(File.basename(value.sub(/\.[^.]+$/,"")),[]).report_html
           exit
         }
       ]

      ])
      opts
    end


    # from Michael Grosser's parallel
    # https://github.com/grosser/parallel
    def processor_count
      host_os = RbConfig::CONFIG['host_os']
      case host_os
      when /linux|cygwin/
        ncpu = 0
        open("/proc/cpuinfo").each do |l|
          ncpu += 1 if /^processor\s+: \d+/=~l
        end
        ncpu
      when /darwin9/
        `hwprefs cpu_count`.to_i
      when /darwin/
        (hwprefs_available? ? `hwprefs thread_count` : `sysctl -n hw.ncpu`).to_i
      when /(open|free)bsd/
        `sysctl -n hw.ncpu`.to_i
      when /mswin|mingw/
        require 'win32ole'
        wmi = WIN32OLE.connect("winmgmts://")
        cpu = wmi.ExecQuery("select NumberOfLogicalProcessors from Win32_Processor")
        cpu.to_enum.first.NumberOfLogicalProcessors
      when /solaris2/
        `psrinfo -p`.to_i # physical cpus
      else
        raise "Unknown architecture: #{host_os}"
      end
    end


  end # class MasterApplication < ::Rake::Application
end # mocule Pwrake
