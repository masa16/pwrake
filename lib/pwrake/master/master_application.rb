require "pwrake/master/master"

module Pwrake

  # a mixin for managing Rake application.
  module MasterApplication

    def pwrake_options
      @role.option
    end

    def task_queue
      @role.task_queue
    end

    def current_flow
      @role.current_flow
    end

    # Run the Pwrake application.
    def run
      standard_exception_handling do
        init("pwrake")  # <- parse options here
        @role = @master = Master.new
        t = Time.now
        @master.setup_branches
        load_rakefile
        begin
          Log.debug "init: #{Time.now-t} sec"
          t = Time.now
          top_level
          Log.debug "main: #{Time.now-t} sec"
          t = Time.now
        rescue Exception => e
          # Exit with error message
          m = Log.bt(e)
          if @master.thread
            m += "\nIn branch thread #{@master.thread}:\n "
            m += @master.thread.backtrace.join("\n ")
          end
          Log.fatal m
          $stderr.puts m
          @master.signal_trap("INT")
        ensure
          @failed = @master.finish
          Log.debug "finish: #{Time.now-t} sec"
          Log.info "pwrake elapsed time: #{Time.now-START_TIME} sec"
        end
        Kernel.exit(false) if @failed
      end
    end

    # Find the rakefile and then load it and any pending imports.
    def load_rakefile
      if subdirs = pwrake_options['SUBDIR']
        subdirs.each do |x|
          begin
            Rake.application.current_flow[Fiber.current] = x
            super
          ensure
            Rake.application.current_flow[Fiber.current] = nil
          end
        end
      else
        super
      end
    end

    def find_rakefile_location # :nodoc:
      if subdir = Rake.application.current_flow[Fiber.current]
        Dir.chdir(subdir)
        if fn = have_rakefile
          [File.join(subdir,fn), Rake.original_dir]
        end
      else
        super
      end
    ensure
      Dir.chdir(Rake.original_dir)
    end

    def invoke(t,*args)
      @master.invoke(t,args)
    end

    def standard_rake_options
      opts = super
      opts.each_with_index do |a,i|
        if a[0] == '--version'
          a[3] = lambda { |value|
            if defined? RAKEVERSION
              puts "rake, version #{RAKEVERSION}"
            elsif defined? Rake::VERSION
              puts "rake, version #{Rake::VERSION}"
            end
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
            if /^[+-]?\d+$/ =~ value
              options.num_threads = value.to_i
            else
              raise ArgumentError,"Invalid argument for -j: #{value}"
            end
          else
            options.num_threads = 0
          end
        }
       ],
       ['-L', '--log', '--log-dir [DIRECTORY]', "[Pw] Write log to DIRECTORY",
        lambda { |value|
          if value.kind_of? String
            options.log_dir = value
          else
            options.log_dir = ""
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
       ['--subdir "dir1 dir2 ..."',
        "[Pw] Join workflows in subdirectories",
        lambda { |value|
          options.subdir = value.split(/[\s,;:]+/)
        }
       ],
       ['--show-conf','--show-config',
        "[Pw] Show Pwrake configuration options",
        lambda {|value| options.show_conf = true }
       ],
       ['--report LOGDIR',"[Pw] Generate `report.html' (Report of workflow statistics) in LOGDIR and exit.",
        lambda {|value| options.report_dir = value }
       ],
       ['--report-image IMAGE_TYPE',"[Pw] Gnuplot output format (png,jpg,svg etc.) in report.html.",
        lambda {|value| options.report_image = value }
       ],
       ['--clear-gfarm2fs',"[Pw] Clear gfarm2fs mountpoints left after failure.",
         lambda { |value|
           require "pwrake/option/option_gfarm2fs"
           Option.new.clear_gfarm2fs
           exit
         }
       ],


      ])
      opts
    end

  end
end
