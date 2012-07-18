module Rake
  class << self
    def application
      @application ||= Pwrake::MainApplication.new
    end
  end
end


module Pwrake

  # The TaskManager module is a mixin for managing tasks.
  class MainApplication < ::Rake::Application
    #include Pwrake::Log

    # Run the Pwrake application.
    def run
      standard_exception_handling do
        init("pwrake")
        load_rakefile
        t = Time.now
        @main = Main.new
        begin
          @main.setup_branches
          $stderr.print "init: #{Time.now-t} sec\n"
          t = Time.now
          top_level
          $stderr.print "main: #{Time.now-t} sec\n"
          t = Time.now
        ensure
          @main.finish
        end
        $stderr.print "finish: #{Time.now-t} sec\n"
      end
    end

    def invoke_task(task_string)
      name, args = parse_task_string(task_string)
      t = self[name]
      @main.invoke(t,args)
    end

    # Read and handle the command line options.
    def handle_options
      options.rakelib = ['rakelib']

      OptionParser.new do |opts|
        opts.banner = $PROGRAM_NAME+" [-f rakefile] {options} targets..."
        opts.separator ""
        opts.separator "Options are ..."

        opts.on_tail("-h", "--help", "-H", "Display this help message.") do
          puts opts
          exit
        end

        standard_rake_options.each { |args| opts.on(*args) }
        opts.environment('RAKEOPT')
      end.parse!

      # If class namespaces are requested, set the global options
      # according to the values in the options structure.
      if options.classic_namespace
        $show_tasks = options.show_tasks
        $show_prereqs = options.show_prereqs
        $trace = options.trace
        $dryrun = options.dryrun
        $silent = options.silent
      end
    end

    def standard_rake_options
      opts = super
      opts.each_with_index do |a,i|
        if a[0] == '--version'
          a[3] = lambda { |value|
            puts "rake, version #{RAKEVERSION}"
            puts "pwrake, version #{Pwrake::PWRAKEVERSION}"
            exit
          }
        end
      end
      opts.push ['--pwrake-conf [FILE]',"Pwrake configuation file in YAML:\n"+
                 Main::DEFAULT_CONF.map{|k,v| "\t\t#{k}: #{v}"}.join("\n"),
                 lambda {|value| options.pwrake_conf = value}]
      opts
    end

  end # class MainApplication < ::Rake::Application
end # mocule Pwrake
