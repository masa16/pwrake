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
          top_level
        ensure
          @main.finish
        end
        $stderr.print "invoker: #{Time.now-t} sec\n"
      end
    end

    def invoke_task(task_string)
      name, args = parse_task_string(task_string)
      t = self[name]
      @main.invoke(t,args)
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
      opts
    end

  end
end
