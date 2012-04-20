$stderr = $stdout

module Rake
  class << self
    def application
      @application ||= Pwrake::BranchApplication.new
    end
  end
end


module Pwrake

  # The TaskManager module is a mixin for managing tasks.
  class BranchApplication < ::Rake::Application

    def run
      standard_exception_handling do
        init("pwrake_branch")
        load_rakefile
        @branch = Branch.new
        begin
          @branch.run
        ensure
          @branch.finish
        end
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
      opts
    end

  end
end
