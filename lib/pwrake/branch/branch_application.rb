$stderr = $stdout

module Pwrake

  # The TaskManager module is a mixin for managing tasks.
  module BranchApplication

    def run
      standard_exception_handling do
        init("pwrake_branch")
        opts = pwrake_options
        @branch = Branch.new(opts)
        @branch.init
        load_rakefile
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

    def pwrake_options
      opts = Marshal.load($stdin)
      # p opts

      if !opts.kind_of?(Hash)
        p opts
        raise "options is not Hash"
      end

      standard_rake_options.each do |opt|
        k = opt[0].sub(/^--/o,'').tr('a-z-','A-Z_')
        if v=opts[k]
          #p [k,v]
          b = opt.last
          if b.kind_of?(Proc)
            b.call(v)
          end
        end
      end

      opts
    end

  end
end
