$stderr = $stdout

module Pwrake

  # The TaskManager module is a mixin for managing tasks.
  module BranchApplication

    def run_branch
      standard_exception_handling do
        init("pwrake_branch")
        opts = branch_options
        @branch = Branch.new(opts)
        load_rakefile
        @branch.run
      end
    end

    def branch_options
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
