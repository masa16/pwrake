# $stderr = $stdout

module Pwrake

  # The TaskManager module is a mixin for managing tasks.
  module BranchApplication

    def run_branch(r,w)
      w.puts "pwrake_branch start"
      w.flush
      standard_exception_handling do
        init("pwrake_branch")
        opts = Marshal.load(r)
        if !opts.kind_of?(Hash)
          raise "opts is not a Hash: opts=#{opts.inspect}"
        end
        @branch = Branch.new(opts,r,w)
        @branch.init_logger
        opts.feedback_options
        load_rakefile
        @branch.run
      end
    end

    def run_branch_in_thread(r,w,opts)
      standard_exception_handling do
        @branch = Branch.new(opts,r,w)
        @branch.run
      end
    end

  end
end
