require "pwrake/logger"
require "pwrake/branch/branch"

module Pwrake

  # The TaskManager module is a mixin for managing tasks.
  module BranchApplication

    def run_branch(r,w)
      #standard_exception_handling do
        init("pwrake_branch")
        opts = Marshal.load(r)
        if !opts.kind_of?(Hash)
          raise "opts is not a Hash: opts=#{opts.inspect}"
        end
        @branch = Branch.new(opts,r,w)
        opts.feedback_options
        load_rakefile
        w.puts "pwrake_branch start"
        w.flush
        begin
          @branch.run
        rescue => e
          Log.fatal e
          $stderr.puts e
          $stderr.puts e.backtrace
          @branch.kill
        ensure
          @branch.finish
        end
      #end
    end

    def run_branch_in_thread(r,w,opts)
      #standard_exception_handling do
        @branch = Branch.new(opts,r,w)
        begin
          @branch.run
        rescue => e
          Log.fatal e
          $stderr.puts e
          $stderr.puts e.backtrace
          @branch.kill
        ensure
          @branch.finish
        end
      #end
    end

  end
end
