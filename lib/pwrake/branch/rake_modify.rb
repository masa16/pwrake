module FileUtils

  # Run the system command +cmd+. If multiple arguments are given the command
  # is not run with the shell (same semantics as Kernel::exec and
  # Kernel::system).
  #
  # Example:
  #   sh %{ls -ltr}
  #
  #   sh 'ls', 'file with spaces'
  #
  #   # check exit status after command runs
  #   sh %{grep pattern file} do |ok, res|
  #     if ! ok
  #       puts "pattern not found (status = #{res.exitstatus})"
  #     end
  #   end
  #
  def sh(*cmd, &block)
    options = (Hash === cmd.last) ? cmd.pop : {}
    shell_runner = block_given? ? block : create_shell_runner(cmd)
    set_verbose_option(options)
    options[:noop] ||= Rake::FileUtilsExt.nowrite_flag
    Rake.rake_check_options options, :noop, :verbose
    Rake.rake_output_message cmd.join(" ") if options[:verbose]

    unless options[:noop]
      res,status = pwrake_system(*cmd)
      #res = rake_system(*cmd)
      #status = $?
      #status = PseudoStatus.new(1) if !res && status.nil?
      shell_runner.call(res, status)
    end
  end


  def pwrake_system(*cmd)
    cmd_log = cmd.join(" ").inspect
    #tm = Pwrake.timer("sh",cmd_log)
    #
    chan = Pwrake::Channel.current
    if chan
      res    = chan.system(*cmd)
      status = Rake::PseudoStatus.new(chan.status)
    else
      res    = system(*cmd)
      status = $?
      status = Rake::PseudoStatus.new(1) if !res && status.nil?
    end
    #
    #tm.finish("status=%s cmd=%s"%[status.exitstatus,cmd_log])
    [res,status]
  end
  private :pwrake_system


  #module_function

  def bq(cmd)
    cmd_log = cmd.inspect
    tm = Pwrake.timer("bq",cmd_log)
    #
    chan = Pwrake::Channel.current
    if chan
      res    = chan.backquote(*cmd)
      status = chan.status
    else
      res    = Kernel.backquote(cmd)
      if !res && status.nil?
        status = 1
      else
        status = $?.exitstatus
      end
    end
    #
    tm.finish("status=%s cmd=%s"%[status,cmd_log])
    res
  end

end
