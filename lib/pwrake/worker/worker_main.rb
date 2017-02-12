require "thread"
require "fileutils"
require "timeout"

begin
  Pwrake::Invoker.new.run
rescue => exc
  log = Pwrake::LogExecutor.instance
  log.error exc
  log.error exc.backtrace.join("\n")
  open("pwrake_worker_err-#{ENV['USER']}-#{Process.pid}","w") do |f|
    f.puts exc
    f.puts exc.backtrace.join("\n")
  end
end
