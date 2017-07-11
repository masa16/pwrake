require "thread"
require "fileutils"
require "timeout"
require "socket"

begin
  Pwrake::Invoker.new.run
rescue => exc
  log = Pwrake::LogExecutor.instance
  log.error exc
  log.error exc.backtrace.join("\n")
  open("pwrake_worker_err-#{Socket.gethostname}-#{Process.pid}","w") do |f|
    f.puts exc
    f.puts exc.backtrace.join("\n")
  end
end
