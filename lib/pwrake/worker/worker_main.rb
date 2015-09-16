require "thread"
require "fileutils"

ncore = Marshal.load($stdin)
opts = Marshal.load($stdin)
begin
  dc = Pwrake.const_get(opts[:shared_directory])
  dc.init(opts)
  Pwrake::Invoker.new(dc, ncore, opts).run
rescue => exc
  log = Pwrake::LogExecutor.instance
  log.error exc
  log.error exc.backtrace.join("\n")
end
