require "thread"
require "fileutils"
require "timeout"

begin
  io = $stdin
  ncore,len = io.read(8).unpack("V2")
  opts = Marshal.load(io.read(len))
  dc = Pwrake.const_get(opts[:shared_directory])
  dc.init(opts)
  Pwrake::Invoker.new(dc, ncore, opts).run
rescue => exc
  log = Pwrake::LogExecutor.instance
  log.error exc
  log.error exc.backtrace.join("\n")
end
