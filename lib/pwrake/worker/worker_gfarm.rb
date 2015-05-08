require "thread"
require "fileutils"

base_dir, work_dir, log_dir, n_core = 4.times.map{ARGF.gets.chomp}
begin
  dir_cls = Pwrake::GfarmDirectory
  dir_cls.init(base_dir, work_dir, log_dir)
  Pwrake::Invoker.new(dir_cls, n_core).run
rescue => exc
  log = Pwrake::LogExecutor.instance
  log.error exc
  log.error exc.backtrace.join("\n")
end
