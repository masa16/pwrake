require "fileutils"
require "pathname"

require "pwrake/ioevent"
# require "pwrake/comm_io"
require "pwrake/util"
require "pwrake/transmitter"

require "pwrake/master/master"
require "pwrake/master/rake_modify"
require "pwrake/master/master_application"
require "pwrake/master/scheduler"
require "pwrake/master/tracer"
require "pwrake/master/worker_channel"

class Rake::Application
  prepend Pwrake::MasterApplication
end
