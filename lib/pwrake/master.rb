require "fileutils"
require "pathname"
require "yaml"

require "pwrake/io_dispatcher"
# require "pwrake/comm_io"
require "pwrake/util"

require "pwrake/master/branch_communicator"
require "pwrake/master/master"
require "pwrake/master/master_options"
require "pwrake/master/rake_modify"
require "pwrake/master/master_application"
#require "pwrake/master/scheduler"
#require "pwrake/master/tracer"
require "pwrake/master/worker_channel"

require "pwrake/task_queue"
require "pwrake/task_algorithm"
require "pwrake/task_search"

class Rake::Application
  prepend Pwrake::MasterApplication
end

class Rake::Task
  include Pwrake::TaskAlgorithm
  include Pwrake::TaskSearch
end
