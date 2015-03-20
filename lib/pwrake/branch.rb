require "pwrake/util.rb"
require "pwrake/log"
require "pwrake/logger"

#require "pwrake/ioevent.rb"
require "pwrake/communicator"

require "pwrake/branch/branch_application"
require "pwrake/branch/rake_modify.rb"
require "pwrake/branch/task.rb"
require "pwrake/branch/branch.rb"
require "pwrake/branch/fiber_queue.rb"

require "pwrake/io_dispatcher"
require "pwrake/branch/shell"
require "pwrake/branch/multiplex_handler"
require "pwrake/branch/channel.rb"
require "pwrake/branch/branch_handler.rb"

class Rake::Application
  prepend Pwrake::BranchApplication
end
