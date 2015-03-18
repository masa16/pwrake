require "pwrake/util.rb"
require "pwrake/log"
require "pwrake/logger"

require "pwrake/ioevent.rb"
require "pwrake/transmitter"

require "pwrake/branch/branch_application"
require "pwrake/branch/rake_modify.rb"
require "pwrake/branch/task.rb"
require "pwrake/branch/branch.rb"
require "pwrake/branch/channel.rb"
require "pwrake/branch/fiber_queue.rb"

class Rake::Application
  prepend Pwrake::BranchApplication
end
