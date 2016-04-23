require "logger"

module Pwrake

  module Log

    module_function

    def method_missing(meth_id,*args)
      Rake.application.logger.send(meth_id,*args)
    end

    def bt(e)
      "#{e.class}: #{e.message}\n "+(e.backtrace||[]).join("\n ")
    end
  end
end
