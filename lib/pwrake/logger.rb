module Pwrake

  module Log

    module_function

    def method_missing(meth_id,*args)
      Rake.application.logger.send(meth_id,*args)
    end

  end
end
