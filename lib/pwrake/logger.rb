require "logger"

module Pwrake

  module Log

    @@logger = nil

    module_function

    def set_logger(option)
      unless @@logger
        if logdir = option['LOG_DIR']
          ::FileUtils.mkdir_p(logdir)
          logfile = File.join(logdir, option['LOG_FILE'])
          @@logger = Logger.new(logfile)
        else
          if option['DEBUG']
            @@logger = Logger.new($stderr)
          else
            @@logger = Logger.new(File::NULL)
          end
        end

        if option['DEBUG']
          @@logger.level = Logger::DEBUG
        else
          @@logger.level = Logger::INFO
        end

        at_exit{@@logger.close}
      end
    end

    def method_missing(meth_id,*args)
      if @@logger
        @@logger.send(meth_id,*args)
      end
    end

    def bt(e)
      "#{e.class}: #{e.message}\n "+(e.backtrace||[]).join("\n ")
    end
  end
end
