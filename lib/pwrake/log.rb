module Pwrake

  module Log

    def log(*args)
      if Rake.application.options.trace
        if block_given?
          a = yield(*args)
        elsif args.size > 1
          a = args
        else
          a = args[0]
        end
        if Pwrake.respond_to?(:manager)
          if a.kind_of? Array
            a.each{|x| Pwrake.manager.logger.puts(x)}
          else
            Pwrake.manager.logger.puts(a)
          end
        end
      end
    end

    def time_str(t)
      t.strftime("%Y-%m-%dT%H:%M:%S.%%06d") % t.usec
    end

    def timer(prefix,*args)
      Timer.new(prefix,*args)
    end

    module_function :log, :time_str, :timer
  end


  class Timer
    include Log

    def initialize(prefix,*extra)
      @prefix = prefix
      @start_time = Time.now
      str = "%s[start]:%s %s" % [@prefix, Pwrake.time_str(@start_time), extra.join(' ')]
      log(str)
    end

    def finish(*extra)
      end_time = Time.now
      elap_time = end_time - @start_time
      str = "%s[end]:%s elap=%.3f %s" %
        [@prefix, Pwrake.time_str(end_time), elap_time, extra.join(' ')]
        log(str)
    end
  end


  # Pwrake.log
  def self.log(*args)
    Log.log(*args)
  end

  # Pwrake.time_str
  def self.time_str(t)
    Log.time_str(t)
  end

  # Pwrake.timer
  def self.timer(x,*a)
    Log.timer(x,*a)
  end
end
