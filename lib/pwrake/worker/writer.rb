require "singleton"

module Pwrake

  class Writer
    include Singleton

    def initialize
      @out = $stderr
      @mutex = Mutex.new
      @cond_hb = true
      @heartbeat = 120
      @thread = Thread.new{ heartbeat_loop }
    end

    attr_accessor :out

    def heartbeat=(t)
      @heartbeat = t.to_i
      @thread.run
    end

    def heartbeat_loop
      sleep
      loop do
        sleep(@heartbeat)
        if @cond_hb
          _puts "heartbeat"
        end
        @cond_hb = true
      end
    end

    def add_logger(log)
      @log = log
    end

    def puts(s)
      @cond_hb = false
      @thread.run
      _puts(s)
    end

    def flush
      begin
        @out.flush
      rescue
      end
    end

    def dputs(s)
      puts(s) if $DEBUG
    end

    private
    def _puts(s)
      begin
        @mutex.synchronize do
          @out.print s+"\n"
        end
        @out.flush
      rescue Errno::EPIPE => e
        @log.info "<#{e.inspect}" if @log
      end
      @log.info "<#{s}" if @log
    end
  end
end
