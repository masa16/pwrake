require "singleton"

module Pwrake

  class Writer
    include Singleton

    def initialize
      @out = $stderr
      @mutex = Mutex.new
      @cond_hb = true
      @heartbeat = nil
      @thread = Thread.new{ heartbeat_loop }
    end

    attr_accessor :out

    def heartbeat=(t)
      if t
        t = t.to_i
        t = 15 if t < 15
      end
      @heartbeat = t
      @thread.run
    end

    def heartbeat_loop
      loop do
        @heartbeat ? sleep(@heartbeat) : sleep
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
      _puts(s)
      @cond_hb = false
      @thread.run
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
