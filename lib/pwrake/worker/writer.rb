require "singleton"

module Pwrake

  class Writer
    include Singleton

    def initialize
      @out = $stdout
      @mutex = Mutex.new
      @mutex_hb = Mutex.new
      @cond_hb = true
      @heartbeat = nil
      @thread = Thread.new{ heartbeat_loop }
    end

    attr_accessor :out

    def heartbeat=(t)
      @heartbeat = t.to_i
      @thread.run
    end

    def heartbeat_loop
      loop do
        @heartbeat ? sleep(@heartbeat) : sleep
        @mutex_hb.synchronize do
          if @cond_hb
            _puts "heartbeat"
          end
          @cond_hb = true
        end
      end
    end

    def add_logger(log)
      @log = log
    end

    def puts(s)
      @mutex_hb.synchronize do
        @cond_hb = false
        @thread.run
      end
      _puts(s)
    end

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

    def flush
      begin
        @out.flush
      rescue
      end
    end

    def dputs(s)
      puts(s) if $DEBUG
    end

  end
end
