require "singleton"
require "timeout"

module Pwrake

  class Writer
    include Singleton

    def initialize
      @out = $stdout
      @mutex = Mutex.new
      @queue = Queue.new
      @heartbeat = 0
      @thread = Thread.new do
        loop do
          begin
            Timeout.timeout(@heartbeat) do
              if s = @queue.deq
                _puts s
              end
            end
          rescue Timeout::Error
            _puts "heartbeat"
          end
        end
      end
    end

    attr_accessor :out

    def heartbeat=(heartbeat)
      @heartbeat = heartbeat
      @queue.enq(nil)
    end

    def add_logger(log)
      @log = log
    end

    def puts(s)
      @queue.enq(s)
    end

    def _puts(s)
      begin
        @mutex.synchronize do
          @out.print s+"\n"
        end
        @out.flush
      rescue Errno::EPIPE
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
