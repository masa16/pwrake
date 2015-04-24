module Pwrake

  class Writer
    include Singleton

    def initialize
      @out = $stdout
      @log = LogExecutor.instance
      @mutex = Mutex.new
    end

    def puts(s)
      @mutex.synchronize do
        begin
          @out.print s+"\n"
          @out.flush
        rescue Errno::EPIPE
        end
        @log.info "<#{s}" if @log.opened?
      end
    end

    def print(s)
      @mutex.synchronize do
        begin
          @out.print s
          @out.flush
        rescue Errno::EPIPE
        end
        @log.info "<#{s}" if @log.opened?
      end
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
