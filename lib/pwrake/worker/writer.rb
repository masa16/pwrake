module Pwrake

  class Writer
    include Singleton

    def initialize
      @out = $stdout
      @mutex = Mutex.new
      pipe_in, pipe_out = IO.pipe
      Thread.new(pipe_in,"log:") do |pin,pre|
        while s = pin.gets
          s.chomp!
          @out.puts pre+s
        end
      end
      $stderr = pipe_out
    end

    def add_logger(log)
      @log = log
    end

    def puts(s)
      begin
        @mutex.synchronize do
          @out.print s+"\n"
        end
        @out.flush
      rescue Errno::EPIPE
      end
      @log.info "<#{s}" if @log
    end

    def print(s)
      begin
        @mutex.synchronize do
          @out.print s
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
