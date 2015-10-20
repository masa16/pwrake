module Pwrake

  class Handler

    def initialize(runner,*io,&blk)
      if !runner.kind_of?(Runner)
        raise TypeError, "Argument must be Runner but #{runner.class}"
      end
      @runner = runner
      @channel = {}
      if !io.empty?
        if !block_given?
          @ior,@iow,@ioe = *io
        end
      else
        if block_given?
          @ior,w0 = IO.pipe
          @ioe,w1 = IO.pipe
          r2,@iow = IO.pipe
          yield(w0,w1,r2)
        end
      end
      if @ior.nil? && @iow.nil?
        raise "fail to initialize IO"
      end
      @runner.set_handler(self)
    end

    attr_reader :runner, :ior, :iow, :ioe
    attr_accessor :host

    def set_close_block(&blk)
      @close_block = blk
    end

    def set_channel(chan)
      if !chan.kind_of?(Channel)
        raise TypeError, "Argument must be Channel but #{chan.class}"
      end
      if ch = chan.id
        @channel[ch] = chan
      else
        @default_channel = chan
      end
    end

    def process_line
      if s = (@ior.eof?) ? nil : @ior.gets
        if !@channel.empty?
          if /^(\d+):(.*)$/ =~ s
            ch,line = $1,$2
            ch = ch.to_i
            if chan = @channel[ch]
              return chan.run_fiber(line)
            else
              raise "No channel[#{ch}]"
            end
          end
        end
        if @default_channel.nil?
          raise "No default_channel"
        end
        return @default_channel.run_fiber(s.chomp)
      else
        # End of IO
        @channel.each do |ch,chan|
          chan.run_fiber(nil)
        end
      end
    end

# -- writer

    def put_line(line)
      begin
        @iow.print line.to_str+"\n"
        @iow.flush
      rescue Errno::EPIPE => e
        $stderr.puts "Errno::EPIPE in #{self.class}.print '#{line.chomp}'"
        if Rake.application.options.debug
          $stderr.puts e.backtrace.join("\n")
        end
        raise e
        #exit(1)
      end
    end

    def close
      if @close_block
        @close_block.call(self)
      end
      #put_line @close_command
    end

    def kill(sig)
      put_line "kill:#{sig}"
    end

  end
end
