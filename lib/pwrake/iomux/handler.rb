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
    end

    attr_reader :runner, :ior, :iow, :ioe
    attr_accessor :host

    def add_channel(chan)
      if !chan.kind_of?(Channel)
        raise TypeError, "Argument must be Channel but #{chan.class}"
      end
      @channel[chan.id] = chan
      @runner.add_handler(self)
    end

    def delete_channel(chan)
      if !chan.kind_of?(Channel)
        raise TypeError, "Argument must be Channel but #{chan.class}"
      end
      @channel.delete(chan.id)
      if @channel.empty?
        @runner.delete_handler(self)
      end
    end

    def process_line
      if s = (@ior.eof?) ? nil : @ior.gets
        default_channel = @channel[nil]
        if @channel.size > ((default_channel) ? 1 : 0)
          if /^(\d+):(.*)$/ =~ s
            ch,line = $1,$2
            if chan = @channel[ch.to_i]
              return chan.run_fiber(line)
            else
              raise "No channel##{ch}"
            end
          end
        end
        if default_channel
          return default_channel.run_fiber(s.chomp)
        else
          raise "No default_channel"
        end
      else
        # End of IO
        @channel.values.each do |chan|
          if chan.fiber
            chan.run_fiber(nil)
          end
        end
      end
    end

    def finish
      @channel.values.each do |chan|
        chan.finish
      end
    end

# -- writer

    def put_line(line)
      begin
        @iow.print line.to_str+"\n"
        @iow.flush
        #Log.debug "Handler#put_line #{line.inspect} @iow=#{@iow.inspect}"
      rescue Errno::EPIPE => e
        if Rake.application.options.debug
          $stderr.puts "Errno::EPIPE in #{self.class}.put_line '#{line.chomp}'"
          $stderr.puts e.backtrace.join("\n")
        end
        Log.error "Errno::EPIPE in #{self.class}.put_line '#{line.chomp}'\n"+
          e.backtrace.join("\n")
        #raise e
      end
    end

    def puts(line)
      @iow.puts(line)
    end

    def gets
      @ior.gets
    end

    def wait_message(end_msg)
      if line = @ior.gets
        line.chomp!
        m = "Handler#wait_message: #{line} host=#{@host}"
        if line == end_msg
          Log.debug m
        else
          Log.error m
        end
      else
        Log.error "Handler#wait_message: fail to read @ior"
      end
    end

  end
end
