module Pwrake

class CommChannel

  def initialize(host,id,queue,writer)
    @host = host
    @id = id
    @queue = queue
    @writer = writer
  end

  attr_reader :host, :id

  def put_line(s)
    @writer.put_line(s,@id)
  end

  def get_line
    @queue.deq
  end
end

class Communicator

  class ConnectError < IOError; end

  attr_reader :id, :host, :ncore, :channel
  attr_reader :reader, :writer, :handler

  def initialize(set,id,host,ncore,selector,option)
    @set = set
    @id = id
    @host = host
    @ncore = @ncore_given = ncore
    @selector = selector
    @option = option
    if hb = @option[:heartbeat]
      @heartbeat_timeout = hb + 15
    end
  end

  def new_channel
    i,q = @reader.new_queue
    CommChannel.new(@host,i,q,@writer)
  end

  def connect(worker_code)
    rb_cmd = "ruby -e 'eval ARGF.read(#{worker_code.size})'"
    if ['localhost','localhost.localdomain','127.0.0.1'].include? @host
    #if /^localhost/ =~ @host
      cmd = "cd; #{rb_cmd}"
    else
      cmd = "ssh -x -T #{@option[:ssh_option]} #{@host} \"#{rb_cmd}\""
    end
    #
    @ior,w0 = IO.pipe
    @ioe,w1 = IO.pipe
    r2,@iow = IO.pipe
    @pid = Kernel.spawn(cmd,:pgroup=>true,:out=>w0,:err=>w1,:in=>r2)
    w0.close
    w1.close
    r2.close
    sel = @set.selector
    @handler = AIO::Handler.new(sel,@ior,@iow,@host)
    @reader = @handler.reader
    @writer = @handler.writer
    @rd_err = AIO::Reader.new(sel,@ioe)
    #
    @writer.write(worker_code)
    @writer.write(Marshal.dump(@ncore))
    @writer.write(Marshal.dump(@option))
    # read ncore
    while s = @reader.get_line
      if /^ncore:(.*)$/ =~ s
        a = $1
        Log.debug "ncore=#{a} @#{@host}"
        if /^(\d+)$/ =~ a
          @ncore = $1.to_i
          return false
        else
          raise ConnectError, "invalid for ncore: #{a.inspect}"
        end
      else
        return false if !common_line(s)
      end
    end
    raise ConnectError, "fail to connect #{cmd.inspect}"
  rescue => e
    dropout(e)
  end

  def common_line(s)
    Log.debug "Communicator#common_line(#{s.inspect}) id=#{@id} host=#{@host}"
    case s
    when /^heartbeat$/
      @selector.heartbeat(@reader.io)
    when /^exited$/
      return false
    when /^log:(.*)$/
      Log.info "worker(#{host})>#{$1}"
    when String
      Log.warn "worker(#{host}) out> #{s.inspect}"
    else
      raise ConnectError, "invalid for read: #{s.inspect}"
    end
    true
  end

  def dropout(exc=nil)
    # Error output
    err_out = []
    begin
      @iow.close
      while s = @rd_err.get_line
        err_out << s
      end
    rescue => e
      m = "#{e.class}: #{e.message}\n" +
        e.backtrace.dup.map{|x|"\tfrom #{x}"}.join("\n")
      $stderr.puts m
      Log.error(m)
    end
    # Exception
    if exc
      m = "#{exc.class}: #{exc.message}\n" +
        exc.backtrace.dup.map{|x|"\tfrom #{x}"}.join("\n")
      $stderr.puts m
      Log.error m
    end
    # Error output
    if !err_out.empty?
      m = "Error message from external process:\n"+err_out.join("\n")
      $stderr.puts m
      Log.error m
    end
    #@stage = false
  ensure
    @set.delete(self)
  end

  def finish
    @iow.close
    while s=@ior.gets
      puts "out=#{s.chomp}"
    end
    while s=@ioe.gets
      puts "err=#{s.chomp}"
    end
  end

end
end
