module Pwrake

class CommChannel

  def initialize(host,id,queue,writer,ios=[])
    @host = host
    @id = id
    @queue = queue
    @writer = writer
    @ios = ios
  end

  attr_reader :host, :id

  def put_line(s)
    if $cause_fault
      $cause_fault = nil
      Log.warn("closing writer io caller=\n#{caller.join("\n")}")
      @ios.each{|io| io.close}
    end
    @writer.put_line(s,@id)
  end

  def get_line
    @queue.deq
  end

  def halt
    @queue.halt
    @writer.halt
  end
end

class Communicator

  class ConnectError < IOError; end

  attr_reader :id, :host, :ncore, :channel
  attr_reader :reader, :writer, :handler
  attr_reader :shells
  attr_reader :ipaddr

  def initialize(set,id,host,ncore,selector,option)
    @set = set
    @id = id
    @host = host
    @ncore = @ncore_given = ncore
    @selector = selector
    @option = option
    @shells = {}
    @ipaddr = []
  end

  def inspect
    "#<#{self.class} @id=#{@id},@host=#{@host},@ncore=#{@ncore}>"
  end

  def new_channel
    i,q = @reader.new_queue
    CommChannel.new(@host,i,q,@writer,[@ior,@iow,@ioe])
  end

  def setup_pipe(worker_code)
    rb_cmd = "ruby -e 'eval ARGF.read(#{worker_code.size})'"
    if %w[127.0.0.1 ::1].include?(IPSocket.getaddress(@host))
      cmd = rb_cmd
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
    # send worker_code
    @iow.write(worker_code)
  end

  def connect(worker_code)
    setup_pipe(worker_code)

    # send ncore and options
    opts = Marshal.dump(@option)
    s = [@ncore||0, opts.size].pack("V2")
    @iow.write(s)
    @iow.write(opts)

    sel = @set.selector
    @reader = NBIO::MultiReader.new(sel,@ior)
    @writer = NBIO::Writer.new(sel,@iow)
    @handler = NBIO::Handler.new(@reader,@writer,@host)

    # read ncore
    while s = @reader.get_line
      case s
      when /^ip:(.*)$/
        a = $1
        @ipaddr.push(a)
        Log.debug "ip=#{a} @#{@host}"
      when /^ncore:(.*)$/
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
    raise ConnectError, "lost connection to #{@host} during setup"
  rescue => e
    dropout(e)
  end

  def common_line(s)
    x = "Communicator#common_line(id=#{@id},host=#{@host})"
    case s
    when /^heartbeat$/
      Log.debug "#{x}: #{s.inspect}"
    when /^exited$/
      Log.debug "#{x}: #{s.inspect}"
      return false
    when /^log:(.*)$/
      Log.info "#{x}: log>#{$1}"
    when String
      Log.warn "#{x}: out>#{s.inspect}"
    when Exception
      Log.warn "#{x}: err>#{s.class}: #{s.message}"
      dropout(s)
      return false
    else
      raise ConnectError, "#{x}: invalid for read: #{s.inspect}"
    end
    true
  end

  def finish_shells
    @shells.keys.each{|sh| sh.finish_task_q}
  end

  def dropout(exc=nil)
    # Finish worker
    begin
      finish_shells
      if @handler
        @handler.exit
        @handler = nil
      end
    rescue => e
      m = Log.bt(e)
      $stderr.puts(m)
      Log.error(m)
    end
    # Error output from worker
    if @ioe
      err_out = ["standard error from worker:"]
      while s = @ioe.gets
        err_out << s.chomp
      end
      if err_out.size > 1
        m = err_out.join("\n ")
        $stderr.puts(m)
        Log.error(m)
      end
    end
    # Exception message
    if exc
      m = Log.bt(exc)
      $stderr.puts(m)
      Log.error(m)
    end
  ensure
    @set.delete(self)
  end

end
end
