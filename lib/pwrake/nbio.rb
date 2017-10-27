require "fiber"

$debug=false

module Pwrake
module NBIO

  class TimeoutError < StandardError
  end

  class Selector

    def initialize(io_class=IO)
      @reader = {}
      @writer = {}
      @running = false
      @io_class = io_class
    end

    attr_reader :reader, :writer

    def add_reader(hdl)
      @reader[hdl.io] = hdl
    end

    def delete_reader(hdl)
      @reader.delete(hdl.io)
    end

    def add_writer(hdl)
      @writer[hdl.io] = hdl
    end

    def delete_writer(hdl)
      @writer.delete(hdl.io)
    end

    def empty?
      @reader.empty? && @writer.empty?
    end

    def clear
      @reader.clear
      @writer.clear
    end

    def halt
      @running = false
      @writer.each_value{|w| w.halt}
      @reader.each_value{|r| r.halt}
    end

    # used to print an error message
    def get_host(io)
      hdl = @reader[io] || @writer[io]
      hdl.respond_to?(:host) ? hdl.host : nil
    end

    def run(timeout=nil)
      @running = true
      init_heartbeat if timeout
      while @running && !empty?
        if $debug && defined? Log
          rd_insp = @reader.map{|k,v|
            "%s=>%s,%s" % [k.inspect,v.class.inspect,v.waiter.inspect]
          }.join(",")
          Log.debug "Selector#run:\n "+caller[0..1].join("\n ")+
            "\n @reader={#{rd_insp}}\n @writer.size=#{@writer.size}"
          $stderr.puts "Selector#run: "+caller[0]
        end
        run_select(timeout)
      end
    ensure
      @running = false
      @hb_time = nil
    end

    private
    def run_select(timeout)
      to = (timeout) ? timeout*0.75 : nil
      r, w, = @io_class.select(@reader.keys,@writer.keys,[],to)
      check_heartbeat(r,timeout) if timeout
      r.each{|io| x = @reader[io]; x.call if x} if r
      w.each{|io| x = @writer[io]; x.call if x} if w
    rescue IOError => e
      em = "#{e.class.name}: #{e.message}"
      @reader.keys.each do |io|
        if io.closed?
          m = "#{em} io=#{io}"
          Log.error(m) if defined? Log
          $stderr.puts m
          hdl = @reader.delete(io)
          hdl.error(e)
        end
      end
      @writer.keys.each do |io|
        if io.closed?
          m = "#{em} io=#{io}"
          Log.error(m) if defined? Log
          $stderr.puts m
          hdl = @writer.delete(io)
          hdl.error(e)
        end
      end
    end

    def init_heartbeat
      t = Time.now
      @hb_check_time = t
      @hb_time = {}
      @reader.each_key{|io| @hb_time[io] = t}
    end

    def check_heartbeat(ios,timeout)
      t = Time.now
      if t - @hb_check_time < 3
        if ios
          ios.each do |io|
            @hb_time[io] = t
          end
        end
        return
      end
      @hb_check_time = t
      rds = @reader.dup
      if ios
        ios.each do |io|
          @hb_time[io] = t
          rds.delete(io)
        end
      end
      rds.each do |io,hdl|
        if hdl.check_timeout
          tdif = t - @hb_time[io]
          if tdif > timeout
            m = "Heartbeat Timeout: no response during #{tdif}s "+
              "> timeout #{timeout}s from host=#{get_host(io)} " +
              "@hb_time[io=#{io.inspect}]=#{@hb_time[io].strftime('%FT%T.%6N')}"
            hdl.error(TimeoutError.new(m))
          end
        end
      end
    end

  end

#------------------------------------------------------------------

  class Writer
    def initialize(selector, io)
      @selector = selector
      @io = io
      @waiter = []
      @pool = []
    end
    attr_reader :io
    attr_accessor :host

    # call from Selector
    def call
      w = @waiter
      @waiter = []
      w.each{|f| f.resume}
    ensure
      @selector.delete_writer(self) if @waiter.empty?
    end

    # call from Fiber context
    def put_line(line, ch=nil)
      line = line.chomp
      line = "#{ch}:#{line}" if ch
      write(line+"\n")
    end

    def halt
      @halting = true
      call
    ensure
      @halting = false
    end

    def error(e)
      @closed = true
      raise e
    end

    # from Bartender

    def write(buf, buffered=false)
      push(buf)
      flush unless buffered
    end

    alias print :write

    def puts(s)
      write(s+"\n")
    end

    def flush
      until @pool.empty?
        len = _write(@pool[0])
        pop(len)
      end
    end

    def select_io
      @selector.add_writer(self) if @waiter.empty?
      @waiter.push(Fiber.current)
      Fiber.yield
    end

    private
    def _write(buf)
      return @io.write_nonblock(buf)
    rescue IO::WaitWritable
      return nil if @halting
      select_io
      retry
    end

    def push(string)
      if string.bytesize > 0
        @pool << string
      end
    end

    def pop(size)
      return if size < 0
      raise if @pool[0].bytesize < size

      if @pool[0].bytesize == size
        @pool.shift
      else
        unless @pool[0].encoding == Encoding::BINARY
          @pool[0] = @pool[0].dup.force_encoding(Encoding::BINARY)
        end
        @pool[0].slice!(0...size)
      end
    end
  end

#------------------------------------------------------------------

  class Reader

    def initialize(selector, io)
      @selector = selector
      @io = io
      @waiter = []
      @buf = ''
      @eof = false
      @sep = "\n"
      @chunk_size = 8192
    end
    attr_reader :io, :waiter
    attr_accessor :check_timeout, :host

    # call from Selector#run
    def call
      @waiter.each{|f| f.resume}
    end

    # call from MultiReader#call
    def read_line_nonblock
      until index = @buf.index(@sep)
        @buf << @io.read_nonblock(@chunk_size)
      end
      @buf.slice!(0, index+@sep.bytesize)
    rescue EOFError => e
      if @buf.empty?
        raise e
      else
        buf = @buf; @buf = ''
        return buf
      end
    end

    # call from Reader#_read and FiberReaderQueue#deq
    def select_io
      if @waiter.empty?
        @selector.add_reader(self)
      else
        if @selector.reader[@io] != self
          raise RuntimeError, "access from multiple Fiber"
        end
      end
      @waiter.push(Fiber.current)
      if $debug && defined? Log
        Log.debug("Reader#select_io: #{Fiber.current.inspect}\n "+caller.join("\n "))
      end
      Fiber.yield
    ensure
      @waiter.delete(Fiber.current)
      @selector.delete_reader(self) if @waiter.empty?
    end

    def error(e)
      @closed = true
      raise e
    end

    def halt
      @halting = true
      call
    ensure
      @halting = false
    end

    def eof?
      @eof && @buf.empty?
    end

    # from Bartender

    def _read(sz)
      @io.read_nonblock(sz)
    rescue EOFError
      @eof = true
      nil
    rescue IO::WaitReadable
      return nil if @halting
      select_io
      retry
    end

    def read(n)
      while @buf.bytesize < n
        chunk = _read(n)
        break if chunk.nil? || chunk.empty?
        @buf += chunk
      end
      @buf.slice!(0, n)
    end

    def read_until(sep="\r\n", chunk_size=8192)
      until i = @buf.index(sep)
        if s = _read(chunk_size)
          @buf += s
        else
          if @buf.empty?
            return nil
          else
            buf = @buf; @buf = ''
            return buf
          end
        end
      end
      @buf.slice!(0, i+sep.bytesize)
    end

    def readln
      read_until(@sep)
    end

    alias get_line :readln
    alias gets :readln

  end

#------------------------------------------------------------------

  class MultiReader < Reader

    def initialize(selector, io, n_chan=0)
      super(selector, io)
      @n_chan = n_chan
      @queue = @n_chan.times.map{|i| FiberReaderQueue.new(self)}
      @default_queue = FiberReaderQueue.new(self)
      @check_timeout = true
    end
    attr_reader :queue
    attr_accessor :default_queue

    def [](ch)
      @queue[ch]
    end

    def new_queue
      n = @n_chan
      @queue << q = FiberReaderQueue.new(self)
      @n_chan += 1
      [n,q]
    end

    # call from Fiber context
    def get_line(ch=nil)
      if ch && !@queue.empty?
        @queue[ch].deq
      else
        @default_queue.deq
      end
    end

    def call
      while line = read_line_nonblock
        if /^(\d+):(.*)$/ =~ line
          ch,str = $1,$2
          if q = @queue[ch.to_i]
            q.enq(str)
          else
            raise "No queue ##{ch}, received: #{line}"
          end
        elsif @default_queue
          @default_queue.enq(line)
        else
          raise "No default_queue, received: #{line}"
        end
      end
    rescue EOFError
      halt
    rescue IO::WaitReadable
    end

    def error(e)
      @closed = true
      @queue.each{|q| q.enq(e)}
      @default_queue.enq(e)
    end

    def halt
      Log.debug("Handler.halt") if defined? Log
      @queue.each{|q| q.halt}
      @default_queue.halt
    end
  end

#------------------------------------------------------------------

  class FiberReaderQueue

    def initialize(reader)
      @reader = reader
      @q = []
      @waiter = []
      @halting = false
    end

    def enq(x)
      @q.push(x)
      f = @waiter.shift
      f.resume if f
    end

    def deq
      while @q.empty?
        return nil if @halting
        @waiter.push(Fiber.current)
        @reader.select_io
      end
      @q.shift
    end

    alias get_line :deq

    def halt
      @halting = true
      while f = @waiter.shift
        f.resume
      end
    ensure
      @halting = false
    end

  end

#------------------------------------------------------------------

  class Handler

    def initialize(reader,writer,hostname=nil)
      @writer = writer
      @reader = reader
      @host = hostname
      @reader.host = @host
      @writer.host = @host
      @exited = false
    end
    attr_reader :reader, :writer, :host

    def get_line
      @reader.get_line
    end

    def put_line(s)
      @writer.put_line(s)
    end

    def put_kill(sig="INT")
      @writer.io.write("kill:#{sig}\n")
      @writer.io.flush
    end

    def put_exit
      @writer.put_line "exit"
    end

    def exit
      iow = @writer.io
      Log.debug "Handler#exit iow=#{iow.inspect}" if defined? Log
      if @exited
        Log.debug "Handler<##{object_id}#exit multiple called" if defined? Log
        bt = caller.join("\n ")
        Log.debug "Handler#exit bt=\n #{bt}" if defined? Log
        return
      end
      @exited = true
      exit_msg = "exited"
      #return if iow.closed?
      @writer.put_line "exit"
      Log.debug "Handler#exit: end: @writer.put_line \"exit\"" if defined? Log
      #
      if @reader.class == Reader # MultiReader not work
        while line = @reader.get_line
          # here might receive "retire:0" from branch...
          line.chomp!
          Log.debug "Handler#exit: #{line} host=#{@host}" if defined? Log
          return if line == exit_msg
        end
      end
    rescue Errno::EPIPE => e
      Log.error "Errno::EPIPE in #{self.class}.exit iow=#{iow.inspect}\n"+
        e.backtrace.join("\n") if defined? Log
    end

    def halt
      @writer.halt
      @reader.halt
    end

    def self.kill(hdl_set,sig)
      hdl_set.each do |hdl|
        Fiber.new do
          hdl.put_kill(sig)
        end.resume
      end
    end

    def self.exit(hdl_set)
      hdl_set.each do |hdl|
        Fiber.new do
          hdl.exit
        end.resume
      end
    end

  end

end
end
