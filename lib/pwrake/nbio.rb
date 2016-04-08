require "fiber"

module Pwrake
module NBIO

  class TimeoutError < IOError
  end

  class Selector

    def initialize
      @reader = {}
      @writer = {}
      @hb_time = {}
      @running = false
    end

    attr_reader :reader, :writer

    def add_reader(hdl)
      @reader[hdl.io] = hdl
      heartbeat(hdl.io) if hdl.timeout
    end

    def delete_reader(hdl)
      @reader.delete(hdl.io)
      delete_heartbeat(hdl.io)
    end

    def add_writer(hdl)
      @writer[hdl.io] = hdl
    end

    def delete_writer(hdl)
      @writer.delete(hdl.io)
    end

    def run(timeout=nil)
      @running = true
      while @running && !empty?
        r, w = IO.select(@reader.keys,@writer.keys,[],timeout)
        r.each{|io| @reader[io].call} if r
        w.each{|io| @writer[io].call} if w
        while timeout && @hb_earliest && Time.now - @hb_earliest > timeout
          io = @hb_time.key(@hb_earliest)
          if hdl = @reader[io]
            e = TimeoutError.new("HB Timeout (#{timeout}s) #<Reader:%x> #{io.inspect}"%hdl.__id__)
            hdl.error(e)
          end
          delete_heartbeat(io)
        end
      end
    ensure
      @running = false
    end

    def empty?
      @reader.empty? && @writer.empty?
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

    # called when IO start and receive heartbeat
    def heartbeat(io)
      @hb_time[io] = Time.now
      @hb_earliest = @hb_time.values.min
    end

    def delete_heartbeat(io)
      @hb_time.delete(io)
      @hb_earliest = @hb_time.values.min
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

    # from Bartender

    def write(buf, buffered=false)
      push(buf)
      flush unless buffered
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
      @sep = "\n"
      @chunk_size = 8192
    end
    attr_reader :io
    attr_accessor :timeout

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
        #return nil
        raise e
      else
        buf = @buf; @buf = ''
        return buf
      end
    #rescue IO::WaitReadable
    end

    # call from Reader#_read and FiberReaderQueue#deq
    def select_io
      @selector.add_reader(self) if @waiter.empty?
      @waiter.push(Fiber.current)
      Fiber.yield
    ensure
      @waiter.delete(Fiber.current)
      @selector.delete_reader(self) if @waiter.empty?
    end

    def error(e)
      raise e
    end

    def halt
      @halting = true
      call
    ensure
      @halting = false
    end

    # from Bartender

    def _read(sz)
      @io.read_nonblock(sz)
    rescue EOFError
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

  end

#------------------------------------------------------------------

  class MultiReader < Reader

    def initialize(selector, io, n_chan=0)
      super(selector, io)
      @n_chan = n_chan
      @queue = @n_chan.times.map{|i| FiberReaderQueue.new(self)}
      @default_queue = FiberReaderQueue.new(self)
      @timeout = true
    end
    attr_reader :queue
    attr_accessor :timeout
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
      #p IO::WaitReadable
    end

    def error(e)
      @queue.each{|q| q.enq(e)}
      @default_queue.enq(e)
    end

    def halt
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
    end
    attr_reader :reader, :writer, :host

    def get_line
      @reader.get_line
    end

    def put_line(s)
      @writer.put_line(s)
    end

    def put_kill(sig="INT")
      #@writer.put_line("kill:#{sig}")
      @writer.io.puts("kill:#{sig}")
      @writer.io.flush
    end

    def put_exit
      @writer.put_line "exit"
    end

    def exit
      exit_msg = "exited"
      iow = @writer.io
      Log.debug "Handler#exit iow=#{iow.inspect}"
      @writer.put_line "exit"
      if line = @reader.get_line
        line.chomp!
        m = "Handler#exit: #{line} host=#{@host}"
        if line == exit_msg
          Log.debug m
        else
          Log.error m
        end
      else
        Log.error "Handler#exit: fail to get_line : #{line.inspect} closed?=#{@reader.io.closed?}"
      end
    rescue Errno::EPIPE => e
      if Rake.application.options.debug
        $stderr.puts "Errno::EPIPE in #{self.class}#exit iow=#{iow.inspect}"
        $stderr.puts e.backtrace.join("\n")
      end
      Log.error "Errno::EPIPE in #{self.class}.exit iow=#{iow.inspect}\n"+
        e.backtrace.join("\n")
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

#------------------------------------------------------------------

if __FILE__ == $0
  iosel = NBIO::Selector.new

  io = 5.times.map do
    IO.pipe
  end

  io.each do |ior,iow|
    rd = NBIO::MultiReader.new(iosel,ior,1)
    Fiber.new do
      while s = rd.get_line(0)
        puts s
      end
      puts "fiber end"
    end.resume
  end

  io.each do |ior,iow|
    wr = NBIO::Writer.new(iosel,iow)
    Fiber.new do
      2000.times do |i|
        wr.put_line("test str#{i}"+"-"*80,0)
        #iow.puts "0:test str#{i}"+"-"*80
      end
      #iow.print "hage"
      iow.close
    end.resume
  end

  iosel.run
end
end
