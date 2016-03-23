require "fiber"

module Pwrake module AIO

  class Selector

    def initialize(timeout=nil)
      @reader = {}
      @writer = {}
      @running = false
      @timeout = timeout
      @hb_time = {}
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

    def run
      @running = true
      while @running && !empty?
        r, w = IO.select(@reader.keys,@writer.keys,[],@timeout)
        if r.nil? && w.nil?
          raise TimeoutError,"Timeout (#{@timeout} s) in IO.select"
        end
        r.each{|io| @reader[io].call}
        w.each{|io| @writer[io].call}
        if @timeout && @hb_earliest
          if Time.now - @hb_earliest > @timeout
            io = @hb_time.key(@hb_earliest)
            raise TimeoutError,"Timeout (#{@timeout}s) "+
              "in Heartbeat from host=#{get_host(io)}"
          end
        end
      end
      @running = false
    end

    def empty?
      @reader.empty? && @writer.empty?
    end

    def finish
      @writer.each_value{|hdl| hdl.finish}
      @reader.each_value{|hdl| hdl.finish}
      @running = false
    end

    # used to print an error message
    def get_host(io)
      hdl = @reader[io] || @writer[io]
      h = hdl.respond_to?(:host) ? hdl.host : nil
    end

    # called when IO start and receive heartbeat
    def heartbeat(io)
      @hb_time[io] = Time.now
      @hb_earliest = @hb_time.values.min
    end

  end

#------------------------------------------------------------------

  class Writer
    def initialize(iosel, io)
      @iosel = iosel
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
      @iosel.delete_writer(self) if @waiter.empty?
    end

    # call from Fiber context
    def put_line(line, ch=nil)
      line = line.chomp
      line = "#{ch}:#{line}" if ch
      write(line+"\n")
    end

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

    def finish
      #@io.close
    end

    private
    def _write(buf)
      return @io.write_nonblock(buf)
    rescue IO::WaitWritable
      select_io
      retry
    end

    def select_io
      @iosel.add_writer(self) if @waiter.empty?
      @waiter.push(Fiber.current)
      Fiber.yield
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

    def initialize(iosel, io, n_chan=0)
      @iosel = iosel
      @io = io
      @n_chan = n_chan
      @queue = @n_chan.times.map{|i| FiberReaderQueue.new(self)}
      @default_queue = FiberReaderQueue.new(self)
      @sel_chan = {}
      @buf = ''
      @sep = "\n"
      @chunk_size = 8192
    end
    attr_reader :io, :queue
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

    # call from FiberReaderQueue
    def select_io
      @iosel.add_reader(self) if @sel_chan.empty?
      @sel_chan[Fiber.current] = true
      Fiber.yield
    ensure
      @sel_chan.delete(Fiber.current)
      @iosel.delete_reader(self) if @sel_chan.empty?
    end

    # call from Selector
    def call
      read_lines
    end

    def finish
      @queue.each{|q| q.finish}
      @default_queue.finish if @default_queue
      #@io.close
    end

    private
    def read_lines
      while true
        while index = @buf.index(@sep)
          enq_line(@buf.slice!(0, index+@sep.bytesize))
        end
        @buf << @io.read_nonblock(@chunk_size)
      end
    rescue EOFError
      if !@buf.empty?
        enq_line(@buf)
        @buf = ''
      end
      enq_line(nil)
    rescue IO::WaitReadable
    end

    def enq_line(s)
      if s.nil? # EOF
        @default_queue.enq(nil)
        @queue.each{|q| q.enq(nil)}
        return
      end
      s = s.chomp
      if !@queue.empty? && /^(\d+):(.*)$/ =~ s
        ch,line = $1,$2
        if q = @queue[ch.to_i]
          q.enq(line)
        else
          raise "No queue ##{ch}"
        end
      elsif @default_queue
        @default_queue.enq(s)
      else
        raise "No default_queue, received: #{s}"
      end
    end

  end

  class FiberReaderQueue

    def initialize(reader)
      @reader = reader
      @q = []
      @waiter = []
      @finished = false
    end

    def enq(x)
      @q.push(x)
      f = @waiter.shift
      f.resume if f
    end

    def deq
      while @q.empty?
        return nil if @finished
        @waiter.push(Fiber.current)
        @reader.select_io
      end
      @q.shift
    end

    alias get_line :deq

    def finish
      @finished = true
      while f = @waiter.shift
        f.resume
      end
    end

  end

end end


if __FILE__ == $0

module Pwrake
  ior,iow = IO.pipe
  iosel = AIO::Selector.new
  rd = AIO::Reader.new(iosel,ior,1)
  wr = AIO::Writer.new(iosel,iow)

  Fiber.new do
    2000.times do |i|
      wr.put_line "0:test str#{i}"+"-"*80
    end
    iow.close
  end.resume

  Fiber.new do
    while s=rd[0].get_line
      p s
    end
    puts "fiber end"
  end.resume

  iosel.run
end; end
