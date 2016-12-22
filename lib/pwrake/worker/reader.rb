module Pwrake

  class Reader

    def initialize(io,mode="")
      @io = io
      @buf = ''
      @eof = false
      @mode = mode
    end

    attr_reader :io, :mode

    def eof?
      @eof && @buf.empty?
    end

    def gets
      read_until("\n")
    end

    def read_until(sep="\r\n", chunk_size=8192)
      until i = @buf.index(sep)
        if s = _read(chunk_size)
          @buf += s
        else
          if !@buf.empty? && @eof
            buf = @buf; @buf = ''
            return buf
          else
            return nil
          end
        end
      end
      @buf.slice!(0, i+sep.bytesize)
    end

    def _read(sz)
      @io.read_nonblock(sz)
    rescue EOFError
      @eof = true
      nil
    rescue IO::WaitReadable
      nil
    end

  end


  class Selector

    def initialize
      @readers = {}
    end

    def add_reader(io,&callback)
      @readers[io] = callback
    end

    def delete_reader(io)
      @readers.delete(io)
    end

    def loop
      while !@readers.empty?
        r, = IO.select(@readers.keys,nil,nil)
        r.each{|io| @readers[io].call} if r
      end
    end

  end

end
