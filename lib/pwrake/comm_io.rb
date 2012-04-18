class CommIO < IO

  def initialize(io)
    @io = io
  end

  def puts(*args)
    @io.print args.join+"\n"
    @io.flush
  end

  def print(*args)
    @io.print args.join
    @io.flush
  end

  def gets
    @io.gets
  end

  def close
    @io.close
  end

  def to_io
    @io
  end
end
