require "thread"
require "pp"

class Communicator
  def initialize
    @in = $stdin
    @out = $stdout
  end

  def puts(s)
    @out.print s+"\n"
    @out.flush
  end

  def print(s)
    @out.print s
    @out.flush
  end

  def gets
    @in.gets
  end
end


class Worker
  # @@io = Communicator.new
  @@channels = {}
  @@threads = []
  @@dir = '.'

  class << self
    def count_cpu
      ncpu = 0
      open("/proc/cpuinfo").each do |l|
        if /^processor\s+: \d+/=~l
          ncpu += 1
        end
      end
      ncpu
    end

    def chdir(dir)
      raise "directory #{dir} not found" if !File.directory?(dir)
      @@dir = dir
    end

    def [](id)
      @@channels[id]
    end

    def []=(id,value)
      @@channels[id] = value
    end

    def exit
      @@channels.each{|id,ch| ch.queue.enq(nil)}
      @@threads.each{|th| th.join}
      $io.puts "worker_end"
      Kernel.exit
   end
  end

  # instance methods
  def initialize(id)
    @id = id
    @@channels[@id] = self
    @queue = Queue.new
    @dir = @@dir
    new_thread
  end
  attr_accessor :dir, :queue, :thread

  def dir
    @dir || @@dir
  end

  def new_thread
    pipe_in, pipe_out = IO.pipe
    @@threads << Thread.new(pipe_in,"#{@id}:") do |pin,pre|
      while s = pin.gets
        $io.print pre+s
      end
    end
    @@threads << Thread.new do
      while cmd = @queue.deq
        @pid = spawn(cmd,:out=>pipe_out,:chdir=>dir)
        $io.puts "start:#{@id}:#{@pid}"
        pid,status = Process.wait2(@pid)
        status_s = status_to_str(status)
        $io.puts "end:#{@id}:#{@pid}:#{status_s}"
      end
      pipe_out.close
    end
  end

  def status_to_str(s)
    if s.exited?
      x = "#{s.to_i},exited"
    elsif s.signaled?
      x = "#{s.to_i},signaled,signal=#{s.termsig}"
    elsif s.stopped?
      x = "#{s.to_i},stopped,signal=#{s.stopsig}"
    elsif s.coredump?
      x = "#{s.to_i},coredumped"
    end
    return x
  end

  def cd(dir)
    raise "directory #{dir} not found" if !File.directory?(dir)
    @dir = dir
  end

  def execute(cmd)
    @queue.enq(cmd)
  end

  def exit
    @q.enq(nil)
  end

  def kill(sig)
    if /^\d+$/=~sig
      sig = sig.to_i
    end
    Process.kill(sig,@pid)
  end
end

def check_queue(id)
  $queue[id] ||= new_thread(id)
end

# --- start ---

$io = Communicator.new

@node_id = ARGV[0]
@ncore = ARGV[1] ? ARGV[1].to_i : Worker.count_cpu

$io.puts "ncore:#{@ncore}"

# --- event loop ---

while line = $io.gets
  line.chomp!
  line.strip!
  case line
  when /^(\d+):(.*)$/o
    id,cmd = $1,$2
    if /cd (\S+)/o =~ cmd
      dir = $1
      Worker[id].cd(dir)
    else
      Worker[id].execute(cmd)
    end
    #
  when /^new:(\d+)$/o
    id = $1
    Worker.new(id)
    #
  when /^cd:(.*)$/o
    dir = $1
    Worker.chdir(dir)
    #
  when /^kill:(\d+):(.*)$/o
    id,signal = $1,$2
    Worker[id].kill(signal)
    #
  when /^exit:$/o
    Worker.exit
    #
  else
    raise "invalid line: #{line}"
  end
end
