require "thread"
require "pathname"
require "fileutils"

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

  def dputs(s)
    puts(s) if $DEBUG
  end
end

class Dummy
  def method_missing(*args)
  end
end


class Worker
  @@workers = {}
  @@id_list = []
  @@current_dir = '.'
  @@project = "#{Process.pid}"
  @@dummy = Dummy.new

  class << self
    def p
      puts "@@workers = #{@@workers.inspect}"
    end

    def count_cpu
      ncpu = 0
      open("/proc/cpuinfo").each do |l|
        ncpu += 1 if /^processor\s+: \d+/=~l
      end
      ncpu
    end

    def chdir(dir)
      raise "directory #{dir} not found" if !File.directory?(dir)
      @@current_dir = dir
    end

    def [](id)
      if @@workers.key?(id)
        @@workers[id]
      else
        $io.puts "no worker id:#{id}"
        @@dummy
      end
    end

    def close_all
      @@workers.each{|id,ch| ch.close}
      @@workers.each{|id,ch| ch.join}
      $io.dputs "worker:end:#{@@id_list.inspect}"
      Kernel.exit
    end
  end


  # instance methods

  def initialize(id)
    @id = id
    @@id_list << id
    @@workers[@id] = self
    @queue = Queue.new
    @current_dir = nil
    new_thread
  end

  # attr_accessor :dir, :queue, :thread

  def dir
    @current_dir || @@current_dir
  end

  def new_thread
    pipe_in, pipe_out = IO.pipe
    @pipe_thread = Thread.new(pipe_in,"#{@id}:") do |pin,pre|
      while s = pin.gets
        $io.print pre+s
      end
    end
    @exec_thread = Thread.new do
      while cmd = @queue.deq
        case cmd
        when Proc
          begin
            cmd.call
          rescue => exc
            $stderr.puts exc
          else
            $stderr.puts "end proc"
          end
        when String
          begin
            @pid = spawn(cmd,[:out,:err]=>pipe_out,:chdir=>dir)
          rescue => exc
            $io.puts "end:error:#{exc}"
          else
            $io.puts "start:#{@id}:#{@pid}"
            pid,status = Process.wait2(@pid)
            status_s = status_to_str(status)
            $io.puts "end:#{@id}:#{@pid}:#{status_s}"
          end
        end
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
    @current_dir = dir
  end

  def execute(cmd)
    @queue.enq(cmd)
  end

  def close
    @queue.enq(nil)
  end

  def join
    @pipe_thread.join if @pipe_thread
    @exec_thread.join if @exec_thread
    @@workers.delete(@id)
  end

  def kill(sig)
    sig = sig.to_i if /^\d+$/=~sig
    Process.kill(sig,@pid)
  end
end


class GfarmWorker < Worker
  @@gfarm_top = "/tmp"
  @@gfarm_prefix = nil

  class << self
    def init
      @@gfarm_prefix = "#{@@gfarm_top}/pwrake_#{ENV['USER']}_#{@@project}_"
      if !Dir.glob(@@gfarm_prefix+"*").empty?
        raise "Already running worker:#{@@project}"
      end
    end
  end

  def initialize(id)
    super(id)
    raise "Gfarm uninitialized" if @@gfarm_prefix.nil?
    @gfarm_mountpoint = "#{@@gfarm_prefix}#{@id}"

    execute proc{
      FileUtils.mkdir_p @gfarm_mountpoint
      pid = spawn("gfarm2fs "+@gfarm_mountpoint)
      Process.wait(pid)
    }
  end

  def close
    if File.directory? @gfarm_mountpoint
      execute proc{
        pid = spawn("fusermount -u "+@gfarm_mountpoint)
        Process.wait(pid)
      }
      execute proc{
        FileUtils.rmdir @gfarm_mountpoint
      }
    end
    super
  end

  def cd(dir)
    pn = Pathname(dir)
    if pn.absolute?
      pn = Pathname(@gfarm_mountpoint) + pn
    end
    super(pn.to_s)
  end
end


# --- start ---

$io = Communicator.new
$worker_class = Worker

@node_id = ARGV[0]
@ncore = ARGV[1] ? ARGV[1].to_i : Worker.count_cpu

$io.puts "ncore:#{@ncore}"

END{ Worker.close_all }

[:TERM,:INT,:KILL].each do |sig|
  Signal.trap(sig) do
    Worker.close_all
    Kernel.exit
  end
end

# --- initialize ---

while line = $io.gets
  line.chomp!
  line.strip!
  case line
  when /^p$/o
    Worker.p

  when /^fs:gfarm$/o
    $worker_class = GfarmWorker
    GfarmWorker.init
    #
  when /^new:(.*)$/o
    $1.split.each do |id|
      $worker_class.new(id)
    end
    #
  when /^cd:(.*)$/o
    dir = $1
    Worker.chdir(dir)
    #
  when /^export:(\w+)=(.*)$/o
    k,v = $1,$2
    ENV[k] = v
    #
  when /^start:$/o
    break
    #
  else
    raise "invalid line: #{line}"
  end
end

# Worker.start_all
# $io.puts "start"

# --- event loop ---

while line = $io.gets
  line.chomp!
  line.strip!
  case line
    #
  when /^(\d+):(.*)$/o
    id,cmd = $1,$2
    if /cd (\S+)/o =~ cmd
      dir = $1
      Worker[id].cd(dir)
    else
      Worker[id].execute(cmd)
    end
    #
  when /^kill:(\d+):(.*)$/o
    id,signal = $1,$2
    Worker[id].kill(signal)
    #
  when /^kill:(.*)$/o
    sig = $1
    sig = sig.to_i if /^\d+$/=~sig
    $io.puts "worker killed. signal=#{sig}"
    Process.kill(sig, 0)
    #
  when /^exit_connection$/o
    Kernel.exit
    #
  else
    raise "invalid line: #{line}"
  end
end
