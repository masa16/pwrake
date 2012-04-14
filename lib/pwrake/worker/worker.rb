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

def count_cpu
  ncpu = 0
  open("/proc/cpuinfo").each do |l|
    if /^processor\s+: \d+/=~l
      ncpu += 1
    end
  end
  ncpu
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

def new_thread(id)
  pipe_in,pipe_out = IO.pipe
  $threads << Thread.new(pipe_in,"#{id}:") do |pin,pre|
    while s = pin.gets
      $io.print pre+s
    end
  end
  q = Queue.new
  $threads << Thread.new(id) do |id_|
    while cmd = q.deq
      pid = spawn(cmd,:out=>pipe_out)
      pid,status = Process.wait2(pid)
      status_s = status_to_str(status)
      $io.puts "end:#{id}:#{status_s}"
    end
    pipe_out.close
  end
  return q
end

def check_queue(id)
  $queue[id] ||= new_thread(id)
end

# --- start ---

$queue = {}
$threads = []
$io = Communicator.new

@node_id = ARGV[0]
@ncore = ARGV[1] ? ARGV[1].to_i : count_cpu

$io.puts "ncore:#{@ncore}"

# --- event loop ---

while line = $io.gets
  line.chomp!
  case line
  when /^(\d+):(.*)$/o
    id,cmd = $1,$2
    check_queue(id)
    $queue[id].enq(cmd)
  when /kill:^(\d+):(.*)$/o
    id,signal = $1,$2
    #
  when /^exit:$/o
    $io.puts "exited"
    $queue.each{|k,q| q.enq(nil)}
    $threads.each{|th| th.join}
    exit
  else
    raise "invalid line: #{line}"
  end
end
