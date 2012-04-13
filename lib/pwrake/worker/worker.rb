require "thread"
require "pp"

$queue = {}
$threads = []

def count_cpu
  ncpu = 0
  open("/proc/cpuinfo").each do |l|
    if /^processor\s+: \d+/=~l
      ncpu += 1
    end
  end
  ncpu
end

def new_thread(id)
  pipe_in,pipe_out = IO.pipe
  $threads << Thread.new(pipe_in,"#{id}:") do |pin,pre|
    while s = pin.gets
      $stdout.print pre+s
      $stdout.flush
    end
  end
  q = Queue.new
  $threads << Thread.new do
    while cmd = q.deq
      pid = spawn(cmd,:out=>pipe_out)
      Process.wait(pid)
      $stdout.puts "end:#{id}"
      $stdout.flush
    end
    pipe_out.close
  end
  return q
end

def check_queue(id)
  $queue[id] ||= new_thread(id)
end


@node_id = ARGV[0]
@ncore = ARGV[1] ? ARGV[1].to_i : count_cpu

$stdout.puts "ncore:#{@ncore}"
$stdout.flush

# event loop

while line = $stdin.gets
  line.chomp!
  case line
  when /^(\d+):(.*)$/o
    id,cmd = $1,$2
    check_queue(id)
    $queue[id].enq(cmd)
  when /^exit:$/o
    $stdout.puts "exited"
    $stdout.flush
    $queue.each{|k,q| q.enq(nil)}
    $threads.each{|th| th.join}
    exit
  else
    raise "invalid line: #{line}"
  end
end
