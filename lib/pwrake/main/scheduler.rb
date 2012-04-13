module Pwrake

  class RoundRobinScheduler
    def initialize
      @i = 0
    end

    def assign(tasks,worker_set)
      #t = Time.now
      tasks.each do |t|
        #worker_set.assign_task_by_index(t.name, @i)
        worker_chan = worker_set[@i]
        worker_chan.add_task(t)
        @i = (@i+1) % worker_set.size
      end
      #$stderr.puts "assign: #{Time.now-t} sec"
    end
  end


  class GfarmAffinityScheduler
    def assign(tasks,worker_set)
      filenames = []
      tasks.each do |t|
        if t.kind_of? Rake::FileTask and name = t.prerequisites[0]
          filenames << name
        end
      end
      gfwhere_result = GfarmSSH.gfwhere(filenames)
      tasks.each do |t|
        if t.kind_of? Rake::FileTask and prereq_name = t.prerequisites[0]
          hosts = gfwhere_result[GfarmSSH.gf_path(prereq_name)]
          worker_set.assign_task_by_host(t.name, hosts)
        end
      end
    end
  end

  class GraphScheduler
    def assign(tasks,workers)
    end
  end

end
