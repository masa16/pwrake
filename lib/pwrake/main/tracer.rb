module Pwrake

  class Tracer
    #include Log

    def initialize
      @fetched = {}
    end

    def fetch_tasks( root )
      @footprint = {}
      @fetched_tasks = []
      #tm = timer("trace")
      t = Time.now
      status = find_task( root, [] )
      msg = [ "num_tasks=%i" % [@fetched_tasks.size] ]
      tk = @fetched_tasks[0]
      msg << "task[0]=%s" % tk.name.inspect if tk.kind_of?(Rake::Task)
      #tm.finish(msg.join(' '))
      $stderr.puts "fetch task: #{Time.now-t}"
      if status
        return @fetched_tasks
      else
        return nil
      end
    end

    def find_task( tsk, chain )
      name = tsk.name

      if tsk.already_invoked
        #puts "name=#{name} already_invoked"
        return nil
      end

      if chain.include?(name)
        fail RuntimeError, "Circular dependency detected: #{chain.join(' => ')} => #{name}"
      end

      if @footprint[name] || @fetched[name]
        return :traced
      end
      @footprint[name] = true

      chain.push(name)
      prerequisites = tsk.prerequisites
      all_invoked = true
      i = 0
      while i < prerequisites.size
        prereq = tsk.application[prerequisites[i], tsk.scope]
        if find_task( prereq, chain )
          all_invoked = false
        end
        i += 1
      end
      chain.pop


      if all_invoked
        @fetched[name] = true
        if tsk.needed?
          #puts "name=#{name} task.needed"
          @fetched_tasks << tsk
        else
          #puts "name=#{name} task.needed"
          tsk.already_invoked = true
          return nil
        end
      end

      :fetched
    end
  end
end
