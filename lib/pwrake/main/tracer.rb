module Pwrake

  class Tracer
    #include Log

    def initialize
      @fetched = {}
    end

    def fetch_tasks( root )
      case $test
      when :orig
        @footprint = {}
      else
        Rake.application.clear_footprint
      end
      @fetched_tasks = {}
      t = Time.now
      status = find_task( root, [] )
      $stderr.puts "fetch task: #{Time.now-t}"
      fetched_tasks = @fetched_tasks
      @fetched_tasks = nil
      if status
        return fetched_tasks
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

      case $test
      when :orig
        if @footprint[name] || @fetched[name]
          return :traced
        end
        @footprint[name] = true
      else
        if tsk.already_fetched || tsk.footprint
          return :traced
        end
        tsk.footprint = true
      end

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
        case $test
        when :orig
          @fetched[name] = true
        when :array
          tsk.already_fetched = true
        end
        if tsk.needed?
          #puts "name=#{name} task.needed"
          @fetched_tasks[name] = tsk
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
