module Pwrake

  class LocalityAwareQueue < TaskQueue

    def init_queue(core_map,group_map=nil)
      # core_map = {hid1=>ncore1, ...}
      # group_map = {gid1=>[hid1,hid2,...], ...}
      @size_q = 0
      @q = {}
      core_map.each{|hid,ncore| @q[hid] = @array_class.new(ncore)}
      @q_group = {}
      group_map = {1=>core_map.keys} if group_map.nil?
      group_map.each do |gid,ary|
        other = core_map.dup
        q1 = {} # same group
        ary.each{|hid| q1[hid] = @q[hid]; other.delete(hid)}
        q2 = {} # other groups
        other.each{|hid,nc| q2[hid] = @q[hid]}
        a = [q1,q2]
        ary.each{|hid| @q_group[hid] = a}
      end
      @q_remote = @array_class.new(0)
      @idle_cores = core_map.dup
      @disable_steal = Rake.application.pwrake_options['DISABLE_STEAL']
      @last_enq_time = Time.now
    end


    def enq_impl(t)
      hints = t && t.suggest_location
      if hints.nil? || hints.empty?
        @q_remote.push(t)
      else
        stored = false
        hints.each do |h|
          id = WorkerChannel::HOST2ID[h]
          if q = @q[id]
            t.assigned.push(id)
            q.push(t)
            stored = true
          end
        end
        if stored
          @size_q += 1
        else
          @q_remote.push(t)
        end
      end
      @last_enq_time = Time.now
    end


    def deq_task(&block) # locality version
      return super if @disable_steal
      queued = 0
      3.times do |turn|
        next if turn_empty?(turn)
        queued += deq_loop(turn,&block)
      end
      if queued>0
        Log.debug "queued:#{queued} @idle_cores:#{@idle_cores.inspect}"
      end
    end

    def turn_empty?(turn)
      case turn
      when 0
        @q_no_action.empty? && @q_size == 0
      when 1
        @q_remote.empty?
      when 2
        @q_size == 0
      end
    end

    def deq_impl(host, turn)
      Log.debug "deq_impl\n#{inspect_q}"
      case turn
      when 0
        deq_locate0(host)
      when 1
        deq_remote
      when 2
        deq_steal0(host)
      end
    end

    def deq_locate0(host)
      if t = @q_no_action.shift
        Log.debug "deq_no_action task=#{t&&t.name} host=#{host}"
        return t
      end
      #
      if t = deq_locate(host)
        Log.debug "deq_locate task=#{t&&t.name} host=#{host}"
        return t
      end
      nil
    end

    def deq_remote
      if t = @q_remote.shift
        Log.debug "deq_remote task=#{t&&t.name}"
        return t
      end
      nil
    end

    def deq_steal0(host)
      if t = deq_steal(host)
        Log.debug "deq_steal task=#{t&&t.name} host=#{host}"
        return t
      end
      nil
    end

    def deq_locate(host)
      q = @q[host]
      if q && !q.empty?
        t = q.shift
        if t
          t.assigned.each do |h|
            @q[h].delete(t)
          end
        end
        @size_q -= 1
        return t
      else
        nil
      end
    end

    def deq_steal(host)
      # select a task based on many and close
      max_host = nil
      max_num  = 0
      @q_group[host].each do |qg|
        qg.each do |h,a|
          #Log.debug "deq_steal h=#{h.inspect}\na=#{a.inspect}\n"
          if !a.empty?
            d = a.size
            if d > max_num
              max_host = h
              max_num  = d
            end
          end
        end
        if max_num > 0
          Log.debug "deq_steal max_host=#{max_host} max_num=#{max_num}"
          t = deq_locate(max_host)
          return t if t
        end
      end
      nil
    end

    def inspect_q
      s = ""
      b = proc{|h,q|
        s += " #{h}: size=#{q.size} "
        case q.size
        when 0
          s += "[]\n"
        when 1
          s += "[#{q.first.name}]\n"
        when 2
          s += "[#{q.first.name}, #{q.last.name}]\n"
        else
          s += "[#{q.first.name},.. #{q.last.name}]\n"
        end
      }
      b.call("noaction",@q_no_action)
      @q.each(&b)
      b.call("remote",@q_remote)
      s
    end

    def clear
      @q_no_action.clear
      @q.each{|h,q| q.clear}
      @q_remote.clear
    end

    def empty?
      #@q.all?{|h,q| q.empty?} &&
      @size_q == 0 &&
        @q_no_action.empty? &&
        @q_remote.empty?
    end

    def finish
      super
    end

  end
end
