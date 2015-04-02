module Pwrake

  class LocalityAwareQueue < TaskQueue

    def init_queue(core_map,group_map=nil)
      # core_map = {hid1=>ncore1, ...}
      # group_map = {gid1=>[hid1,hid2,...], ...}
      @size = 0
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
      @q_later = Array.new
      @idle_cores = core_map.dup
      @disable_steal = Rake.application.pwrake_options['DISABLE_STEAL']
      @last_enq_time = Time.now
    end

    attr_reader :size


    def enq_impl(t)
      hints = t && t.suggest_location
      if hints.nil? || hints.empty?
        @q_later.push(t)
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
        if !stored
          @q_remote.push(t)
        end
      end
      @last_enq_time = Time.now
      @size += 1
    end


    def deq_task(&block) # locality version
      return super if @disable_steal
      queued = deq_loop(false,&block) + deq_loop(true,&block)
      if queued>0
        Log.debug "queued:#{queued} @idle_cores:#{@idle_cores.inspect}"
      end
    end


    def deq_impl(host, steal=nil)
      if t = @q_no_action.shift
        Log.debug "deq_no_action task=#{t&&t.name} host=#{host}"
        return t
      end
      #
      if t = deq_locate(host)
        Log.debug "deq_locate steal=#{steal} task=#{t&&t.name} host=#{host}"
        Log.debug "deq_impl\n#{inspect_q}"
        return t
      end
      #
      if !@q_remote.empty?
        t = @q_remote.shift
        Log.debug "deq_remote task=#{t&&t.name} host=#{host}"
        Log.debug "deq_impl\n#{inspect_q}"
        return t
      end
      #
      if !@q_later.empty?
        t = @q_later.shift
        Log.debug "deq_later task=#{t&&t.name} host=#{host}"
        Log.debug "deq_impl\n#{inspect_q}"
        return t
      end
      #
      if steal
        if t = deq_steal(host)
          Log.debug "deq_steal task=#{t&&t.name} host=#{host}"
          Log.debug "deq_impl\n#{inspect_q}"
          return t
        end
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
        @size -= 1
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
      b.call("later",@q_later)
      s
    end

    def size
      @size
    end

    def clear
      @q_no_action.clear
      @q.each{|h,q| q.clear}
      @q_remote.clear
      @q_later.clear
    end

    def empty?
      @q.all?{|h,q| q.empty?} &&
        @q_no_action.empty? &&
        @q_remote.empty? &&
        @q_later.empty?
    end

    def finish
      super
    end

  end
end
