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
      @disable_steal = Rake.application.pwrake_options['DISABLE_STEAL']
      @last_enq_time = Time.now
      @n_turn = @disable_steal ? 1 : 2
    end


    def enq_impl(t)
      hints = t && t.suggest_location
      Log.debug "enq #{t.name} hints=#{hints.inspect}"
      if hints.nil? || hints.empty?
        @q_remote.push(t)
      else
        stored = false
        hints.each do |h|
          id = WorkerCommunicator::HOST2ID[h]
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

    def turn_empty?(turn)
      case turn
      when 0
        @q_no_action.empty? && @size_q == 0 && @q_remote.empty?
      when 1
        @size_q == 0
      end
    end

    def deq_impl(host, turn)
      nc = @idle_cores[host]
      case turn
      when 0
        if t = @q_no_action.shift
          Log.debug "deq_no_action task=#{t&&t.name} host=#{host}"
          return t
        elsif t = deq_locate(host,nc)
          Log.debug "deq_locate task=#{t&&t.name} host=#{host}"
          return t
        elsif t = @q_remote.shift(nc)
          Log.debug "deq_remote task=#{t&&t.name}"
          return t
        else
          nil
        end
      when 1
        if t = deq_steal(host,nc)
          Log.debug "deq_steal task=#{t&&t.name} host=#{host}"
          return t
        else
          nil
        end
      end
    end

    def deq_locate(host,nc)
      q = @q[host]
      if q && !q.empty?
        t = q.shift(nc)
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

    def deq_steal(host,nc)
      # select a task based on many and close
      max_host = nil
      max_num  = 0
      @q_group[host].each do |qg|
        qg.each do |h,a|
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
          t = deq_locate(max_host,nc)
          return t if t
        end
      end
      nil
    end

    def inspect_q
      s = _qstr("noaction",@q_no_action)
      if @size_q == 0
        n = @q.size
      else
        n = 0
        @q.each do |h,q|
          if q.size > 0
            s << _qstr(h,q)
          else
            n += 1
          end
        end
      end
      s << _qstr("local*#{n}",[]) if n > 0
      s << _qstr("remote",@q_remote)
      s
    end

    def clear
      @q_no_action.clear
      @q.each{|h,q| q.clear}
      @q_remote.clear
    end

    def empty?
      @size_q == 0 &&
        @q_no_action.empty? &&
        @q_remote.empty?
    end

  end
end
