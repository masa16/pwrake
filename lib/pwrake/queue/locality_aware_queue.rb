module Pwrake

  class LocalityAwareQueue < TaskQueue

    def init_queue(group_map=nil)
      # group_map = {gid1=>[hid1,hid2,...], ...}
      @size_q = 0
      @q = {}
      @hostinfo_by_id.each do |id,h|
        @q[id] = @array_class.new(h.ncore)
      end
      @q_group = {}
      group_map ||= {1=>@hostinfo_by_id.map{|id,h| id}}
      group_map.each do |gid,ary|
        q1 = {}     # same group
        q2 = @q.dup # other groups
        ary.each{|hid| q1[hid] = q2.delete(hid)}
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
        kv = {}
        hints.each do |h|
          HostMap.ipmatch_for_name(h).each{|id| kv[id] = true}
        end
        if !kv.empty?
          kv.each_key do |id|
            t.assigned.push(id)
            @q[id].push(t)
          end
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

    def deq_impl(host_info, turn)
      host = host_info.name
      case turn
      when 0
        if t = @q_no_action.shift
          Log.debug "deq_no_action task=#{t&&t.name} host=#{host}"
          return t
        elsif t = deq_locate(host_info,host_info)
          Log.debug "deq_locate task=#{t&&t.name} host=#{host}"
          return t
        elsif t = @q_remote.shift(host_info)
          Log.debug "deq_remote task=#{t&&t.name}"
          return t
        else
          nil
        end
      when 1
        if t = deq_steal(host_info)
          Log.debug "deq_steal task=#{t&&t.name} host=#{host}"
          return t
        else
          nil
        end
      end
    end

    def deq_locate(q_host,run_host)
      q = @q[q_host.id]
      if q && !q.empty?
        t = q.shift(run_host)
        if t
          t.assigned.each do |h|
            if q_h = @q[h]
              q_h.delete(t)
            end
          end
          @size_q -= 1
        end
        return t
      else
        nil
      end
    end

    def deq_steal(host_info)
      # select a task based on many and close
      max_host = nil
      max_num  = 0
      @q_group[host_info.id].each do |qg|
        qg.each do |h,a|
          if !a.empty? # && h!=host_info.id
            d = a.size
            if d > max_num
              max_host = h
              max_num  = d
            end
          end
        end
        if max_num > 0
          max_info = @hostinfo_by_id[max_host]
          Log.debug "deq_steal max_host=#{max_info.name} max_num=#{max_num}"
          t = host_info.steal_phase{|h| deq_locate(max_info,h)}
          #Log.debug "deq_steal task=#{t.inspect}"
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
            hinfo = @hostinfo_by_id[h]
            if hinfo
              s << _qstr(hinfo.name,q)
            else
              s << _qstr("(#{hinfo.inspect})",q)
            end
          else
            n += 1
          end
        end
      end
      s << _qstr("local*#{n}",[]) if n > 0
      s << _qstr("remote",@q_remote)
      s << "@size_q=#{@size_q}"
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

    def drop_host(host_info)
      hid = host_info.id
      if q_drop = @q.delete(hid)
        n_move = 0
        q_size = q_drop.size
        while t = q_drop.shift
          assigned_other = false
          t.assigned.each do |h|
            if h != hid && @q[h]
              assigned_other = true
              break
            end
          end
          if !assigned_other
            @size_q -= 1
            @q_remote.push(t)
            n_move += 1
          end
        end
        Log.debug "LAQ#drop_host: host=#{host_info.name} q.size=#{q_size} n_move=#{n_move}"
      end
    end

  end
end
