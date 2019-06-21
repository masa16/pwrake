module Pwrake

  class LocalityAwareQueue

    def initialize(hostinfo_by_id, array_class, median_core, group_map=nil)
      @hostinfo_by_id = hostinfo_by_id
      @array_class = array_class
      @median_core = median_core
      @half_core = @median_core/2

      # group_map = {gid1=>[hid1,hid2,...], ...}
      @total_core = 0
      @q = {}
      @hostinfo_by_id.each do |id,host_info|
        @total_core += c = host_info.ncore
        @q[id] = @array_class.new(c)
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

      @q_remote = @array_class.new(@median_core)
      @q_all = @array_class.new(@total_core)
      @disable_steal = Rake.application.pwrake_options['DISABLE_STEAL']
      @multicore_task_priority = Rake.application.pwrake_options['MULTICORE_TASK_PRIORITY']
      Log.debug "MULTICORE_TASK_PRIORITY=#{@multicore_task_priority.inspect}"
      if @half_core == 0 || !@multicore_task_priority
        @turns = @disable_steal ? [2] : [2,3]
      else
        @turns = @disable_steal ? [0,2] : [0,1,2,3]
      end
      @last_enq_time = Time.now
    end

    attr_reader :turns

    def enq_impl(t)
      hints = t && t.suggest_location
      Log.debug "enq #{t.name} hints=#{hints.inspect}"
      @q_all.push(t)
      if hints.nil? || hints.empty?
        @q_remote.push(t)
      else
        kv = {}
        hints.each do |h|
          HostMap.ipmatch_for_name(h).each{|id| kv[id] = true}
        end
        q_success = false
        if !kv.empty?
          kv.each_key do |id|
            q = @q[id]
            if q
              q.push(t)
              q_success = true
              t.assigned.push(id)
            else
              Log.warn("lost queue for host id=#{id.inspect}: @q.keys=#{@q.keys.inspect}")
            end
          end
        end
        unless q_success
          @q_remote.push(t)
        end
      end
      @last_enq_time = Time.now
    end

    def turn_empty?(turn)
      case turn
      when 0,2
        empty?
      when 1,3
        @q_all.size == @q_remote.size
      end
    end

    def deq_impl(host_info, turn)
      case turn
      when 0
        deq_local(host_info,@half_core) ||
        deq_remote(host_info,@half_core)
      when 1
        deq_steal(host_info,@half_core)
      when 2
        deq_local(host_info,0) ||
        deq_remote(host_info,0)
      when 3
        deq_steal(host_info,0)
      end
    end

    def deq_local(run_host, min_core)
      q = @q[run_host.id]
      if q && !q.empty?
        t = q.shift(run_host,min_core)
        if t
          q_delete_assigned_to(t)
          @q_all.delete(t)
          Log.debug "deq_local task=#{t&&t.name} host=#{run_host.name}"
          return t
        end
      end
      nil
    end

    def deq_remote(host_info, min_core)
      if t = @q_remote.shift(host_info,min_core)
        @q_all.delete(t)
        Log.debug "deq_remote task=#{t&&t.name} host=#{host_info.name}"
        return t
      end
      nil
    end

    def deq_steal(run_host, min_core)
      if t = @q_all.shift(run_host,min_core)
        q_delete_assigned_to(t)
        @q_remote.delete(t)
        Log.debug "deq_steal task=#{t&&t.name} host=#{run_host.name}"
        return t
      end
      nil
    end

    def q_delete_assigned_to(t)
      t.assigned.each do |h|
        if q_h = @q[h]
          q_h.delete(t)
        end
      end
    end

    def inspect_q
      s = ""
      if @q_all.size == @q_remote.size
        n = @q.size
      else
        n = 0
        @q.each do |h,q|
          if q.size > 0
            hinfo = @hostinfo_by_id[h]
            if hinfo
              s << TaskQueue._qstr(hinfo.name,q)
            else
              s << TaskQueue._qstr("(#{hinfo.inspect})",q)
            end
          else
            n += 1
          end
        end
      end
      s << TaskQueue._qstr("local*#{n}",[]) if n > 0
      s << TaskQueue._qstr("remote",@q_remote)
      s << TaskQueue._qstr("all",@q_all)
      s
    end

    def size
      @q_all.size
    end

    def clear
      @q.each{|h,q| q.clear}
      @q_remote.clear
      @q_all.clear
    end

    def empty?
      @q_all.empty?
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
            @q_remote.push(t)
            n_move += 1
          end
        end
        Log.debug "LAQ#drop_host: host=#{host_info.name} q.size=#{q_size} n_move=#{n_move}"
      end
    end

  end
end
