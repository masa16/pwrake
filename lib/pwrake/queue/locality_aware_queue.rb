module Pwrake

  class LocalityAwareQueue < TaskQueue

    def init_queue(host_map)
      @host_map = host_map
      @size = 0
      @q = {}
      host_count = @host_map.host_count
      host_count.each{|h,n| @q[h] = @array_class.new(n)}
      @q_group = {}
      @host_map.each do |sub,grp|
        other = host_count.dup
        q1 = {}
        grp.each{|info| h=info.name; q1[h] = @q[h]; other.delete(h)}
        q2 = {}
        other.each{|h,v| q2[h] = @q[h]}
        a = [q1,q2]
        grp.each{|info| @q_group[info.name] = a}
      end
      @q_remote = @array_class.new(0)
      @q_later = Array.new
      @enable_steal = !Rake.application.pwrake_options['DISABLE_STEAL']
      @steal_wait = (Rake.application.pwrake_options['STEAL_WAIT'] || 0).to_i
      @steal_wait_max = (Rake.application.pwrake_options['STEAL_WAIT_MAX'] || 10).to_i
      @steal_wait_after_enq = (Rake.application.pwrake_options['STEAL_WAIT_AFTER_ENQ'] || 0.1).to_f
      @last_enq_time = Time.now
      Log.info("-- @enable_steal=#{@enable_steal.inspect} @steal_wait=#{@steal_wait} @steal_wait_max=#{@steal_wait_max} @steal_wait_after_enq={@steal_wait_after_enq}")
    end

    attr_reader :size


    def enq_impl(t)
      hints = t && t.suggest_location
      if hints.nil? || hints.empty?
        @q_later.push(t)
      else
        stored = false
        hints.each do |h|
          if q = @q[h]
            t.assigned.push(h)
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


    def deq_impl(host,n)
      if t = deq_locate(host)
        #Log.info "-- deq_locate n=#{n} task=#{t&&t.name} host=#{host}"
        #Log.debug "--- deq_impl\n#{inspect_q}"
        return t
      end

      #hints = []
      #@q.each do |h,q|
      #  hints << h if !q.empty?
      #end
      #if (!hints.empty?) && @cv.signal_with_hints(hints)
      #  return nil
      #end

      if !@q_remote.empty?
        t = @q_remote.shift
        #Log.info "-- deq_remote n=#{n} task=#{t&&t.name} host=#{host}"
        #Log.debug "--- deq_impl\n#{inspect_q}"
        return t
      end

      if !@q_later.empty?
        t = @q_later.shift
        #Log.info "-- deq_later n=#{n} task=#{t&&t.name} host=#{host}"
        #Log.debug "--- deq_impl\n#{inspect_q}"
        return t
      end

      if @enable_steal && n > 0 && Time.now-@last_enq_time > @steal_wait_after_enq
        if t = deq_steal(host)
          #Log.info "-- deq_steal n=#{n} task=#{t&&t.name} host=#{host}"
          #Log.debug "--- deq_impl\n#{inspect_q}"
          return t
        end
      end

      #m = [@steal_wait*(2**n), @steal_wait_max].min
      #@cv.wait(@mutex,m)
      #@cv.wait(@mutex)
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
          if !a.empty?
            d = a.size
            if d > max_num
              max_host = h
              max_num  = d
            end
          end
        end
        if max_num > 0
          Log.info "-- deq_steal max_host=#{max_host} max_num=#{max_num}"
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
