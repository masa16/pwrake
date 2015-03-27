module Pwrake

#  module TaskAlgorithm
#    def assigned
#      @assigned ||= []
#    end
#  end
#
#  # copy from ruby's thread.rb
#  class ConditionVariable
#    #
#    # Creates a new ConditionVariable
#    #
#    def initialize
#      @waiters = {}
#      @waiters_mutex = Mutex.new
#    end
#
#    #
#    # Releases the lock held in +mutex+ and waits; reacquires the lock on wakeup.
#    #
#    # If +timeout+ is given, this method returns after +timeout+ seconds passed,
#    # even if no other thread doesn't signal.
#    #
#    def wait(mutex, timeout=nil)
#      Thread.handle_interrupt(StandardError => :never) do
#        begin
#          Thread.handle_interrupt(StandardError => :on_blocking) do
#            @waiters_mutex.synchronize do
#              @waiters[Thread.current] = true
#            end
#            mutex.sleep timeout
#          end
#        ensure
#          @waiters_mutex.synchronize do
#            @waiters.delete(Thread.current)
#          end
#        end
#      end
#      self
#    end
#
#    #
#    # Wakes up the first thread in line waiting for this lock.
#    #
#    def signal
#      Thread.handle_interrupt(StandardError => :on_blocking) do
#        begin
#          t, _ = @waiters_mutex.synchronize { @waiters.shift }
#          t.run if t
#        rescue ThreadError
#          retry # t was already dead?
#        end
#      end
#      self
#    end
#
#    #
#    # Wakes up all threads waiting for this lock.
#    #
#    def broadcast
#      Thread.handle_interrupt(StandardError => :on_blocking) do
#        threads = nil
#        @waiters_mutex.synchronize do
#          threads = @waiters.keys
#          @waiters.clear
#        end
#        for t in threads
#          begin
#            t.run
#          rescue ThreadError
#          end
#        end
#      end
#      self
#    end
#  end

#  class LocalityConditionVariable < ConditionVariable
#
#    def signal(hints=nil)
#      if hints.nil?
#        super()
#      elsif Array===hints
#          thread = nil
#          @waiters_mutex.synchronize do
#            @waiters.each do |t,v|
#              if hints.include?(t[:hint])
#                thread = t
#                break
#              end
#            end
#            if thread
#              @waiters.delete(thread)
#            else
#              thread,_ = @waiters.shift
#            end
#          end
#          Log.debug "--- LCV#signal: hints=#{hints.inspect} thread_to_run=#{thread.inspect} @waiters.size=#{@waiters.size}"
#          begin
#            thread.run if thread
#          rescue ThreadError
#            retry # t was already dead?
#          end
#      else
#        raise ArgumentError,"argument must be an Array"
#      end
#      self
#    end
#
#
#    def signal_with_hints(hints)
#      if !Array===hints
#        raise ArgumentError,"argument must be an Array"
#      end
#      thread =
#        @waiters_mutex.synchronize do
#        th = nil
#        @waiters.each do |t,v|
#          Log.debug "--- LCV#signal_with_hints: t[:hint]=#{t[:hint]}"
#          if hints.include?(t[:hint])
#            th = t
#            break
#          end
#        end
#        Log.debug "--- LCV#signal_with_hints: hints=#{hints.inspect} thread_to_run=#{th.inspect} @waiters.size=#{@waiters.size}"
#        if th
#          @waiters.delete(th)
#        end
#        th
#      end
#      begin
#        thread.run if thread
#      rescue ThreadError
#        retry # t was already dead?
#      end
#      thread
#    end
#
#
#    def broadcast(hints=nil)
#      if hints.nil?
#        super()
#      elsif Array===hints
#          threads = []
#          @waiters_mutex.synchronize do
#            hints.each do |h|
#              @waiters.each do |t,v|
#                if t[:hint] == h
#                  threads << t
#                  break
#                end
#              end
#            end
#            threads.each do |t|
#              @waiters.delete(t)
#            end
#          end
#          Log.debug "--- LCV#broadcast: hints=#{hints.inspect} threads_to_run=#{threads.inspect} @waiters.size=#{@waiters.size}"
#          threads.each do |t|
#            begin
#              t.run
#            rescue ThreadError
#            end
#          end
#      else
#        raise ArgumentError,"argument must be an Array"
#      end
#      self
#    end
#  end


  class LocalityAwareQueue < TaskQueue

#    class Throughput
#
#      def initialize(list=nil)
#        @interdomain_list = {}
#        @interhost_list = {}
#        if list
#          values = []
#          list.each do |x,y,v|
#            hash_x = (@interdomain_list[x] ||= {})
#            hash_x[y] = n = v.to_f
#            values << n
#          end
#          @min_value = values.min
#        else
#          @min_value = 1
#        end
#      end
#
#      def interdomain(x,y)
#        hash_x = (@interdomain_list[x] ||= {})
#        if v = hash_x[y]
#          return v
#        elsif v = (@interdomain_list[y] || {})[x]
#          hash_x[y] = v
#        else
#          if x == y
#            hash_x[y] = 1
#          else
#            hash_x[y] = 0.1
#          end
#        end
#        hash_x[y]
#      end
#
#      def interhost(x,y)
#        return @min_value if !x
#        hash_x = (@interhost_list[x] ||= {})
#        if v = hash_x[y]
#          return v
#        elsif v = (@interhost_list[y] || {})[x]
#          hash_x[y] = v
#        else
#          x_short, x_domain = parse_hostname(x)
#          y_short, y_domain = parse_hostname(y)
#          v = interdomain(x_domain,y_domain)
#          hash_x[y] = v
#        end
#        hash_x[y]
#      end
#
#      def parse_hostname(host)
#        /^([^.]*)\.?(.*)$/ =~ host
#        [$1,$2]
#      end
#
#    end # class Throughput


    def init_queue(host_map)
      @host_map = host_map
      #@cv = LocalityConditionVariable.new
      @size = 0
      @q = {}
      host_count = @host_map.host_count
      host_count.each{|h,n| @q[h] = @array_class.new(n)}
      @q_group = {}
      #@host_map.group_hosts.each do |g|
      #  other = @host_map.host_count.dup
      #  q1 = {}
      #  g.each{|h| q1[h] = @q[h]; other.delete(h)}
      #  q2 = {}
      #  other.each{|h,v| q2[h] = @q[h]}
      #  a = [q1,q2]
      #  g.each{|h| @q_group[h] = a}
      #end
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
      b.call("noaction",@q_noaction)
      @q.each(&b)
      b.call("remote",@q_remote)
      b.call("later",@q_later)
      s
    end

    def size
      @size
    end

    def clear
      @q_noaction.clear
      @q.each{|h,q| q.clear}
      @q_remote.clear
      @q_later.clear
    end

    def empty?
      @q.all?{|h,q| q.empty?} &&
        @q_noaction.empty? &&
        @q_remote.empty? &&
        @q_later.empty?
    end

    def finish
      super
    end

  end
end
