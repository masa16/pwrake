require "forwardable"
require "pwrake/task/task_rank"

module Pwrake

  class QueueArray < Array

    def initialize(nproc)
      @nproc = nproc
      super()
    end

    def shift(host_info, min_core)
      return super() unless host_info
      i_tried = []
      count = 0
      size.times do |i|
        tw = q_at(i)
        if tw.tried_host?(host_info)
          i_tried << i
        elsif tw.acceptable_for(host_info)
          use_core = tw.use_cores(host_info)
          if use_core > min_core
            Log.debug "qa1: task=#{tw.name} use_core=#{use_core} i=#{i}/#{size} min_core=#{min_core}"
            return q_delete_at(i)
          end
          if min_core > 0
            count += use_core
            if count >= @nproc
              break
            end
          end
        end
      end
      i_tried.each do |i|
        tw = q_at(i)
        if tw.acceptable_for(host_info)
          use_core = tw.use_cores(host_info)
          Log.debug "qa4: task=#{tw.name} use_core=#{use_core} i=#{i}/#{size}"
          return q_delete_at(i)
        end
      end
      nil
    end
  end

  class LifoQueueArray < QueueArray
    def q_at(i)
      at(size-1-i)
    end
    def q_delete_at(i)
      delete_at(size-1-i)
    end
  end

  class FifoQueueArray < QueueArray
    def q_at(i)
      at(i)
    end
    def q_delete_at(i)
      delete_at(i)
    end
  end


  # HRF mixin module
  module HrfQueue

    def hrf_init(nproc)
      @nproc = nproc
      @count = []
    end

    def hrf_push(t)
      r = t.rank
      n = t.use_cores(@nproc)
      @count[r] = (@count[r] || 0) + n
    end

    def hrf_get(host_info, min_core)
      (@count.size-1).downto(0) do |r|
        c = @count[r]
        if c && c>0
          t = (c <= @nproc) ?
            pop_last_rank(r, host_info, min_core) :
            pop_super(host_info, min_core)
          hrf_delete(t) if t
          return t
        end
      end
      raise "no element"
      nil
    end

    def pop_last_rank(r, host_info, min_core)
      i_tried = []
      count = 0
      size.times do |i|
        tw = q_at(i)
        if tw.rank == r
          if tw.tried_host?(host_info)
            i_tried << i
          elsif tw.acceptable_for(host_info)
            use_core = tw.use_cores(@nproc)
            if use_core > min_core
              Log.debug "qa5: task=#{tw.name} use_core=#{use_core} @count[rank=#{r}]=#{@count[r]} i=#{i}/#{size} min_core=#{min_core}"
              return q_delete_at(i)
            end
            if min_core > 0
              count += use_core
              if count >= @nproc
                break
              end
            end
          end
        end
      end
      i_tried.each do |i|
        tw = q_at(i)
        if tw.acceptable_for(host_info)
          use_core = tw.use_cores(host_info)
          Log.debug "qa6: task=#{tw.name} use_core=#{use_core}"
          return q_delete_at(i)
        end
      end
      nil
    end

    def hrf_delete(t)
      @count[t.rank] -= t.use_cores(@nproc)
    end
  end


  # LIFO + HRF
  class LifoHrfQueueArray
    include HrfQueue
    extend Forwardable
    def_delegators :@a, :empty?, :size, :first, :last, :q_at, :q_delete_at

    def initialize(nproc)
      @a = LifoQueueArray.new(nproc)
      hrf_init(nproc)
    end

    def push(t)
      @a.push(t)
      hrf_push(t)
    end

    def shift(host_info, min_core)
      return nil if empty?
      hrf_get(host_info, min_core)
    end

    def delete(t)
      if x=@a.delete(t)
        hrf_delete(t)
      end
      x
    end

    def pop_super(host_info, min_core)
      @a.shift(host_info, min_core)
    end
  end

end
