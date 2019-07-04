require "forwardable"
require "pwrake/task/task_rank"

module Pwrake

  class QueueArray < Array

    def initialize(nproc)
      @nproc = nproc
      super()
    end

    def shift(host_info, req_rank=0)
      i_tried = nil
      size.times do |i|
        tw = q_at(i)
        if tw.rank >= req_rank && tw.acceptable_for(host_info)
          if tw.tried_host?(host_info)
            i_tried ||= i
          else
            Log.debug "#{self.class}: task=#{tw.name} i=#{i}/#{size} rank=#{tw.rank}"
            return q_delete_at(i)
          end
        end
      end
      if i_tried
        Log.debug "#{self.class}(retry): task=#{tw.name} i=#{i}/#{size} rank=#{tw.rank}"
        return q_delete_at(i_tried)
      end
      nil
    end

    def find_rank(ncore)
      if empty?
        return 0
      end
      count = []
      size.times do |i|
        tw = q_at(i)
        r = tw.rank
        c = (count[r]||0) + tw.use_cores(ncore)
        if c >= @nproc
          return r
        end
        count[r] = c
      end
      count.size - 1
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

    def hrf_get(host_info, rank)
      (@count.size-1).downto(rank) do |r|
        c = @count[r]
        if c && c>0
          t = (c <= @nproc) ?
            pop_last_rank(r, host_info) :
            pop_super(host_info, rank)
          hrf_delete(t) if t
          return t
        end
      end
      Log.debug "#{self.class}#hrf_get: no item for rank=#{rank} @count=#{@count.inspect}"
      nil
    end

    def pop_last_rank(r, host_info)
      i_tried = nil
      size.times do |i|
        tw = q_at(i)
        if tw.rank == r && tw.acceptable_for(host_info)
          if tw.tried_host?(host_info)
            i_tried ||= i
          else
            Log.debug "#{self.class}: task=#{tw.name} i=#{i}/#{size} rank=#{tw.rank}"
            return q_delete_at(i)
          end
        end
      end
      if i_tried
        Log.debug "#{self.class}(retry): task=#{tw.name} i=#{i}/#{size} rank=#{tw.rank}"
        return q_delete_at(i_tried)
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
    def_delegators :@a, :empty?, :size, :first, :last, :q_at, :q_delete_at, :find_rank

    def initialize(nproc)
      @a = LifoQueueArray.new(nproc)
      hrf_init(nproc)
    end

    def push(t)
      @a.push(t)
      hrf_push(t)
    end

    def shift(host_info, rank)
      return nil if empty?
      hrf_get(host_info, rank)
    end

    def delete(t)
      if x=@a.delete(t)
        hrf_delete(t)
      end
      x
    end

    def pop_super(host_info, rank)
      @a.shift(host_info, rank)
    end
  end

end
