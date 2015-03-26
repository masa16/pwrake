module Pwrake

  class RankStat

    def initialize
      @lock = Mutex.new
      @stat = []
    end

    def add_sample(rank,elap)
      @lock.synchronize do
        stat = @stat[rank]
        if stat.nil?
          @stat[rank] = stat = [0,0.0]
        end
        stat[0] += 1
        stat[1] += elap
        Log.debug "--- add_sample rank=#{rank} stat=#{stat.inspect} weight=#{stat[0]/stat[1]}"
      end
    end

    def rank_weight
      @lock.synchronize do
        sum = 0.0
        count = 0
        weight = @stat.map do |stat|
          if stat
            w = stat[0]/stat[1]
            sum += w
            count += 1
            w
          else
            nil
          end
        end
        if count == 0
          avg = 1.0
        else
          avg = sum/count
        end
        [weight, avg]
      end
    end
  end

  RANK_STAT = RankStat.new


end
