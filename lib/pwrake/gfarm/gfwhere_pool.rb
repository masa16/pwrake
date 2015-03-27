module Pwrake

  class WorkerPool

    def initialize(wk_class, max)
      @worker_class = wk_class
      @max = max
      @pool = []
      @cond_pool = ConditionVariable.new
      @mutex = Mutex.new
    end

    def find_worker
      @mutex.synchronize do
        while true
          @pool.each do |w|
            return w if w.acquire
          end
          if @pool.size < @max
            Log.debug "--- #{@worker_class}:new_worker #{@pool.size+1}"
            w = @worker_class.new(@cond_pool)
            @pool << w
            return w if w.acquire
          end
          # wait for end of work in @pool
          @cond_pool.wait(@mutex)
        end
      end
    end

    def work(*args)
      w = find_worker
      w.run(*args)
    end
  end


  class Worker

    def initialize(cond_pool)
      @cond_pool = cond_pool
      @mutex = Mutex.new
      @aquired = false
    end

    def acquire
      return false if @mutex.locked?
      if @aquired
        return false
      else
        @aquired = true
        return true
      end
    end

    def run(*args)
      @mutex.synchronize do
        raise "no aquired" unless @aquired
        r = work(*args)
        @aquired = false
        @cond_pool.signal
        return r
      end
    end

    def work(*args)
      # inplement in subclass
      return nil
    end
  end


  class GfwhereWorker < Worker

    def initialize(cond_pool)
      super(cond_pool)
      @io = IO.popen('gfwhere-pipe','r+')
      @io.sync = true
    end

    def work(file)
      return [] if file==''
      t = Time.now
      @io.puts(file)
      @io.flush
      s = @io.gets
      if s.nil?
        raise "gfwhere: unexpected end"
      end
      s.chomp!
      if s != file
        raise "gfwhere: file=#{file}, result=#{s}"
      end
      while s = @io.gets
        s.chomp!
        case s
        when /^gfarm:\/\//
          next
        when /^Error:/
          return []
        else
          Log.debug "gfwhere:path %.6f sec, file=%s" % [Time.now-t,file]
          return s.split(/\s+/)
        end
      end
    end
  end

end
