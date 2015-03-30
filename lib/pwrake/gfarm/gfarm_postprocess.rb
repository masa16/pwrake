#require 'pwrake/gfwhere_pool'

module Pwrake

  class GfarmPostprocess

    def initialize
      max = Rake.application.pwrake_options['MAX_GFWHERE_WORKER']
      @gfwhere_pool = WorkerPool.new(GfwhereWorker,max)
    end

    def postprocess(t)
      if t.kind_of?(Rake::FileTask) && t.wrapper.location.empty?
        t.wrapper.location = @gfwhere_pool.work(t.name)
      end
    end

    def postprocess_bulk(tasks)
      list = []
      tasks.each do |t|
       list << t.name if t.kind_of? Rake::FileTask
      end
      if !list.empty?
       Log.info "-- after_check: size=#{list.size} #{list.inspect}"
       gfwhere_result = GfarmPath.gfwhere(list)
       tasks.each do |t|
         if t.kind_of? Rake::FileTask
           t.wrapper.location = gfwhere_result[GfarmPath.local_to_fs(t.name)]
         end
       end
       #puts "'#{self.name}' exist? => #{File.exist?(self.name)} loc => #{loc}"
      end
    end

  end
end
