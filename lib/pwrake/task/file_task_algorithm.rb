module Pwrake

  module FileTaskAlgorithm

    def needed?
      !_exist?(name) || out_of_date?(timestamp) || @application.options.build_all
    end

    # Cache time stamp to reduce load on file system.
    def timestamp
      @file_mtime ||
        if _exist?(name)
          @file_mtime = _mtime(name.to_s)
        else
          Rake::LATE
        end
    end

    private

    @@t_mtime = 0
    @@n_mtime = 0
    @@l_mtime = 100
    @@c_mtime = 0
    @@t_exist = 0
    @@n_exist = 0
    @@l_exist = 100
    @@c_exist = 0

    def _mtime(name)
      t = Pwrake.clock
      m = File.mtime(name.to_s)
      @@t_mtime = @@t_mtime + (Pwrake.clock-t)
      @@n_mtime += 1
      if @@n_mtime >= 100
        Log.debug('mtime: mean=%.9f s (%d times)'%[@@t_mtime/@@n_mtime,@@n_mtime])
        @@t_mtime = 0
        @@n_mtime = 0
        @@c_mtime += 1
        @@l_mtime = 1000 if @@c_mtime == 10
      end
      m
    end

    def _exist?(name)
      t = Pwrake.clock
      e = File.exist?(name.to_s)
      @@t_exist = @@t_exist + (Pwrake.clock-t)
      @@n_exist += 1
      if @@n_exist >= @@l_exist
        Log.debug('exist: mean=%.9f s (%d times)'%[@@t_exist/@@n_exist,@@n_exist])
        @@t_exist = 0
        @@n_exist = 0
        @@c_exist += 1
        @@l_exist = 1000 if @@c_exist == 10
      end
      e
    end

  end
end
