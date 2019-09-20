module Pwrake

  module FileTaskAlgorithm

    # Cache time stamp to reduce load on file system.
    def timestamp
      @file_mtime ||
        if File.exist?(name)
          c = Pwrake.clock
          @file_mtime = File.mtime(name.to_s)
          t = Pwrake.clock - c
          Log.debug('File.mtime(%s): %.6f s'%[name,t]) if t > 0.1
          @file_mtime
        else
          Rake::LATE
        end
    end

  end
end
