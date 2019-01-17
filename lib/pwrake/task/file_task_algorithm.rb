module Pwrake

  module FileTaskAlgorithm

    # Cache time stamp to reduce load on file system.
    def timestamp
      @file_mtime ||
        if File.exist?(name)
          @file_mtime = File.mtime(name.to_s)
        else
          Rake::LATE
        end
    end

  end
end
