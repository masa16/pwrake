module Pwrake

  class GfarmDirectory < SharedDirectory
    @@prefix = nil
    @@work_dir = nil
    @@log_dir = nil
    @@current_id = 0

    def self.init(*args)
      @@prefix, @@work_dir, @@log_dir, = args
    end

    def initialize
      @id = @@current_id
      @@current_id += 1
      @gfarm_mountpoint = (@@prefix+"_%03d") % @id
    end

    def home_path
      Pathname.new(@gfarm_mountpoint)
    end

    def open
      Dir.chdir(ENV['HOME']) do
        FileUtils.mkdir_p @gfarm_mountpoint
        system "gfarm2fs #{@gfarm_mountpoint} >& /dev/null"
      end
      super
    end

    def open_messages
      ["mount gfarm2fs: #{@gfarm_mountpoint}"] + super
    end

    def close
      # $log.info "GfarmWorker.close #{@gfarm_mountpoint}"
      if File.directory? @gfarm_mountpoint
        cd ENV['HOME']
        system "fusermount -u #{@gfarm_mountpoint} >& /dev/null"
        system "sync"
        FileUtils.rmdir @gfarm_mountpoint
      end
    end

    def close_messages
      super + ["unmount gfarm2fs: #{@gfarm_mountpoint}"]
    end

  end
end
