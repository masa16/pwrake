module Pwrake

  class GfarmDirectory < SharedDirectory
    @@prefix = nil
    @@work_dir = nil
    @@log_dir = nil
    @@current_id = 0
    @@hostname = `hostname`.chomp

    def self.init(*args)
      @@prefix, @@work_dir, @@log_dir, = args
      Dir.chdir(ENV['HOME'])
    end

    def initialize
      @id = @@current_id
      @@current_id += 1
      @gfarm_mountpoint = (@@prefix+"_%05d_%03d") % [Process.pid,@id]
    end

    def home_path
      Pathname.new(@gfarm_mountpoint)
    end

    def open
      FileUtils.mkdir_p @gfarm_mountpoint
      n = 0
      while !system "(cd; gfarm2fs #{@gfarm_mountpoint}) >& /dev/null"
        raise "fail in gfarm2fs #{@gfarm_mountpoint}" if n > 5
        $stderr.puts "sleep #{2**n} s for gfarm2fs #{@gfarm_mountpoint}"
        sleep 2**n
        n += 1
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
        n = 0
        while !system("fusermount -u #{@gfarm_mountpoint} >& /dev/null")
          raise "fail in fusermount -u #{@gfarm_mountpoint}" if n > 5
          $stderr.puts "sleep #{2**n} s for fusermount -u #{@gfarm_mountpoint}"
          sleep 2**n
          n += 1
        end
        system "sync"
        FileUtils.rmdir @gfarm_mountpoint
        $stderr.puts "removed: #{@@hostname}:#{@gfarm_mountpoint}"
      end
    end

    def close_messages
      super + ["unmount gfarm2fs: #{@gfarm_mountpoint}"]
    end

  end
end
