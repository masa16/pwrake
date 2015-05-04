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

    def spawn_cmd(cmd)
      r,w = IO.pipe
      pid = spawn(cmd,[:out,:err]=>w)
      w.close
      Process.waitpid(pid)
      status = $?
      a = []
      while s = r.gets
        a << s
      end
      if !status.success?
        msg = "fail to execute #{cmd}: #{a.join(' ')}"
        $stderr.puts msg
        $stderr.flush
        raise msg
      end
      a
    end

    def open
      FileUtils.mkdir_p @gfarm_mountpoint
      @open_msg = spawn_cmd "gfarm2fs #{@gfarm_mountpoint}"
      super
    end

    def open_messages
      ["mount gfarm2fs: #{@gfarm_mountpoint}"] + @open_msg + super
    end

    def close
      if File.directory? @gfarm_mountpoint
        begin
          spawn_cmd "fusermount -u #{@gfarm_mountpoint}"
        rescue
        end
        system "sync"
        begin
          FileUtils.rmdir @gfarm_mountpoint
          $stderr.puts "removed: #{@@hostname}:#{@gfarm_mountpoint}"
        rescue
          $stderr.puts "fail to remove: #{@@hostname}:#{@gfarm_mountpoint}"
        end
      end
    end

    def close_messages
      super + ["unmount gfarm2fs: #{@gfarm_mountpoint}"]
    end

  end
end
