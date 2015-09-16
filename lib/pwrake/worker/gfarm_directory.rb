module Pwrake

  class GfarmDirectory < SharedDirectory
    @@prefix = nil
    @@work_dir = nil
    @@log_dir = nil
    @@debug_gfarm2fs = nil
    @@current_id = 0
    @@hostname = `hostname`.chomp

    def self.init(opts)
      @@prefix   = opts[:base_dir]
      @@work_dir = opts[:work_dir]
      @@log_dir  = opts[:log_dir]
      @@debug_gfarm2fs = opts[:debug_gfarm2fs]
      Dir.chdir(ENV['HOME'])
    end

    def initialize
      super
      @id = @@current_id
      @@current_id += 1
      @suffix = "%05d_%03d" % [Process.pid,@id]
      @gfarm_mountpoint = @@prefix+"_"+@suffix
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
        a << s.chomp
      end
      if status.success?
        msg = a.empty? ? cmd : cmd+" => #{a.join(',')}"
        @log.info msg
      else
        msg = "failed to execute `#{cmd}' => #{a.join(',')}"
        raise msg
      end
      a
    end

    def open
      FileUtils.mkdir_p @gfarm_mountpoint
      path = @log.path
      if @@debug_gfarm2fs && path
        spawn_cmd "gfarm2fs -d #{@gfarm_mountpoint} > #{path+("gfarm2fs"+@suffix)} 2>&1 & sleep 0.3"
      else
        spawn_cmd "gfarm2fs #{@gfarm_mountpoint}"
      end
      super
    end

    def close
      super
      if File.directory? @gfarm_mountpoint
        begin
          spawn_cmd "fusermount -u #{@gfarm_mountpoint}"
        rescue
        end
        system "sync"
        begin
          FileUtils.rmdir @gfarm_mountpoint
          @log.info "rmdir #{@gfarm_mountpoint} @#{@@hostname}"
        rescue
          @log.error "failed to rmdir #{@gfarm_mountpoint} @#{@@hostname}"
        end
      end
    end

  end
end
