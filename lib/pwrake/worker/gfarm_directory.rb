require "socket"

module Pwrake

  class GfarmDirectory < SharedDirectory
    @@prefix = nil
    @@work_dir = nil
    @@log_dir = nil
    @@gfarm2fs_option = nil
    @@gfarm2fs_debug = nil
    @@gfarm2fs_debug_wait = 1
    @@current_id = 0
    @@hostname = Socket.gethostname

    def self.init(opts)
      @@prefix   = opts[:base_dir]
      @@work_dir = opts[:work_dir]
      @@log_dir  = opts[:log_dir]
      @@gfarm2fs_option = opts[:gfarm2fs_option]
      @@gfarm2fs_debug = opts[:gfarm2fs_debug]
      @@gfarm2fs_debug_wait = opts[:gfarm2fs_debug_wait]
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
      @log.info "spawn_cmd: "+cmd
      r,w = IO.pipe
      pid = spawn(cmd,[:out,:err]=>w)
      w.close
      pidmy,status = Process.waitpid2(pid)
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
      begin
        if @@gfarm2fs_debug && path
          f = path+("gfarm2fs-"+@@hostname+"-"+@suffix)
          spawn_cmd "gfarm2fs #{@@gfarm2fs_option} -d #{@gfarm_mountpoint} > #{f} 2>&1 & sleep #{@@gfarm2fs_debug_wait}"
        else
          spawn_cmd "gfarm2fs #{@@gfarm2fs_option} #{@gfarm_mountpoint}"
        end
      rescue => exc
        sleep 1
        raise exc
      end
      super
    end

    def close
      super
      n = 0
      while n < 4
        begin
          if mounted?
            sleep 0.15 * 4**n
            spawn_cmd "fusermount -u #{@gfarm_mountpoint}"
          end
          n = 99
        rescue => e
          @log.error e.message+" n=#{n}"
          n += 1
        end
      end
      if File.directory? @gfarm_mountpoint
        begin
          FileUtils.rmdir @gfarm_mountpoint
          @log.info "rmdir #{@@hostname}:#{@gfarm_mountpoint}"
        rescue
          @log.error "failed to rmdir #{@@hostname}:#{@gfarm_mountpoint}"
        end
      end
      if File.exist? @gfarm_mountpoint
        @log.warn "mountpoint #{@@hostname}:#{@gfarm_mountpoint} remains"
      end
    end

    def mounted?
      File.open('/etc/mtab','r') do |f|
        f.each_line do |l|
          a = l.split
          if a[1] == @gfarm_mountpoint && a[2] =~ /gfarm2fs/
            return true
          end
        end
      end
      false
    end

    def check_mountpoint
      # try to recover from gfarm2fs segfault
      unless File.directory?(@gfarm_mountpoint)
        @log.warn "remounting #{@@hostname}:#{@gfarm_mountpoint}"
        close
        open
      end
    end

  end
end
