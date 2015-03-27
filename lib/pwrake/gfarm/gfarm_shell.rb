#require 'pwrake/gfwhere_pool'

module Pwrake

  class GfarmShell < Shell

    @@core_id = {}
    @@prefix = "pwrake_#{ENV['USER']}"

    def initialize(host,opt={})
      super(host,opt)
      @single_mp = @option[:single_mp]
      @basedir   = @option[:basedir]
      @prefix    = @option[:prefix] || @@prefix
      @work_dir  = @option[:work_dir]

      @core_id = @@core_id[host] || 0
      @@core_id[host] = @core_id + 1

      if @single_mp
        @remote_mountpoint = "#{@basedir}/#{@prefix}_00"
      else
        @remote_mountpoint = "#{@basedir}/#{@prefix}_%02d" % @core_id
      end
    end

    def start
      Log.debug "--- mountpoint=#{@remote_mountpoint}"
      open(system_cmd)
      cd
      if not _system "test -d #{@remote_mountpoint}"
        _system "mkdir -p #{@remote_mountpoint}" or die
      else
        lines = _backquote("sync; mount")
        if /#{@remote_mountpoint} (?:type )?(\S+)/om =~ lines
          _system "sync; fusermount -u #{@remote_mountpoint}"
          _system "sync"
        end
      end
      subdir = GfarmPath.subdir
      if ["/","",nil].include?(subdir)
        _system "gfarm2fs #{@remote_mountpoint}"
      else
        _system "gfarm2fs -o modules=subdir,subdir=#{subdir} #{@remote_mountpoint}"
      end
      path = ENV['PATH'].gsub( /#{GfarmPath.mountpoint}/, @remote_mountpoint )
      _system "export PATH=#{path}" or die
      cd_work_dir
    end

    def close
      if @remote_mountpoint
        cd
        _system "fusermount -u #{@remote_mountpoint}"
        _system "rmdir #{@remote_mountpoint}"
      end
      super
      self
    end

    def cd_work_dir
      # modify local work_dir -> remote work_dir
      dir = Pathname.new(@remote_mountpoint) + GfarmPath.pwd
      cd dir
    end

  end
end
