#require 'pwrake/gfwhere_pool'

module Pwrake

  module GfarmPath

    module_function

    def mountpoint_of_cwd
      path = Pathname.pwd
      while !path.mountpoint?
        path = path.parent
      end
      path
    end

    @@local_mountpoint = mountpoint_of_cwd
    @@fs_subdir = Pathname.new('/')

    def mountpoint=(d)
      @@local_mountpoint = Pathname.new(d)
    end

    def mountpoint
      @@local_mountpoint
    end

    def subdir=(d)
      if d
        @@fs_subdir = Pathname.new(d)
        if @@fs_subdir.relative?
          @@fs_subdir = Pathname.new('/') + @@fs_subdir
        end
      end
    end

    def subdir
      @@fs_subdir.to_s
    end

    def pwd
      Pathname.pwd.relative_path_from(@@local_mountpoint)
    end

    def gfarm2fs?(d=nil)
      d ||= @@local_mountpoint
      mount_type = nil
      open('/etc/mtab','r') do |f|
        f.each_line do |l|
          if /#{d} (?:type )?(\S+)/o =~ l
            mount_type = $1
            break
          end
        end
      end
      /gfarm2fs/ =~ mount_type
    end

    def from_local(x)
      pn = Pathname(x)
      if pn.absolute?
        pn.relative_path_from(@@local_mountpoint)
      else
        Pathname.pwd.relative_path_from(@@local_mountpoint) + pn
      end
    end

    def from_fs(x)
      Pathname(x).relative_path_from(@@fs_subdir)
    end

    def to_fs(x)
      @@fs_subdir + Pathname(x)
    end

    def to_local(x)
      @@local_mountpoint + Pathname(x)
    end

    def local_to_fs(x)
      x = from_local(x)
      x = to_fs(x)
      x.to_s
    end

    def fs_to_local(x)
      x = from_fs(x)
      x = to_local(x)
      x.to_s
    end

    def gfpath(file='.')
      begin
	IO.popen("gfstat '#{file}'") do |f|
	  if /File: "([^"]+)"/ =~ f.gets #"
	    return $1
	  end
	end
      rescue
      end
      nil
    end

=begin
    def gfwhere(list)
      system "sync"
      result = {}
      count = 0
      cmd = "gfwhere"
      parse_proc = proc{|x|
        if count==1
          result[cmd[8..-1]] = x.split
        else
          x.scan(/^(?:gfarm:\/\/[^\/]+)?([^\n]+):\n([^\n]*)$/m) do |file,hosts|
            h = hosts.split
            result[file] = h if !h.empty?
          end
        end
      }

      list.each do |a|
        if a
          path = local_to_fs(a)
          if cmd.size + path.size + 1 > 20480 # 131000
            x = `#{cmd} 2> /dev/null`
            parse_proc.call(x)
            cmd = "gfwhere"
            count = 0
          end
          cmd << " "
          cmd << path
          count += 1
        end
      end
      if count > 0
        x = `#{cmd} 2> /dev/null`
        parse_proc.call(x)
      end
      result
    end
=end

  end
end
