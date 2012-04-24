require "fileutils"
require "pathname"

class GfarmPath
  @@list = []
  @@gfarm_top = "/tmp"
  @@gfarm_prefix = nil

  @@gfarm_prefix = "#{@@gfarm_top}/pwrake_#{ENV['USER']}_#{Process.pid}"

  if !Dir.glob(@@gfarm_prefix+"*").empty?
    raise "Already running Gfarm client:#{@@gfarm_prefix}"
  end

  def initialize(id=nil)
    @id = id
    @gfarm_mountpoint = @@gfarm_prefix
    @gfarm_mountpoint += "_#{@id}" if @id

    # puts "mkdir_p #{@gfarm_mountpoint}"
    FileUtils.mkdir_p @gfarm_mountpoint
    cmd = "gfarm2fs "+@gfarm_mountpoint
    # puts cmd
    pid = spawn(cmd)
    Process.wait(pid)
    @@list.push(self)
  end

  def close
    if File.directory? @gfarm_mountpoint
      cmd = "fusermount -u "+@gfarm_mountpoint
      # puts cmd
      pid = spawn(cmd)
      Process.wait(pid)
      system "sync"
      # puts "rmdir #{@gfarm_mountpoint}"
      FileUtils.rmdir @gfarm_mountpoint
      @@list.delete(self)
    end
  end

  def chdir(dir)
    pn = Pathname(dir)
    if pn.absolute?
      pn = pn.relative_path_from(Pathname("/"))
      pn = Pathname(@gfarm_mountpoint) + pn
    end
    # puts "cd #{pn}"
    Dir.chdir(pn.to_s)
    # puts Dir.pwd
  end

  END{Dir.chdir; @@list.each{|x| x.close}}
end

#m=GfarmPath.new("001")
#sleep 1
#m.cd("/home")
#sleep 1
#m.close
