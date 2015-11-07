class Helper

  @@spec_dir = File.expand_path(File.dirname(__FILE__))+"/"
  @@pwrake = @@spec_dir+'../bin/pwrake'

  @@show_command = false
  @@show_result = false

  def self.show=(f)
    if f
      @@show_command = true
      @@show_result = true
    else
      @@show_command = false
      @@show_result = false
    end
  end

  def initialize(dir=nil,args=nil)
    @dir = @@spec_dir+(dir||"")
    @args = args
  end

  attr_reader :n_files, :filelist, :result, :status
  attr_reader :elapsed_time

  def clean
    Dir.chdir(@dir) do
      `rake -q clean`
    end
    self
  end

  def run
    cmd = "sh -c '#{@@pwrake} #{@args} 2>&1'"
    if @@show_command
      puts
      puts "-- dir: #{@dir}"
      puts "-- cmd: #{cmd}"
    end
    Dir.chdir(@dir) do
      tm = Time.now
      @result = `#{cmd}`
      @status = $?
      @elapsed_time = Time.now - tm
      system "touch dummy; rm dummy"
      @filelist = Dir.glob("*")
      @n_files = @filelist.size
    end
    if @@show_result
      puts @result
      puts "-- status: #{@status.inspect}\n"
    end
    self
  end

  def success?
    @status && @status.success?
  end

  def output_lines
    @result.split("\n")
  end


  def self.read_hosts(file,ssh=nil)
    cores = []
    open(file) do |f|
      while l = f.gets
        l = $1 if /^([^#]*)#/ =~ l
        host, ncore, group = l.split
        case host
        when /^localhost(\.localdomain)?$/
          host = `hostname`.chomp if ssh
        when nil
        else
          host = `ssh #{host} hostname`.chomp if ssh
        end
        ncore = (ncore || 1).to_i
        cores.concat( [host] * ncore )
      end
    end
    cores
  end

end
