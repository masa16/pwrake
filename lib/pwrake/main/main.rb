require "yaml"

module Pwrake

  class Main

    DEFAULT_CONFFILES = ["pwrake_conf.yaml"]
    DEFAULT_CONF = {
      'PWRAKE_CONF'=>'pwrake_conf.yaml',
      'HOSTFILE'=>'hosts.yaml',
      'FILESYSTEM'=>nil,
      'LOGFILE'=>"Pwrake-%Y%m%d%H%M%S-%$.log",
      'TRACE'=>true,
      'MAIN_HOSTNAME'=>`hostname -f`.chomp
    }

    def initialize(hosts=nil)
      setup_options
      setup_filesystem

      if hosts
        @hosts = hosts.dup
      else
        @hosts = YAML.load(open(@confopt['HOSTFILE']))
      end
      if @hosts.kind_of? Hash
        @hosts = [@hosts]
      end
      Util.dputs "@hosts=#{@hosts.inspect}"

      @branch_set = []
      @worker_set = []

      @scheduler = RoundRobinScheduler.new
      @tracer = Tracer.new

      @ioevent = IOEvent.new
      @task_set = {}
    end

    def setup_options
      @pwrake_conf = Rake.application.options.pwrake_conf

      if @pwrake_conf
        if !File.exist?(@pwrake_conf)
          raise "Configuration file not found: #{@pwrake_conf}"
        end
      else
        @pwrake_conf = DEFAULT_CONFFILES.find{|fn| File.exist?(fn)}
      end

      if @pwrake_conf.nil?
        @confopt = {}
      else
        Util.dputs "@pwrake_conf=#{@pwrake_conf}"
        @confopt = YAML.load(open(@pwrake_conf))
      end

      DEFAULT_CONF.each do |key,value|
        if !@confopt[key]
          @confopt[key] = value
        end
        if value = ENV[key]
          @confopt[key] = value
        end
      end

      @confopt['TRACE'] = Rake.application.options.trace
      @confopt['VERBOSE'] = true if Rake.verbose
      @confopt['SILENT'] = true if !Rake.verbose
      @confopt['DRY_RUN'] = Rake.application.options.dryrun
      #@confopt['RAKEFILE'] =
      #@confopt['LIBDIR'] =
      @confopt['RAKELIBDIR'] = Rake.application.options.rakelib.join(':')
    end

    def setup_filesystem
      @filesystem = @confopt['FILESYSTEM']

      if @filesystem.nil?
        # get mountpoint
        path = Pathname.pwd
        while ! path.mountpoint?
          path = path.parent
        end
        @mount_point = path
        # get filesystem
        open('/etc/mtab','r') do |f|
          f.each_line do |l|
            if /#{@mount_point} (?:type )?(\S+)/o =~ l
              @mount_type = $1
              break
            end
          end
        end
        case @mount_type
        when /gfarm2fs/
          @filesystem = 'gfarm'
        when 'nfs'
          @filesystem = 'nfs'
        else
          # raise "unknown filesystem : #{@mount_point} type #{@mount_type}"
          @filesystem = 'local'
        end

        @confopt['FILESYSTEM'] = @filesystem
      end

      puts "FILESYSTEM=#{@filesystem}"

      case @filesystem
      when 'gfarm'
        @cwd = "/"+Pathname.pwd.relative_path_from(@mount_point).to_s
      when 'nfs'
        @cwd = Dir.pwd
      else
        @cwd = Dir.pwd
      end
      @confopt['DIRECTORY'] = @cwd

      puts "@cwd=#{@cwd}"
    end

    def setup_branches
      tm = Time.now
      wk_count = 0
      conn_by_host = {}
      @hosts.each do |a|
        a.each_key do |sub_host|
          dir = File.absolute_path(File.dirname($PROGRAM_NAME))
          args = Shellwords.shelljoin(ARGV)
          cmd = "ssh -x -T -q #{sub_host} '" +
            "cd #{@cwd}; PATH=#{dir}:${PATH} exec pwrake_branch #{args}'"
          conn = Connection.new(sub_host,cmd)
          @ioevent.add_io(conn.ior,conn)
          conn_by_host[sub_host] = conn
        end
      end
      puts "pass1 t=#{Time.now-tm}"
      tm = Time.now

      @ioevent.event_each do |conn,s|
        if !s or s.chomp != "pwrake_branch started"
          p s
          raise "pwrake_branch start failed: conn=#{conn.inspect}"
        end
        puts conn.host
      end
      puts "pass2 t=#{Time.now-tm}"
      tm = Time.now

      @ioevent.each do |conn|
        Marshal.dump(@confopt,conn.iow)
        conn.send_cmd "begin_worker_list"
      end

      puts "pass3 t=#{Time.now-tm}"
      tm = Time.now

      @hosts.each do |a|
        a.each do |sub_host,wk_hosts|
          conn = conn_by_host[sub_host]
          wk_hosts.map do |s|
            h, ncore = s.split
            ncore = ncore.to_i if ncore
            if /(.*)\[([^-]+)(?:-|\.\.)([^-]+)\](.*)/o =~ h
              range = ($1+$2+$4)..($1+$3+$4)
            else
              range = h..h
            end
            range.each do |host|
              # puts "connecting #{host} #{ncore}"
              wk = WorkerChannel.new(conn.iow,host,ncore)
              @worker_set.push(wk)
              wk.send_worker
              wk_count += 1
            end
          end
          conn.send_cmd "end_worker_list"
        end
      end
      puts "wk_count=#{wk_count}"
      puts "pass4 t=#{Time.now-tm}"
      tm = Time.now
    end

    def invoke(root, args)
      while task_hash = @tracer.fetch_tasks(root)
        return if task_hash.empty?

        # scheduling
        @scheduler.assign(task_hash,@worker_set)

        # send tasks
        @worker_set.each do |wk|
          wk.send_tasks
        end

        @ioevent.each do |conn|
          conn.send_cmd "end_task_list"
        end

        #$stderr.puts "send task: #{Time.now-t} sec"
        #t= Time.now

        # event loop
        @ioevent.event_loop do |conn,s|
          s.chomp!
          if /^taskend:(.*)$/o =~ s
            task_name = $1
            if t = task_hash.delete(task_name)
              t.already_invoked = true
            end
            break if task_hash.empty?
          else
            Util.puts s
          end
        end
        task_hash = nil
      end
    end


    def finish
      if defined?(measure)
        measure
      end
      Util.dputs "main:exit_branch"
      @ioevent.each do |conn|
        conn.close if conn # finish if conn.respond_to?(:finish)
      end
      @ioevent.each_io do |io|
        while s=io.gets
          Util.print s
        end
      end
      Util.dputs "branch:finish"

      # @ioevent.finish "exit_branch"
    end

  end

end
