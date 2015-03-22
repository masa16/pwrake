module Pwrake

  class Master

    DEFAULT_CONFFILES = ["pwrake_conf.yaml"]
    DEFAULT_CONF = {
      'PWRAKE_CONF'=>'pwrake_conf.yaml',
      'HOSTFILE'=>'hosts.yaml',
      'FILESYSTEM'=>nil,
      'LOGFILE'=>"Pwrake-%Y%m%d%H%M%S-%$.log",
      'TRACE'=>true,
      'MAIN_HOSTNAME'=>`hostname -f`.chomp
    }

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

    def setup_pass_env
      if envs = @confopt['PASS_ENV']
        pass_env = {}

        case envs
        when Array
          envs.each do |k|
            k = k.to_s
            if v = ENV[k]
              pass_env[k] = v
            end
          end
        when Hash
          envs.each do |k,v|
            k = k.to_s
            if v = ENV[k] || v
              pass_env[k] = v
            end
          end
        else
          raise "invalid option for PASS_ENV in pwrake_conf.yaml"
        end

        if pass_env.empty?
          @confopt.delete('PASS_ENV')
        else
          @confopt['PASS_ENV'] = pass_env
        end
      end
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

  end
end
