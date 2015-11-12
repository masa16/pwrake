module Pwrake

  class HostInfo

    def initialize(name,id,ncore,weight,group=nil)
      @name = name
      @ncore = ncore
      @weight = weight || 1.0
      @group = group || 0
      @id = id
    end

    attr_reader :name, :ncore, :weight, :group, :id
    attr_accessor :idle_cores

    def set_ncore(n)
      @ncore = @idle_cores = n
    end

    def increase(n)
      @idle_cores += n
    end

    def decrease(n)
      @idle_cores -= n
      if @idle_cores < 0
        raise RuntimeError,"# of cores must be non-negative"
      end
    end
  end

  class HostMap < Hash

    def initialize(arg=nil)
      @host_map = {}
      @by_id = []
      @by_name = {}
      require "socket"
      case arg
      when /\.yaml$/
        read_yaml(arg)
      when String
        read_host(arg)
      when Integer
        parse_hosts(["localhost #{arg}"])
      when NilClass
        parse_hosts(["localhost 1"])
      else
        raise ArgumentError, "arg=#{arg.inspect}"
        #@num_threads = 1 if !@num_threads
        #@core_list = ['localhost'] * @num_threads
      end
    end
    attr_reader :by_id, :by_name

    def host_count
      @by_id.size
    end

    def group_hosts
      a = []
      self.each do |sub,list|
        list.each{|h| (a[h.group] ||= []) << h.name}
      end
      a
    end

    def group_core_weight
      a = []
      self.each do |sub,list|
        list.each{|h| (a[h.group] ||= []) << h.weight}
      end
      a
    end

    def group_weight_sum
      a = []
      self.each do |sub,list|
        list.each{|h| a[h.group] = (a[h.group]||0) + h.weight}
      end
      a
    end

    private

    def read_host(file)
      ary = []
      File.open(file) do |f|
        while l = f.gets
          ary << l
        end
      end
      parse_hosts(ary)
    end

    def read_yaml(file)
      parse_hosts(YAML.load(open(file))[0])
    end

    def parse_hosts(hosts)
      #p hosts
      if hosts.kind_of? Array
        hosts = {"localhost"=>hosts}
      end
      hosts.each do |branch, list|
        self[branch] = parse_list(list)
      end
    end

    def parse_list(line_list)
      info_list = []
      line_list.each do |line|
        parse_line(info_list,line)
      end
      info_list
    end

    def parse_line(info_list,line)
      line = $1 if /^([^#]*)#/ =~ line
      host, ncore, weight, group = line.split
      if host
        if /\[\[([\w\d]+)-([\w\d]+)\]\]/o =~ host
          hosts = ($1..$2).map{|i| host.sub(re,i)}
        else
          hosts = [host]
        end
        hosts.each do |host|
          begin
            host = Socket.gethostbyname(host)[0]
          rescue
            Log.warn "FQDN not resoved : #{host}"
          end
          ncore  &&= ncore.to_i
          weitht &&= weight.to_i
          #weight = (weight || 1).to_f
          group  &&= group.to_i
          if host_info = @by_name[host]
            raise RuntimeError,"duplicated hostname: #{host}"
          else
            id = @by_id.size
            host_info = HostInfo.new(host,id,ncore,weight,group)
            @by_name[host] = host_info
            info_list << host_info
            @by_id << host_info
          end
        end
      end
    end

  end
end
