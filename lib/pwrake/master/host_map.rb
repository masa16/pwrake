module Pwrake

  class HostInfo
    def initialize(name,ncore,weight,group=nil)
      @name = name
      @ncore = ncore
      @weight = weight
      @group = group
    end

    def merge(info)
      if @name != info.name || @group != info.group
        raise RuntimeError, "Cannot merge different host or group"
      end
      if info.ncore
        if @ncore
          @ncore += info.ncore
        else
          @ncore = info.ncore
        end
      end
      if info.weight
        if @weight
          @weight += info.weight
        else
          @weight = info.weight
        end
      end
    end

    attr_reader :name, :ncore, :weight, :group
  end

  class HostMap < Hash

    def initialize(file=nil)
      @file = file
      require "socket"
      case @file
      when /\.yaml$/
        read_yaml(@file)
      when String
        read_host(@file)
      else
        parse_hosts(['localhost'])
        #@num_threads = 1 if !@num_threads
        #@core_list = ['localhost'] * @num_threads
      end
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
      p hosts
      if hosts.kind_of? Array
        hosts = {"localhost"=>hosts}
      end
      hosts.each do |branch, list|
        self[branch] = parse_list(list)
      end
    end

    def parse_list(list)
      h = {}
      a = list.map{|s| parse_line(s)}.flatten
      # merge same host
      a.each do |d|
        host = d.name
        if x = h[host]
          x.merge(d)
        else
          h[host] = d.dup
        end
      end
      h.values
    end

    def parse_line(line)
      list = []
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
            Log.info "-- FQDN not resoved : #{host}"
          end
          ncore &&= ncore.to_i
          weitht &&= weight.to_i
          #weight = (weight || 1).to_f
          list << HostInfo.new(host,ncore,weight)
        end
      end
      list
    end

  end
end
