require "socket"

module Pwrake

  class HostInfo

    @@local_ip = nil

    def self.local_ip
      @@local_ip ||=
        Socket.getifaddrs.select{|a| a.addr.ip?}.map{|a| a.addr.ip_address}
    end

    def initialize(name,id,ncore,weight,group=nil)
      @name = name
      @ncore = ncore || 1
      @weight = weight || 1.0
      @group = group || 0
      @id = id
      @continuous_fail = 0
      @total_fail = 0
      @count_task = 0
      @ipaddr = []
      begin
        @ipaddr << IPSocket.getaddress(@name)
      rescue
      end
    end

    attr_reader :name, :ncore, :weight, :group, :id, :steal_flag
    attr_reader :ipaddr
    attr_reader :continuous_fail
    attr_accessor :idle_cores

    def add_line(ncore=nil,weight=nil,group=nil)
      ncore ||= 1
      weight ||= 1.0
      group ||= 0
      if @group != group
        raise "different group=#{group} for host=#{@name}"
      end
      @weight = (@weight*@ncore + weight*ncore)/(@ncore+ncore)
      @ncore += ncore
    end

    def local?
      ipa = IPSocket.getaddress(@name)
      HostInfo.local_ip.include?(ipa)
    end

    def set_ncore(n)
      @busy_cores = 0
      @ncore = @idle_cores = n
    end

    def set_ip(ipa)
      @ipaddr.push(ipa)
    end

    def idle(n)
      @busy_cores -= n
      @idle_cores += n
    end

    def busy(n)
      @busy_cores += n
      @idle_cores -= n
    end

    def retire(n)
      @idle_cores -= n
    end

    def retired?
      @idle_cores + @busy_cores < 1 # all retired
    end

    def steal_phase
      @steal_flag = true
      t = yield(self)
      @steal_flag = false
      t
    end

    def count_result(result)
      @count_task += 1
      case result
      when "end"
        @continuous_fail = 0
      when "fail"
        @continuous_fail += 1
        @total_fail += 1
      else
        raise "unknown result: #{result}"
      end
      @continuous_fail
    end

    def accept_core(use_cores)
      use_cores <= @idle_cores
    end
  end

  class HostMap < Hash

    def self.ipmatch_for_name(name)
      @@hostmap.ipmatch_for_name(name)
    end

    def initialize(arg=nil)
      @host_map = {}
      @by_id = []
      @by_name = {}
      @is_local = false
      @ipmatch_for_name = {}
      @@hostmap = self
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
      end

      # local check
      if @by_id.size == 1
        if @by_id[0].local?
          @is_local = true
        end
      end
    end
    attr_reader :by_id, :by_name

    def max_ncore
      by_id.map{|host_info| host_info.ncore}.max
    end

    def min_ncore
      by_id.map{|host_info| host_info.ncore}.max
    end

    def total_ncore
      by_id.inject(0){|sum,host_info| host_info.ncore + sum}
    end

    def local?
      @is_local
    end

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

    def ipmatch_for_name(node)
      unless a = @ipmatch_for_name[node]
        @ipmatch_for_name[node] = a = []
        ip = IPSocket.getaddress(node)
        @by_id.each_with_index do |h,id|
          a << id if h.ipaddr.include?(ip)
        end
        Log.debug "node:#{node} hosts:#{a.map{|id|@by_id[id].name}.inspect}"
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

    REGEX_RANGE = /\[\[([\w\d]+)-([\w\d]+)\]\]/o

    def parse_line(info_list,line)
      line = $1 if /^([^#]*)#/ =~ line
      host, ncore, weight, group = line.split
      if host
        if REGEX_RANGE =~ host
          hosts = ($1..$2).map{|i| host.sub(REGEX_RANGE,i)}
        else
          hosts = [host]
        end
        hosts.each do |host|
          ncore  &&= ncore.to_i
          weitht &&= weight.to_i
          #weight = (weight || 1).to_f
          group  &&= group.to_i
          if host_info = @by_name[host]
            host_info.add_line(ncore,weight,group)
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
