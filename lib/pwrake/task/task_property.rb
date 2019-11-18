module Pwrake

  class TaskProperty

    attr_reader :ncore, :exclusive, :allow, :deny, :order_allow_deny,
      :retry, :disable_steal, :reserve
    attr_accessor :subflow

    def parse_description(description)
      if /\bn_?cores?[=:]\s*(-?[\/\d]+)/i =~ description
        case x = $1
        when /^\/\d+$/
          @ncore = ('1'+x).to_r
        when /^\d+\/\d+$/
          @ncore = x.to_r
        when /^-?\d+$/
          @ncore = x.to_i
        else
          m = "invalid task property: ncore=#{x.inspect}"
          Log.fatal m
          raise RuntimeError,m
        end
      end
      if /\bretry[=:]\s*(\d+)/i =~ description
        @retry = $1.to_i
      end
      if /\bexclusive[=:]\s*(\S+)/i =~ description
        if /^(y|t)/i =~ $1
          @exclusive = true
        end
      end
      @reserve = Rake.application.pwrake_options["RESERVE_NODE"]
      if /\breserve[=:]\s*(\S+)/i =~ description
        case $1
        when /^(y|t|on)/i
          @reserve = true
        when /^(n|f|off)/i
          @reserve = false
        end
      end
      if /\ballow[=:]\s*(\S+)/i =~ description
        @allow = $1
      end
      if /\bdeny[=:]\s*(\S+)/i =~ description
        @deny = $1
      end
      if /\border[=:]\s*(\S+)/i =~ description
        case $1
        when /allow,deny/i
          @order_allow_deny = true
        when /deny,allow/i
          @order_allow_deny = false
        end
      end
      if /\bsteal[=:]\s*(\S+)/i =~ description
        if /^(n|f)/i =~ $1
          @disable_steal = true
        end
      end
    end

    def merge(prop)
      @ncore = prop.ncore if prop.ncore
      @exclusive = prop.exclusive if prop.exclusive
      @reserve = prop.reserve if prop.reserve
      @allow = prop.allow if prop.allow
      @deny = prop.deny if prop.deny
      @order_allow_deny = prop.order_allow_deny if prop.order_allow_deny
      @retry = prop.retry if prop.retry
      @disable_steal = prop.disable_steal if prop.disable_steal
      @subflow = prop.subflow if prop.subflow
    end

    def use_cores(arg)
      case arg
      when HostInfo
        ppn = arg.ncore
      when Integer
        ppn = arg
        if ppn < 1
          raise "invalid ppn: #{ppn}"
        end
      else
        raise "invalid ppn: #{ppn}"
      end

      if @exclusive
        return ppn
      end

      case @ncore
      when nil
        return 1
      when 1-ppn..ppn
        return (@ncore>0) ? @ncore : @ncore+ppn
      end

      m = "ncore=#{@ncore} is out of range of cores per node: #{ppn}"
      Log.fatal m
      raise RuntimeError,m
    end

    def accept_host(host_info)
      return true unless host_info
      if @disable_steal && host_info.steal_flag
        #Log.debug("@disable_steal && host_info.steal_flag")
        return false
      end
      hn = host_info.name
      if @allow
        if @deny
          if @order_allow_deny
            return false if !File.fnmatch(@allow,hn) || File.fnmatch(@deny,hn)
          else
            return false if File.fnmatch(@deny,hn) && !File.fnmatch(@allow,hn)
          end
        else
          return false if !File.fnmatch(@allow,hn)
        end
      else
        if @deny
          return false if File.fnmatch(@deny,hn)
        end
      end
      return true
    end

  end
end
