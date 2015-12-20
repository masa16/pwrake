module Pwrake

  class TaskProperty

    attr_reader :ncore, :exclusive, :allow, :deny, :order_allow_deny,
      :retry, :disable_steal

    def parse_description(description)
      if /\bn_?cores?[=:]\s*([+-]?\d+)/i =~ description
        @ncore = $1.to_i
      end
      if /\bretry[=:]\s*(\d+)/i =~ description
        @retry = $1.to_i
      end
      if /\bexclusive[=:]\s*(\S+)/i =~ description
        if /^(y|t)/i =~ $1
          @exclusive = true
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

    def acceptable_for(host_info)
      if @disable_steal && host_info.steal_flag
        #Log.debug("@disable_steal && host_info.steal_flag")
        return false
      end
      ncore = (@exclusive) ? 0 : (@ncore || 1)
      if ncore > 0
        return false if ncore > host_info.idle_cores
      else
        n = host_info.ncore + ncore
        return false if n < 1 || n > host_info.idle_cores
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

    def n_used_cores(host_info=nil)
      nc_node = host_info && host_info.ncore
      if @ncore.nil?
        return 1
      elsif @ncore > 0
        if nc_node && @ncore > nc_node
          m = "ncore=#{@ncore} must be <= nc_node=#{nc_node}"
          Log.fatal m
          raise RuntimeError,m
        end
        return @ncore
      else
        if nc_node.nil?
          m = "host_info.ncore is not set"
          Log.fatal m
          raise RuntimeError,m
        end
        n = @ncore + nc_node
        if n > 0
          return n
        else
          m = "ncore+nc_node=#{n} must be > 0"
          Log.fatal m
          raise RuntimeError,m
        end
      end
    end

  end
end
