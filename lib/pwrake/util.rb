module Pwrake
  $DEBUG=true
  module Util
    module_function
    def puts(s)
      $stdout.print s.to_str+"\n"
      $stdout.flush
    end
    def print(s)
      $stdout.print s
      $stdout.flush
    end

    def dputs(s)
      if $DEBUG
        $stdout.print s.to_str+"\n"
        $stdout.flush
      end
    end
    def dprint(s)
      if $DEBUG
        $stdout.print s
        $stdout.flush
      end
    end
  end
end
