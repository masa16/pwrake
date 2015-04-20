module Pwrake

  class IdleCores < Hash

    def increase(k,n)
      if x = self[k]
        n += x
      end
      self[k] = n
    end

    def decrease(k,n)
      x = (self[k]||0) - n
      if x == 0
        delete(k)
      elsif x < 0
        raise "# of cores must be non-negative"
      else
        self[k] = x
      end
    end

  end
end
