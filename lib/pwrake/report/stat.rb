module Pwrake

  class Stat

    def initialize(data)
      @data = data
      @n = data.size
      if @n>0
        @min = data.min
        @max = data.max
        @sum = data.inject(0){|s,x| s+x}
        @mean = @sum/@n
        @median = calc_median
        @mean_absolute_deviation = data.inject(0){|s,x| (x-@mean).abs} / @n
        if false and @n>1
          @variance = data.inject(0){|s,x| y=x-@mean; y**2} / (@n-1)
          @sdev = Math.sqrt(@variance)
          @skew = data.inject(0){|s,x| y=(x-@mean)/@sdev; y**3} / @n
          @kurt = data.inject(0){|s,x| y=(x-@mean)/@sdev; y**4} / @n - 3
        end
      end
    end

    attr_reader :n
    attr_reader :min, :max, :sum, :mean, :median
    attr_reader :mean_absolute_deviation
    attr_reader :variance, :sdev, :skew, :kurt
    attr_reader :hist, :hist_min, :hist_max, :bin

    def make_logx_histogram(bin)
      if @min>0
      @bin = bin # 1.0/10
      @i_max = (Math.log10(@max)/@bin).floor
      @i_min = (Math.log10(@min)/@bin).floor
      @hist_min = 10**(@i_min * @bin)
      @hist_max = 10**((@i_max+1) * @bin)
      @hist = Array.new(@i_max-@i_min+1,0)
      @data.each do |x|
        i = (Math.log10(x)/@bin-@i_min).floor
        raise "invalid index i=#{i}" if i<0 || i>@i_max-@i_min
        @hist[i] += 1
      end
      end
    end

    def hist_each
      if @hist
        n = @hist.size
        n.times do |i|
          x1 = 10**(@bin*(i+@i_min))
          x2 = 10**(@bin*(i+1+@i_min))
          y  = @hist[i]
          yield x1,x2,y
        end
      end
    end

    def calc_median
      if @n==1
        @data[0]
      elsif @n==2
        @mean
      else
        case @n%2
        when 1
          i = (@n-1)/2
          @data.sort[i]
        else
          i = @n/2
          s = @data.sort
          (s[i]+s[i+1])/2
        end
      end
    end

    def fmt(x)
      case x
      when Numeric
        a = x.abs
        if a == 0
          "0"
        elsif a < 1
          "%.3g" % x
        else
          "%.3f" % x
        end
      else
        x.to_s
      end
    end

    def html_td
      '<td align="right">%i</td><td align="right">%s</td><td align="right">%s</td><td align="right">%s</td><td align="right">%s</td><td align="right">%s</td>' %
        [@n, fmt(@sum), fmt(@mean), fmt(@median), fmt(@min), fmt(@max)]
    end

    def self.html_th
      a = %w[command count sum mean median min max]
      "<tr>" + "<th></th>"*2 +
        "<th colspan=#{a.size-2}>time (seconds)</th>" +
        "</tr>\n<tr>" +
        "<th>%s</th>" * a.size % a +
        "</tr>\n"
    end

  end
end
