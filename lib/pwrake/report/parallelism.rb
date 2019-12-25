module Pwrake

  module Parallelism
    module_function

    def push_time_event(a, row, start_time)
      command = row['command']
      if command == 'pwrake_profile_start'
        start_time[0] = Time.parse(row['start_time'])
      elsif command == 'pwrake_profile_end'
        t = Time.parse(row['start_time']) - start_time[0]
        a << [t,0]
      elsif start_time[0]
        n = begin Integer(row['ncore']) rescue 1 end
        t = Time.parse(row['start_time']) - start_time[0]
        a << [t,+n]
        t = Time.parse(row['end_time']) - start_time[0]
        a << [t,-n]
      end
    end

    def count_start_end_from_csv(file)
      a = []
      start_time = []

      CSV.foreach(file,:headers=>true) do |row|
        push_time_event(a, row, start_time)
      end

      a.sort{|x,y| x[0]<=>y[0]}
    end

    def count_start_end_from_csv_table(csvtable)
      a = []
      start_time = []

      csvtable.each do |row|
        push_time_event(a, row, start_time)
      end

      a.sort{|x,y| x[0]<=>y[0]}
    end

    def exec_density(a)
      reso = 0.1
      delta = 1/reso
      t_end = a.last[0]
      n = (t_end/reso).round + 1
      d = (n+1).times.map{|i| [reso*i,0]}
      i = 0
      a.each do |x|
        while d[i+1][0] <= x[0]
          i += 1
        end
        if x[1] > 0
          d[i][1] += delta
        end
      end
      d
    end


    def plot_parallelism(csvtable, base, fmt)
      fimg = base+'/parallelism.'+fmt
      a = count_start_end_from_csv_table(csvtable)
      return fimg if a.size < 4

      #density = exec_density(a)

      n = a.size
      i = 0
      y = 0
      y_max = 0

      para = []
      begin
        t = 0
        y_pre = 0
        while i < n
          if a[i][0]-t > 0.001
            para.push "%.3f %d" % [t, y_pre]
            t = a[i][0]
            para.push "%.3f %d" % [t, y]
          end
          y += a[i][1]
          y_pre = y
          y_max = y if y > y_max
          i += 1
        end
      rescue
        p a[i]
      end

      t_end = (a.last)[0]

      begin
      if system("which gnuplot >/dev/null 2>&1")
        IO.popen("gnuplot","r+") do |f|
          f.print "
set terminal #{fmt}
set output '#{fimg}'
set xlabel 'time (sec)'
set ylabel '# of cores'
set ytics nomirror

set arrow 1 from #{t_end},#{y_max*0.5} to #{t_end},0 linecolor rgb 'blue'
set label 1 \"#{t_end}\\nsec\" at first #{t_end},#{y_max*0.5} right front textcolor rgb 'blue'

plot '-' w l notitle
"
          para.each do |x|
            f.puts x
          end
        end
      end
      rescue => exc
        $stderr.puts exc
        $stderr.puts exc.backtrace.join("\n")
      end

      #puts "Parallelism plot: #{fimg}"
      fimg
    end


    def plot_parallelism2(csvtable, base, fmt)
      fimg = base+'/parallelism.'+fmt
      a = count_start_end_from_csv_table(csvtable)
      return fimg if a.size < 4

      density = exec_density(a)

      n = a.size
      i = 0
      y = 0
      y_max = 0

      para = []
      begin
        t = 0
        y_pre = 0
        while i < n
          if a[i][0]-t > 0.001
            para.push "%.3f %d" % [t, y_pre]
            t = a[i][0]
            para.push "%.3f %d" % [t, y]
          end
          y += a[i][1]
          y_pre = y
          y_max = y if y > y_max
          i += 1
        end
      rescue
        p a[i]
      end

      t_end = (a.last)[0]

      begin
      if system("which gnuplot >/dev/null 2>&1")
      IO.popen("gnuplot","r+") do |f|
        f.print "
set terminal #{fmt}
set output '#{fimg}'
set xlabel 'time (sec)'
set ylabel '# of cores'
set y2tics
set ytics nomirror
set y2label 'exec/sec'

set arrow 1 from #{t_end},#{y_max*0.5} to #{t_end},0 linecolor rgb 'blue'
set label 1 \"#{t_end}\\nsec\" at first #{t_end},#{y_max*0.5} right front textcolor rgb 'blue'

plot '-' w l axis x1y1 title 'parallelism', '-' w l axis x1y2 title 'exec/sec'
"
        para.each do |x|
          f.puts x
        end
        f.puts "e"

        density.each do |t,d|
          f.puts "#{t} #{d}"
        end
      end
      end
      rescue => exc
        $stderr.puts exc
        $stderr.puts exc.backtrace.join("\n")
      end

      #puts "Parallelism plot: #{fimg}"
      fimg
    end


    def push_time_by_key(h, row, key, start_time)
      command = row['command']
      if command == 'pwrake_profile_start'
        start_time[0] = Time.parse(row['start_time'])
      elsif command == 'pwrake_profile_end'
        t = Time.parse(row['start_time']) - start_time[0]
        h.each_value do |v|
          v << [t,0]
        end
      elsif start_time[0]
        a = (h[key] ||= [])
        n = begin Integer(row['ncore']) rescue 1 end
        t = Time.parse(row['start_time']) - start_time[0]
        a << [t,+n]
        t = Time.parse(row['end_time']) - start_time[0]
        a << [t,-n]
      end
    end

    def read_time_by_host_from_csv(csvtable)
      a = {}
      start_time = []

      csvtable.each do |row|
        push_time_by_key(a, row, row['host'], start_time)
      end
      a
    end


    def get_command_key(s, pattern=[])
      pattern.each do |cmd,regex|
        if regex =~ s
          return cmd
        end
      end
      case s
      when /^\s*\((.*)$/
        get_command_key($1)
      when /^\s*\w+=\S+\s+(.*)$/
        get_command_key($1)
      when /^\s*([\w.,~^\/=+-]+)(.*)$/
        cmd, rest = $1, $2
        if cmd == "cd" && /[^;]*;(.*)$/ =~ rest
          return get_command_key($1)
        else
          cmd
        end
      else
        s[0..15]
      end
    end

    def count_start_end_by_pattern(csvtable, pattern)
      h = Hash.new
      start_time = []

      csvtable.each do |row|
        cmd = get_command_key(row['command'], pattern)
        push_time_by_key(h, row, cmd, start_time)
      end

      h.each do |k,a|
        a.sort!{|x,y| x[0]<=>y[0]}
      end
      h
    end

    def plot_parallelism_by_pattern(csvtable, base, pattern, fmt)
      y_max = 0
      t_end = 0
      para = {}
      t_pat = count_start_end_by_pattern(csvtable, pattern)
      t_pat.each do |cmd,a|
        n = a.size
        i = 0
        y = 0
        para[cmd] = dat = []
        begin
          t = 0
          y_pre = 0
          while i < n
            if a[i][0]-t > 0.001
              dat.push "%.3f %d" % [t, y_pre]
              t = a[i][0]
              dat.push "%.3f %d" % [t, y]
            end
            y += a[i][1]
            y_pre = y
            y_max = y if y > y_max
            i += 1
          end
        rescue
          p a[i]
        end
        if (a.last)[0] > t_end
          t_end = (a.last)[0]
        end
      end

      fimg = base+'/para_cmd.'+fmt

      if system("which gnuplot >/dev/null 2>&1")
      IO.popen("gnuplot","r+") do |f|
        #begin f = $stdout
        f.print "
set terminal #{fmt}
set output '#{fimg}'
set xlabel 'time (sec)'
set ylabel '# of cores'
"
        f.print "plot "
        f.puts para.map{|cmd,re| "'-' w l title #{cmd.inspect}"}.join(",")
        para.each do |cmd,dat|
          dat.each do |x|
            f.puts x
          end
          f.puts "e"
        end
      end
      end

      #puts "Parallelism plot: #{fimg}"
      fimg
    end

    def timeline_to_grid(a,resolution)
      a = a.sort{|x,y| x[0]<=>y[0]}
      grid = [[0,0]]

      j = 0
      a.each do |x|
        i = (x[0]/resolution).floor
        while j < i
          grid[j+1] = [j*resolution,grid[j][1]]
          j += 1
        end
        grid[i][1] += x[1]
      end
      return grid
    end

    def plot_parallelism_by_host(csvtable,base,fmt,start_time,end_time)
      fimg = base+"/para_host."+fmt
      data = read_time_by_host_from_csv(csvtable)
      return fimg if data.size == 0

      e = ([Math.log10(end_time-start_time),1].max * 3).floor
      reso = [1,2,5][e%3] * 10**(e/3-3)
      grid = []
      hosts = data.keys.sort
      hosts.each do |h|
        a = timeline_to_grid(data[h],reso)
        grid << a
      end

      begin
      if system("which gnuplot >/dev/null 2>&1")
      IO.popen("gnuplot","r+") do |f|
        f.puts "
set terminal #{fmt}
set output '#{fimg}'
#set rmargin 7
set lmargin 16
set pm3d map
set pm3d corners2color c1
set xlabel 'time (sec)'
set ytics nomirror
set ticslevel 0
set format y ''
"
        hosts.each_with_index do |h,i|
          if /^([^.]+)\./ =~ h
            h = $1
          end
          f.puts "set ytics add ('#{h}' #{i+0.5})"
        end
        f.puts "splot '-' using 2:1:3 with pm3d title ''"

        grid.each_with_index do |a,j|
          a.each do |x|
            f.printf "%g %g %d\n", j, x[0], x[1]
          end
          f.printf "\n"
        end
        j = grid.size
        grid.last.each do |x|
          f.printf "%g %g %d\n", j, x[0], x[1]
        end
        f.printf "e\n"
      end
      end
      rescue => exc
        $stderr.puts exc
        $stderr.puts exc.backtrace.join("\n")
      end
      fimg
    end

  end
end
