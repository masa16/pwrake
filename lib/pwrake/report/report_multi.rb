module Pwrake

  class ReportMulti

    def initialize(list,pattern)
      @reports = list.map do |base|
        r = Report.new(base,pattern)
        puts r.base+" elap=#{r.elap}"
        r
      end
      @pattern = pattern
      @elap_svg = 'elap.svg'
    end

    def report(stat_html)
      if true
        @reports.each do |r|
          r.report_html
        end
        plot_elap
      end
      html = Report::HTML_HEAD + "<body><h1>Pwrake Statistics</h1>\n"
      html << "<h2>Log files</h2>\n"
      html << "<table>\n"
      html << "<tr><th>log file</th><th>id</th><th>ncore</th><th>elapsed time(sec)</th><tr>\n"
      @reports.each do |r|
        html << "<tr><td><a href='#{r.html_file}'>#{r.base}</a></td>"
        html << "<td>#{r.id_str}</td><td>#{r.ncore}</td><td>#{r.elap}</td><tr>\n"
      end
      html << "</table>\n"
      html << "<h2>Elapsed time</h2>\n"
      html << "<img src='./#{File.basename(@elap_svg)}'  align='top'/></br>\n"

      html << "<h2>Histogram of Execution time</h2>\n"
      html << report_histogram()
      html << "</body></html>\n"

      File.open(stat_html,"w") do |f|
        f.puts html
      end
    end

    def plot_elap
      a = @reports.map{|r| r.ncore * r.elap}.min
      elaps = @reports.map{|r| r.elap}
      logmin = Math.log10(elaps.min)
      logmax = Math.log10(elaps.max)
      mid = (logmin+logmax)/2
      wid = (logmax-logmin).ceil*0.5
      ymin = 10**(mid-wid)
      ymax = 10**(mid+wid)
      IO.popen("gnuplot","r+") do |f|
        f.puts "
set terminal svg size 640,480
set output '#{@elap_svg}'
set xlabel 'ncore'
set ylabel 'time (sec)'
set yrange [#{ymin}:#{ymax}]
set logscale xy
plot #{a}/x,'-' w lp lw 2 ps 2 title 'elapsed time'
"
        @reports.sort_by{|r| r.ncore}.each do |r|
          f.puts "#{r.ncore} #{r.elap}"
        end
        f.puts "e"
      end
      puts "Ncore-time plot: "+@elap_svg
    end

    def report_histogram
      @images = {}
      @cmd_rep = {}

      @reports.each do |r|
        r.cmd_stat.each do |cmd,stat|
          if stat.n > 2
            @cmd_rep[cmd] ||= {}
            @cmd_rep[cmd][r.id_str] = r # stat
          end
        end
      end

      @cmd_rep.each_key do |cmd|
        @images[cmd] = 'hist_'+cmd.gsub(/[\/.]/,'_')+'.svg'
      end
      histogram_plot
      histogram_html
    end

    def histogram_html
      html = ""
      @cmd_rep.each do |cmd,cmd_rep|
        html << "<p>Statistics of Elapsed time of #{cmd}</p>\n<table>\n"
        html << "<th>id</th><th>ncore</th>"+Stat.html_th
        cmd_rep.each do |id,r|
          s = r.cmd_stat[cmd]
          html << "<tr><td>#{id}</td><td>#{r.ncore}</td>" + s.html_td + "</tr>\n"
        end
        html << "</table>\n"
        html << "<img src='./#{File.basename(@images[cmd])}'/>\n"
      end
      html
    end

    def histogram_plot
      @cmd_rep.each do |cmd,cmd_rep|
        IO.popen("gnuplot","r+") do |f|
          f.puts "
set terminal svg # size 480,360
set output '#{@images[cmd]}'
set ylabel 'histogram'
set xlabel 'Execution time (sec)'
set logscale x
set title '#{cmd}'"
          a = []
          ncores = cmd_rep.keys
          ncores.each_with_index{|n,i|
            a << "'-' w histeps ls #{i+1} title ''"
            a << "'-' w lines ls #{i+1} title '#{n}'"
          }
          f.puts "plot "+ a.join(',')

          cmd_rep.each do |ncore,r|
            s = r.cmd_stat[cmd]
            2.times do
              s.hist_each do |x1,x2,y|
                x = Math.sqrt(x1*x2)
                f.printf "%f %d\n", x, y
              end
              f.puts "e"
            end
          end
        end
        puts "Histogram plot: #{@images[cmd]}"
      end
    end

    def histogram_plot2
      @cmd_rep.each do |cmd,cmd_rep|
        IO.popen("gnuplot","r+") do |f|
          f.puts "
set terminal svg # size 480,360
set output '#{@images[cmd]}'
set nohidden3d
set palette rgb 33,13,10
set pm3d
set ticslevel 0
unset colorbox
set yrange [#{cmd_rep.size}:0]
set logscale x
set title '#{cmd}'"
          a = []
          ncores = cmd_rep.keys.sort
          ncores.each_with_index{|n,i|
            a << "'-' w lines ls #{i+1} title '#{n} cores'"
          }
          f.puts "splot "+ a.join(',')

          ncores.each_with_index do |ncore,i|
            s = cmd_rep[ncore]
            y = i
            s.hist_each do |x1,x2,z|
              f.printf "%g %g 0\n", x1,y
              f.printf "%g %g 0\n", x2,y
              f.printf "%g %g 0\n", x2,y
            end
            f.puts ""
            s.hist_each do |x1,x2,z|
              f.printf "%g %g %g\n", x1,y,z
              f.printf "%g %g %g\n", x2,y,z
              f.printf "%g %g 0\n", x2,y,z
            end
            f.puts ""
            y = i+1
            s.hist_each do |x1,x2,z|
              f.printf "%g %g %g\n", x1,y,z
              f.printf "%g %g %g\n", x2,y,z
              f.printf "%g %g 0\n", x2,y,z
            end
            f.puts ""
            s.hist_each do |x1,x2,z|
              f.printf "%g %g 0\n", x1,y
              f.printf "%g %g 0\n", x2,y
              f.printf "%g %g 0\n", x2,y
            end
            f.puts "e"
            i = i+1
          end
        end
        puts "Histogram plot: #{@images[cmd]}"
      end
    end

  end
end

