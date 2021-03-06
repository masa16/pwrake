require "csv"

module Pwrake

  class Report

    HTML_HEAD = <<EOL
<html><head><style>
<!--
h2 {
  background-color:#eee;
}
h1 {
  background-color:#0ff;
}
table {
 margin:0px;
 border-style:solid;
 border-width:1px;
}
td {
  margin:0px;
  border-style:solid;
  border-width:1px;
}
-->
</style>
</head>
<body>
EOL

    @@id = 0
    @@id_fmt = nil

    def initialize(option,pattern)
      @dir = option['REPORT_DIR']
      if !File.directory?(@dir)
        raise ArgumentError,"Could not find log directory: #{@dir}"
      end
      @pattern = pattern

      @@id = @@id.succ
      @id = @@id
      @base = @dir

      @img_fmt = option['REPORT_IMAGE'] || 'png'

      @cmd_file = File.join(@dir,option['COMMAND_CSV_FILE'])
      @task_file = File.join(@dir,option['TASK_CSV_FILE'])
      @html_file = File.join(@dir,'report.html')

      begin
        @sh_table = CSV.read(@cmd_file,:headers=>true,:skip_lines=>/\A#/)
      rescue
        $stderr.puts "error in reading "+@cmd_file
        $stderr.puts $!, $@
        exit
      end

      h = {}
      @elap_sum = 0
      @elap_core_sum = 0
      @start_time = @end_time = Time.parse(@sh_table[0]["start_time"])
      @sh_table.each do |row|
        if host = row['host']
          h[host] = true
        end
        t = row['elap_time'].to_f
        if t>0
          @elap_sum += t
          @elap_core_sum += t * Rational(row['ncore']||0)
        end
        t_start = Time.parse(row["start_time"])
        if @start_time > t_start
          @start_time = t_start
        end
        t_end = Time.parse(row["end_time"])
        if @end_time < t_end
          @end_time = t_end
        end
      end
      @hosts = h.keys.sort
      @elap = @end_time - @start_time
      read_elap_each_cmd
      make_cmd_stat

      @stat = TaskStat.new(@task_file,@sh_table)
      @ncore = @stat.ncore
    end

    attr_reader :base, :ncore, :elap
    attr_reader :cmd_file, :html_file
    attr_reader :cmd_elap, :cmd_stat
    attr_reader :sh_table, :task_table
    attr_reader :id

    def find_single_file(pattern)
      g = Dir.glob(File.join(@dir,pattern))
      case g.size
      when 0
        raise ArgumentError, "Could not find any file with '#{pattern}' in #{@dir}"
      when 1
      else
        raise ArgumentError, "Found multiple files '#{pattern}' in #{@dir}"
      end
      g[0]
    end

    def id_str
      if @@id_fmt.nil?
        id_len = Math.log10(@@id).floor + 1
        @@id_fmt = "#%0#{id_len}d"
      end
      @@id_fmt % @id
    end

    def read_elap_each_cmd
      @cmd_elap = {}
      @sh_table.each do |row|
        command = row['command']
        elap = row['elap_time']
        if command && elap
          elap = elap.to_f
          found = nil
          @pattern.each do |cmd,regex|
            if regex =~ command
              if a = @cmd_elap[cmd]
                a << elap
              else
                @cmd_elap[cmd] = [elap]
              end
              found = true
            end
          end
          if !found
            if cmd = get_command( command )
              if a = @cmd_elap[cmd]
                a << elap
              else
                @cmd_elap[cmd] = [elap]
              end
            end
          end
        end
      end
      @cmd_elap
    end

    def get_command(s)
      case s
      when /^\s*\((.*)$/
        get_command($1)
      when /^\s*\w+=\S+\s+(.*)$/
        get_command($1)
      when /^\s*([\w.,~^\/=+-]+)(.*)$/
        cmd, rest = $1, $2
        case cmd
        when "cd"
          if /[^;]*;(.*)$/ =~ rest
            return get_command($1)
          end
        when /^ruby[\d.]*$/
          case rest
          when /([\w.,~^\/=+-]+\.rb)\b/
            return "#{cmd} #{$1}"
          when /\s+-e\s+("[^"]*"|'[^']*'|\S+)/
            return "#{cmd} -e #{$1}"
          end
        when /^python[\d.]*$/
          case rest
          when /([\w.,~^\/=+-]+\.py)\b/
            return "#{cmd} #{$1}"
          when /\s+-c\s+("[^"]*"|'[^']*'|\S+)/
            return "python -c #{$1}"
          end
        when /^perl[\d.]*$/
          case rest
          when /([\w.,~^\/=+-]+\.pl)\b/
            return "#{cmd} #{$1}"
          when /\s+-e\s+("[^"]*"|'[^']*'|\S+)/
            return "#{cmd} -e #{$1}"
          end
        end
        cmd
      else
        s[0..15]
      end
    end

    def make_cmd_stat
      @cmd_stat = {}
      @cmd_elap.each do |cmd,elap|
        @cmd_stat[cmd] = s = Stat.new(elap)
        if elap.size > 1
          s.make_logx_histogram(1.0/8)
        end
      end
    end

    def format_comma(x)
      x.to_s.gsub(/(?<=\d)(?=(?:\d\d\d)+(?!\d))/, ',')
    end

    def tr_count(x,y)
      sum = x+y
      if sum==0
        xp = "--%"
        yp = "--%"
      else
        xp = "%.2f%%"%(x*100.0/sum)
        yp = "%.2f%%"%(y*100.0/sum)
      end
      td = "<td align='right' valign='top'>"
      return \
        td + '%s<br/>%s</td>' % [format_comma(x),xp] +
        td + '%s<br/>%s</td>' % [format_comma(y),yp] +
        td + "%s</td>" % format_comma(sum)
    end

    def report_html
      html = HTML_HEAD + "<body><h1>Pwrake Statistics</h1>\n"
      html << "<h2>Workflow</h2>\n"
      html << "<table>\n"
      html << "<tr><th>log directory</th><td>#{@base}</td><tr>\n"
      html << "<tr><th>ncore</th><td>#{@ncore}</td><tr>\n"
      html << "<tr><th>elapsed time</th><td>%.3f sec</td><tr>\n"%[@elap]
      html << "<tr><th>accumulated process time</th><td>%.3f sec</td><tr>\n"%[@elap_sum]
      html << "<tr><th>occupancy</th><td>%.3f %%</td><tr>\n"%[@elap_core_sum/@elap/@ncore*100]
      html << "<tr><th>start time</th><td>#{@start_time}</td><tr>\n"
      html << "<tr><th>end time</th><td>#{@end_time}</td><tr>\n"
      html << "</table><br/>\n"
      html << "<table>\n"
      html << "<tr><th colspan=5>#{@hosts.size} hosts &times; #{@ncore.fdiv @hosts.size} cores/host</th><tr>\n"
      @hosts.each_slice(5) do |a|
        html << "<tr>"+a.map{|h|"<td>#{h}</td>"}.join("")+"<tr>\n"
      end
      html << "</table>\n"
      html << "<h2>Parallelism</h2>\n"
      fimg = Parallelism.plot_parallelism(@sh_table,@base,@img_fmt)
      html << "<img src='./#{File.basename(fimg)}' align='top'/></br>\n"

      html << "<h2>Parallelism by command</h2>\n"
      fimg3 = Parallelism.plot_parallelism_by_pattern(@sh_table,@base,@pattern,@img_fmt)
      html << "<img src='./#{File.basename(fimg3)}' align='top'/></br>\n"

      html << "<h2>Parallelism by host</h2>\n"
      fimg2 = Parallelism.plot_parallelism_by_host(@sh_table,@base,@img_fmt,@start_time,@end_time)
      html << "<img src='./#{File.basename(fimg2)}' align='top'/></br>\n"

      html << "<h2>Command time statistics</h2>\n"
      html << "<table>\n"
      html << Stat.html_th
      @cmd_stat.each do |cmd,stat|
        html << "<tr><td>#{cmd}</td>"
        html << stat.html_td
        html << "</tr>\n"
      end
      html << "<table>\n"
      html << "<img src='./#{File.basename(histogram_plot)}' align='top'/></br>\n"

      html << "<h2>Locality statistics</h2>\n"
      html << "<table>\n"

      html << "<tr><th></th><th rowspan=3>gross elapsed time (sec)</th><th></th>"
      html << "<th colspan=6>read</th>"
      html << "<th></th>"
      html << "<th colspan=6>write</th>"
      html << "</tr>\n"


      html << "<tr><th></th><th></th>"
      html << "<th colspan=3>count</th><th colspan=3>file size (bytes)</th>"
      html << "<th></th>"
      html << "<th colspan=3>count</th><th colspan=3>file size (bytes)</th>"
      html << "</tr>\n"

      html << "<tr><th>host</th><th></th>"
      html << "<th>local</th><th>remote</th><th>total</th>"
      html << "<th>local</th><th>remote</th><th>total</th>"
      html << "<th></th>"
      html << "<th>local</th><th>remote</th><th>total</th>"
      html << "<th>local</th><th>remote</th><th>total</th>"
      html << "</tr>\n"
      @stat.exec_hosts.each do |h|
        if h.to_s!="" || @stat[h,nil,:elap]!=0
          html << "<tr><td>#{h}</td>"
          html << "<td align='right'>%.3f</td>" % @stat[h,nil,:elap]
          html << "<td></td>"
          html << tr_count(@stat[h,true,:in_num],@stat[h,false,:in_num])
          html << tr_count(@stat[h,true,:in_size],@stat[h,false,:in_size])
          html << "<td></td>"
          html << tr_count(@stat[h,true,:out_num],@stat[h,false,:out_num])
          html << tr_count(@stat[h,true,:out_size],@stat[h,false,:out_size])
          html << "</tr>\n"
        end
      end
      html << "<tr><td>total</td>"
      html << "<td align='right'>%.3f</td>" % @stat.total(nil,:elap)
      html << "<td></td>"
      html << tr_count(@stat.total(true,:in_num),@stat.total(false,:in_num))
      html << tr_count(@stat.total(true,:in_size),@stat.total(false,:in_size))
      html << "<td></td>"
      html << tr_count(@stat.total(true,:out_num),@stat.total(false,:out_num))
      html << tr_count(@stat.total(true,:out_size),@stat.total(false,:out_size))

      html << "</tr>\n"
      html << "<table>\n"

      html << "</body></html>\n"
      File.open(@html_file,"w") do |f|
        f.puts html
      end
      #puts "generate "+@html_file

      printf "%s,%d,%d,%d,%d\n",@html_file, @stat.total(true,:in_num),@stat.total(false,:in_num),@stat.total(true,:in_size),@stat.total(false,:in_size)
    end


    def histogram_plot
      command_list = []
      @cmd_stat.each do |cmd,stat|
        if stat.n > 2
          command_list << cmd
        end
      end
      hist_image = @base+"/hist."+@img_fmt
      if system("which gnuplot >/dev/null 2>&1")
      IO.popen("gnuplot","r+") do |f|
        f.puts "
set terminal #{@img_fmt} # size 480,360
set output '#{hist_image}'
set ylabel 'histogram'
set xlabel 'Execution time (sec)'
set logscale x
set title 'histogram of elapsed time'"
        a = []

        command_list.each_with_index do |cmd,i|
          a << "'-' w histeps ls #{i+1} title ''"
          a << "'-' w lines ls #{i+1} title #{cmd.inspect}"
        end
        f.puts "plot "+ a.join(',')

        command_list.each do |cmd|
          stat = @cmd_stat[cmd]
          2.times do
            stat.hist_each do |x1,x2,y|
              x = Math.sqrt(x1*x2)
              f.printf "%f %d\n", x, y
            end
            f.puts "e"
          end
        end
      end
      end
      hist_image
    end

  end
end
