def measure
  hosts =
    case ENV['HOSTFILE']
    when /tkb1/; ['tsukuba000']
    when /tkb2/; ['tsukuba000','tsukuba001']
    when /tkb4/; ['tsukuba000','tsukuba001','tsukuba002','tsukuba003']
    when /tkb8/; ['tsukuba000','tsukuba001','tsukuba002','tsukuba003','tsukuba004','tsukuba005','tsukuba006','tsukuba007']
    else; ['localhost']
    end

  p hosts

  if File.basename($0)=='pwrake' and hosts
    mem_main = nil
    mem_brch = 0
    count = 0

    hosts.each do |h|
      #l = `top -b -n1`
      l = `ssh #{h} ps vxw`
      a = l.split("\n")
      while x = a.shift
        #puts x
        break if /\s*PID/=~x
      end

      a.each do |s|
        if /bin\/pwrake / =~ s and /ssh/ !~ s
          puts "#{h}: #{s}"
          b = s.split
          mem_main = b[6].to_i
        end

        if /bin\/pwrake_branch/ =~ s and /ssh/ !~ s
          puts "#{h}: #{s}"
          b = s.split
          mem_brch += b[6].to_i
          count += 1
        end
      end
    end

    puts "- main   #{N} #{mem_main/1000.0}"
    puts "- branch #{N} #{mem_brch/1000.0/count} #{count}"
  end
end
