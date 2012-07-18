require "fileutils"
n=ARGV[0].to_i
d="src#{n}"
FileUtils.mkdir_p d
(1..n).each do |i| File.open(d+"/%010d.c"%i,"w").close end
