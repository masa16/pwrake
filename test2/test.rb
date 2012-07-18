system "ps v"
N=1000000
case ARGV[0]
when "0"
  h={}
  N.times{|i| x=i.to_s; h[x]=i}
when "1"
  a=[]
  N.times{|i| x=i.to_s; a[i]=x}
else
  raise "no arg"
end
system "ps v"
