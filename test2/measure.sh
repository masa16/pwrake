hosts="tsukuba000 tsukuba001 tsukuba002 tsukuba003 tsukuba004 tsukuba005 tsukuba006 tsukuba007"
items="1000 5000 10000 50000 100000 200000 400000"
#files="tkb1 tkb2 tkb4 tkb8"

#items="1000 5000"
#files="tkb1 tkb2"
files="tkb8"

for f in $files; do
  tee="tee -a measure.log.$f"
  for i in $items; do
    for h in $hosts; do ssh $h pkill -u tanakams ruby; done
    echo ---------------------------------------- | $tee
    if [ ! -d "src$i" ]; then
	echo "src$i not found"
	ruby mkdir.rb "$i"
    fi

    cmd="../bin/pwrake N=$i HOSTFILE=hosts.yaml.$f"
    echo $cmd
    /usr/bin/time $cmd 2>&1 | $tee
    echo ---------------------------------------- | $tee
  done
done
