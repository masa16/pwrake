items="1000 5000 10000 50000 100000 200000 400000"

tee="tee -a measure-rake.log"

for i in $items; do
    if [ ! -d "src$i" ]; then
	echo "src$i not found"
	ruby mkdir.rb "$i"
    fi

    echo ---------------------------------------- | $tee
    cmd="./rake N=$i"
    echo $cmd
    /usr/bin/time $cmd 2>&1 | $tee
    echo ---------------------------------------- | $tee
  done
done
