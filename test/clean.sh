kill -KILL `ps auxw|grep worker.rb|awk '{print $2}'`
kill -KILL `ps auxw|grep pwrake_branch|awk '{print $2}'`
