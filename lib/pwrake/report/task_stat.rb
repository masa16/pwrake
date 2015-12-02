module Pwrake

  class TaskStat

    def initialize(task_file, sh_table)
      begin
        @task_table = CSV.read(task_file,:headers=>true,:skip_lines=>/\A#/)
      rescue
        $stderr.puts "error in reading "+task_file
        $stderr.puts $!, $@
        exit
      end
      shell_id = {}
      @task_table.each do |row|
        if id=row['shell_id']
          shell_id[id.to_i] = true
        end
      end
      @ncore = shell_id.size
      @count = Hash.new(0)
      task_locality
      stat_sh_table(sh_table)
    end

    attr_reader :exec_hosts, :ncore

    def count(exec_host, loc, key, val)
      @count[[exec_host,loc,key]] += val
      @count[[loc,key]] += val
    end

    def total(loc,key)
      @count[[loc,key]]
    end

    def [](exec_host,loc,key)
      @count[[exec_host,loc,key]]
    end

    def task_locality
      file_size = {}
      file_host = {}
      h = {}
      @task_table.each do |row|
        name            = row['task_name']
        file_size[name] = row['file_size'].to_i
        file_host[name] = (row['file_host']||'').split('|')
        exec_host = row['exec_host'] || ""
        h[exec_host] = true
      end
      @exec_hosts = h.keys.sort

      @task_table.each do |row|
        if row['executed']=='1'
          name      = row['task_name']
          exec_host = row['exec_host']
          loc = file_host[name].include?(exec_host)
          count(exec_host, loc, :out_num, 1)
          count(exec_host, loc, :out_size, file_size[name])

          preq_files = (row['preq']||'').split('|')
          preq_files.each do |preq|
            sz = file_size[preq]
            if sz && sz > 0
              loc = file_host[preq].include?(exec_host)
              count(exec_host, loc, :in_num, 1)
              count(exec_host, loc, :in_size, sz)
            end
          end
        end
      end
    end

    def stat_sh_table(sh_table)
      sh_table.each do |row|
        if (h = row['host']) && (t = row['elap_time'])
          count(h, nil, :elap, t.to_f)
        end
      end
    end

  end
end
