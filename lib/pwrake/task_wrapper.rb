module Pwrake

  class TaskWrapper

    @@current_id = 1

    def initialize(task,task_args=nil)
      @task = task
      @name = task.name
      @task_args = task_args
      #@arg_data = task_args
      @task_id = @@current_id
      @@current_id += 1
      @location = []
      @group = []
      @group_id
      @suggest_location = []
      @file_stat
      @input_file_size
      @input_file_mtime
      @rank
      @priority
      @lock_rank = Monitor.new
      @executed = false
      @n_used_cores = 1
    end

    #@task.prerequisites
    #@task.subsequents
    #@task.actions

    attr_reader :name, :task_id, :group, :group_id, :file_stat
    attr_reader :location
    attr_accessor :executed, :n_used_cores

    def self.format_time(t)
      t.strftime("%F %T.%L")
    end

    def actions
      @task.actions
    end

    def preprocess
      if @shell = Pwrake::Shell.current
        @shell.current_task = self
      end
      @time_start = Time.now
    end

    def postprocess
      @executed = true if !@task.actions.empty?
      if @task.kind_of?(Rake::FileTask)
        t = Time.now
        #Rake.application.postprocess(@task)
        if File.exist?(@name)
          @file_stat = File::Stat.new(@name)
        end
      end
      log_task
      @shell.current_task = nil if @shell
      @task.pw_enq_subsequents
    end

    def log_task
      @time_end = Time.now
      #
      loc = suggest_location()
      shell = Pwrake::Shell.current
      #
      if loc && !loc.empty? && shell && !@task.actions.empty?
        Rake.application.count( loc, shell.host )
      end
      return if !Rake.application.task_logger
      #
      elap = @time_end - @time_start
      if !@task.actions.empty? && @task.kind_of?(Rake::FileTask)
        #RANK_STAT.add_sample(rank,elap)
      end
      #
      row = [ @task_id, @name, @time_start, @time_end, elap, @task.prerequisites.join('|') ]
      #
      if loc
        row << loc.join('|')
      else
        row << ''
      end
      #
      if shell
        row.concat [shell.host, shell.id]
      else
        row.concat ['','']
      end
      #
      row << ((@task.actions.empty?) ? 0 : 1)
      row << ((@executed) ? 1 : 0)
      #
      if @file_stat
        row.concat [@file_stat.size, @file_stat.mtime, self.location.join('|')]
      else
        row.concat ['','','']
      end
      #
      s = row.map do |x|
        if x.kind_of?(Time)
          TaskWrapper.format_time(x)
        elsif x.kind_of?(String) && x!=''
          '"'+x+'"'
        else
          x.to_s
        end
      end.join(',')
      #
      # task_id task_name start_time end_time elap_time preq preq_host
      # exec_host shell_id has_action executed file_size file_mtime file_host
      Rake.application.task_logger.print s+"\n"
    end

    def has_input_file?
      @task.kind_of?(Rake::FileTask) && !@task.prerequisites.empty?
    end

    def location=(a)
      @location = a
      @group = []
      @location.each do |host|
        @group |= [Rake.application.host_list.host2group[host]]
      end
    end

    def suggest_location=(a)
      @suggest_location = a
    end

    def suggest_location
      if has_input_file? && @suggest_location.nil?
        @suggest_location = []
        loc_fsz = Hash.new(0)
        @task.prerequisites.each do |preq|
          t = Rake.application[preq].wrapper
          loc = t.location
          fsz = t.file_size
          if loc && fsz > 0
            loc.each do |h|
              loc_fsz[h] += fsz
            end
          end
        end
        if !loc_fsz.empty?
          half_max_fsz = loc_fsz.values.max / 2
          #Log.debug "--- loc_fsz=#{loc_fsz.inspect} half_max_fsz=#{half_max_fsz}"
          loc_fsz.each do |h,sz|
            if sz > half_max_fsz
              @suggest_location << h
            end
          end
        end
      end
      @suggest_location
    end

    def rank
      @lock_rank.synchronize do
        if @rank.nil?
          if @task.subsequents.nil? || @task.subsequents.empty?
            @rank = 0
          else
            max_rank = 0
            @task.subsequents.each do |subsq|
              r = subsq.rank
              if max_rank < r
                max_rank = r
              end
            end
            if @task.actions.empty? || !@task.kind_of?(Rake::FileTask)
              step = 0
            else
              step = 1
            end
            @rank = max_rank + step
          end
          #Log.debug "--- Task[#{@name}] rank=#{@rank.inspect}"
        end
      end
      @rank
    end

    def file_size
      @file_stat ? @file_stat.size : 0
    end

    def file_mtime
      @file_stat ? @file_stat.mtime : Time.at(0)
    end

    def input_file_size
      unless @input_file_size
        @input_file_size = 0
        @task.prerequisites.each do |preq|
          @input_file_size += Rake.application[preq].wrapper.file_size
        end
      end
      @input_file_size
    end

    def input_file_mtime
      if has_input_file? && @input_file_mtime.nil?
        hash = Hash.new
        max_sz = 0
        @task.prerequisites.each do |preq|
          t = Rake.application[preq].wrapper
          sz = t.file_size
          if sz > 0
            hash[t] = sz
            if sz > max_sz
              max_sz = sz
            end
          end
        end
        half_max_sz = max_sz / 2
        hash.each do |t,sz|
          if sz > half_max_sz
            time = t.file_mtime
            if @input_file_mtime.nil? || @input_file_mtime < time
              @input_file_mtime = time
            end
          end
        end
      end
      @input_file_mtime
    end

    def priority
      if has_input_file? && @priority.nil?
        sum_tm = 0
        sum_sz = 0
        @task.prerequisites.each do |preq|
          pq = Rake.application[preq].wrapper
          sz = pq.file_size
          if sz > 0
            tm = pq.file_mtime - START_TIME
            sum_tm += tm * sz
            sum_sz += sz
          end
        end
        if sum_sz > 0
          @priority = sum_tm / sum_sz
        else
          @priority = 0
        end
        #Log.debug "--- task_name=#{@name} priority=#{@priority} sum_file_size=#{sum_sz}"
      end
      @priority || 0
    end

  end
end
