require "forwardable"
require "csv"
require "pwrake/task/task_rank"

module Pwrake

  class TaskWrapper
    extend Forwardable

    @@current_id = 1
    @@task_logger = nil

    def initialize(task,task_args=nil)
      @task = task
      @task_args = task_args
      @property = task.property
      @task_id = @@current_id
      @@current_id += 1
      @location = []
      @group = []
      @group_id = nil
      @suggest_location = nil
      @file_stat = nil
      @input_file_size = nil
      @input_file_mtime = nil
      @rank = nil
      @priority = nil
      @lock_rank = Monitor.new
      @executed = false
      @assigned = []
      @exec_host = nil
      @nretry = @property.retry || Rake.application.pwrake_options["RETRY"] || 1
    end

    def_delegators :@task, :name, :actions, :prerequisites, :subsequents
    def_delegators :@property, :acceptable_for

    attr_reader :task, :task_id, :group, :group_id, :file_stat
    attr_reader :location
    attr_reader :assigned
    attr_accessor :executed
    attr_accessor :exec_host
    attr_accessor :shell_id, :status

    def self.format_time(t)
      t.strftime("%F %T.%L")
    end

    def self.init_task_logger(option)
      if dir = option['LOG_DIR']
        fn = File.join(dir,option['TASK_CSV_FILE'])
        @@task_logger = CSV.open(fn,'w')
        @@task_logger.puts %w[
          task_id task_name start_time end_time elap_time preq preq_host
          exec_host shell_id has_action executed file_size file_mtime file_host
        ]
      end
    end

    def self.close_task_logger
      @@task_logger.close if @@task_logger
    end

    def preprocess
      if @shell = Pwrake::Shell.current
        @shell.current_task = self
      end
      @time_start = Time.now
    end

    def retry?
      @status != "end" && @nretry > 0
    end

    def no_more_retry
      @nretry == 0
    end

    def postprocess(location)
      @executed = true if !@task.actions.empty?
      #tm_taskend = Time.now
      if is_file_task?
        #t = Time.now
        if File.exist?(name)
          @file_stat = File::Stat.new(name)
          @location = location
        end
      end
      #Log.debug "postprocess time=#{Time.now-tm_taskend}"
      log_task
      @shell.current_task = nil if @shell
    end

    def retry_or_subsequent
      if @status=="end"
        @task.pw_enq_subsequents
      elsif @nretry > 0
        s="retry task n=#{@nretry}: #{name}"
        Log.warn(s)
        $stderr.puts(s)
        @nretry -= 1
        Rake.application.task_queue.enq(self)
      else
        s="retry task n=0 (no retry): #{name}"
        Log.error(s)
        $stderr.puts(s)
      end
    end

    def log_task
      @time_end = Time.now
      #
      loc = suggest_location()
      shell = Pwrake::Shell.current
      #
      if loc && !loc.empty? && shell && !actions.empty?
        Rake.application.count( loc, shell.host )
      end
      return if !@@task_logger
      #
      elap = @time_end - @time_start
      if has_output_file?
        RANK_STAT.add_sample(rank,elap)
      end
      #
      if @file_stat
        fstat = [@file_stat.size, @file_stat.mtime, self.location.join('|')]
      else
        fstat = [nil]*3
      end
      #
      # task_id task_name start_time end_time elap_time preq preq_host
      # exec_host shell_id has_action executed file_size file_mtime file_host
      #
      row = [ @task_id, name, @time_start, @time_end, elap,
              prerequisites, loc, @exec_host, @shell_id,
              (actions.empty?) ? 0 : 1,
              (@executed) ? 1 : 0,
              *fstat ]
      row.map!{|x|
        if x.kind_of?(Time)
          TaskWrapper.format_time(x)
        elsif x.kind_of?(Array)
          if x.empty?
            nil
          else
            x.join('|')
          end
        else
          x
        end
      }
      @@task_logger << row
      #
      clsname = @task.class.to_s.sub(/^(Rake|Pwrake)::/o,"")
      msg = '%s:"%s" %s: id=%d elap=%.6f exec_host=%s' %
        [clsname,name,@status,@task_id,elap,@exec_host]
      if @status=="end"
        Log.info msg
      else
        Log.error msg
      end
    end

    def is_file_task?
      @task.kind_of?(Rake::FileTask)
    end

    def has_output_file?
      is_file_task? && !actions.empty?
    end

    def has_input_file?
      is_file_task? && !prerequisites.empty?
    end

    def has_action?
      !@task.actions.empty?
    end

    def location=(a)
      @location = a
      @group = []
      #@location.each do |host|
      #  @group |= [Rake.application.host_list.host2group[host]]
      #end
    end

    def suggest_location=(a)
      @suggest_location = a
    end

    def suggest_location
      if has_input_file? && @suggest_location.nil?
        @suggest_location = []
        loc_fsz = Hash.new(0)
        prerequisites.each do |preq|
          t = Rake.application[preq].wrapper
          loc = t.location
          fsz = t.file_size
          if loc && fsz > 0
            loc.each do |h|
              loc_fsz[h] += fsz
            end
          end
        end
        #Log.debug "input=#{prerequisites.join('|')}"
        if !loc_fsz.empty?
          half_max_fsz = loc_fsz.values.max / 2
          Log.debug "loc_fsz=#{loc_fsz.inspect} half_max_fsz=#{half_max_fsz}"
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
      #@lock_rank.synchronize do
        if @rank.nil?
          if subsequents.nil? || subsequents.empty?
            @rank = 0
          else
            max_rank = 0
            subsequents.each do |subsq|
              r = subsq.wrapper.rank
              if max_rank < r
                max_rank = r
              end
            end
            if has_output_file?
              step = 1
            else
              step = 0
            end
            @rank = max_rank + step
          end
          Log.debug "Task[#{name}] rank=#{@rank.inspect}"
        end
      #end
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
        prerequisites.each do |preq|
          @input_file_size += Rake.application[preq].wrapper.file_size
        end
      end
      @input_file_size
    end

    def input_file_mtime
      if has_input_file? && @input_file_mtime.nil?
        hash = Hash.new
        max_sz = 0
        prerequisites.each do |preq|
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
        prerequisites.each do |preq|
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
        Log.debug "task_name=#{name} priority=#{@priority} sum_file_size=#{sum_sz}"
      end
      @priority || 0
    end

    def n_used_cores(host_info=nil)
      @n_used_cores ||= @property.n_used_cores(host_info)
    end

  end
end
