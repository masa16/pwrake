module Pwrake

  class WorkerChannel

    @@wk_id = 0

    def initialize(io,host,ncore)
      @io = io
      @host = host
      @ncore = ncore
      @id = @@wk_id
      @@wk_id = @@wk_id.succ
      @tasks = []
    end

    attr_reader :io, :host, :id, :tasks

    def send_worker
      x = "#{@id}:#{@host} #{@ncore}\n"
      @io.print(x)
      # @io.write_nonblock(x)
      @io.flush
    end

    def add_task(t)
      @tasks.push(t)
    end

    def send_tasks
      @tasks.each do |t|
        x = "#{@id}:#{t}\n"
        @io.print(x)
      end
      @io.flush
      @tasks.clear
    end
  end

end
