module Pwrake

  class WorkerChannel

    @@current_id = 0
    HOST2ID = {}

    def initialize(io,host,ncore)
      @io = io
      @host = host
      @ncore = ncore
      @id = @@current_id
      @@current_id = @@current_id.succ
      HOST2ID[@host] = @id
      send_cmd "#{@id}:#{@host} #{@ncore}"
    end

    attr_reader :io, :host, :id
    attr_accessor :ncore

    def send_cmd(x)
      Log.debug "send_cmd: #{x}"
      @io.print(x+"\n")
      @io.flush
    end

    def send_task(t)
      send_cmd("#{@id}:#{t.task_id}:#{t.name}")
    end
  end

end
