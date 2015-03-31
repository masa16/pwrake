module Pwrake

  class WorkerChannel

    @@current_id = 0

    def initialize(io,host,ncore)
      @io = io
      @host = host
      @ncore = ncore # || 1
      @id = @@current_id
      @@current_id = @@current_id.succ
      #x = "#{@id}:#{@host} #{@ncore}\n"
      #@io.print(x)
      #@io.flush
      send_cmd "#{@id}:#{@host} #{@ncore}"
    end

    attr_reader :io, :host, :id
    attr_accessor :ncore

    def send_cmd(x)
      @io.print(x+"\n")
      @io.flush
    end

    def send_task(t)
      send_cmd("#{@id}:#{t.name}")
    end
  end

end
