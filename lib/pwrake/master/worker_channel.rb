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
    end

    attr_reader :io, :host, :id
    attr_accessor :ncore

    def send_cmd(x)
      @io.print(x+"\n")
      @io.flush
    end
  end

end
