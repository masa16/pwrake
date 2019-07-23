require "pathname"

module Pwrake

  class SharedDirectory

    @@prefix = nil
    @@work_dir = nil
    @@log_dir = nil

    def self.init(opts)
      @@prefix   = opts[:base_dir]
      @@work_dir = opts[:work_dir]
      @@log_dir  = opts[:log_dir]
    end

    # instance methods

    def initialize
      @log = LogExecutor.instance
    end

    def home_path
      Pathname.new(ENV['HOME'])
    end

    def open
      @current_path = work_path
    end

    def open_messages
      ["enter workdir: #{work_path}"]
    end

    def close
    end

    def close_messages
      ["leave workdir: #{work_path}"]
    end

    def cd(d='')
      if d==''
        @current_path = home_path
      else
        pn = Pathname(d.sub(/^\$HOME\b/,ENV['HOME']))
        if pn.relative?
          pn = @current_path + pn
        end
        if !Dir.exist?(pn)
          raise "Cannot chdir to #{pn}"
        end
        @current_path = pn.realpath
      end
    end

    def current
      @current_path.to_s
    end

    def work_path
      home_path + @@work_dir
    end

    def work_dir
      work_path.to_s
    end

    def log_path
      work_path + @@log_dir
    end

    def check_mountpoint
    end

  end
end
