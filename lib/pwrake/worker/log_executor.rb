require "singleton"
require "forwardable"
require "logger"

module Pwrake

  DELEGATE_METHODS = [
    :debug, :info, :error, :fatal, :warn, :unknown,
    :debug?, :info?, :error?, :fatal?, :warn?, :unknown?,
    :level, :level=,
    :formatter, :formatter=,
    :datetime_format, :datetime_format=
  ]

  class DummyLogger
    DELEGATE_METHODS.each do |m|
      define_method(m){|*a|}
    end
  end

  class LogExecutor
    include Singleton
    extend Forwardable

    def_delegators :@logger, *DELEGATE_METHODS

    def initialize
      @level = ::Logger::DEBUG
      @logger = @logger_stderr = DummyLogger.new
      @logger.level = @level
    end

    attr_reader :path

    def init(option)
      @option = option
      @log_dir = @option[:log_dir]
    end

    def opened?
      @opened
    end

    def open(dir_class)
      if @log_dir
        @dir = dir_class.new
        @dir.open
        @path = @dir.log_path
        fn = "worker-#{`hostname`.chomp}-#{Process.pid}.log"
        @logfile = (@path + fn).to_s
        ::FileUtils.mkdir_p(@path.to_s)
        @logger = @logger_file = ::Logger.new(@logfile)
        @opened = true
        @logger.level = @level
        @dir.open_messages.each{|m| @logger.info(m)}
      end
    end

    def close
      if @log_dir
        @dir.close_messages.each{|m| @logger.info(m)}
        @logger = @logger_stderr
        @opened = false
        @logger_file.close
        @logger_file = nil
        @dir.close
      end
    end

    def join
    end

    def kill(sig)
    end

  end
end
