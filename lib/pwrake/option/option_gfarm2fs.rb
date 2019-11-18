require "pwrake/queue/locality_aware_queue"
require "pwrake/gfarm/gfarm_path"
require "pwrake/gfarm/gfarm_postprocess"

module Pwrake

  module GfarmFileSystemOption

    def option_data_filesystem
      [
        'GFARM2FS_COMMAND',
        'GFARM2FS_OPTION',
        'GFARM2FS_DEBUG',
        ['GFARM2FS_DEBUG_WAIT', proc{|v| v ? v.to_i : 1}],
        ['DISABLE_AFFINITY', proc{|v| v || ENV['AFFINITY']=='off'}],
        ['DISABLE_STEAL', proc{|v| v || ENV['STEAL']=='off'}],
        ['GFARM_BASEDIR', proc{|v| v || '/tmp'}],
        ['GFARM_PREFIX', proc{|v| v || "pwrake_#{ENV['USER']}"}],
        ['GFARM_SUBDIR', proc{|v| v || '/'}],
        ['MAX_GFWHERE_WORKER', proc{|v| (v || 8).to_i}],
      ]
    end

    def set_filesystem_option
      GfarmPath.subdir = self['GFARM_SUBDIR']
      @worker_option = {
        :log_dir   => self['LOG_DIR'],
        :pass_env  => self['PASS_ENV'],
        :gnu_time  => self['GNU_TIME'],
        :ssh_option => self['SSH_OPTION'],
        :heartbeat => self['HEARTBEAT'],
        #
        :shared_directory => "GfarmDirectory",
        :base_dir => self['GFARM_BASEDIR']+"/"+self['GFARM_PREFIX'],
        :work_dir => GfarmPath.pwd.to_s,
        :gfarm2fs_command => self['GFARM2FS_COMMAND'],
        :gfarm2fs_option => self['GFARM2FS_OPTION'],
        :gfarm2fs_debug => self['GFARM2FS_DEBUG'],
        :gfarm2fs_debug_wait => self['GFARM2FS_DEBUG_WAIT'],
        :single_mp => self['GFARM_SINGLE_MP']
      }
      @worker_progs = %w[
        pwrake/nbio
        pwrake/branch/fiber_queue
        pwrake/worker/writer
        pwrake/worker/log_executor
        pwrake/worker/executor
        pwrake/worker/invoker
        pwrake/worker/shared_directory
        pwrake/worker/gfarm_directory
        pwrake/worker/worker_main
      ]
      if self['DISABLE_AFFINITY']
        @queue_class = "NonLocalityQueue"
      else
        @queue_class = "LocalityAwareQueue"
      end
    end

    def max_postprocess_pool
      self['MAX_GFWHERE_WORKER']
    end

    def postprocess(runner)
      GfarmPostprocess.new(runner)
    end

    def clear_gfarm2fs
      setup_hosts
      d = File.join(self['GFARM_BASEDIR'],self['GFARM_PREFIX'])
      rcmd = "
for i in #{d}*; do
  if [ -d \"$i\" ]; then
    case \"$i\" in
      *_000) ;;
      *) fusermount -u $i; rmdir $i ;;
    esac
  fi
done
sleep 1
for i in #{d}*_000; do
  if [ -d \"$i\" ]; then
    fusermount -u $i; rmdir $i
  fi
done
"
      threads = []
      @host_map.each do |k,hosts|
        hosts.each do |info|
          threads << Thread.new do
            system "ssh #{info.name} '#{rcmd}'"
          end
        end
      end
      threads.each{|t| t.join}
    end
  end

  class Option
    include GfarmFileSystemOption
  end
end
