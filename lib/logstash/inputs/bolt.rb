require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/storm"

require "pathname"
require "socket" # for Socket.gethostname

require "addressable/uri"

# Input portion of a Storm Bolt.
#
# This takes no configuration because it will be called directly from a Shell Bolt
# instance within the storm cluster.
#
class LogStash::Inputs::Bolt < LogStash::Inputs::Base
  include Storm::Protocol
  config_name "bolt"
  plugin_status "beta"

  public
  def initialize(params)
    super
  end

  public
  def register
    LogStash::Util::set_thread_name("inputBolt")
    @logger.info("Registered the input bolt")
    @format = "json"
    Storm::Protocol.mode = 'bolt'
  end # def register
  
  def run(queue)
    prepare(*handshake)
    begin
      while true
        t = Tuple.from_hash(read_command)
        #tuple will contain a single value, a JSON object, to_event will convert it properly
        e = to_event(t.values[0], "storm")
        if e
          queue << e
        end
      end
    rescue Exception => e
      @logger.error('Exception in bolt: ' + e.message + ' - ' + e.backtrace.join('\n'))
    end
    finished
  end # def run

  public
  def teardown
    @tail.quit
  end # def teardown
  
end # class LogStash::Inputs::File
