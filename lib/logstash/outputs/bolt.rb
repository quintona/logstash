require "logstash/namespace"
require "logstash/outputs/base"
require "logstash/storm"

# Storm Bolt output.
#
# Emits a tuple after the filtering has occured.
# 
class LogStash::Outputs::File < LogStash::Outputs::Base
  include Storm::Protocol
  config_name "bolt"
  plugin_status "beta"

  public
  def register
    Storm::Protocol.mode = 'bolt'
  end # def register

  public
  def receive(event)
    return unless output?(event)
    #emit the JSON object straight up
    emit(event)
    
  end # def receive

  
end # class LogStash::Outputs::File
