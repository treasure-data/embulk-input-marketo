require "embulk/input/marketo_api/soap/base"
require "embulk/input/marketo_api/soap/lead"
require "embulk/input/marketo_api/soap/activity_log"

module Embulk
  module Input
    module MarketoApi
      def self.soap_client(config, target)
        arguments = [config[:endpoint_url], config[:wsdl_url], config[:user_id], config[:encryption_key]]

        case target
        when :activity_log
          MarketoApi::Soap::ActivityLog.new(*arguments)
        else
          MarketoApi::Soap::Lead.new(*arguments)
        end
      end
    end
  end
end
