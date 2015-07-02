require "embulk/input/marketo_api/soap"

module Embulk
  module Input
    module MarketoApi
      def self.soap_client(config)
        MarketoApi::Soap.new(config[:endpoint_url], config[:wsdl_url], config[:user_id], config[:encryption_key])
      end
    end
  end
end
