require "embulk/input/marketo_api"

module Embulk
  module Input
    module Marketo
      class Base < InputPlugin
        def self.soap_client(config)
          @soap ||=
            begin
              endpoint_url = config.param(:endpoint, :string),
              soap_config = {
                endpoint_url: endpoint_url,
                wsdl_url: config.param(:wsdl, :string, default: "#{endpoint_url}?WSDL"),
                user_id: config.param(:user_id, :string),
                encryption_key: config.param(:encryption_key, :string),
              }

              MarketoApi.soap_client(soap_config)
            end
        end
      end
    end
  end
end
