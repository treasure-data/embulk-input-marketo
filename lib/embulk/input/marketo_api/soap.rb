require "savon"

module Embulk
  module Input
    module MarketoApi
      class Soap
        attr_reader :endpoint, :wsdl, :user_id, :encryption_key

        def initialize(endpoint, wsdl, user_id, encryption_key)
          @endpoint = endpoint
          @wsdl = wsdl
          @user_id = user_id
          @encryption_key = encryption_key
        end

        def signature
          timestamp = Time.now.to_s
          encryption_string = timestamp + user_id
          digest = OpenSSL::Digest.new('sha1')
          hashed_signature = OpenSSL::HMAC.hexdigest(digest, encryption_key, encryption_string)
          {
            'requestTimestamp' => timestamp,
            'requestSignature' => hashed_signature.to_s
          }
        end

        def lead_metadata
          # http://developers.marketo.com/documentation/soap/describemobject/
          response = savon.call(:describe_m_object, message: {object_name: "LeadRecord"})
          response.body[:success_describe_m_object][:result][:metadata][:field_list][:field]
        end

        private

        def savon
          @savon ||=
            begin
              headers = {
                'ns1:AuthenticationHeader' => {
                  "mktowsUserId" => user_id,
                }.merge(signature)
              }
              Savon.client(
                wsdl: wsdl,
                soap_header: headers,
                endpoint: endpoint,
                open_timeout: 10,
                read_timeout: 300,
                namespace_identifier: :ns1,
                env_namespace: 'SOAP-ENV'
              )
            end
        end
      end
    end
  end
end
