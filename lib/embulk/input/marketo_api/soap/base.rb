require "savon"

module Embulk
  module Input
    module MarketoApi
      module Soap
        class Base
          attr_reader :endpoint, :wsdl, :user_id, :encryption_key

          def initialize(endpoint, wsdl, user_id, encryption_key)
            @endpoint = endpoint
            @wsdl = wsdl
            @user_id = user_id
            @encryption_key = encryption_key
          end

          private

          def savon
            headers = {
              'ns1:AuthenticationHeader' => {
                "mktowsUserId" => user_id,
              }.merge(signature)
            }
            # NOTE: Do not memoize this to use always fresh signature (avoid 20016 error)
            # ref. https://jira.talendforge.org/secure/attachmentzip/unzip/167201/49761%5B1%5D/Marketo%20Enterprise%20API%202%200.pdf (41 page)
            Savon.client(
              log: true,
              logger: Embulk.logger,
              wsdl: wsdl,
              soap_header: headers,
              endpoint: endpoint,
              open_timeout: 90,
              read_timeout: 300,
              raise_errors: true,
              namespace_identifier: :ns1,
              env_namespace: 'SOAP-ENV'
            )
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
        end
      end
    end
  end
end
