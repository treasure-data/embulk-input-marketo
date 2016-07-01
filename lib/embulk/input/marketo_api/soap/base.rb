require "savon"
require "httpclient" # net/http can't verify cert correctly
require "perfect_retry"

module Embulk
  module Input
    module MarketoApi
      module Soap
        class Base
          attr_reader :endpoint, :wsdl, :user_id, :encryption_key

          RETRY_TIMEOUT_COUNT = 5

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
              log: false,
              logger: Embulk.logger,
              wsdl: wsdl,
              soap_header: headers,
              endpoint: endpoint,
              open_timeout: 90,
              read_timeout: 90,
              raise_errors: true,
              namespace_identifier: :ns1,
              env_namespace: 'SOAP-ENV',
            )
          end

          def retryer(retry_options)
            PerfectRetry.new do |config|
              config.sleep = proc{|n| retry_options[:retry_initial_wait_sec] * (2 ** (n - 1))}
              config.limit = retry_options[:retry_limit]
              config.dont_rescues = [Embulk::ConfigError]
              config.rescues = [StandardError, Timeout::Error]
              config.logger = Embulk.logger
              config.log_level = nil
              config.raise_original_error = true
            end
          end

          def savon_call(operation, locals={}, retry_options={})
            retryer(retry_options).with_retry do
              catch_unretryable_error do
                savon.call(operation, locals.merge(advanced_typecasting: false))
              end
            end
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

          def catch_unretryable_error(&block)
            yield
          rescue Savon::SOAPFault => e
            Embulk.logger.debug "#{e.class}: #{e.to_hash}"
            if e.to_hash[:fault][:faultcode].to_str == "SOAP-ENV:Client"
              raise ConfigError.new e.message
            end
          rescue Savon::HTTPError => e
            # NOTE: Marketo API always return error as HTTP 500
            # ref. https://jira.talendforge.org/secure/attachmentzip/unzip/167201/49761%5B1%5D/Marketo%20Enterprise%20API%202%200.pdf
            Embulk.logger.debug "#{e.class}: #{e.http.body}"
            soap_code = e.http.body[%r|<code>(.*?)</code>|, 1]
            soap_message = e.http.body[%r|<message>(.*?)</message>|, 1]
            case soap_code
            when "10001", "20011"
              # Internal Error
              raise e
            when "20015"
              # Request Limit Exceeded
              raise e
            when "20024"
              # 20024 - Concurrent limit exceeded
              #
              # Undocumented in a SOAP document but REST document has it
              # http://developers.marketo.com/documentation/soap/error-codes/
              # http://developers.marketo.com/documentation/rest/error-codes/
              raise e
            else
              # unretryable error such as Authentication Failed, Invalid Request, etc.
              raise ConfigError.new soap_message
            end
          rescue SocketError, ::Java::JavaNet::UnknownHostException, Errno::ECONNREFUSED => e
            # maybe endpoint/wsdl domain was wrong
            Embulk.logger.debug "Connection error: endpoint=#{endpoint} wsdl=#{wsdl}"
            raise ConfigError.new "Connection error: #{e.message} (endpoint is '#{endpoint}')"
          end
        end
      end
    end
  end
end
