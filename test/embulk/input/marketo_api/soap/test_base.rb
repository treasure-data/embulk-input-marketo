require "embulk/input/marketo_api/soap/base"
require "lead_fixtures"

module Embulk
  module Input
    module MarketoApi
      module Soap
        class BaseTest < Test::Unit::TestCase
          include LeadFixtures

          class TestSignature < self
            def setup
              @signature = soap.__send__(:signature)
            end

            def test_sigature_keys
              assert_equal(%w(requestTimestamp requestSignature).sort, @signature.keys.sort)
            end

            def test_is_hash
              assert_equal(Hash, @signature.class)
            end
          end

          private

          def soap
            @soap ||= Base.new(settings[:endpoint], settings[:wsdl], settings[:user_id], settings[:encryption_key])
          end

          def settings
            {
              endpoint: "https://marketo.example.com",
              wsdl: "https://marketo.example.com/?wsdl",
              user_id: "user_id",
              encryption_key: "TOPSECRET",
            }
          end
        end
      end
    end
  end
end
