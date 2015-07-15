require "embulk/input/marketo_api"

module Embulk
  module Input
    class MarketoApiTest < Test::Unit::TestCase
      class SoapClientTest < self
        data do
          {
            lead: [:lead, MarketoApi::Soap::Lead],
            activity_log: [:activity_log, MarketoApi::Soap::ActivityLog],
          }
        end

        def test_valid_target(data)
          target, expected = data
          actual = MarketoApi.soap_client({}, target)
          assert_equal(expected, actual.class)
        end

        def test_unknown_target
          assert_raise do
            MarketoApi.soap_client({}, "unknown")
          end
        end
      end
    end
  end
end
