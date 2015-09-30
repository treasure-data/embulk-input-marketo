require "prepare_embulk"
require "embulk/input/marketo/base"

module Embulk
  module Input
    module Marketo
      class BaseTest < Test::Unit::TestCase
        def test_target
          assert_raise(NotImplementedError) do
            Base.target
          end
        end

        class SoapClientTest < self
          def setup
            stub(Base).target { :lead }
            @client = Base.soap_client(config)
          end

          def test_endpoint
            assert_equal(settings[:endpoint], @client.endpoint)
          end

          def test_wsdl
            assert_equal(settings[:wsdl], @client.wsdl)
          end

          def test_user_id
            assert_equal(settings[:user_id], @client.user_id)
          end

          def test_encryption_key
            assert_equal(settings[:encryption_key], @client.encryption_key)
          end
        end

        private

        def config
          DataSource[settings.to_a]
        end

        def settings
          {
            endpoint: "https://marketo.example.com",
            wsdl: "https://marketo.example.com/?wsdl",
            user_id: "user_id",
            encryption_key: "TOPSECRET",
            from_datetime: from_datetime,
            columns: [
              {"name" => "Name", "type" => "string"},
            ]
          }
        end

        def from_datetime
          "2015-07-01 00:00:00+00:00"
        end

        def task
          {
            endpoint_url: "https://marketo.example.com",
            wsdl_url: "https://marketo.example.com/?wsdl",
            user_id: "user_id",
            encryption_key: "TOPSECRET",
            from_datetime: from_datetime,
            columns: [
              {"name" => "Name", "type" => "string"},
            ]
          }
        end

      end
    end
  end
end
