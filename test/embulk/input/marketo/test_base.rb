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

        def test_transaction
          control = proc {} # dummy
          columns = task[:columns].map do |col|
            Column.new(nil, col["name"], col["type"].to_sym)
          end

          mock(Base).resume(task, columns, 1, &control)
          Base.transaction(config, &control)
        end

        def test_resume
          next_config_diff = {last_updated_at: last_updated_at}
          control = proc { [next_config_diff] } # In actual, embulk prepares control block returning Array.
          columns = task[:columns].map do |col|
            Column.new(nil, col["name"], col["type"].to_sym)
          end

          actual = Base.resume(task, columns, 1, &control)
          assert_equal(next_config_diff, actual)
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
            last_updated_at: last_updated_at,
            columns: [
              {"name" => "Name", "type" => "string"},
            ]
          }
        end

        def last_updated_at
          "2015-07-01 00:00:00+00:00"
        end

        def task
          {
            endpoint_url: "https://marketo.example.com",
            wsdl_url: "https://marketo.example.com/?wsdl",
            user_id: "user_id",
            encryption_key: "TOPSECRET",
            last_updated_at: last_updated_at,
            columns: [
              {"name" => "Name", "type" => "string"},
            ]
          }
        end

      end
    end
  end
end
