require "embulk/input/marketo_api/soap/base"
require "lead_fixtures"
require "override_assert_raise"

module Embulk
  module Input
    module MarketoApi
      module Soap
        class BaseTest < Test::Unit::TestCase
          include LeadFixtures
          include OverrideAssertRaise

          class TestRetry < self
            def setup
              stub(Embulk.logger).warn {}
            end

            def test_retry_timeout
              any_instance_of(Savon::Client) do |klass|
                stub(klass).call(:timeout_test, advanced_typecasting: false) { raise ::Timeout::Error }
              end

              mock(Embulk.logger).warn(/Retrying/).times(retry_options[:retry_limit])

              assert_raise(PerfectRetry::TooManyRetry) do
                soap.send(:savon_call, :timeout_test, {}, retry_options)
              end
            end

            def test_retry_common_error
              any_instance_of(Savon::Client) do |klass|
                stub(klass).call(:timeout_test, advanced_typecasting: false) { raise "something error" }
              end

              mock(Embulk.logger).warn(/Retrying/).times(retry_options[:retry_limit])

              assert_raise do
                soap.send(:savon_call, :timeout_test, {}, retry_options)
              end
            end

            def test_not_retry_config_error
              any_instance_of(Savon::Client) do |klass|
                stub(klass).call(:timeout_test, advanced_typecasting: false) { raise Embulk::ConfigError.new("config error") }
              end

              mock(Embulk.logger).warn(/Retrying/).never

              assert_raise(Embulk::ConfigError) do
                soap.send(:savon_call, :timeout_test, {}, retry_options)
              end
            end

            private

            def retry_options
              {
                retry_limit: 4,
                retry_initial_wait_sec: 0,
              }
            end
          end

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
