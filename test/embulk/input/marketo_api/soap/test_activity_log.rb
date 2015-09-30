require "embulk/input/marketo_api/soap/activity_log"
require "activity_log_fixtures"

module Embulk
  module Input
    module MarketoApi
      module Soap
        class ActivityLogTest < Test::Unit::TestCase
          include ActivityLogFixtures

          def setup
            stub(Embulk).logger { ::Logger.new(IO::NULL) }
          end

          def test_each
            request = {
              start_position: {
                oldest_created_at: Time.parse(from_datetime).iso8601,
              },
              batch_size: 100
            }

            any_instance_of(Savon::Client) do |klass|
              mock(klass).call(:get_lead_changes, message: request) do
                next_stream_activity_logs_response
              end
            end

            proc = proc{ "" }
            activity_logs = next_stream_activity_logs_response[:body][:success_get_lead_changes][:result][:lead_change_record_list][:lead_change_record]
            last_activity_log = activity_logs.last

            mock(proc).call(anything).times(activity_logs.size)
            assert_equal(last_activity_log[:activity_date_time], soap.each(from_datetime, &proc))
          end

          def test_each_with_no_response
            request = {
              start_position: {
                oldest_created_at: Time.parse(from_datetime).iso8601,
              },
              batch_size: 100
            }

            any_instance_of(Savon::Client) do |klass|
              mock(klass).call(:get_lead_changes, message: request) do
                none_activity_log_response
              end
            end

            proc = proc{ "" }

            assert_nil(soap.each(from_datetime, &proc))
          end

          class TestMetadata < self
            def setup
              super

              @savon = soap.__send__(:savon)
              stub(soap).savon { @savon } # Pin savon instance for each call soap.savon for mocking/stubbing
            end

            def test_savon_call
              mock(@savon).call(:get_lead_changes, message: request) {
                next_stream_activity_logs_response
              }
              soap.metadata(from_datetime)
            end

            def test_return_schema
              stub(@savon).call(:get_lead_changes, message: request) {
                next_stream_activity_logs_response
              }
              assert_equal(schema, soap.metadata(from_datetime))
            end

            private

            def request
              {
                start_position: {
                  oldest_created_at: Time.parse(from_datetime).iso8601,
                },
                batch_size: 100
              }
            end

            def schema
              metadata = [
                {index: 0, name: "id", type: :long},
                {index: 1, name: "activity_date_time", type: :timestamp, format: "%Y-%m-%dT%H:%M:%S%z"}, # NOTE: `format` is the same as-is response (e.g. "2015-07-14T00:00:11+0000" to "%Y-%m-%dT%H:%M:%S%z")
                {index: 2, name: "activity_type", type: :string},
                {index: 3, name: "mktg_asset_name", type: :string},
                {index: 4, name: "mkt_person_id", type: :long},
                {index: 5, name: "Attribute Name", type: :string},
                {index: 6, name: "Old Value", type: :long},
              ]

              metadata.map do |column|
                Column.new(column)
              end
            end
          end

          private

          def from_datetime
            "2015-07-06"
          end

          def soap
            @soap ||= ActivityLog.new(settings[:endpoint], settings[:wsdl], settings[:user_id], settings[:encryption_key])
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
