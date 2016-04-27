require "prepare_embulk"
require "override_assert_raise"
require "embulk/input/marketo/activity_log"
require "activity_log_fixtures"

module Embulk
  module Input
    module Marketo
      class ActivityLogTest < Test::Unit::TestCase
        include ActivityLogFixtures
        include OverrideAssertRaise

        class TransactionTest < self
          def test_generate_task
            control = proc {} # dummy
            columns = task[:columns].map do |col|
              Column.new(nil, col["name"], col["type"].to_sym, col["format"])
            end

            mock(ActivityLog).resume(task, columns, 1, &control)
            ActivityLog.transaction(config, &control)
          end

          def test_resume
            next_config_diff = {from_datetime: from_datetime}
            control = proc { [next_config_diff] } # In actual, embulk prepares control block returning Array.
            columns = task[:columns].map do |col|
              Column.new(nil, col["name"], col["type"].to_sym)
            end

            actual = ActivityLog.resume(task, columns, 1, &control)
            assert_equal(next_config_diff, actual)
          end

          private

          def settings
            {
              endpoint: "https://marketo.example.com",
              wsdl: "https://marketo.example.com/?wsdl",
              user_id: "user_id",
              encryption_key: "TOPSECRET",
              from_datetime: from_datetime,
              to_datetime: to_datetime,
              retry_initial_wait_sec: 3,
              retry_limit: 2,
              columns: [
                {"name" => :id, "type" => :long},
                {"name" => :activity_date_time, "type" => :timestamp, "format" => "%Y-%m-%dT%H:%M:%S%z"},
                {"name" => :activity_type, "type" => :string},
                {"name" => :mktg_asset_name, "type" => :string},
                {"name" => :mkt_person_id, "type" => :long},
                {"name" => "Attribute Name", "type" => :string},
                {"name" => "Old Value", "type" => :string},
              ]
            }
          end

          def config
            DataSource[settings.to_a]
          end

          def task
            {
              endpoint_url: "https://marketo.example.com",
              wsdl_url: "https://marketo.example.com/?wsdl",
              user_id: "user_id",
              encryption_key: "TOPSECRET",
              from_datetime: from_datetime,
              to_datetime: to_datetime,
              retry_initial_wait_sec: 3,
              retry_limit: 2,
              columns: [
                {"name" => :id, "type" => :long},
                {"name" => :activity_date_time, "type" => :timestamp, "format" => "%Y-%m-%dT%H:%M:%S%z"},
                {"name" => :activity_type, "type" => :string},
                {"name" => :mktg_asset_name, "type" => :string},
                {"name" => :mkt_person_id, "type" => :long},
                {"name" => "Attribute Name", "type" => :string},
                {"name" => "Old Value", "type" => :string},
              ]
            }
          end
        end

        def test_target
          assert_equal(:activity_log, ActivityLog.target)
        end

        class GuessTest < self
          setup :setup_soap

          def setup_soap
            @soap = MarketoApi::Soap::ActivityLog.new(settings[:endpoint], settings[:wsdl], settings[:user_id], settings[:encryption_key])

            stub(ActivityLog).soap_client(config) { @soap }
          end

          def test_include_metadata
            stub(@soap).metadata(from_datetime, batch_size: ActivityLog::PREVIEW_COUNT) { Guess::SchemaGuess.from_hash_records(records) }

            assert_equal(
              {"columns" => expected_guessed_columns},
              Marketo::ActivityLog.guess(config)
            )
          end

          private

          def records
            [
              {
                id: "12",
                activity_date_time: "2015-06-25T00:12:00+00:00",
                activity_type: "Visit Webpage",
                mktg_asset_name: "webpage.example.com/person/1/edit",
                mkt_person_id: "34",
                "Webpage ID" => "56",
                "Webpage URL" => "/person/1/edit",
                "Referrer URL" => "https://webpage.example.com",
                "Client IP Address" => "127.0.0.1",
                "User Agent" => "UserAgent",
                "Message Id" => "78",
                "Created At" => "2015-07-06 19:00:02",
                "Lead ID" => "90"
              }
            ]
          end

          def expected_guessed_columns
            [
              {name: :id, type: :long},
              {name: :activity_date_time, type: :timestamp, format: "%Y-%m-%dT%H:%M:%S%:z"},
              {name: :activity_type, type: :string},
              {name: :mktg_asset_name, type: :string},
              {name: :mkt_person_id, type: :long},
              {name: "Webpage ID", type: :long},
              {name: "Webpage URL", type: :string},
              {name: "Referrer URL", type: :string},
              {name: "Client IP Address", type: :string},
              {name: "User Agent", type: :string},
              {name: "Message Id", type: :long},
              {name: "Created At", type: :timestamp, format: "%Y-%m-%d %H:%M:%S"},
              {name: "Lead ID", type: :long}
            ]
          end
        end

        class TestRetry < self
          def setup
            @soap = MarketoApi::Soap::ActivityLog.new(settings[:endpoint], settings[:wsdl], settings[:user_id], settings[:encryption_key])
            stub(ActivityLog).soap_client(task) { @soap }

            @page_builder = Object.new
            @plugin = ActivityLog.new(task, nil, nil, @page_builder)
          end

          def test_retry
            any_instance_of(Savon::Client) do |klass|
              stub(klass).call(:get_lead_changes, anything) do
                raise "foo"
              end
            end

            mock(Embulk.logger).warn(/Retrying/).times(task[:retry_limit])
            stub(Embulk.logger).info {}

            assert_raise do
              @plugin.run
            end
          end
        end

        class FormatRecordTest < self
          def setup
            @page_builder = Object.new
            plugin_task = task.dup
            plugin_task[:columns] = [
              {"name" => "time", "type" => :timestamp, "format" => "%Y-%m-%d %H:%M:%S"}
            ]
            @plugin = ActivityLog.new(plugin_task, nil, nil, @page_builder)
          end

          data do
            [
              ["valid timestamp", [Time.parse("2000-01-01 00:00:00"), "2000-01-01 00:00:00"]],
              ["nil", [nil, nil]],
              ["empty", [nil, ""]],
            ]
          end
          def test_valid_timestamp(data)
            expected, actual = data
            record = { "time" => actual }
            assert_equal [expected], @plugin.format_record(record)
          end

          def test_invalid_timestamp
            record = { "time" => "123" }
            assert_raise(Embulk::ConfigError) do
              @plugin.format_record(record)
            end
          end
        end

        class RunTest < self
          def setup_soap
            @soap = MarketoApi::Soap::ActivityLog.new(settings[:endpoint], settings[:wsdl], settings[:user_id], settings[:encryption_key])

            stub(ActivityLog).soap_client(task) { @soap }
          end

          def setup_plugin
            @page_builder = Object.new
            @plugin = ActivityLog.new(task, nil, nil, @page_builder)
            stub(Embulk).logger { ::Logger.new(IO::NULL) }
          end

          def setup
            setup_soap
            setup_plugin
          end

          def test_run_through
            stub(@plugin).preview? { false }

            any_instance_of(Savon::Client) do |klass|
              mock(klass).call(:get_lead_changes, message: request) do
                savon_response(xml_ac_response)
              end

              mock(klass).call(:get_lead_changes, message: offset_request) do
                savon_response(xml_ac_next_response)
              end
            end

            mock(@page_builder).add(["1", Time.parse("2015-07-14T00:00:09+0000"), "at1", "score1", "100", "Attribute1", "402"])
            mock(@page_builder).add(["2", Time.parse("2015-07-14T00:00:10+0000"), "at2", "score2", "90", "Attribute2", "403"])
            mock(@page_builder).add(["3", Time.parse("2015-07-14T00:00:11+0000"), "at3", "score3", "100", "Attribute3", "404"])
            mock(@page_builder).finish

            @plugin.init
            @plugin.run
          end

          def test_run_with_no_response
            stub(@plugin).preview? { false }

            any_instance_of(Savon::Client) do |klass|
              mock(klass).call(:get_lead_changes, message: request) do
                savon_response xml_ac_none_response
              end
            end

            mock(@page_builder).finish

            @plugin.init
            @plugin.run
          end

          def test_preview_through
            stub(@plugin).preview? { true }

            any_instance_of(Savon::Client) do |klass|
              mock(klass).call(:get_lead_changes, message: preview_request) do
                savon_response xml_ac_preview_response
              end
            end

            1.upto(ActivityLog::PREVIEW_COUNT) do |count|
              mock(@page_builder).add([count.to_s, Time.parse("2015-07-14T00:00:11+0000"), "at#{count}", "score#{count}", "100", "Attribute#{count}", "404"])
            end
            mock(@page_builder).finish

            @plugin.init
            @plugin.run
          end

          def test_wrong_type
            any_instance_of(Savon::Client) do |klass|
              stub(klass).call(:get_lead_changes, message: request) do
                savon_response xml_ac_next_response
              end
            end
            stub(@page_builder).add {}
            stub(@page_builder).finish {}

            task = task()
            task[:columns] = [
              {"name" => "Old Value", "type" => :timestamp}
            ]
            @plugin = ActivityLog.new(task, nil, nil, @page_builder)

            assert_raise(Embulk::ConfigError) do
              @plugin.init
              @plugin.run
            end
          end

          private

          def request
            {
              start_position: {
                oldest_created_at: Time.parse(from_datetime).iso8601,
                latest_created_at: Time.parse(to_datetime).iso8601,
              },
              batch_size: 100
            }
          end

          def offset_request
            {
              start_position: {
                oldest_created_at: Time.parse(from_datetime).iso8601,
                latest_created_at: Time.parse(to_datetime).iso8601,
                offset: "offset"
              },
              batch_size: 100
            }
          end
        end

          def preview_request
            {
              start_position: {
                oldest_created_at: Time.parse(from_datetime).iso8601,
              },
              batch_size: ActivityLog::PREVIEW_COUNT
            }
          end

        private

        def settings
          {
            endpoint: "https://marketo.example.com",
            wsdl: "https://marketo.example.com/?wsdl",
            user_id: "user_id",
            encryption_key: "TOPSECRET",
            from_datetime: from_datetime,
            to_datetime: to_datetime,
          }
        end

        def config
          DataSource[settings.to_a]
        end

        def task
          {
            endpoint_url: "https://marketo.example.com",
            wsdl_url: "https://marketo.example.com/?wsdl",
            user_id: "user_id",
            encryption_key: "TOPSECRET",
            from_datetime: from_datetime,
            to_datetime: to_datetime,
            retry_initial_wait_sec: 0,
            retry_limit: 2,
            columns: [
              {"name" => :id, "type" => :long},
              {"name" => :activity_date_time, "type" => :timestamp, "format" => "%Y-%m-%dT%H:%M:%S%z"},
              {"name" => :activity_type, "type" => :string},
              {"name" => :mktg_asset_name, "type" => :string},
              {"name" => :mkt_person_id, "type" => :long},
              {"name" => "Attribute Name", "type" => :string},
              {"name" => "Old Value", "type" => :string},
            ]
          }
        end

        def from_datetime
          "2015-07-01 00:00:00+00:00"
        end

        def to_datetime
          "2015-11-01 00:00:00+00:00"
        end
      end
    end
  end
end
