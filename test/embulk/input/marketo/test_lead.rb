require "prepare_embulk"
require "lead_fixtures"
require "mute_logger"
require "override_assert_raise"
require "embulk/input/marketo/lead"

module Embulk
  module Input
    module Marketo
      class LeadTest < Test::Unit::TestCase
        include LeadFixtures
        include MuteLogger
        include OverrideAssertRaise

        def test_target
          assert_equal(:lead, Lead.target)
        end

        def setup_soap
          @soap = MarketoApi::Soap::Lead.new(settings[:endpoint], settings[:wsdl], settings[:user_id], settings[:encryption_key])

          stub(Lead).soap_client(task) { @soap }
        end

        def setup_plugin
          @page_builder = Object.new
          any_instance_of(Embulk::Input::Marketo::Lead) do |klass|
            stub(klass).index { 0 }
          end
          @plugin = Lead.new(task, nil, nil, @page_builder)
        end

        def test_invalid_from_datetime_to_datetime
          control = proc {} # dummy

          settings = {
            endpoint: "https://marketo.example.com",
            wsdl: "https://marketo.example.com/?wsdl",
            user_id: "user_id",
            encryption_key: "TOPSECRET",
            columns: [
              {"name" => "Name", "type" => "string"},
            ],
            from_datetime: Time.now + 3600,
            to_datetime: Time.now,
          }
          config = DataSource[settings.to_a]

          assert_raise(ConfigError) do
            Lead.transaction(config, &control)
          end
        end

        def test_invalid_datetime_given
          control = proc {} # dummy

          settings = {
            endpoint: "https://marketo.example.com",
            wsdl: "https://marketo.example.com/?wsdl",
            user_id: "user_id",
            encryption_key: "TOPSECRET",
            from_datetime: "invalid time from",
            to_datetime: "invalid time to",
            columns: [
              {"name" => "Name", "type" => "string"},
            ]
          }
          config = DataSource[settings.to_a]

          assert_raise(ConfigError) do
            Lead.transaction(config, &control)
          end
        end

        def test_wrong_type
          control = proc {} # dummy

          settings = {
            endpoint: "https://marketo.example.com",
            wsdl: "https://marketo.example.com/?wsdl",
            user_id: "user_id",
            encryption_key: "TOPSECRET",
            from_datetime: "invalid time from",
            to_datetime: "invalid time to",
            columns: [
              {"name" => "Name", "type" => "timestamp"},
            ]
          }

          config = DataSource[settings.to_a]

          assert_raise(ConfigError) do
            Lead.transaction(config, &control)
          end
        end

        def test_same_datetimes_given
          control = proc { [] } # dummy (#resume method returns [])
          datetime = Time.now.to_s

          settings = {
            endpoint: "https://marketo.example.com",
            wsdl: "https://marketo.example.com/?wsdl",
            user_id: "user_id",
            encryption_key: "TOPSECRET",
            from_datetime: datetime,
            to_datetime: datetime,
            columns: [
              {"name" => "Name", "type" => "string"},
            ]
          }
          config = DataSource[settings.to_a]

          assert_equal({}, Lead.transaction(config, &control))
        end

        class RunTest < self
          def setup
            setup_soap
            setup_plugin
          end

          def test_run_through
            mute_logger
            stub(@plugin).preview? { false }

            now = Time.now
            stub(Time).now { now }

            any_instance_of(Savon::Client) do |klass|
              mock(klass).call(:get_multiple_leads, message: request) do
                leads_response
              end

              mock(klass).call(:get_multiple_leads, message: request.merge(stream_position: stream_position)) do
                next_stream_leads_response
              end
            end

            mock(@page_builder).add(["manyo", now])
            mock(@page_builder).add(["everyleaf", now])
            mock(@page_builder).add(["ten-thousand-leaf", now])
            mock(@page_builder).finish

            @plugin.run
          end

          def test_run_no_processed_time_columns
            mute_logger

            no_processed__task = task.merge(append_processed_time_column: false)
            @plugin = Lead.new(no_processed__task, nil, nil, @page_builder)
            stub(@plugin).preview? { false }

            any_instance_of(Savon::Client) do |klass|
              mock(klass).call(:get_multiple_leads, message: request) do
                leads_response
              end

              mock(klass).call(:get_multiple_leads, message: request.merge(stream_position: stream_position)) do
                next_stream_leads_response
              end
            end

            mock(@page_builder).add(["manyo"])
            mock(@page_builder).add(["everyleaf"])
            mock(@page_builder).add(["ten-thousand-leaf"])
            mock(@page_builder).finish

            @plugin.run
          end

          def test_run_task_report
            mute_logger
            # do not requests
            stub(@page_builder).finish
            stub(@plugin.soap).each { }

            task_report = @plugin.run
            assert_equal({}, task_report)
          end

          def test_preview_through
            mute_logger
            stub(@plugin).preview? { true }

            now = Time.now
            stub(Time).now { now }

            any_instance_of(Savon::Client) do |klass|
              mock(klass).call(:get_multiple_leads, message: request.merge(batch_size: Lead::PREVIEW_COUNT)) do
                preview_leads_response
              end
            end

            Lead::PREVIEW_COUNT.times do |count|
              mock(@page_builder).add(["manyo#{count}", now])
            end
            mock(@page_builder).finish

            @plugin.run
          end

          def test_preview_will_stop_fetching_when_defined_times_added
            mute_logger
            stub(@plugin).preview? { true }
            @plugin.instance_variable_set(:@ranges, @plugin.task[:ranges].first * 3) # multiple ranges

            any_instance_of(Savon::Client) do |klass|
              mock(klass).call(:get_multiple_leads, anything) do
                preview_leads_response
              end
            end

            mock(@page_builder).add(anything).times(Lead::PREVIEW_COUNT)
            mock(@page_builder).finish

            @plugin.run
          end

          def test_retry
            setup_plugin

            any_instance_of(Savon::Client) do |klass|
              stub(klass).call(:get_multiple_leads, anything) do
                raise "foo"
              end
            end

            any_instance_of(::Embulk::Input::MarketoApi::Soap::Base) do |klass|
              task[:retry_limit].times do |n|
                mock(klass).sleep(task[:retry_initial_wait_sec] * (2**n))
              end
            end

            mock(Embulk.logger).warn(/Retrying/).times(task[:retry_limit])
            stub(Embulk.logger).info {}

            assert_raise do
              @plugin.run
            end
          end

          class SavonCallTest < self
            def test_soap_error
              assert_raise(Embulk::ConfigError) do
                @soap.send(:catch_unretryable_error) do
                  raise Savon::SOAPFault.new(nil, Nori.new(default_nori_options), xml)
                end
              end
            end

            def test_http_error_on_client
              assert_raise(Embulk::ConfigError) do
                @soap.send(:catch_unretryable_error) do
                  raise Savon::HTTPError.new(HTTPI::Response.new(500, {}, xml("20000")))
                end
              end
            end

            def test_http_error_on_server
              assert_raise(Savon::HTTPError) do
                @soap.send(:catch_unretryable_error) do
                  # Internal Error
                  raise Savon::HTTPError.new(HTTPI::Response.new(500, {}, xml("10001")))
                end
              end
            end

            def test_socket_error
              mute_logger
              stub(@soap).endpoint { "http://foo.test/" }

              assert_raise(Embulk::ConfigError) do
                @plugin.run
              end
            end

            def xml(code = nil, message = nil)
              <<-XML
<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"><SOAP-ENV:Body><SOAP-ENV:Fault>
<faultcode>SOAP-ENV:Client</faultcode>
<faultstring>#{message || "20014 - Authentication failed"}</faultstring>
<detail>
<ns1:serviceException xmlns:ns1="http://www.marketo.com/mktows/">
<name>mktServiceException</name>
<message>#{message || "Authentication failed (20014)"}</message>
<code>#{code || "20014"}</code>
</ns1:serviceException></detail></SOAP-ENV:Fault></SOAP-ENV:Body></SOAP-ENV:Envelope>
              XML
            end

            def default_nori_options
              # https://github.com/savonrb/savon/blob/v2.11.1/lib/savon/options.rb#L75-L94
              {
                :strip_namespaces          => true,
                :convert_tags_to  => lambda { |tag| tag.snakecase.to_sym},
                :convert_attributes_to     => lambda { |k,v| [k,v] },
              }
            end
          end

          class TestTimeslice < self
            class TestGenerateTimeRange < self
              def setup
                super
                mute_logger
              end

              data do
                {
                  "8/1 to 8/2" => ["2015-08-01 00:00:00", "2015-08-02 00:00:00", 24],
                  "over the days" => ["2015-08-01 19:00:00", "2015-08-03 05:00:00", 34],
                  "odd times" => ["2015-08-01 11:11:11", "2015-08-01 22:22:22", 12],
                }
              end
              def test_generate_time_range_by_1hour(data)
                from, to, count = data
                range = Lead.generate_time_range(from, to)
                assert_equal count, range.length
              end

              def test_if_to_is_nil_use_time_now
                from = "2000-01-01"
                now = Time.now
                stub(Time).now { now }

                range = Lead.generate_time_range(from, nil)
                assert_equal now, range.last["to"]
              end
            end

            def test_timeslice
              from = "2015-08-02 20:00:00"
              to = "2015-08-03 08:08:08"
              count = 4

              raw_expect = [
                [
                  {from: "2015-08-02 20:00:00", to: "2015-08-02 21:00:00"},
                  {from: "2015-08-02 21:00:00", to: "2015-08-02 22:00:00"},
                  {from: "2015-08-02 22:00:00", to: "2015-08-02 23:00:00"},
                  {from: "2015-08-02 23:00:00", to: "2015-08-03 00:00:00"},
                ],
                [
                  {from: "2015-08-03 00:00:00", to: "2015-08-03 01:00:00"},
                  {from: "2015-08-03 01:00:00", to: "2015-08-03 02:00:00"},
                  {from: "2015-08-03 02:00:00", to: "2015-08-03 03:00:00"},
                  {from: "2015-08-03 03:00:00", to: "2015-08-03 04:00:00"},
                ],
                [
                  {from: "2015-08-03 04:00:00", to: "2015-08-03 05:00:00"},
                  {from: "2015-08-03 05:00:00", to: "2015-08-03 06:00:00"},
                  {from: "2015-08-03 06:00:00", to: "2015-08-03 07:00:00"},
                  {from: "2015-08-03 07:00:00", to: "2015-08-03 08:00:00"},
                ],
                [
                  {from: "2015-08-03 08:00:00", to: "2015-08-03 08:08:08"},
                ]
              ]

              expect = raw_expect.map do |slice|
                slice.map do |range|
                  {
                    "from" => Time.parse(range[:from]),
                    "to" => Time.parse(range[:to])
                  }
                end
              end
              assert_equal(expect, Lead.timeslice(from, to, count))
            end
          end

          private

          def request
            {
              lead_selector: {
                oldest_updated_at: timerange.first["from"].iso8601,
                latest_updated_at: timerange.first["to"].iso8601,
              },
              attributes!: {lead_selector: {"xsi:type"=>"ns1:LastUpdateAtSelector"}},
              batch_size: MarketoApi::Soap::Lead::BATCH_SIZE_DEFAULT,
            }
          end
        end

        class GuessTest < self
          setup :setup_soap

          def setup_soap
            @soap = MarketoApi::Soap::Lead.new(settings[:endpoint], settings[:wsdl], settings[:user_id], settings[:encryption_key])

            stub(Lead).soap_client(config) { @soap }
          end

          def test_include_metadata
            stub(@soap).metadata { metadata }

            assert_equal(
              {"columns" => expected_guessed_columns},
              Lead.guess(config)
            )
          end
        end

        def test_generate_columns
          assert_equal(expected_guessed_columns, Lead.generate_columns(metadata))
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
            to_datetime: to_datetime,
            columns: [
              {"name" => "Name", "type" => "string"},
            ]
          }
        end

        def from_datetime
          "2015-07-01 00:00:00+00:00"
        end

        def to_datetime
          "2015-07-01 00:00:05+00:00"
        end

        def timerange
          Lead.generate_time_range(from_datetime, to_datetime)
        end

        def task
          {
            endpoint_url: "https://marketo.example.com",
            wsdl_url: "https://marketo.example.com/?wsdl",
            user_id: "user_id",
            encryption_key: "TOPSECRET",
            from_datetime: from_datetime,
            to_datetime: to_datetime,
            retry_initial_wait_sec: 2,
            retry_limit: 3,
            append_processed_time_column: true,
            ranges: Lead.timeslice(from_datetime, to_datetime, Lead::TIMESLICE_COUNT_PER_TASK),
            columns: [
              {"name" => "Name", "type" => "string"},
            ]
          }
        end

        def metadata
          [
            {
              name: "FieldName",
              description: nil,
              display_name: "The Name of Field",
              source_object: "Lead",
              data_type: "datetime",
              size: nil,
              is_readonly: false,
              is_update_blocked: false,
              is_name: nil,
              is_primary_key: false,
              is_custom: true,
              is_dynamic: true,
              dynamic_field_ref: "leadAttributeList",
              updated_at: DateTime.parse("2000-01-01 22:22:22")
            },
            {
              name: "FieldInt",
              description: nil,
              display_name: "The Name of Field",
              source_object: "Lead",
              data_type: "integer",
              size: nil,
              is_readonly: false,
              is_update_blocked: false,
              is_name: nil,
              is_primary_key: false,
              is_custom: true,
              is_dynamic: true,
              dynamic_field_ref: "leadAttributeList",
              updated_at: DateTime.parse("2000-01-01 22:22:22")
            },
            {
              name: "FieldBoolean",
              description: nil,
              display_name: "The Name of Field",
              source_object: "Lead",
              data_type: "boolean",
              size: nil,
              is_readonly: false,
              is_update_blocked: false,
              is_name: nil,
              is_primary_key: false,
              is_custom: true,
              is_dynamic: true,
              dynamic_field_ref: "leadAttributeList",
              updated_at: DateTime.parse("2000-01-01 22:22:22")
            },
            {
              name: "FieldFloat",
              description: nil,
              display_name: "The Name of Field",
              source_object: "Lead",
              data_type: "float",
              size: nil,
              is_readonly: false,
              is_update_blocked: false,
              is_name: nil,
              is_primary_key: false,
              is_custom: true,
              is_dynamic: true,
              dynamic_field_ref: "leadAttributeList",
              updated_at: DateTime.parse("2000-01-01 22:22:22")
            },
            {
              name: "FieldString",
              description: nil,
              display_name: "The Name of Field",
              source_object: "Lead",
              data_type: "string",
              size: nil,
              is_readonly: false,
              is_update_blocked: false,
              is_name: nil,
              is_primary_key: false,
              is_custom: true,
              is_dynamic: true,
              dynamic_field_ref: "leadAttributeList",
              updated_at: DateTime.parse("2000-01-01 22:22:22")
            }
          ]
        end

        def expected_guessed_columns
          [
            {name: "id", type: "long"},
            {name: "email", type: "string"},
            {name: "FieldName", type: "timestamp"},
            {name: "FieldInt", type: "long"},
            {name: "FieldBoolean", type: "boolean"},
            {name: "FieldFloat", type: "double"},
            {name: "FieldString", type: "string"},
          ]
        end
      end
    end
  end
end
