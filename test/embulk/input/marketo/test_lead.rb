require "prepare_embulk"
require "lead_fixtures"
require "mute_logger"
require "embulk/input/marketo/lead"

module Embulk
  module Input
    module Marketo
      class LeadTest < Test::Unit::TestCase
        include LeadFixtures
        include MuteLogger

        def test_target
          assert_equal(:lead, Lead.target)
        end

        def setup_soap
          @soap = MarketoApi::Soap::Lead.new(settings[:endpoint], settings[:wsdl], settings[:user_id], settings[:encryption_key])

          stub(Lead).soap_client(task) { @soap }
        end

        def setup_plugin
          @page_builder = Object.new
          @plugin = Lead.new(task, nil, nil, @page_builder)
          mute_logger
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

        class RunTest < self
          def setup
            setup_soap
            setup_plugin
          end

          def test_run_through
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

          def test_run_commit_report
            # do not requests
            stub(@page_builder).finish
            stub(@plugin.soap).each { }

            commit_report = @plugin.run
            assert_equal to_datetime, commit_report[:from_datetime]
          end

          def test_preview_through
            stub(@plugin).preview? { true }

            any_instance_of(Savon::Client) do |klass|
              mock(klass).call(:get_multiple_leads, message: request.merge(batch_size: Lead::PREVIEW_COUNT)) do
                preview_leads_response
              end
            end

            Lead::PREVIEW_COUNT.times do |count|
              mock(@page_builder).add(["manyo#{count}"])
            end
            mock(@page_builder).finish

            @plugin.run
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
              stub(@soap).endpoint { "http://192.0.2.0/" }

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

          private

          def request
            {
              lead_selector: {
                oldest_updated_at: timerange.first[:from].iso8601,
                latest_updated_at: timerange.first[:to].iso8601,
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
          soap = MarketoApi::Soap::Lead.new(settings[:endpoint], settings[:wsdl], settings[:user_id], settings[:encryption_key])
          soap.send(:generate_time_range, from_datetime, to_datetime)
        end

        def task
          {
            endpoint_url: "https://marketo.example.com",
            wsdl_url: "https://marketo.example.com/?wsdl",
            user_id: "user_id",
            encryption_key: "TOPSECRET",
            from_datetime: from_datetime,
            to_datetime: to_datetime,
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
            }
          ]
        end

        def expected_guessed_columns
          [
            {name: "id", type: "long"},
            {name: "email", type: "string"},
            {name: "FieldName", type: "string"},
          ]
        end
      end
    end
  end
end
