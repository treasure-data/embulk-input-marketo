require "prepare_embulk"
require "lead_fixtures"
require "embulk/input/marketo"

module Embulk
  module Input
    class MarketoTest < Test::Unit::TestCase
      include LeadFixtures

      def setup_soap
        @soap = MarketoApi::Soap.new(settings[:endpoint], settings[:wsdl], settings[:user_id], settings[:encryption_key])

        stub(MarketoApi).soap_client(task) { @soap }
      end

      def setup_plugin
        @page_builder = Object.new
        @plugin = Marketo.new(task, nil, nil, @page_builder)
        stub(@plugin).logger { ::Logger.new(File::NULL) }
      end

      def test_transaction
        control = proc {} # dummy
        columns = task[:columns].map do |col|
          Column.new(nil, col["name"], col["type"].to_sym)
        end

        mock(Marketo).resume(task, columns, 1, &control)
        Marketo.transaction(config, &control)
      end

      class RunTest < self
        def setup
          setup_soap
          setup_plugin
        end

        def test_run_through
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

        private

        def request
          {
            lead_selector: {oldest_updated_at: Time.parse(last_updated_at).iso8601},
            attributes!: {lead_selector: {"xsi:type"=>"ns1:LastUpdateAtSelector"}},
           batch_size: 100
          }
        end
      end

      class GuessTest < self
        setup :setup_soap

        def setup_soap
          @soap = MarketoApi::Soap.new(settings[:endpoint], settings[:wsdl], settings[:user_id], settings[:encryption_key])

          stub(Marketo).soap_client(config) { @soap }
        end

        def test_include_metadata
          stub(@soap).lead_metadata { metadata }

          assert_equal(
            {"columns" => expected_guessed_columns},
            Marketo.guess(config)
          )
        end
      end

      def test_generate_columns
        setup_soap

        stub(@soap).lead_metadata { metadata }

        assert_equal(expected_guessed_columns, Marketo.generate_columns(metadata))
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
