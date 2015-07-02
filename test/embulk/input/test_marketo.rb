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

        def task
          {
            endpoint: "https://marketo.example.com",
            wsdl_url: "https://marketo.example.com/?wsdl",
            user_id: "user_id",
            encryption_key: "TOPSECRET",
            last_updated_at: last_updated_at,
            columns: [
              {"name" => "Name", "type" => "string"},
            ]
          }
        end

        def request
          {
            lead_selector: {oldest_updated_at: "2015-07-01T00:00:00+09:00"},
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

        def test_guess
          stub(@soap).lead_metadata { metadata }
          assert_equal(
            {"columns" => Marketo.generate_columns(metadata)},
            Marketo.guess(config)
          )
        end

        private

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
        }
      end

      def last_updated_at
        "2015-07-01 00:00:00+00:00"
      end
    end
  end
end
