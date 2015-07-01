require "prepare_embulk"
require "embulk/input/marketo"

module Embulk
  module Input
    class MarketoTest < Test::Unit::TestCase
      class GuessTest < self
        def setup
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

        def config
          DataSource[*settings.to_a]
        end

        def settings
          {
            endpoint: "https://marketo.example.com",
            wsdl: "https://marketo.example.com/?wsdl",
            user_id: "user_id",
            encryption_key: "TOPSECRET",
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
      end
    end
  end
end
