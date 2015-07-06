require "embulk/input/marketo_api/soap"

module Embulk
  module Input
    module MarketoApi
      class SoapTest < Test::Unit::TestCase
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

        class TestLeadMetadata < self
          def setup
            @savon = soap.__send__(:savon)
            stub(soap).savon { @savon }
          end

          def test_savon_call
            mock(@savon).call(:describe_m_object, message: {object_name: "LeadRecord"}) {
              Struct.new(:body).new(body)
            }
            soap.lead_metadata
          end

          def test_return_fields
            stub(@savon).call(:describe_m_object, message: {object_name: "LeadRecord"}) {
              Struct.new(:body).new(body)
            }
            assert_equal(fields, soap.lead_metadata)
          end

          private

          def body
            {
              success_describe_m_object: {
                result: {
                  metadata: {
                    field_list: {
                      field: fields
                    }
                  }
                }
              }
            }
          end

          def fields
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

        def soap
          @soap ||= Soap.new(settings[:endpoint], settings[:wsdl], settings[:user_id], settings[:encryption_key])
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

