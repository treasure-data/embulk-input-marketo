require "embulk/input/marketo_api/soap/lead"
require "lead_fixtures"

module Embulk
  module Input
    module MarketoApi
      module Soap
        class LeadTest < Test::Unit::TestCase
          include LeadFixtures

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

          def test_each_lead
            stub(Embulk).logger { ::Logger.new(IO::NULL) }
            last_updated_at = "2015-07-06"

            request = {
              lead_selector: {oldest_updated_at: Time.parse(last_updated_at).iso8601},
              attributes!: {lead_selector: {"xsi:type"=>"ns1:LastUpdateAtSelector"}},
              batch_size: 1000
            }

            any_instance_of(Savon::Client) do |klass|
              mock(klass).call(:get_multiple_leads, message: request) do
                next_stream_leads_response
              end
            end

            proc = proc{ "" }
            leads_count = next_stream_leads_response.xpath('//leadRecord').length
            mock(proc).call(anything).times(leads_count)

            soap.each_lead(last_updated_at, &proc)
          end

          class TestLeadMetadata < self
            def setup
              @savon = soap.__send__(:savon)
              stub(soap).savon { @savon } # Pin savon instance for each call soap.savon for mocking/stubbing
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
            @soap ||= Lead.new(settings[:endpoint], settings[:wsdl], settings[:user_id], settings[:encryption_key])
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
