require "embulk/input/marketo_api/soap/base"
require "lead_fixtures"

module Embulk
  module Input
    module MarketoApi
      module Soap
        class BaseTest < Test::Unit::TestCase
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

          class TestActivityLogMetadata < self
            def setup
              pend "TODO: Move this testcase to test_activity_log.rb"

              @savon = soap.__send__(:savon)
              stub(soap).savon { @savon } # Pin savon instance for each call soap.savon for mocking/stubbing
            end

            def test_savon_call
              mock(@savon).call(:get_lead_changes, message: request) {
                Struct.new(:body).new(body)
              }
              soap.activity_log_metadata(last_updated_at)
            end

            def test_return_metadata
              stub(@savon).call(:get_lead_changes, message: request) {
                Struct.new(:body).new(body)
              }
              assert_equal(metadata, soap.activity_log_metadata(last_updated_at))
            end

            private

            def request
              {
                :start_position => {
                  :oldest_created_at => last_updated_at
                },
                :batch_size => 1000,
              }
            end

            def last_updated_at
              "2015-07-01 00:00:00+00:00"
            end

            def body
              {
                success_get_lead_changes: {
                  result: {
                    lead_change_record_list: {
                      lead_change_record: records
                    }
                  }
                }
              }
            end

            def metadata
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

            def records
              [
                {
                  id: "12",
                  activity_date_time: "2015-06-25T00:12:00+00:00",
                  activity_type: "Visit Webpage",
                  mktg_asset_name: "webpage.example.com/person/1/edit",
                  activity_attributes:
                    {
                      attribute:
                        [
                          {attr_name: "Webpage ID", attr_type: nil, attr_value: "56"},
                          {attr_name: "Webpage URL", attr_type: nil, attr_value: "/person/1/edit"},
                          {attr_name: "Referrer URL", attr_type: nil, attr_value: "https://webpage.example.com"},
                          {attr_name: "Client IP Address", attr_type: nil, attr_value: "127.0.0.1"},
                          {attr_name: "User Agent", attr_type: nil, attr_value: "UserAgent"},
                          {attr_name: "Message Id", attr_type: nil, attr_value: "78"},
                          {attr_name: "Created At", attr_type: nil, attr_value: "2015-07-06 19:00:02"},
                          {attr_name: "Lead ID", attr_type: nil, attr_value: "90"}
                        ]
                    },
                  mkt_person_id: "34"
                }
              ]
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
