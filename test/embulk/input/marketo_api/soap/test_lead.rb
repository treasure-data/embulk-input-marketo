require "embulk/input/marketo_api/soap/lead"
require "lead_fixtures"
require "mute_logger"

module Embulk
  module Input
    module MarketoApi
      module Soap
        class LeadTest < Test::Unit::TestCase
          include LeadFixtures
          include MuteLogger

          def setup
            mute_logger
          end

          class TestEach < self
            def setup
              super
            end

            def test_each_invoke_fetch_with_specified_time
              timerange = {
                "from" => Time.parse("2015-07-06 00:00:00"),
                "to" => Time.parse("2015-07-06 12:00:00"),
              }

              request = {
                lead_selector: {
                  oldest_updated_at: timerange["from"].iso8601,
                  latest_updated_at: timerange["to"].iso8601,
                },
                attributes!: {lead_selector: {"xsi:type"=>"ns1:LastUpdateAtSelector"}},
                batch_size: Lead::BATCH_SIZE_DEFAULT,
              }

              mock(soap).fetch(request, {})

              soap.each(timerange) { }
            end

            def test_each_fetch_next_page
              timerange = {
                "from" => Time.parse("2015-07-06 23:30:00"),
                "to" => Time.parse("2015-07-07 00:00:00"),
              }

              any_instance_of(Savon::Client) do |klass|
                mock(klass).call(:get_multiple_leads, anything) do
                  next_stream_leads_response
                end
              end

              proc = proc{ "" }
              leads_count = next_stream_leads_response.xpath('//leadRecord').length
              mock(proc).call(anything).times(leads_count)

              soap.each(timerange, {}, &proc)
            end
          end

          class TestMetadata < self
            def setup
              super
              @savon = soap.__send__(:savon)
              stub(soap).savon { @savon } # Pin savon instance for each call soap.savon for mocking/stubbing
            end

            def test_savon_call
              mock(@savon).call(:describe_m_object, message: {object_name: "LeadRecord"}) {
                Struct.new(:body).new(body)
              }
              soap.metadata
            end

            def test_return_fields
              stub(@savon).call(:describe_m_object, message: {object_name: "LeadRecord"}) {
                Struct.new(:body).new(body)
              }
              assert_equal(fields, soap.metadata)
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
