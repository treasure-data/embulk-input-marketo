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

            def test_each_invoke_fetch
              since_at = "2015-07-06"
              until_at = "2015-07-07"
              timerange = soap.send(:generate_time_range, since_at, until_at)

              stub(soap).fetch { nil }
              mock(soap).fetch(anything).times(timerange.length)

              soap.each(since_at, until_at) { }
            end

            def test_each_invoke_fetch_with_specified_time
              since_at = "2015-07-06"
              until_at = "2015-07-07"
              timerange = soap.send(:generate_time_range, since_at, until_at)

              request = {
                lead_selector: {
                  oldest_updated_at: timerange.first[:from].iso8601,
                  latest_updated_at: timerange.first[:to].iso8601,
                },
                attributes!: {lead_selector: {"xsi:type"=>"ns1:LastUpdateAtSelector"}},
                batch_size: Lead::BATCH_SIZE_DEFAULT,
              }

              stub(soap).fetch { nil }
              mock(soap).fetch(request)

              soap.each(since_at, until_at) { }
            end

            def test_each_fetch_next_page
              since_at = "2015-07-06 00:00:00"
              until_at = "2015-07-06 00:00:01"

              any_instance_of(Savon::Client) do |klass|
                mock(klass).call(:get_multiple_leads, anything) do
                  next_stream_leads_response
                end
              end

              proc = proc{ "" }
              leads_count = next_stream_leads_response.xpath('//leadRecord').length
              mock(proc).call(anything).times(leads_count)

              soap.each(since_at, until_at, &proc)
            end
          end

          class TestGenerateTime < self
            def setup
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
              range = soap.send(:generate_time_range, from, to)
              assert_equal count, range.length
            end

            def test_if_to_is_nil_use_time_now
              from = "2000-01-01"
              now = Time.now
              stub(Time).now { now }

              range = soap.send(:generate_time_range, from, nil)
              assert_equal now, range.last[:to]
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
