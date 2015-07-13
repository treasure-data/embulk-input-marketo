require "prepare_embulk"
require "embulk/input/marketo/activity_log"

module Embulk
  module Input
    module Marketo
      class ActivityLogTest < Test::Unit::TestCase
        class GuessTest < self
          setup :setup_soap

          def setup_soap
            @soap = MarketoApi::Soap::ActivityLog.new(settings[:endpoint], settings[:wsdl], settings[:user_id], settings[:encryption_key])

            stub(ActivityLog).soap_client(config) { @soap }
          end

          def test_include_metadata
            stub(@soap).activity_log_metadata(last_updated_at) { records }

            assert_equal(
              {"columns" => expected_guessed_columns},
              Marketo::ActivityLog.guess(config)
            )
          end

          private

          def records
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
        end

        def expected_guessed_columns
          [
            {name: :id, type: :long},
            {name: :activity_date_time, type: :timestamp, format: "%Y-%m-%dT%H:%M:%S%z"},
            {name: :activity_type, type: :string},
            {name: :mktg_asset_name, type: :string},
            {name: :mkt_person_id, type: :long},
            {name: "Webpage ID", type: :long},
            {name: "Webpage URL", type: :string},
            {name: "Referrer URL", type: :string},
            {name: "Client IP Address", type: :string},
            {name: "User Agent", type: :string},
            {name: "Message Id", type: :long},
            {name: "Created At", type: :timestamp, format: "%Y-%m-%d %H:%M:%S"},
            {name: "Lead ID", type: :long}
          ]
        end

        private

        def settings
          {
            endpoint: "https://marketo.example.com",
            wsdl: "https://marketo.example.com/?wsdl",
            user_id: "user_id",
            encryption_key: "TOPSECRET",
            last_updated_at: last_updated_at,
          }
        end

        def config
          DataSource[settings.to_a]
        end

        def task
          {
            endpoint_url: "https://marketo.example.com",
            wsdl_url: "https://marketo.example.com/?wsdl",
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
end
