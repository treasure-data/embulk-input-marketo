require "embulk/input/marketo_api/soap"

module Embulk
  module Input
    class Marketo < InputPlugin
      Plugin.register_input("marketo", self)

      def self.transaction(config, &control)
        # configuration code:
        task = {
          "property1" => config.param("property1", :string),
          "property2" => config.param("property2", :integer, default: 0),
        }

        columns = [
          Column.new(0, "example", :string),
          Column.new(1, "column", :long),
          Column.new(2, "value", :double),
        ]

        resume(task, columns, 1, &control)
      end

      def self.resume(task, columns, count, &control)
        commit_reports = yield(task, columns, count)

        next_config_diff = {}
        return next_config_diff
      end

      def self.guess(config)
        client = soap_client(config)
        metadata = client.lead_metadata

        return {"columns" => generate_columns(metadata)}
      end

      def self.soap_client(config)
        @soap ||=
          begin
            endpoint_url = config.param(:endpoint, :string)
            wsdl_url = config.param(:wsdl, :string, default: "#{endpoint_url}?WSDL")
            user_id = config.param(:user_id, :string)
            encryption_key = config.param(:encryption_key, :string)
            MarketoApi::Soap.new(endpoint_url, wsdl_url, user_id, encryption_key)
          end
      end

      def self.generate_columns(metadata)
        metadata.map do |field|
          type =
            case field[:data_type]
            when "integer"
              "long"
            when "dateTime", "date"
              "timestamp"
            when "string", "text", "phone", "currency"
              "string"
            when "boolean"
              "boolean"
            else
              "string"
            end

          {name: field[:name], type: type}
        end
      end

      def init
        # initialization code:
        @property1 = task["property1"]
        @property2 = task["property2"]
      end

      def run
        page_builder.add(["example-value", 1, 0.1])
        page_builder.add(["example-value", 2, 0.2])
        page_builder.finish

        commit_report = {}
        return commit_report
      end
    end
  end
end
