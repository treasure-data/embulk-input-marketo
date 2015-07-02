require "embulk/input/marketo_api"

module Embulk
  module Input
    class Marketo < InputPlugin
      Plugin.register_input("marketo", self)

      def self.transaction(config, &control)
        endpoint_url = config.param(:endpoint, :string)

        task = {
          endpoint_url: endpoint_url,
          wsdl_url: config.param(:wsdl, :string, default: "#{endpoint_url}?WSDL"),
          user_id: config.param(:user_id, :string),
          encryption_key: config.param(:encryption_key, :string),
          last_updated_at: config.param(:last_updated_at, :string),
          columns: config.param(:columns, :array)
        }

        columns = []

        task[:columns].each do |column|
          name = column["name"]
          type = column["type"].to_sym

          columns << Column.new(nil, name, type, column["format"])
        end

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

        # TODO: Add id, email
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
            when "float"
              "double"
            else
              "string"
            end

          {name: field[:name], type: type}
        end
      end

      def init
        # TODO: integrate process to generate SOAP client to one method
        endpoint_url = task[:endpoint_url]
        wsdl_url = task[:wsdl_url]
        user_id = task[:user_id]
        encryption_key = task[:encryption_key]

        @last_updated_at = task[:last_updated_at]
        @columns = task[:columns]
        @soap = MarketoApi.soap_client(task)
      end

      def run
        # TODO: preview
        @soap.each_leads(@last_updated_at) do |lead|
          values = @columns.map do |column|
            name = column["name"].to_s
            (lead[name] || {})[:value]
          end

          page_builder.add(values)
        end

        page_builder.finish

        commit_report = {}
        return commit_report
      end
    end
  end
end
