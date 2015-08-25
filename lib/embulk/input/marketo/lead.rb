require "embulk/input/marketo/base"

module Embulk
  module Input
    module Marketo
      class Lead < Base
        PREVIEW_COUNT = 15

        Plugin.register_input("marketo/lead", self)

        def self.target
          :lead
        end

        def self.transaction(config, &control)
          endpoint_url = config.param(:endpoint, :string)

          task = {
            endpoint_url: endpoint_url,
            wsdl_url: config.param(:wsdl, :string, default: "#{endpoint_url}?WSDL"),
            user_id: config.param(:user_id, :string),
            encryption_key: config.param(:encryption_key, :string),
            until_at: config.param(:until_at, :string, default: nil),
            since_at: config.param(:since_at, :string, default: nil),
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

        def self.guess(config)
          client = soap_client(config)
          metadata = client.metadata

          return {"columns" => generate_columns(metadata)}
        end

        def self.generate_columns(metadata)
          columns = [
            {name: "id", type: "long"},
            {name: "email", type: "string"},
          ]

          metadata.each do |field|
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

            columns << {name: field[:name], type: type}
          end

          columns
        end

        def run
          count = 0
          since_at = task[:since_at]
          until_at = task[:until_at] || Time.now.to_s
          @soap.each(since_at, until_at) do |lead|
            values = @columns.map do |column|
              name = column["name"].to_s
              (lead[name] || {})[:value]
            end

            page_builder.add(values)

            count += 1
            break if preview? && count >= PREVIEW_COUNT
          end

          page_builder.finish

          commit_report = {since_at: until_at}
          return commit_report
        end
      end
    end
  end
end
