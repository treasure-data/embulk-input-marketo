require "embulk/input/marketo/base"

module Embulk
  module Input
    module Marketo
      class Lead < Base
        include Timeslice

        PREVIEW_COUNT = 15

        Plugin.register_input("marketo/lead", self)

        def self.target
          :lead
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
          until_at = task[:until_at]
          options = {}
          options[:batch_size] = PREVIEW_COUNT if preview?

          soap.each(since_at, until_at, options) do |lead|
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
