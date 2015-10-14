require "embulk/input/marketo/base"

module Embulk
  module Input
    module Marketo
      class Lead < Base
        TIMESLICE_COUNT_PER_TASK = 24

        Plugin.register_input("marketo/lead", self)

        def self.target
          :lead
        end

        def self.guess(config)
          if config.param(:last_updated_at, :string, default: nil)
            Embulk.logger.warn "config: last_updated_at is deprecated. Use from_datetime/to_datetime"
          end

          client = soap_client(config)
          metadata = client.metadata

          return {"columns" => generate_columns(metadata)}
        end

        def self.resume(task, columns, count, &control)
          task_reports = yield(task, columns, count)

          # When no task ran, task_reports is empty
          return {} if task_reports.empty?
          # all task returns same report as {from_datetime: to_datetime}
          return task_reports.first
        end

        def self.transaction(config, &control)
          endpoint_url = config.param(:endpoint, :string)

          range = format_range(config)

          ranges = timeslice(range[:from], range[:to], TIMESLICE_COUNT_PER_TASK)
          task = {
            endpoint_url: endpoint_url,
            wsdl_url: config.param(:wsdl, :string, default: "#{endpoint_url}?WSDL"),
            user_id: config.param(:user_id, :string),
            encryption_key: config.param(:encryption_key, :string),
            from_datetime: range[:from],
            to_datetime: range[:to],
            ranges: ranges,
            columns: config.param(:columns, :array)
          }

          resume(task, embulk_columns(config), ranges.size, &control)
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
              when "datetime", "date"
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

        def init
          @columns = task[:columns]
          @ranges = task[:ranges][index]
          @soap = MarketoApi.soap_client(task, target)
        end

        def run
          options = {}
          options[:batch_size] = PREVIEW_COUNT if preview?

          counter = 0
          catch(:finish) do
            @ranges.each do |range|
              soap.each(range, options) do |lead|
                values = @columns.map do |column|
                  name = column["name"].to_s
                  value = (lead[name] || {})[:value]
                  cast_value(column, value)
                end

                page_builder.add(values)
                throw(:finish) if preview? && (counter += 1) >= PREVIEW_COUNT
              end
            end
          end

          page_builder.finish

          task_report = {
            from_datetime: task[:to_datetime]
          }
          return task_report
        end
      end
    end
  end
end
