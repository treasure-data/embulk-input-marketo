require "embulk/input/marketo/base"

module Embulk
  module Input
    module Marketo
      class ActivityLog < Base
        BATCH_SIZE_DEFAULT = 100

        Plugin.register_input("marketo/activity_log", self)

        def self.target
          :activity_log
        end

        def self.resume(task, columns, count, &control)
          task_reports = yield(task, columns, count)

          # NOTE: If this plugin supports to run by multi threads, this
          # implementation is terrible.
          next_config_diff = task_reports.first
          return next_config_diff
        end

        def self.transaction(config, &control)
          endpoint_url = config.param(:endpoint, :string)

          range = format_range(config)

          task = {
            endpoint_url: endpoint_url,
            wsdl_url: config.param(:wsdl, :string, default: "#{endpoint_url}?WSDL"),
            user_id: config.param(:user_id, :string),
            encryption_key: config.param(:encryption_key, :string),
            from_datetime: range[:from],
            to_datetime: range[:to],
            columns: config.param(:columns, :array)
          }

          resume(task, embulk_columns(config), 1, &control)
        end

        def self.guess(config)
          client = soap_client(config)
          range = format_range(config)

          schema = client.metadata(range[:from], batch_size: PREVIEW_COUNT)
          columns = schema.map do |c|
            column = {name: c.name, type: c.type}
            column[:format] = c.format if c.format
            column
          end

          return {"columns" => columns}
        end

        def init
          @columns = task[:columns]
          @soap = MarketoApi.soap_client(task, target)
        end

        def run
          options = {
            to: task[:to_datetime],
            batch_size: (preview? ? PREVIEW_COUNT : BATCH_SIZE_DEFAULT),
          }

          counter = 0
          latest_updated_at = @soap.each(task[:from_datetime], options) do |activity_log|
            values = @columns.map do |column|
              name = column["name"].to_s
              value = activity_log[name]
              cast_value(column, value)
            end

            page_builder.add(values)
            break if preview? && (counter += 1) >= PREVIEW_COUNT
          end

          page_builder.finish

          task_report = {}
          if !preview? && latest_updated_at
            task_report = {from_datetime: latest_updated_at}
          end

          return task_report
        end
      end
    end
  end
end
