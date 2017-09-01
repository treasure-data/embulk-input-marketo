require "embulk/input/marketo/base"

module Embulk
  module Input
    module Marketo
      class ActivityLog < Base
        BATCH_SIZE_DEFAULT = 100

        Plugin.register_input("marketo/activity_log", self) # for compatibility
        Plugin.register_input("marketo_activity_log", self)

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
            retry_initial_wait_sec: config.param(:retry_initial_wait_sec, :integer, default: 1),
            retry_limit: config.param(:retry_limit, :integer, default: 5),
            columns: config.param(:columns, :array)
          }
          validate_url(task[:endpoint_url], "endpoint")
          validate_url(task[:wsdl_url], "wsdl")

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
          @options = {
            retry_initial_wait_sec: task[:retry_initial_wait_sec],
            retry_limit: task[:retry_limit],
            to: task[:to_datetime],
            batch_size: (preview? ? PREVIEW_COUNT : BATCH_SIZE_DEFAULT),
          }
        end

        def run
          counter = 0
          latest_updated_at = @soap.each(task[:from_datetime], @options) do |activity_log|
            page_builder.add(format_record(activity_log))
            break if preview? && (counter += 1) >= PREVIEW_COUNT
          end

          page_builder.finish

          task_report = {}
          if !preview?
            from_datetime = latest_updated_at || task[:from_datetime]
            if from_datetime
              task_report = {
                from_datetime: from_datetime
              }
            end
          end

          return task_report
        end

        def format_record(activity_log)
          @columns.map do |column|
            name = column["name"].to_s
            value = activity_log[name]
            cast_value(column, value)
          end
        end
      end
    end
  end
end
