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

          return {}
        end

        def self.transaction(config, &control)
          endpoint_url = config.param(:endpoint, :string)

          range = format_range(config)
          ranges = timeslice(range[:from], range[:to], TIMESLICE_COUNT_PER_TASK)

          append_processed_time_column = config.param(:append_processed_time_column, :bool, default: true)

          task = {
            endpoint_url: endpoint_url,
            wsdl_url: config.param(:wsdl, :string, default: "#{endpoint_url}?WSDL"),
            user_id: config.param(:user_id, :string),
            encryption_key: config.param(:encryption_key, :string),
            from_datetime: range[:from],
            to_datetime: range[:to],
            ranges: ranges,
            retry_initial_wait_sec: config.param(:retry_initial_wait_sec, :integer, default: 1),
            retry_limit: config.param(:retry_limit, :integer, default: 5),
            append_processed_time_column: append_processed_time_column,
            columns: config.param(:columns, :array),
          }

          columns = embulk_columns(config)
          if append_processed_time_column
            processed_time_column = Column.new(nil, :processed_time, :timestamp, "%Y-%m-%dT%H:%M:%S%z")
            columns << processed_time_column
          end

          resume(task, columns, ranges.size, &control)
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
          if preview?
            # Try newer date at first to reduce cache miss hit
            @ranges = task[:ranges].flatten.sort_by{|range| Time.parse(range["to"])}.reverse
          else
            @ranges = task[:ranges][index]
          end
          @soap = MarketoApi.soap_client(task, target)
          @append_processed_time_column = task[:append_processed_time_column]
          @options = {
            retry_initial_wait_sec: task[:retry_initial_wait_sec],
            retry_limit: task[:retry_limit],
          }
          @options[:batch_size] = PREVIEW_COUNT if preview?
        end

        def run
          counter = 0
          catch(:finish) do
            @ranges.each do |range|
              soap.each(range, @options) do |lead|
                page_builder.add(format_record(lead, range))
                throw(:finish) if preview? && (counter += 1) >= PREVIEW_COUNT
              end
            end
          end

          page_builder.finish

          task_report = {}
          return task_report
        end

        def format_record(lead, range)
          values = @columns.map do |column|
            name = column["name"].to_s
            value = (lead[name] || {})[:value]
            cast_value(column, value)
          end

          if @append_processed_time_column
            values << Time.parse(range["from"])
          end
          values
        end
      end
    end
  end
end
