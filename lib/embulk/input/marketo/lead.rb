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
          commit_reports = yield(task, columns, count)

          # When no task ran, commit_reports is empty
          return {} if commit_reports.empty?
          # all task returns same report as {from_datetime: to_datetime}
          return commit_reports.first
        end

        def self.transaction(config, &control)
          endpoint_url = config.param(:endpoint, :string)

          if config.param(:last_updated_at, :string, default: nil)
            Embulk.logger.warn "config: last_updated_at is deprecated. Use from_datetime/to_datetime"
          end

          from_datetime = config.param(:from_datetime, :string)
          to_datetime = config.param(:to_datetime, :string, default: Time.now.to_s)

          # check from/to format to parse
          begin
            Time.parse(from_datetime)
            Time.parse(to_datetime)
          rescue => e
            # possibly Time.parse fail
            raise ConfigError, e.message
          end

          if Time.parse(from_datetime) > Time.parse(to_datetime)
            raise ConfigError, "config: from_datetime '#{from_datetime}' is later than '#{to_datetime}'."
          end

          ranges = timeslice(from_datetime, to_datetime, TIMESLICE_COUNT_PER_TASK)
          task = {
            endpoint_url: endpoint_url,
            wsdl_url: config.param(:wsdl, :string, default: "#{endpoint_url}?WSDL"),
            user_id: config.param(:user_id, :string),
            encryption_key: config.param(:encryption_key, :string),
            from_datetime: from_datetime,
            to_datetime: to_datetime,
            ranges: ranges,
            columns: config.param(:columns, :array)
          }

          columns = []

          task[:columns].each do |column|
            name = column["name"]
            type = column["type"].to_sym

            columns << Column.new(nil, name, type, column["format"])
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
          @ranges = task[:ranges][index]
          @soap = MarketoApi.soap_client(task, target)
        end

        def run
          from_datetime = task[:from_datetime]
          to_datetime = task[:to_datetime] || Time.now

          options = {}
          options[:batch_size] = PREVIEW_COUNT if preview?

          @ranges.each do |range|
            soap.each(range, options) do |lead|
              values = @columns.map do |column|
                name = column["name"].to_s
                value = (lead[name] || {})[:value]
                cast_value(column, value)
              end

              page_builder.add(values)
            end
          end

          page_builder.finish

          commit_report = {
            from_datetime: to_datetime
          }
          return commit_report
        end
      end
    end
  end
end
