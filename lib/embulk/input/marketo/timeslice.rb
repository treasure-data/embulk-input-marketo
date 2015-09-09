module Embulk
  module Input
    module Marketo
      module Timeslice
        TIMESLICE_COUNT_PER_TASK = 24

        def self.included(klass)
          klass.extend ClassMethods
        end

        module ClassMethods
          def guess(config)
            if config.param(:last_updated_at, :string, default: nil)
              Embulk.logger.warn "config: last_updated_at is deprecated. Use from_datetime/to_datetime"
            end

            client = soap_client(config)
            metadata = client.metadata

            return {"columns" => generate_columns(metadata)}
          end

          def transaction(config, &control)
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

          def timeslice(from, to, count)
            generate_time_range(from, to).each_slice(count).to_a
          end

          def generate_time_range(from, to)
            # e.g. from = 2010-01-01 15:00, to = 2010-01-03 09:30
            # convert to such array:
            # [
            #   {from: 2010-01-01 15:00, to: 2010-01-01 16:00},
            #   {from: 2010-01-01 16:00, to: 2010-01-01 17:00},
            #   ...
            #   {from: 2010-01-03 08:00, to: 2010-01-03 09:00},
            #   {from: 2010-01-03 09:00, to: 2010-01-03 09:30},
            # ]
            # to fetch data from Marketo API with each day as
            # desribed on official blog:
            # http://developers.marketo.com/blog/performance-tuning-api-requests/
            to ||= Time.now
            from = Time.parse(from) unless from.is_a?(Time)
            to = Time.parse(to) unless to.is_a?(Time)

            result = []
            since = from
            while since < to
              next_since = since + 3600
              if to < next_since
                next_since = to
              end
              result << {
                "from" => since,
                "to" => next_since
              }
              since = next_since
            end
            result
          end

          def resume(task, columns, count, &control)
            commit_reports = yield(task, columns, count)

            # all task returns same report as {from_datetime: to_datetime}
            return commit_reports.first
          end

        end
      end
    end
  end
end
