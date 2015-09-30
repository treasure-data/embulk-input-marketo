require "embulk/input/marketo_api"

module Embulk
  module Input
    module Marketo
      class Base < InputPlugin
        PREVIEW_COUNT = 15

        attr_reader :soap

        def self.target
          raise NotImplementedError
        end

        def self.soap_client(config)
          @soap ||=
            begin
              endpoint_url = config.param(:endpoint, :string)
              soap_config = {
                endpoint_url: endpoint_url,
                wsdl_url: config.param(:wsdl, :string, default: "#{endpoint_url}?WSDL"),
                user_id: config.param(:user_id, :string),
                encryption_key: config.param(:encryption_key, :string),
              }

              MarketoApi.soap_client(soap_config, target)
            end
        end

        def self.format_range(config)
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

          {
            from: from_datetime,
            to: to_datetime,
          }
        end

        def self.timeslice(from, to, count)
          generate_time_range(from, to).each_slice(count).to_a
        end

        def self.generate_time_range(from, to)
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

        private

        def preview?
          begin
            org.embulk.spi.Exec.isPreview()
          rescue java.lang.NullPointerException => e
            false
          end
        end

        def cast_value(column, value)
          return unless value

          case column["type"].to_s
          when "timestamp"
            begin
              Time.parse(value)
            rescue => e
              raise ConfigError, "Can't parse as Time '#{value}' (column is #{column["name"]})"
            end
          else
            value
          end
        end

        def target
          self.class.target
        end
      end
    end
  end
end
