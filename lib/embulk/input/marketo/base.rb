require "embulk/input/marketo/timeslice"
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

        def self.resume(task, columns, count, &control)
          commit_reports = yield(task, columns, count)

          # NOTE: If this plugin supports to run by multi threads, this
          # implementation is terrible.
          next_config_diff = commit_reports.first
          return next_config_diff
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

        def init
          @last_updated_at = task[:last_updated_at]
          @columns = task[:columns]
          @soap = MarketoApi.soap_client(task, target)
        end

        private

        def preview?
          begin
            org.embulk.spi.Exec.isPreview()
          rescue java.lang.NullPointerException => e
            false
          end
        end

        def target
          self.class.target
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
      end
    end
  end
end
