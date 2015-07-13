require "embulk/input/marketo_api"

module Embulk
  module Input
    module Marketo
      class Base < InputPlugin
        PREVIEW_COUNT = 15

        def self.target
        end

        def self.transaction(config, &control)
          endpoint_url = config.param(:endpoint, :string)

          task = {
            endpoint_url: endpoint_url,
            wsdl_url: config.param(:wsdl, :string, default: "#{endpoint_url}?WSDL"),
            user_id: config.param(:user_id, :string),
            encryption_key: config.param(:encryption_key, :string),
            last_updated_at: config.param(:last_updated_at, :string),
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

        def self.resume(task, columns, count, &control)
          commit_reports = yield(task, columns, count)

          next_config_diff = {}
          return next_config_diff
        end

        def self.soap_client(config)
          @soap ||=
            begin
              endpoint_url = config.param(:endpoint, :string),
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
          @soap = MarketoApi.soap_client(task, self.class.target)
        end

        def self.logger
          Embulk.logger
        end

        def logger
          self.class.logger
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
      end
    end
  end
end
