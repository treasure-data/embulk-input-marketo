module Embulk
  module Input
    module Marketo
      module Timeslice
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

            if Time.parse(from_datetime) > Time.parse(to_datetime)
              raise ConfigError, "config: from_datetime '#{from_datetime}' is later than '#{to_datetime}'."
            end

            task = {
              endpoint_url: endpoint_url,
              wsdl_url: config.param(:wsdl, :string, default: "#{endpoint_url}?WSDL"),
              user_id: config.param(:user_id, :string),
              encryption_key: config.param(:encryption_key, :string),
              from_datetime: from_datetime,
              to_datetime: to_datetime,
              columns: config.param(:columns, :array)
            }

            columns = []

            task[:columns].each do |column|
              name = column["name"]
              type = column["type"].to_sym

              columns << Column.new(nil, name, type, column["format"])
            end

            # TODO tasks should be executed concurrently.
            resume(task, columns, 1, &control)
          end
        end
      end
    end
  end
end
