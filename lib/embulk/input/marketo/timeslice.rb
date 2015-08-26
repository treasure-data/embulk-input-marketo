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
              Embulk.logger.warn "config: last_updated_at is deprecated. Use since_at/until_at"
            end

            client = soap_client(config)
            metadata = client.metadata

            return {"columns" => generate_columns(metadata)}
          end

          def transaction(config, &control)
            endpoint_url = config.param(:endpoint, :string)

            if config.param(:last_updated_at, :string, default: nil)
              Embulk.logger.warn "config: last_updated_at is deprecated. Use since_at/until_at"
            end

            since_at = config.param(:since_at, :string)
            until_at = config.param(:until_at, :string, default: Time.now.to_s)

            if Time.parse(since_at) > Time.parse(until_at)
              raise ConfigError, "config: since_at '#{since_at}' is later than '#{until_at}'."
            end

            task = {
              endpoint_url: endpoint_url,
              wsdl_url: config.param(:wsdl, :string, default: "#{endpoint_url}?WSDL"),
              user_id: config.param(:user_id, :string),
              encryption_key: config.param(:encryption_key, :string),
              since_at: since_at,
              until_at: until_at,
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
        end
      end
    end
  end
end
