require "embulk/input/marketo/base"

module Embulk
  module Input
    module Marketo
      class ActivityLog < Base
        Plugin.register_input("marketo/activity_log", self)

        def self.target
          :activity_log
        end

        def self.guess(config)
          client = soap_client(config)
          last_updated_at = config.param(:last_updated_at, :string)

          metadata = client.activity_log_metadata(last_updated_at)

          schema = Guess::SchemaGuess.from_hash_records(metadata)

          columns = schema.map do |c|
            column = {name: c.name, type: c.type}
            column[:format] = c.format if c.format
            column
          end

          return {"columns" => columns}
        end
      end
    end
  end
end
