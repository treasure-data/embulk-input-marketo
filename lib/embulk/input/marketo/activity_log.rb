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

        def run
          if preview?
            batch_size = PREVIEW_COUNT
          else
            batch_size = 100
          end

          count = 0

          @soap.each(@last_updated_at, batch_size: batch_size) do |activity_log|
            values = @columns.map do |column|
              name = column["name"].to_s
              activity_log[name]
            end

            page_builder.add(values)
            count += 1
            break if preview? && count >= PREVIEW_COUNT
          end

          page_builder.finish

          commit_report = {}
          return commit_report
        end
      end
    end
  end
end
