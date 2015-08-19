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

          schema = client.metadata(last_updated_at, batch_size: PREVIEW_COUNT)
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

          last_updated_at = @soap.each(@last_updated_at, batch_size: batch_size) do |activity_log|
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
          if !preview? && last_updated_at
            commit_report = {last_updated_at: last_updated_at}
          end

          return commit_report
        end
      end
    end
  end
end
