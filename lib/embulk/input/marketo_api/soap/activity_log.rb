require "embulk/input/marketo_api/soap/base"

module Embulk
  module Input
    module MarketoApi
      module Soap
        class ActivityLog < Base
          GUESS_DURATION = 60 * 30 # 30m

          def metadata(from_datetime, options={})
            activity_logs = []

            from = Time.parse(from_datetime.to_s)
            to = from + GUESS_DURATION
            options[:to] = to.to_s

            fetch_by_from_datetime(from_datetime, options) do |record|
              activity_logs << record
            end

            Guess::SchemaGuess.from_hash_records(activity_logs)
          end

          def each(from_datetime, options={}, &block)
            response = fetch_by_from_datetime(from_datetime, options, &block)
            while response[:remaining_count] > 0 do
              response = fetch_by_from_datetime(from_datetime, options.merge(offset: response[:offset]), &block)
            end

            response[:from_datetime]
          end

          private

          def fetch_by_from_datetime(from_datetime, options={}, &block)
            from = Time.parse(from_datetime.to_s).iso8601

            to =
              if options[:to]
                Time.parse(options[:to]).iso8601
              else
                Time.now.iso8601
              end

            request = {
              start_position: {
                oldest_created_at: from,
                latest_created_at: to,
              },
              batch_size: options[:batch_size] || 100
            }
            request[:start_position][:offset] = options[:offset] if options[:offset]

            Embulk.logger.info "Fetching from '#{from}' to '#{to}'..."
            fetch(request, options, &block)
          end

          def fetch(request, options={}, &block)
            response = savon_call(:get_lead_changes, {message: request}, options)
            remaining = response.body[:success_get_lead_changes][:result][:remaining_count].to_i
            Embulk.logger.info "Remaining records: #{remaining}"

            activities_list = response.body[:success_get_lead_changes][:result][:lead_change_record_list]

            if activities_list.nil?
              Embulk.logger.info "No record is fetched."
              return {remaining_count: 0, offset: nil, from_datetime: nil}
            end

            activities = activities_list[:lead_change_record].sort_by { |activity| Time.parse(activity[:activity_date_time]) }

            activities.each do |activity|
              record = {
                "id" => activity[:id],
                "activity_date_time" => activity[:activity_date_time],
                "activity_type" => activity[:activity_type],
                "mktg_asset_name" => activity[:mktg_asset_name],
                "mkt_person_id" => activity[:mkt_person_id],
              }

              activity[:activity_attributes][:attribute].each do |attributes|
                name = attributes[:attr_name]
                value = attributes[:attr_value]

                record[name] = value
              end

              block.call(record)
            end

            {
              remaining_count: remaining,
              offset: response.body[:success_get_lead_changes][:result][:new_start_position][:offset],
              from_datetime: activities.last[:activity_date_time]
            }
          end
        end
      end
    end
  end
end

