module Embulk
  module Input
    module MarketoApi
      module Soap
        module Timeslice
          private

          def generate_time_range(from, to = nil)
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
                from: since,
                to: next_since
              }
              since = next_since
            end
            result
          end
        end
      end
    end
  end
end
