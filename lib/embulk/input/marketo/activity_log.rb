require "embulk/input/marketo/base"

module Embulk
  module Input
    module Marketo
      class Lead < Base
        Plugin.register_input("marketo/activity_log", self)

      end
    end
  end
end
