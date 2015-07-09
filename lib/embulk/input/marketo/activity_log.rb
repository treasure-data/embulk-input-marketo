require "embulk/input/marketo_api"

module Embulk
  module Input
    module Marketo
      class Lead < InputPlugin
        Plugin.register_input("marketo/activity_log", self)
      end
    end
  end
end
