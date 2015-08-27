module MuteLogger
  private

  def mute_logger
    stub(Embulk).logger { ::Logger.new(IO::NULL) }
  end
end
