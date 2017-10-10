Embulk::JavaPlugin.register_input(
  "marketo", "org.embulk.input.marketo.MarketoInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
