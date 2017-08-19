### Are all connections created by the plugin secure?

- [ ] Does it opt secure communication standard? Such as HTTPS, SSH, SFTP, SMTP STARTTLS. If not check with CISO to decide we can deploy the plugin.
- [ ] Does support both authentication and encryption appropriately? Such as: "just encrypting without authentication" that is insecure.

### Does the plugin connect only to its expected external site which the customer explicitly set in their config file?

- [ ] Does NOT connect unexpected external site and our internal endpoints? Such as: “v3/job/:id/set_started” callback endpoint.

### Does NOT the plugin persist any customers' private information? Identify the private information beforehand.

- [ ] Does NOT include them in (temporary) files?
- [ ] Does NOT include them in log messages and exception messages?

### What kind of environments does the plugin interact with?

- [ ] Does NOT execute any shell command?
- [ ] Does NOT read any files on the running instance? Such as: "/etc/passwords". It’s ok to read temporary files that the plugin wrote.
- [ ] Does use to create temporary files by spi.TempFileSpace utility to avoid the conflict of the file names.
- [ ] Does NOT get environment variables or JVM system properties at runtime? Such as AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in environment variables

### Does NOT the plugin use insecure libraries?

- [ ] Line up all depending library so that we can identify the impact of security incident of those library if any.
- [ ] Check libraries usage of the plugin; all security check list must apply to the library usages. Such as "Are all connections created by the library secure?"

### Does NOT the plugin source code repository contain kinds of credentials

- [ ] API keys
- [ ] Passwords

### Make sure to free up all resources allocated during Embulk transaction “committing” or “rolling back”or before.

- [ ] Network (connections, pooled connections)
- [ ] Memory (cache in static variables)
- [ ] File (temporary files)
- [ ] CPU (threads, processes)