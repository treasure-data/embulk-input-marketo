## 0.2.3 - 2015-09-14

* [enhancement] Catch config error [#33](https://github.com/treasure-data/embulk-input-marketo/pull/33)
* [enhancement] Concurrent worker [#31](https://github.com/treasure-data/embulk-input-marketo/pull/31)

## 0.2.2 - 2015-09-08

* [fixed] Fix handling for activity_date_time [#32](https://github.com/treasure-data/embulk-input-marketo/pull/32)

## 0.2.1 - 2015-09-01

* [fixed] activity_log: Avoid to cast values unexpectedly [#29](https://github.com/treasure-data/embulk-input-marketo/pull/29)
* [maintenance] Add a minor comment [#28](https://github.com/treasure-data/embulk-input-marketo/pull/28) [Requested by @muga. Thanks!!]
* [maintenance] Fix minor issues [#27](https://github.com/treasure-data/embulk-input-marketo/pull/27) [[Reviewed by @muga.](https://github.com/treasure-data/embulk-input-marketo/pull/25#issuecomment-135570967) Thanks!!]

## 0.2.0 - 2015-08-27

This version breaks backword compatibility of marketo/lead. Please check README.md to modify your config.

* [enhancement] Avoid timeout for marketo/lead input [#25](https://github.com/treasure-data/embulk-input-marketo/pull/25)
* [fixed] Fix timestamp column [#24](https://github.com/treasure-data/embulk-input-marketo/pull/24)
* [enhancement] Raise ConfigError if unretryable error occured [#23](https://github.com/treasure-data/embulk-input-marketo/pull/23)
* [fixed] Fix the bug that the default value for wsdl is broken [#22](https://github.com/treasure-data/embulk-input-marketo/pull/22)

## 0.1.1 - 2015-08-19

* [enhanement] Support scheduled execution [#20](https://github.com/treasure-data/embulk-input-marketo/pull/20) [[Reported by @muga](https://github.com/treasure-data/embulk-input-marketo/issues/18). Thanks!]
* [maintenance] Use everyleaf-embulk_helper [#19](https://github.com/treasure-data/embulk-input-marketo/pull/19)

## 0.1.0 - 2015-07-15

We implemented activity_log plugin for marketo, so config generated from 0.0.1 should be modified. Please check README.md to do it.

* [enhancement] Implement activity_log plugin [#13](https://github.com/treasure-data/embulk-input-marketo/pull/13) [#14](https://github.com/treasure-data/embulk-input-marketo/pull/14) [#15](https://github.com/treasure-data/embulk-input-marketo/pull/15)

## 0.0.1 - 2015-07-06

The first release!!
