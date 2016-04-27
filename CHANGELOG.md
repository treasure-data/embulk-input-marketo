## 0.5.2 - 2016-04-27
* [enhancement] Make debug easier [#54](https://github.com/treasure-data/embulk-input-marketo/pull/54)
* [fixed] Recognize empty string as nil value [#53](https://github.com/treasure-data/embulk-input-marketo/pull/53)

## 0.5.1 - 2016-04-06

* [maintenance] Relax dependency version

## 0.5.0 - 2016-04-06

This version drops old Embulk supports. Embulk 0.8 or later is required since this version.

* [enhancement] Add tests for Embulk 0.8 and drop support old Embulk [#52](https://github.com/treasure-data/embulk-input-marketo/pull/52)
* [maintenance] Refactor lead and activity [#51](https://github.com/treasure-data/embulk-input-marketo/pull/51)
* [maintenance] Refactor retry [#50](https://github.com/treasure-data/embulk-input-marketo/pull/50)

## 0.4.0 - 2015-10-30

This version drops scheduled execution with marketo/lead.

* [enhancement] Append processed time column [#49](https://github.com/treasure-data/embulk-input-marketo/pull/49)
* [enhancement] Exponential backoff retry [#48](https://github.com/treasure-data/embulk-input-marketo/pull/48)
* [fixed] Fix preview didn't stop after fetched if multiple ranges have [#45](https://github.com/treasure-data/embulk-input-marketo/pull/45)
* [enhancement] activity_log: Use from..from+30m range for guess [#47](https://github.com/treasure-data/embulk-input-marketo/pull/47)
* [enhancement] Unsupport scheduled execution for lead [#46](https://github.com/treasure-data/embulk-input-marketo/pull/46) [#41](https://github.com/treasure-data/embulk-input-marketo/pull/41) [Reported by @muga. Thanks!]

## 0.3.2 - 2015-10-13

* [fixed] Prevent memoize in class [#44](https://github.com/treasure-data/embulk-input-marketo/pull/44)

## 0.3.1 - 2015-10-06

* [enhancement] Supports embulk.0.7 [#43](https://github.com/treasure-data/embulk-input-marketo/pull/43)
* [maintenance] Refactor [#40](https://github.com/treasure-data/embulk-input-marketo/pull/40)

## 0.3.0 - 2015-09-30

This version breaks backword compatibility of marketo/activity_log. Please check README.md to modify your config.

* [enhancement] Also activity_log uses from_datetime/to_datetime same as lead [#39](https://github.com/treasure-data/embulk-input-marketo/pull/39)

## 0.2.5 - 2015-09-28

* [fixed] lead: Fix the bug when `from_datetime` and `to_datetime` are same [#37](https://github.com/treasure-data/embulk-input-marketo/pull/37)

## 0.2.4 - 2015-09-17

* [enhancement] Retry to call API until 5 times when Timeout [#36](https://github.com/treasure-data/embulk-input-marketo/pull/36) [[Reported by @muga](https://github.com/treasure-data/embulk-input-marketo/issues/34). Thanks!]

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
