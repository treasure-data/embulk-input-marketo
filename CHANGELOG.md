## 0.6.24 - 2021-10-19
- Add retryable in case response data invalid json format

## 0.6.22 - 2021-08-17
- Support Program Members

## 0.6.21 - 2021-07-15
- Upgrade `embulk-*` to `v0.10.29`.
- Apply new lib `embulk-util-*`.
- Upgrade `embulk-base-restclient` to `v0.10.1`
- Upgrade Gradle to `6.6.1`.
- Apply `org.embulk.embulk-plugins` Gradle plugin.
- Use Java optional 8 in place of Guava
- Miscellaneous code cleanup.

## 0.6.20 - 2020-07-17
* [enhancement] Replace Joda-Time with java.time classes [101](https://github.com/treasure-data/embulk-input-marketo/pull/101)
* [enhancement] Use GitHub Action instead of Travis CI [101](https://github.com/treasure-data/embulk-input-marketo/pull/101)

## 0.6.19 - 2020-07-06
* [enhancement] Support import Lead/Member by input static List,Program IDs [100](https://github.com/treasure-data/embulk-input-marketo/pull/100)
* [enhancement] Support string comma-separated filterValues for Custom Objects [100](https://github.com/treasure-data/embulk-input-marketo/pull/100)

## 0.6.18 - 2020-01-06
* [enhancement] Support Marketo Partner API Key [#98](https://github.com/treasure-data/embulk-input-marketo/pull/98)

## 0.6.17 - 2019-12-03
* [hotfix] Fixed issue issue actTypeIds is required [#97](https://github.com/treasure-data/embulk-input-marketo/pull/97)

## 0.6.16 - 2019-12-03
* [enhancement] Added support for ActivityTypeIds filter PR [#96](https://github.com/treasure-data/embulk-input-marketo/pull/96)

## 0.6.15 - 2019-09-20
* [enhancement] Raise RuntimeException for temp file error [#94](https://github.com/treasure-data/embulk-input-marketo/pull/94)

## 0.6.14 - 2019-08-19
* [enhancement] Improve exception handling [#93](https://github.com/treasure-data/embulk-input-marketo/pull/93)

## 0.6.13 - 2019-01-21
* [enhance] Add more error code to retry [#91](https://github.com/treasure-data/embulk-input-marketo/pull/91)

## 0.6.12 - 2018-11-09
* [enhance] Implement Custom Object [#90](https://github.com/treasure-data/embulk-input-marketo/pull/90)

## 0.6.11 - 2018-09-10
* [enhance] Implement Assets Programs [#89](https://github.com/treasure-data/embulk-input-marketo/pull/89)

## 0.6.10 - 2018-05-28
* [fixed] Add included column option [#87](https://github.com/treasure-data/embulk-input-marketo/pull/87)

## 0.6.9 - 2018-04-16
* [fixed] Fix wrapped TimeoutException not retry [#85](https://github.com/treasure-data/embulk-input-marketo/pull/85)
* [enhance] Make read_timeout configurable [#85](https://github.com/treasure-data/embulk-input-marketo/pull/85)

## 0.6.8 - 2018-04-12
* [fixed] Fix incorrect incorrect retry logic [#84](https://github.com/treasure-data/embulk-input-marketo/pull/84)

## 0.6.7 - 2018-02-26
* [fixed] Remove de-duplication logic [#83](https://github.com/treasure-data/embulk-input-marketo/pull/83)

## 0.6.6 - 2018-01-30
* [fixed] Fix JettyRetryHelper not closed [#82](https://github.com/treasure-data/embulk-input-marketo/pull/82)
## 0.6.5 - 2017-12-19 [fixed] Fix infinite loop when import non bulk extract targets [#80](https://github.com/treasure-data/embulk-input-marketo/pull/80) ## 0.6.4 - 2017-12-13 [fixed] Fix incorrect job timeout calculation  [#78](https://github.com/treasure-data/embulk-input-marketo/pull/78)
* [enhance] Disable incremental import by updatedAt  [#77](https://github.com/treasure-data/embulk-input-marketo/pull/77)
* [enhance] Add log for exported file size  [#76](https://github.com/treasure-data/embulk-input-marketo/pull/76)


## 0.6.3 - 2017-11-13
* [enhance] Ignore records with timestamp smaller or equal to latest_fetch_time  [#74](https://github.com/treasure-data/embulk-input-marketo/pull/74)

## 0.6.2 - 2017-10-16
* [fixed] NullPointerException when building config diff [#73](https://github.com/treasure-data/embulk-input-marketo/pull/73)

## 0.6.1 - 2017-10-12
* [fixed] OutOfMemeory when run Lead by list, Lead by program   [#69](https://github.com/treasure-data/embulk-input-marketo/pull/69)
* [fixed] SOAP only field cause NullPointerException [#69](https://github.com/treasure-data/embulk-input-marketo/pull/69)
* [enhancement] Implement file download resume in Bulk extract [#69](https://github.com/treasure-data/embulk-input-marketo/pull/69)

## 0.6.0 - 2017-10-10
* [major] Migrate to Java by embulk-base-restclient [#66](https://github.com/treasure-data/embulk-input-marketo/pull/66)
* [major] Migrate to REST API [#66](https://github.com/treasure-data/embulk-input-marketo/pull/66)
* [major] Add 3 more target, Campaign, Lead by list and Lead by program [#66](https://github.com/treasure-data/embulk-input-marketo/pull/66)
* [major] Support Marketo bulk extract API for lead and activity targets [#66](https://github.com/treasure-data/embulk-input-marketo/pull/66)
* [major] Support incremental ingestion for lead and activity targets [#66](https://github.com/treasure-data/embulk-input-marketo/pull/66)

## 0.5.6 - 2016-12-14
* [maintenance] Enable tcp keepalive [#64](https://github.com/treasure-data/embulk-input-marketo/pull/64)

## 0.5.6 - 2016-12-14
* [maintenance] Enable tcp keepalive [#64](https://github.com/treasure-data/embulk-input-marketo/pull/64)

## 0.5.5 - 2016-11-24
* [fixed] Generate config_diff even if no records found [#62](https://github.com/treasure-data/embulk-input-marketo/pull/62)
* [maintenance] Fix to use CodeClimate 0.x [#63](https://github.com/treasure-data/embulk-input-marketo/pull/63)

## 0.5.4 - 2016-10-26
* [enhancement] Validate wsdl_url and endpoint_url are the valid form [#61](https://github.com/treasure-data/embulk-input-marketo/pull/61)
* [enhancement] Minor readme change [#58](https://github.com/treasure-data/embulk-input-marketo/pull/58)
* [fixed] Fix error when retrying on guess/preview [#59](https://github.com/treasure-data/embulk-input-marketo/pull/59)
* [enhancement] Try newer date at first on preview to avoid miss hit [#60](https://github.com/treasure-data/embulk-input-marketo/pull/60)

## 0.5.3 - 2016-07-01

* [enhancement] make concurrent limit exceeded error retryable [#56](https://github.com/treasure-data/embulk-input-marketo/pull/56)
* [maintenance] Gathering test coverage on CI [#55](https://github.com/treasure-data/embulk-input-marketo/pull/55)

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

* [enhancement] Support scheduled execution [#20](https://github.com/treasure-data/embulk-input-marketo/pull/20) [[Reported by @muga](https://github.com/treasure-data/embulk-input-marketo/issues/18). Thanks!]
* [maintenance] Use everyleaf-embulk_helper [#19](https://github.com/treasure-data/embulk-input-marketo/pull/19)

## 0.1.0 - 2015-07-15

We implemented activity_log plugin for marketo, so config generated from 0.0.1 should be modified. Please check README.md to do it.

* [enhancement] Implement activity_log plugin [#13](https://github.com/treasure-data/embulk-input-marketo/pull/13) [#14](https://github.com/treasure-data/embulk-input-marketo/pull/14) [#15](https://github.com/treasure-data/embulk-input-marketo/pull/15)

## 0.0.1 - 2015-07-06

The first release!!
