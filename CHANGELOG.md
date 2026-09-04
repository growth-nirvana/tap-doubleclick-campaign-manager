# Changelog

## 1.5.6
  * Deactivate expired active report schedules before updating date ranges to avoid CM360 "Active schedules can't have a past expiration date" errors

## 1.5.5
  * Reuse existing QUEUED/PROCESSING report files for the same date range instead of submitting duplicate runs on job retry

## 1.5.4
  * Increase report file polling timeout from 1 hour to 5 hours while waiting for CM360 QUEUED/PROCESSING reports

## 1.5.3
  * Skip reports during discover that cannot be converted to catalog streams instead of failing the entire discover run

## 1.5.2
  * Paginate through all CM360 reports during discover instead of returning only the first page (default page size of 10)

## 1.5.1
  * Retry transient Google API errors (HTTP 503/500/backendError) while polling and downloading report files
  * Back off while report files are in `PROCESSING`, not only `QUEUED`

## 1.4.1
  * Explicitly tolerate file status of 'QUEUED' [#27](https://github.com/singer-io/tap-doubleclick-campaign-manager/pull/27)

## 1.4.0
  * Bump API version from 3.5 -> 4 [#23](https://github.com/singer-io/tap-doubleclick-campaign-manager/pull/23)

## 1.3.0
  * Bump API version from 3.3 -> 3.5 [#20](https://github.com/singer-io/tap-doubleclick-campaign-manager/pull/20)

## 1.2.0
  * Bump API version to 3.3 [#11](https://github.com/singer-io/tap-doubleclick-campaign-manager/pull/11)

## 1.1.0
  * Bump API version from 3.1 -> 3.2

## 1.0.0
  * General release
