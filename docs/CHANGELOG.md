 Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.9-metrics] - 2020-10-07

### Changed
- Datadog metrics.

## [0.2.8] - 2020-10-02

### Fixed
- ExpiredTokenException handling improvement.

## [0.2.7] - 2020-10-02

### Fixed
- ExpiredTokenException handling.

## [0.2.6] - 2020-09-28

### Changed
- Termination logic is now based on a DB column named `terminate_on`
- EMR credentials are no longer needed in order to terminate a cluster. LDAP login should suffice.

### Removed
- DELETE for `step_clusters`

## [0.2.5] - 2020-09-16

### Changed
- `update_status()` now sets clusters' and steps' status to `NO_UPDATE` instead of `FAILED` and `ERROR` when it fails to update.

## [0.2.4] - 2020-09-15

### Removed
- BigQ publish. Now steps must wait till manager picks them up.

## [0.2.3] - 2020-09-10

### Fixed
- `inserted_on` and `updated_on` now use `datetime.utcnow`.
- `step.cancel()` now works. 

## [0.2.2] - 2020-09-07

### Fixed
- `inserted_on` and `updated_on` now use `server_default`.


## [0.2.1] - 2020-08-27

### Fixed
- /config/\<config\_name\> GET no longer returns 500.

### Added
- Login required to /config/\<config\_name\> GET.


## [0.2.0] - 2020-08-21

### Added
- First development release of the second version of the API.
- Usage metrics were added
- Fully customizable clusters configuration
- Cluster configuration service
- AD login & authentication
