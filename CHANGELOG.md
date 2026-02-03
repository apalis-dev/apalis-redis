# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0-rc.3] - 2026-02-03
- *bump*: Bump to v1.0.0-rc.3 (#35)

## [1.0.0-rc.2] - 2026-01-10
- *bump*: Bump to v1.0.0-rc.1 (#24)
- *ci(deps)*: Bump actions/upload-artifact from 4 to 5 (#3) 
- *deps(deps)*: bump serde_json from 1.0.147 to 1.0.148 (#26) 

## [0.7.4] - 2025-11-18

### 💼 Other

- To v0.7.4 (#632)
## [0.7.3] - 2025-08-14

### 🐛 Bug Fixes

- *(deps)* Update rust crate redis to 0.32 (#584)

### 💼 Other

- Apalis to v0.7.3 (#595)
## [0.7.2] - 2025-05-27

### 🐛 Bug Fixes

- *(apalis-redis)* Enable "allow-undeclared-keys" for dragonflydb in `stats.lua` (#567)
- *(deps)* Update rust crate redis to 0.31 (#555)

### 💼 Other

- Apalis to v0.7.2 (#580)
## [0.7.1] - 2025-04-24

### 💼 Other

- To v0.7.1 (#559)
## [0.7.0] - 2025-03-24

### 🐛 Bug Fixes

- *(deps)* Update rust crate redis to 0.28 (#495)
- Reenqueue oprphaned before starting streaming (#507)
- RedisStorage incorrect running_count stat (#506)
- *(deps)* Update rust crate redis to 0.29 and deadpool-redis to 0.20 (#527)

### 💼 Other

- Generic retry persist check (#498)
- Add associated types to the `Backend` trait (#516)
- To 0.7.0 (#536)

### 🧪 Testing

- Integration tests for aborting jobs and panicking workers (#508)
## [0.6.4] - 2025-01-15

### 🐛 Bug Fixes

- Handle vacuuming correctly for backends (#491)

### 💼 Other

- To v0.6.4 (#500)
## [0.6.3] - 2025-01-05

### 💼 Other

- To v0.6.3 (#490)

### ⚙️ Miscellaneous Tasks

- Improve examples (#484)
## [0.6.2] - 2024-12-15

### 💼 Other

- To v0.6.2 (#482)
## [0.6.1] - 2024-12-10

### 🐛 Bug Fixes

- Re-export redis ConnectionManager & RedisError (#465)
- Reenque_orphaned for RedisStorage (#468)
- Allow polling only when worker is ready (#472)

### 💼 Other

- To v0.6.1 (#479)

### ⚙️ Miscellaneous Tasks

- Remove unused dependencies (#469)
## [0.6.0] - 2024-11-28

### 🐛 Bug Fixes

- Target done jobs with vacuuming instead of dead jobs (#446)
- *(deps)* Update rust crate tower to 0.5 (#461)
- Minor ack bug on redis (#463)
## [0.5.5] - 2024-09-05

### 🐛 Bug Fixes

- Tower changes (#407)

### 💼 Other

- To 0.5.5 (#408)
## [0.5.4] - 2024-09-05

### 💼 Other

- 0.5.4 (#405)

### ⚙️ Miscellaneous Tasks

- Add repository to metadata (#345)
## [0.5.3] - 2024-05-27

### 💼 Other

- Be sure to keep track the job context (#289)
- To version 0.5.3 (#328)
## [0.5.2] - 2024-05-20

### 🐛 Bug Fixes

- *(deps)* Update rust crate async-trait to 0.1.79 (#274)
- *(deps)* Update rust crate async-trait to 0.1.80 (#293)
- *(deps)* Update rust crate redis to 0.25.3 (#310)
- *(deps)* Update rust crate chrono to 0.4.38 (#305)
- *(deps)* Update rust crate futures to 0.3.30 (#307)
- *(deps)* Update rust crate async-stream to 0.3.5 (#304)
- *(deps)* Update rust crate log to 0.4.21 (#308)

### 💼 Other

- To version 0.5.2 (#322)
## [0.5.1] - 2024-03-13

### 🐛 Bug Fixes

- *(deps)* Update rust crate redis to 0.25 (#260)

### 💼 Other

- To version 0.5.1 (#265)
## [0.4.9] - 2024-01-10

### 💼 Other

- To v0.4.9 (#232)
## [0.4.8] - 2024-01-05

### 🚀 Features

- Configurable worker set as dead (#220)
- [**breaking**] Configurable concurrency per worker  (#222)

### 🐛 Bug Fixes

- *(deps)* Update rust crate redis to 0.24 (#216)
- *(deps)* Update rust crate async-trait to 0.1.77 (#225)

### 💼 Other

- To v0.4.8 (#229)
## [0.4.7] - 2023-11-18

### 🐛 Bug Fixes

- Allow cargo build --all-features (#204)
## [0.4.6] - 2023-11-09

### 🐛 Bug Fixes

- *(deps)* Update rust crate async-trait to 0.1.74
## [0.4.5] - 2023-10-11

### 🐛 Bug Fixes

- *(deps)* Update rust crate async-trait to 0.1.73

### 💼 Other

- V0.4.5
## [0.4.4] - 2023-07-31

### 💼 Other

- To version v0.4.4
## [0.3.0] - 2022-06-05
