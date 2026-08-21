# Changelog

## [0.3.3](https://github.com/NatLabRockies/r2x-plexos/compare/v0.3.2...v0.3.3) (2026-08-21)


### 📚 Documentation

* update documentation and package features ([#93](https://github.com/NatLabRockies/r2x-plexos/issues/93)) ([be64547](https://github.com/NatLabRockies/r2x-plexos/commit/be64547d97dbe3fd57e5c75d9d449c4eda888e1f))

## [0.3.2](https://github.com/NatLabRockies/r2x-plexos/compare/v0.3.1...v0.3.2) (2026-08-13)


### 🐛 Bug Fixes

* update fixed load to match correctly its time series ([#89](https://github.com/NatLabRockies/r2x-plexos/issues/89)) ([5d44029](https://github.com/NatLabRockies/r2x-plexos/commit/5d44029bd222e51dd0fc37955e75f6f1de13418d))

## [0.3.1](https://github.com/NatLabRockies/r2x-plexos/compare/v0.3.0...v0.3.1) (2026-08-11)


### 🐛 Bug Fixes

* export generator fixed load time series ([7b90e74](https://github.com/NatLabRockies/r2x-plexos/commit/7b90e7478deeee07b3d160fc710c96ea1c6fc617))
* Export generator fixed-load timeseries to PLEXOS ([#87](https://github.com/NatLabRockies/r2x-plexos/issues/87)) ([7b90e74](https://github.com/NatLabRockies/r2x-plexos/commit/7b90e7478deeee07b3d160fc710c96ea1c6fc617))
* resolve package paths to original source ([#85](https://github.com/NatLabRockies/r2x-plexos/issues/85)) ([51a9328](https://github.com/NatLabRockies/r2x-plexos/commit/51a93284ddeffa3a0964c780cd3c5565b9bd6e8f))
* update solve year matching on time series linking ([#88](https://github.com/NatLabRockies/r2x-plexos/issues/88)) ([d3604cc](https://github.com/NatLabRockies/r2x-plexos/commit/d3604cc62c60c7b20eb24b2a3a078940469aa97c))
* update timestamp handling for dynamic time series entry ([#83](https://github.com/NatLabRockies/r2x-plexos/issues/83)) ([84c693b](https://github.com/NatLabRockies/r2x-plexos/commit/84c693b6eda93910da9b4d1c4fb4023385648418))


### 📦 Build

* **deps-dev:** Bump prek from 0.2.30 to 0.4.10 ([#79](https://github.com/NatLabRockies/r2x-plexos/issues/79)) ([af1d00c](https://github.com/NatLabRockies/r2x-plexos/commit/af1d00c2762a37865877f6e7384786fb7f73a8fc))
* **deps-dev:** Update uv-build requirement ([5e9e3d0](https://github.com/NatLabRockies/r2x-plexos/commit/5e9e3d04b4490591726e54b6f2f2a12c7fc9417e))
* **deps-dev:** Update uv-build requirement from &lt;0.9.0,&gt;=0.8.22 to &gt;=0.8.22,&lt;0.12.0 ([#78](https://github.com/NatLabRockies/r2x-plexos/issues/78)) ([5e9e3d0](https://github.com/NatLabRockies/r2x-plexos/commit/5e9e3d04b4490591726e54b6f2f2a12c7fc9417e))
* **deps:** Bump actions/checkout from 7.0.0 to 7.0.1 ([#80](https://github.com/NatLabRockies/r2x-plexos/issues/80)) ([44c89e3](https://github.com/NatLabRockies/r2x-plexos/commit/44c89e3d5af2783cc2c5cae9898cc8b2cf1e8cc4))
* **deps:** Bump actions/labeler from 6.2.0 to 7.0.0 ([#81](https://github.com/NatLabRockies/r2x-plexos/issues/81)) ([32fef51](https://github.com/NatLabRockies/r2x-plexos/commit/32fef518219ad7c50e2eb8e256de37f595c3d0d4))
* **deps:** Bump actions/setup-python from 6 to 7 ([#82](https://github.com/NatLabRockies/r2x-plexos/issues/82)) ([7670307](https://github.com/NatLabRockies/r2x-plexos/commit/7670307e1f96ba85430073c22ed612aab224f26e))
* **deps:** Bump pypa/gh-action-pypi-publish from 1.14.0 to 1.14.2 ([#86](https://github.com/NatLabRockies/r2x-plexos/issues/86)) ([36dbcf9](https://github.com/NatLabRockies/r2x-plexos/commit/36dbcf994106c806b9b98c7073c6f482aa089e44))

## [0.3.0](https://github.com/NatLabRockies/r2x-plexos/compare/v0.2.0...v0.3.0) (2026-07-17)


### 🚀 Features

* move master files to plexosdb to support more plexos versions on export ([#76](https://github.com/NatLabRockies/r2x-plexos/issues/76)) ([3d1a11f](https://github.com/NatLabRockies/r2x-plexos/commit/3d1a11f77bdd783707732f2d954e9e84c1c670cc))


### 🐛 Bug Fixes

* defaults addition and fixes ([#70](https://github.com/NatLabRockies/r2x-plexos/issues/70)) ([b3854ec](https://github.com/NatLabRockies/r2x-plexos/commit/b3854ec8f2fe698641f41f081512443276644d18))
* resolve serialization issue because of cli process ([#72](https://github.com/NatLabRockies/r2x-plexos/issues/72)) ([b6676bb](https://github.com/NatLabRockies/r2x-plexos/commit/b6676bba5494cab769f86327c5be89e5d1a23279))
* update timestamp year from reeds to plexos translation with horizon/solve year ([#74](https://github.com/NatLabRockies/r2x-plexos/issues/74)) ([76c319f](https://github.com/NatLabRockies/r2x-plexos/commit/76c319f98c1c9057c9ee161b4cc6ad06e5dc2a18))


### 📦 Build

* **deps-dev:** Bump sphinx-reports from 0.9.9 to 0.10.0 ([#68](https://github.com/NatLabRockies/r2x-plexos/issues/68)) ([db748eb](https://github.com/NatLabRockies/r2x-plexos/commit/db748eb697c2a23f7d47225f557b108438a49ed8))
* **deps:** Bump actions/checkout from 4 to 7 ([#66](https://github.com/NatLabRockies/r2x-plexos/issues/66)) ([31aedde](https://github.com/NatLabRockies/r2x-plexos/commit/31aeddec17476fe9615f827186a262611a4345fd))
* **deps:** bump actions/labeler from 6.1.0 to 6.2.0 ([#75](https://github.com/NatLabRockies/r2x-plexos/issues/75)) ([e34e819](https://github.com/NatLabRockies/r2x-plexos/commit/e34e819b943455170e122ad608f0fbf6aa9634ec))
* **deps:** bump actions/setup-python from 5 to 6 ([#73](https://github.com/NatLabRockies/r2x-plexos/issues/73)) ([ea5580f](https://github.com/NatLabRockies/r2x-plexos/commit/ea5580fa93a490ac9d081456a5127f2829167b7e))
* **deps:** Bump googleapis/release-please-action from 4.4.0 to 5.0.0 ([#67](https://github.com/NatLabRockies/r2x-plexos/issues/67)) ([105ba57](https://github.com/NatLabRockies/r2x-plexos/commit/105ba57ea3146b65c737212f7c1c7d60c09839e8))

## [0.2.0](https://github.com/NatLabRockies/r2x-plexos/compare/v0.1.5...v0.2.0) (2026-06-23)


### 🚀 Features

* add purchaser model for new loads translation ([#59](https://github.com/NatLabRockies/r2x-plexos/issues/59)) ([3eab1dc](https://github.com/NatLabRockies/r2x-plexos/commit/3eab1dcac120bc6e8866d4bff55aaa924f19108d))
* resolve EI translation issues updating exporter codebase ([#65](https://github.com/NatLabRockies/r2x-plexos/issues/65)) ([d82ce2f](https://github.com/NatLabRockies/r2x-plexos/commit/d82ce2f9a04425f90a61b0a37aa15977daf8d0db))
* update current code base to handle EI exporter changes ([#60](https://github.com/NatLabRockies/r2x-plexos/issues/60)) ([951aa55](https://github.com/NatLabRockies/r2x-plexos/commit/951aa5563f4aa4db5a8f41598b69757ad97ec78e))


### 📦 Build

* **deps-dev:** Bump pre-commit from 4.3.0 to 4.5.1 ([#57](https://github.com/NatLabRockies/r2x-plexos/issues/57)) ([0eb9181](https://github.com/NatLabRockies/r2x-plexos/commit/0eb91815008e553b881ba2302e4d50f83dbe79c9))
* **deps-dev:** Bump ruff from 0.15.9 to 0.15.10 ([#58](https://github.com/NatLabRockies/r2x-plexos/issues/58)) ([2139218](https://github.com/NatLabRockies/r2x-plexos/commit/213921828bac6ec14f3ed6f67c6d73f48a440751))
* **deps:** Bump actions/labeler from 6.0.1 to 6.1.0 ([#61](https://github.com/NatLabRockies/r2x-plexos/issues/61)) ([773d4be](https://github.com/NatLabRockies/r2x-plexos/commit/773d4be2da11a457b467fa04a07fc91aa8379517))
* **deps:** Bump actions/upload-artifact from 7.0.0 to 7.0.1 ([#54](https://github.com/NatLabRockies/r2x-plexos/issues/54)) ([426556e](https://github.com/NatLabRockies/r2x-plexos/commit/426556e4d3383302c7b9b66454d980379eba67fd))
* **deps:** Bump codecov/codecov-action from 6 to 7 ([#64](https://github.com/NatLabRockies/r2x-plexos/issues/64)) ([af356ea](https://github.com/NatLabRockies/r2x-plexos/commit/af356ea9f3435b5b9e6c81cfbeca12e86b9a4f63))
* **deps:** Bump googleapis/release-please-action ([b047a61](https://github.com/NatLabRockies/r2x-plexos/commit/b047a614bc916360eab916bcbfc9ad457db789c9))
* **deps:** Bump googleapis/release-please-action from c3fc4de07084f75a2b61a5b933069bda6edf3d5c to 16a9c90856f42705d54a6fda1823352bdc62cf38 ([#55](https://github.com/NatLabRockies/r2x-plexos/issues/55)) ([b047a61](https://github.com/NatLabRockies/r2x-plexos/commit/b047a614bc916360eab916bcbfc9ad457db789c9))
* **deps:** Bump pypa/gh-action-pypi-publish from 1.13.0 to 1.14.0 ([#56](https://github.com/NatLabRockies/r2x-plexos/issues/56)) ([1d7fdef](https://github.com/NatLabRockies/r2x-plexos/commit/1d7fdefd7b734e5d7b73e4582c0fc0d14407629e))

## [0.1.5](https://github.com/NatLabRockies/r2x-plexos/compare/v0.1.4...v0.1.5) (2026-04-07)


### 🐛 Bug Fixes

* update current ci and release yaml files ([#51](https://github.com/NatLabRockies/r2x-plexos/issues/51)) ([fdf6a5d](https://github.com/NatLabRockies/r2x-plexos/commit/fdf6a5d17d0ecd8d84b20a51122cfb27a5e1d78e))

## [0.1.4](https://github.com/NatLabRockies/r2x-plexos/compare/v0.1.3...v0.1.4) (2026-04-04)


### 🐛 Bug Fixes

* fix parser ts horizon handling in h5, and add name mapping for ts parsing ([#39](https://github.com/NatLabRockies/r2x-plexos/issues/39)) ([e445fbc](https://github.com/NatLabRockies/r2x-plexos/commit/e445fbc046dc5a3a25337cc20b500bfc9ab9c5b7))
* format files with prek and add author ([#46](https://github.com/NatLabRockies/r2x-plexos/issues/46)) ([8a94903](https://github.com/NatLabRockies/r2x-plexos/commit/8a9490390c4ed3820a1415934f5e5b1ce80ceb4c))
* improve logic for filtering object properties ([#42](https://github.com/NatLabRockies/r2x-plexos/issues/42)) ([1836659](https://github.com/NatLabRockies/r2x-plexos/commit/1836659dd19347834d875baaf47e664cda7cd92c))
* update components with time series missing property ([#45](https://github.com/NatLabRockies/r2x-plexos/issues/45)) ([1fe6ebd](https://github.com/NatLabRockies/r2x-plexos/commit/1fe6ebd7c78d063628e3e6615f9c491f4c5074e2))
* update exporter function and format file ([#38](https://github.com/NatLabRockies/r2x-plexos/issues/38)) ([f330cc3](https://github.com/NatLabRockies/r2x-plexos/commit/f330cc31a73ea33e90d893771e0baa5b3e3f19b6))
* update parser logic and add new data format for time series ([#35](https://github.com/NatLabRockies/r2x-plexos/issues/35)) ([b65a9c9](https://github.com/NatLabRockies/r2x-plexos/commit/b65a9c92f82c9a00648d8a4c36f8b70de8ae09c5))


### 🧹 Refactoring

* update scripts to match latest changes of r2x-core ([#37](https://github.com/NatLabRockies/r2x-plexos/issues/37)) ([e7f2d75](https://github.com/NatLabRockies/r2x-plexos/commit/e7f2d750b680aa0c8eb78516b180a7a71c7503fd))


### 📦 Build

* **deps-dev:** Bump furo from 2025.9.25 to 2025.12.19 ([#33](https://github.com/NatLabRockies/r2x-plexos/issues/33)) ([7988b2f](https://github.com/NatLabRockies/r2x-plexos/commit/7988b2f36d474bb638e8a44459b93bd8b9a82fa9))
* **deps-dev:** Bump ruff from 0.14.1 to 0.14.11 ([#34](https://github.com/NatLabRockies/r2x-plexos/issues/34)) ([a7da085](https://github.com/NatLabRockies/r2x-plexos/commit/a7da085f8c5b602f115834bba2de45f79cbb8d69))
* **deps:** Bump actions/download-artifact from 6 to 8 ([#41](https://github.com/NatLabRockies/r2x-plexos/issues/41)) ([88fe97c](https://github.com/NatLabRockies/r2x-plexos/commit/88fe97c913be31ec7f6a8afff16b38a3e881cfd9))
* **deps:** Bump actions/upload-artifact from 5 to 7 ([#40](https://github.com/NatLabRockies/r2x-plexos/issues/40)) ([f627745](https://github.com/NatLabRockies/r2x-plexos/commit/f6277457e999f4c07a5a88b2622de06f511a556b))

## [0.1.3](https://github.com/NREL/r2x-plexos/compare/v0.1.2...v0.1.3) (2026-01-07)


### 🧹 Refactoring

* update mappings and logger debug/info for exceptions ([#30](https://github.com/NREL/r2x-plexos/issues/30)) ([8b9ca6e](https://github.com/NREL/r2x-plexos/commit/8b9ca6e301494c38741e638cf67e7202d65066a7))

## [0.1.2](https://github.com/NREL/r2x-plexos/compare/v0.1.1...v0.1.2) (2025-12-13)


### 🐛 Bug Fixes

* address time_series handling and category assignments ([#24](https://github.com/NREL/r2x-plexos/issues/24)) ([5e16ab9](https://github.com/NREL/r2x-plexos/commit/5e16ab90bc906cfc8a8ea9242f4055a38bcdefeb))


### 📦 Build

* **deps:** Bump pytest from 8.4.2 to 9.0.1 ([#22](https://github.com/NREL/r2x-plexos/issues/22)) ([85542ef](https://github.com/NREL/r2x-plexos/commit/85542ef73cf184a1d75cabc289636fe24610daa9))
* **deps:** Bump sphinx-reports from 0.9.7 to 0.9.9 ([#23](https://github.com/NREL/r2x-plexos/issues/23)) ([7d8cf72](https://github.com/NREL/r2x-plexos/commit/7d8cf7277de48862da8bf9f360ae71c2839dc455))

## [0.1.1](https://github.com/NREL/r2x-plexos/compare/v0.1.0...v0.1.1) (2025-12-01)


### 🐛 Bug Fixes

* Adding new CI for managing automatic releases ([#20](https://github.com/NREL/r2x-plexos/issues/20)) ([5ef1d99](https://github.com/NREL/r2x-plexos/commit/5ef1d99a0f3eec1fc29e950e83a8381362767415))
