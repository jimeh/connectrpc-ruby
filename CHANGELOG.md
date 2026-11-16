# Changelog

## [0.1.1](https://github.com/jimeh/connectrpc-ruby/compare/v0.1.0...v0.1.1) (2026-11-16)


### Features

* add Prometheus and Yabeda metrics packages ([#66](https://github.com/jimeh/connectrpc-ruby/issues/66)) ([6928ef9](https://github.com/jimeh/connectrpc-ruby/commit/6928ef9edc113855b2e1e175c8fd195b4d396a24))
* **async:** add Connect server streaming and native gRPC to ThreadedTransport ([#67](https://github.com/jimeh/connectrpc-ruby/issues/67)) ([9b1d643](https://github.com/jimeh/connectrpc-ruby/commit/9b1d6433e945f9d620f6d328890b0c97f2e1ea1c))
* **async:** add ThreadedTransport for Connect unary from any thread ([#56](https://github.com/jimeh/connectrpc-ruby/issues/56)) ([1372528](https://github.com/jimeh/connectrpc-ruby/commit/1372528bed02077ef575df3dce60af298e67f213))
* **async:** replace draining GOAWAY connections and retry refused unary calls ([#63](https://github.com/jimeh/connectrpc-ruby/issues/63)) ([38ccfaf](https://github.com/jimeh/connectrpc-ruby/commit/38ccfaf5110d799421d21598d7fa64a04d66f876))


### Bug Fixes

* **async:** finish H2 stream output locally once retirement starts ([#60](https://github.com/jimeh/connectrpc-ruby/issues/60)) ([fc80614](https://github.com/jimeh/connectrpc-ruby/commit/fc806145483f1441c7f33d8d72bcb5807a6a9667))
* **async:** skip stream window updates during retirement and pin reset-stream frame completion ([#72](https://github.com/jimeh/connectrpc-ruby/issues/72)) ([8c2dd4b](https://github.com/jimeh/connectrpc-ruby/commit/8c2dd4b0a0990bf44203ddaf51303613b0b675a8))
* **grpc:** bound rejected-media drains and surplus unary requests ([#61](https://github.com/jimeh/connectrpc-ruby/issues/61)) ([544d320](https://github.com/jimeh/connectrpc-ruby/commit/544d32079c60d42db0161c5c04d3c8f8ff35300c))

## 0.1.0 (2026-11-15)


### ⚠ BREAKING CHANGES

* rename connectrpc-rails to connectrpc-on-rails ([#55](https://github.com/jimeh/connectrpc-ruby/issues/55))

### Features

* add Async HTTP/1.1 Connect streaming client ([#44](https://github.com/jimeh/connectrpc-ruby/issues/44)) ([1b4870e](https://github.com/jimeh/connectrpc-ruby/commit/1b4870ef2bb48957752e6077aeb5fe7968943b97))
* add bounded unary gzip compression ([#6](https://github.com/jimeh/connectrpc-ruby/issues/6)) ([158954f](https://github.com/jimeh/connectrpc-ruby/commit/158954fad475785f2ac21af66cb6353d814022f2))
* add native Connect streaming ([#20](https://github.com/jimeh/connectrpc-ruby/issues/20)) ([bbd14a7](https://github.com/jimeh/connectrpc-ruby/commit/bbd14a72322958f3b759b08166f26946d6cc030c))
* add native HTTP application, managed TLS and Falcon hosting ([#11](https://github.com/jimeh/connectrpc-ruby/issues/11)) ([25a50b0](https://github.com/jimeh/connectrpc-ruby/commit/25a50b07f726e284b357f6d708be10678b73660c))
* add optional Async HTTP/2 host and transport ([#7](https://github.com/jimeh/connectrpc-ruby/issues/7)) ([b43349c](https://github.com/jimeh/connectrpc-ruby/commit/b43349cd494106d5e3ae1dcc1ff39d760521575b))
* add optional in-process testing transport ([#10](https://github.com/jimeh/connectrpc-ruby/issues/10)) ([504eccd](https://github.com/jimeh/connectrpc-ruby/commit/504eccd8ebb99c0d0e78ae35bc7b5fd914160f04))
* add optional Rails controller integration ([#5](https://github.com/jimeh/connectrpc-ruby/issues/5)) ([38eb980](https://github.com/jimeh/connectrpc-ruby/commit/38eb980a9e388868f7b6f037910fda7e1ebdb994))
* add optional RBS signatures and service generation ([#12](https://github.com/jimeh/connectrpc-ruby/issues/12)) ([c268ea6](https://github.com/jimeh/connectrpc-ruby/commit/c268ea6b26e9e491b7aba0a3ae21a2da68096eec))
* add RPC lifecycle instrumentation and optional OpenTelemetry traces ([#13](https://github.com/jimeh/connectrpc-ruby/issues/13)) ([38fa460](https://github.com/jimeh/connectrpc-ruby/commit/38fa4603250aaa56fc75bb206aaae3da30cf5109))
* add runnable server and client examples ([#52](https://github.com/jimeh/connectrpc-ruby/issues/52)) ([0a85d32](https://github.com/jimeh/connectrpc-ruby/commit/0a85d32adf872ee8fd1fd044b44f140734bf118e))
* add typed Connect unary client and Rack server ([#2](https://github.com/jimeh/connectrpc-ruby/issues/2)) ([f34fe51](https://github.com/jimeh/connectrpc-ruby/commit/f34fe5131ef7c226248ebb4071993ea8fef0e99e))
* **async:** manage H2 client connections with shared attempts and backoff ([#54](https://github.com/jimeh/connectrpc-ruby/issues/54)) ([a64d44f](https://github.com/jimeh/connectrpc-ruby/commit/a64d44fb6765c6df16ade7340076eb4ae1487ac4))
* extract optional Rack middleware package ([#9](https://github.com/jimeh/connectrpc-ruby/issues/9)) ([88cdfe9](https://github.com/jimeh/connectrpc-ruby/commit/88cdfe9c3d0173e9622c8fef5417abe78a5395cb))
* generate unary Connect services and clients from protobuf ([#4](https://github.com/jimeh/connectrpc-ruby/issues/4)) ([f9be2d1](https://github.com/jimeh/connectrpc-ruby/commit/f9be2d17082d01a3819c38253bafb63e5b540f63))
* serve Connect HTTP/1.1 streaming with Falcon ([#41](https://github.com/jimeh/connectrpc-ruby/issues/41)) ([298ce60](https://github.com/jimeh/connectrpc-ruby/commit/298ce60f3546d77ddc4d9a419f9c6338d3e5f1c6))
* support browser clients with gRPC-Web ([#37](https://github.com/jimeh/connectrpc-ruby/issues/37)) ([4806f94](https://github.com/jimeh/connectrpc-ruby/commit/4806f94f872507e8c448a96f3663fe1815514d78))
* support explicit unary RPC cancellation ([#16](https://github.com/jimeh/connectrpc-ruby/issues/16)) ([35127f9](https://github.com/jimeh/connectrpc-ruby/commit/35127f93475d09e47481d4667f6d21b0ceece0fd))
* support native gRPC across all RPC shapes ([#25](https://github.com/jimeh/connectrpc-ruby/issues/25)) ([011fc99](https://github.com/jimeh/connectrpc-ruby/commit/011fc9994f6e8854e681c80654258e91d1111ea4))
* support safe Connect unary HTTP GET ([#15](https://github.com/jimeh/connectrpc-ruby/issues/15)) ([e3a3262](https://github.com/jimeh/connectrpc-ruby/commit/e3a3262122b84e1b641b1a544025718693e35bc9))
* type supported public APIs and check core implementation ([#14](https://github.com/jimeh/connectrpc-ruby/issues/14)) ([2551f4f](https://github.com/jimeh/connectrpc-ruby/commit/2551f4fe13460aa9f0dfcb82449f36aef1b06641))


### Bug Fixes

* account for cancellation across conformance call stages ([#17](https://github.com/jimeh/connectrpc-ruby/issues/17)) ([9ace45b](https://github.com/jimeh/connectrpc-ruby/commit/9ace45bed026221b2c7ad9fc37ffb9b6c1cb6ab0))
* bound native HTTP/2 ingress by consumption ([#19](https://github.com/jimeh/connectrpc-ruby/issues/19)) ([d361a91](https://github.com/jimeh/connectrpc-ruby/commit/d361a91225d7aa936c35c76b3d68b9bce349cc69))
* canonicalize gem archive input permissions ([#40](https://github.com/jimeh/connectrpc-ruby/issues/40)) ([1fa07b1](https://github.com/jimeh/connectrpc-ruby/commit/1fa07b1dd2455e641e77514ea4c3eff5e038466a))
* defer gRPC-Web HTTP/1 rejection heads until input cleanup ([#42](https://github.com/jimeh/connectrpc-ruby/issues/42)) ([a5dafbc](https://github.com/jimeh/connectrpc-ruby/commit/a5dafbce217f685ac26f45971e1033e68b0835e7))
* disable Falcon fallback caching by default ([#47](https://github.com/jimeh/connectrpc-ruby/issues/47)) ([073f95a](https://github.com/jimeh/connectrpc-ruby/commit/073f95ad4460464e490c6c816790d748dec96e31))
* join shared resource retirement before completion ([#49](https://github.com/jimeh/connectrpc-ruby/issues/49)) ([2d08080](https://github.com/jimeh/connectrpc-ruby/commit/2d08080274c72653b4f6ca7326619e7417fb7ef8))
* preserve gRPC-Web HTTP/1 rejection delivery ([#38](https://github.com/jimeh/connectrpc-ruby/issues/38)) ([143040b](https://github.com/jimeh/connectrpc-ruby/commit/143040b488866c35c5ac1f85d813b300c64a578a))
* preserve lifecycle ownership across Ruby unwinds ([#39](https://github.com/jimeh/connectrpc-ruby/issues/39)) ([3d34adb](https://github.com/jimeh/connectrpc-ruby/commit/3d34adbbb4b48ee92d33efce2c2453802f25cdca))
* recover transient Async Host accept failures ([#50](https://github.com/jimeh/connectrpc-ruby/issues/50)) ([95ecf9f](https://github.com/jimeh/connectrpc-ruby/commit/95ecf9ffb4fd750f7d73caa84c7eb3f0bb79a4d3))
* unwind observer tokens after escaping detach callbacks ([#48](https://github.com/jimeh/connectrpc-ruby/issues/48)) ([63b70f4](https://github.com/jimeh/connectrpc-ruby/commit/63b70f4319198151084b61551cbf032df88d9da2))


### Performance Improvements

* reduce RBS CI startup and dependency setup ([#18](https://github.com/jimeh/connectrpc-ruby/issues/18)) ([819824e](https://github.com/jimeh/connectrpc-ruby/commit/819824ec7dcc90431dd704a0dd287c78f50ad5b3))


### Code Refactoring

* rename connectrpc-rails to connectrpc-on-rails ([#55](https://github.com/jimeh/connectrpc-ruby/issues/55)) ([de07050](https://github.com/jimeh/connectrpc-ruby/commit/de0705046b414ee45965ed96a5efa763550c02ec))
