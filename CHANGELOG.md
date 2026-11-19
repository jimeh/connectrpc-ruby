# Changelog

## [0.3.0](https://github.com/jimeh/connectrpc-ruby/compare/v0.2.1...v0.3.0) (2026-11-19)


### ⚠ BREAKING CHANGES

* connectrpc-sentry span data renames rpc.response.status_code to rpc.status_code.
* connectrpc-otel renames rpc.response.status_code to rpc.status_code, and replaces rpc.request.metadata.<key>, rpc.response.metadata.<key>, connectrpc.request.metadata.<key> and connectrpc.response.metadata.<key> with rpc.request.header.<key> and rpc.response.header.<key>. Trailers are recorded as rpc.response.trailer.<key> through the new response_trailers: option. Connect unary Outcome#response_metadata no longer includes trailer-* headers.
* Service#methods is now Service#rpcs, and Service.new takes rpcs: instead of methods:, with no alias. Regenerate every _connect.rb with the 0.3.0 protoc-gen-connectrpc-ruby plugin and replace hand-written methods: declarations and .methods reads with rpcs.
* Optional connectrpc gems' RBS manifests no longer load core's signatures. Steepfiles must list library "connectrpc" beside each optional connectrpc library, plus library "connectrpc-async" beside library "connectrpc-falcon". Projects that use rbs collection need no change. See docs/upgrading.md.

### Features

* add Async connection gauges and a Host fallback app ([#100](https://github.com/jimeh/connectrpc-ruby/issues/100)) ([2178274](https://github.com/jimeh/connectrpc-ruby/commit/2178274a44723d5c124d66e4027c58c6c7a2ced9))
* add connectrpc-hyper, a reactor-free Rust client transport ([#173](https://github.com/jimeh/connectrpc-ruby/issues/173)) ([c3a0ad6](https://github.com/jimeh/connectrpc-ruby/commit/c3a0ad6649c57344b12e2f56f53cc4cb16d38dfa)), closes [#171](https://github.com/jimeh/connectrpc-ruby/issues/171)
* add TLS and proxy options to NetHTTP and send a default User-Agent ([#159](https://github.com/jimeh/connectrpc-ruby/issues/159)) ([ccffd92](https://github.com/jimeh/connectrpc-ruby/commit/ccffd92172d768ddcbc3143264c2aea8cfdbc862))
* **async:** configurable HTTP/2 receive windows and a 16 MiB connection window ([#134](https://github.com/jimeh/connectrpc-ruby/issues/134)) ([1817c09](https://github.com/jimeh/connectrpc-ruby/commit/1817c094febdcf6ea40561c7127f7e5524fc5cf7))
* **async:** default HTTP/2 client stream window to 4 MiB ([#141](https://github.com/jimeh/connectrpc-ruby/issues/141)) ([c1c8414](https://github.com/jimeh/connectrpc-ruby/commit/c1c84141a663790fb15fe550ff8c74231dddb379))
* grow HTTP/2 receive windows from measured BDP on the Async clients and hosts ([#153](https://github.com/jimeh/connectrpc-ruby/issues/153)) ([b62044f](https://github.com/jimeh/connectrpc-ruby/commit/b62044fe299ac9418a16718aab1d7878c9cae495))
* name Sentry RPC span data as OpenTelemetry semconv 1.45.0 does ([#167](https://github.com/jimeh/connectrpc-ruby/issues/167)) ([61764bf](https://github.com/jimeh/connectrpc-ruby/commit/61764bf74002527b8512164fea83c1de4bf2fcd3))
* report response trailers and emit OpenTelemetry semconv 1.45.0 ([#164](https://github.com/jimeh/connectrpc-ruby/issues/164)) ([d8aea46](https://github.com/jimeh/connectrpc-ruby/commit/d8aea46f517c96e5eb32948b4a9fdfadbea1b0c4)), closes [#114](https://github.com/jimeh/connectrpc-ruby/issues/114)
* report unexpected server exceptions through an on_error hook ([#122](https://github.com/jimeh/connectrpc-ruby/issues/122)) ([721ae37](https://github.com/jimeh/connectrpc-ruby/commit/721ae37cdfbc4c24be579dd413662815d1aace63))
* return HTTP/2 flow-control credit at a quarter of each window, as grpc-go does ([#157](https://github.com/jimeh/connectrpc-ruby/issues/157)) ([79fdcf7](https://github.com/jimeh/connectrpc-ruby/commit/79fdcf7a707297b6a42f40476da6353120373581))
* shorten generated service setup and pass deadlines downstream ([#152](https://github.com/jimeh/connectrpc-ruby/issues/152)) ([a211280](https://github.com/jimeh/connectrpc-ruby/commit/a211280d98c12f5a2ba9e3bf4113e540ca6a870f))


### Bug Fixes

* allow builtin module packages, linear namespace checks and rbs=false ([#129](https://github.com/jimeh/connectrpc-ruby/issues/129)) ([d0bf811](https://github.com/jimeh/connectrpc-ruby/commit/d0bf811730599038092171cf4a8794ea81683915)), closes [#115](https://github.com/jimeh/connectrpc-ruby/issues/115)
* **async:** shut raw TCP down first and close it after retirement joins ([#147](https://github.com/jimeh/connectrpc-ruby/issues/147)) ([9344569](https://github.com/jimeh/connectrpc-ruby/commit/9344569ea97547a1ec4104ce0f3250d0f0f98d02))
* bound HTTP/2 response write progress on the Async and Falcon hosts ([#138](https://github.com/jimeh/connectrpc-ruby/issues/138)) ([8e9d641](https://github.com/jimeh/connectrpc-ruby/commit/8e9d6419e8794d59f4f86136bc578271f154b592)), closes [#131](https://github.com/jimeh/connectrpc-ruby/issues/131)
* bound per-connection HTTP/2 admission and drain with GOAWAY first ([#126](https://github.com/jimeh/connectrpc-ruby/issues/126)) ([5a2d560](https://github.com/jimeh/connectrpc-ruby/commit/5a2d560032304d02162bfe9600bcf9fb346c3f7c))
* close sessions after failed retirement and retire HTTP/1 shutdown-first ([#158](https://github.com/jimeh/connectrpc-ruby/issues/158)) ([26b8794](https://github.com/jimeh/connectrpc-ruby/commit/26b879401b15e950588c9b05a167916c5b6f4af9))
* close streaming terminal and protocol edge cases from the review ([#125](https://github.com/jimeh/connectrpc-ruby/issues/125)) ([d9801f1](https://github.com/jimeh/connectrpc-ruby/commit/d9801f1e8c0726c388df19225e308f2828f428ff))
* **falcon:** serve Upgrade offers, keep the Rack facet app, configure close grace ([#124](https://github.com/jimeh/connectrpc-ruby/issues/124)) ([015b57e](https://github.com/jimeh/connectrpc-ruby/commit/015b57e27a8b2e7beb16e29ac1af9ac79e52b302))
* honour caller timeouts and owner exit in Async transports, and keep client error causes ([#127](https://github.com/jimeh/connectrpc-ruby/issues/127)) ([87675eb](https://github.com/jimeh/connectrpc-ruby/commit/87675eb1c6a124eea4c7861d91197fe545d6fb09)), closes [#106](https://github.com/jimeh/connectrpc-ruby/issues/106)
* keep Async and Falcon accepting at capacity, on accept errors and on certificate expiry ([#133](https://github.com/jimeh/connectrpc-ruby/issues/133)) ([4a8080a](https://github.com/jimeh/connectrpc-ruby/commit/4a8080a0d2d16efbada8e4bf4a7d93b46cb825fa)), closes [#117](https://github.com/jimeh/connectrpc-ruby/issues/117)
* keep connectrpc gems out of RBS manifests for rbs collection users ([#121](https://github.com/jimeh/connectrpc-ruby/issues/121)) ([9caf3be](https://github.com/jimeh/connectrpc-ruby/commit/9caf3bed7027d0fa8b57bca8935941e01cfa25d9))
* let a graceful HTTP/2 drain finish post-delivery work after the peer leaves ([#146](https://github.com/jimeh/connectrpc-ruby/issues/146)) ([d914dbf](https://github.com/jimeh/connectrpc-ruby/commit/d914dbf7ac8a38b45c241a42d8b3e45b18324fcb))
* let a graceful HTTP/2 drain finish when a control frame write sees the peer leave first ([#151](https://github.com/jimeh/connectrpc-ruby/issues/151)) ([bd9ae72](https://github.com/jimeh/connectrpc-ruby/commit/bd9ae727d8a0194b25f1eb15152396bb25e25f9d))
* **otel:** keep client cancellation off server errors and honor the global propagator ([#130](https://github.com/jimeh/connectrpc-ruby/issues/130)) ([6df1027](https://github.com/jimeh/connectrpc-ruby/commit/6df1027673f88ac85bcdd55cc21ebc26f1af30dd))
* refuse HTTP/2 streams at HEADERS when host admission is full ([#144](https://github.com/jimeh/connectrpc-ruby/issues/144)) ([9e2dbfd](https://github.com/jimeh/connectrpc-ruby/commit/9e2dbfde88997de78d530c15bd790e706a31c528)), closes [#137](https://github.com/jimeh/connectrpc-ruby/issues/137)
* renew the HTTP/2 write watchdog while the peer keeps acknowledging data ([#150](https://github.com/jimeh/connectrpc-ruby/issues/150)) ([2a19f1a](https://github.com/jimeh/connectrpc-ruby/commit/2a19f1aeec91f01866cf0c1dddde9b8d1d23f3fe))
* **sentry:** send one set of Sentry baggage on NetHTTP and match URL propagation targets ([#136](https://github.com/jimeh/connectrpc-ruby/issues/136)) ([80cb291](https://github.com/jimeh/connectrpc-ruby/commit/80cb291c02bdf272aab431568187bb4a398ccba0))
* stop concurrent HTTP/2 responses resetting when connection credit runs out ([#156](https://github.com/jimeh/connectrpc-ruby/issues/156)) ([823365c](https://github.com/jimeh/connectrpc-ruby/commit/823365c406ec9f2622d8a1b1b57eede9ba0e5c93))
* stop conformance telemetry deadlocking native server adapters on Ruby 4 ([#168](https://github.com/jimeh/connectrpc-ruby/issues/168)) ([d521b60](https://github.com/jimeh/connectrpc-ruby/commit/d521b6064215eb3a02e8e5ca528a5d7e90ccf07e))
* synchronize connectrpc-hyper specs on peer and dial barriers ([#179](https://github.com/jimeh/connectrpc-ruby/issues/179)) ([42fde41](https://github.com/jimeh/connectrpc-ruby/commit/42fde41938ba93512f5b863e768cac43c8c2c0e4)), closes [#177](https://github.com/jimeh/connectrpc-ruby/issues/177)
* write HTTP/2 control frames without yielding per frame on the Async and Falcon hosts ([#143](https://github.com/jimeh/connectrpc-ruby/issues/143)) ([014cdc3](https://github.com/jimeh/connectrpc-ruby/commit/014cdc34dc84443d6bd4b675fa0c1fc2ddf974ed))

## [0.2.1](https://github.com/jimeh/connectrpc-ruby/compare/v0.2.0...v0.2.1) (2026-11-17)


### Bug Fixes

* reset cancelled H2 uploads with CANCEL and pass host errors explicitly ([#96](https://github.com/jimeh/connectrpc-ruby/issues/96)) ([358e3e1](https://github.com/jimeh/connectrpc-ruby/commit/358e3e19ee527aa39c622691f6e178cb7a615009))

## [0.2.0](https://github.com/jimeh/connectrpc-ruby/compare/v0.1.2...v0.2.0) (2026-11-16)


### ⚠ BREAKING CHANGES

* TransportRequest#cancellation and StreamRequest#cancellation are now a read-only view instead of ConnectRPC::CallCancellation. The typed SPI (_Cancellation) is unchanged, but custom transports that called commit! or close on request.cancellation must stop; core completes the call.

### Features

* add Rails interceptor hook and document interceptor host coverage ([#95](https://github.com/jimeh/connectrpc-ruby/issues/95)) ([d41f5fb](https://github.com/jimeh/connectrpc-ruby/commit/d41f5fb0edbe67121c5531b8a8dfff614291e287))
* add typed streaming client and server interceptors ([#94](https://github.com/jimeh/connectrpc-ruby/issues/94)) ([31d5fa9](https://github.com/jimeh/connectrpc-ruby/commit/31d5fa957818b231c1a304931ee8343c68a99c79)), closes [#32](https://github.com/jimeh/connectrpc-ruby/issues/32)
* add typed unary client and server interceptors ([#89](https://github.com/jimeh/connectrpc-ruby/issues/89)) ([ca03d6e](https://github.com/jimeh/connectrpc-ruby/commit/ca03d6ecbc6c47903449860e1e83bab2b461ab8b))
* **async:** add HTTP/2 keepalive, ping enforcement and connection age limits ([#76](https://github.com/jimeh/connectrpc-ruby/issues/76)) ([1756faa](https://github.com/jimeh/connectrpc-ruby/commit/1756faa84f367cdbc2aba64e2a314094bae08eb6))
* **async:** fail require when upstream Async contracts change ([#88](https://github.com/jimeh/connectrpc-ruby/issues/88)) ([83b817e](https://github.com/jimeh/connectrpc-ruby/commit/83b817ee451b64746d23d9604a764ce304c39438))


### Bug Fixes

* preserve lifecycle request metadata and caller baggage ([#81](https://github.com/jimeh/connectrpc-ruby/issues/81)) ([54d55fb](https://github.com/jimeh/connectrpc-ruby/commit/54d55fb10f40766b063dbe5cb0ebefc98dccaa7c))
* **rails:** drop the rescued cause from normalized controller errors ([#87](https://github.com/jimeh/connectrpc-ruby/issues/87)) ([66a03a3](https://github.com/jimeh/connectrpc-ruby/commit/66a03a38722fb0ad883bad3d7972220f9c9df4eb))
* resolve audit generator, tooling and packaging findings L13-L16 ([#79](https://github.com/jimeh/connectrpc-ruby/issues/79)) ([915af39](https://github.com/jimeh/connectrpc-ruby/commit/915af39ea54cdc9788676ab351d903f8512446cb))
* resolve audit normalization and transport SPI findings L1-L3, L5 and L6 ([#84](https://github.com/jimeh/connectrpc-ruby/issues/84)) ([712aace](https://github.com/jimeh/connectrpc-ruby/commit/712aace9557bce5c8d1f3a32dbcc8d9c3182bec4))

## [0.1.2](https://github.com/jimeh/connectrpc-ruby/compare/v0.1.1...v0.1.2) (2026-11-16)


### Features

* add connectrpc-sentry native Sentry tracing package ([#74](https://github.com/jimeh/connectrpc-ruby/issues/74)) ([57d806a](https://github.com/jimeh/connectrpc-ruby/commit/57d806af4723bfee7feb2ba5e9e7c32951d83b26))
* add the standard gRPC health service ([#75](https://github.com/jimeh/connectrpc-ruby/issues/75)) ([af231c4](https://github.com/jimeh/connectrpc-ruby/commit/af231c4034784e3cb9cca3047fc7630a58b274b2))

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
