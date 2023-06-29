# SurrealDB HTTP Client

This create is meant to wrap http calls to SurrealDB.
The primary reason this crate exists is that I want to use an embedded device to interact with a SurrealDB database.

Right now I am focusing on a ESP32 microcontroller (with std). Even though it has a std library, there are a lot of things missing, and many crates such as the very ubiquitous reqwest http client do not work. ESP32's own ESP-IDF framework does offer an HTTP client that we can use.

However I don't want to totally tie my client to ESP-IDF, so I've created an abstraction layer: https://github.com/flyaruu/simplehttp that wrappes either ESP-IDF http client, Reqwest HTTP client or Spin (WASM) client

CI:
[![CircleCI](https://circleci.com/gh/flyaruu/surrealdb-http.svg?style=svg)](https://circleci.com/gh/flyaruu/surrealdb-http)
