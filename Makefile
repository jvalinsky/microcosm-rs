.PHONY: check test fmt clippy
all: check

test:
	cargo test --all-features

fmt:
	cargo fmt --package links \
						--package constellation \
						--package ufos \
						--package spacedust \
						--package who-am-i \
						--package slingshot \
						--package pocket \
						--package reflector
	cargo +nightly fmt --package jetstream

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

check: test fmt clippy
