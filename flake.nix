{
  description = "A flake for building the microcosm-rs project";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = { self, nixpkgs, flake-utils, crane, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
        # Switch to the nightly toolchain
        rustVersion = pkgs.rust-bin.nightly."2024-07-29".default;
        craneLib = (crane.mkLib pkgs).overrideToolchain rustVersion;
        src = pkgs.lib.cleanSource ./.;
        cargoArtifacts = craneLib.buildDepsOnly {
          inherit src;
          # Add a pname to silence the crane warning
          pname = "microcosm-rs-deps";
          nativeBuildInputs = with pkgs; [
            pkg-config
            openssl
            protobuf
          ];
        };
        # List of workspace members from the root Cargo.toml
        members = [
          "links"
          "constellation"
          "jetstream"
          "ufos"
          "ufos/fuzz" # This is the member causing the issue
          "spacedust"
          "who-am-i"
          "slingshot"
          "quasar"
          "pocket"
          "reflector"
        ];
        buildPackage = member:
          let
            # Cargo needs the package *name*, not the path.
            # We'll assume the package name for "ufos/fuzz" is "ufos-fuzz".
            packageName = if member == "ufos/fuzz" then "ufos-fuzz" else member;
          in
          craneLib.buildPackage {
            inherit src cargoArtifacts;
            # Use the corrected package name for the derivation name and package argument
            pname = packageName;
            version = "0.1.0";
            cargoExtraArgs = "--package ${packageName}";
            nativeBuildInputs = with pkgs; [
              pkg-config
              openssl
              protobuf # for slingshot
            ];
            buildInputs = with pkgs; [
              zstd # for jetstream, constellation
              lz4 # for ufos
              rocksdb # for constellation
            ];
          };

        # Build each member of the workspace
        packages = pkgs.lib.genAttrs members (member: buildPackage member);
      in
      {
        packages = packages // {
          default = pkgs.linkFarm "microcosm-rs" (pkgs.lib.mapAttrsToList (name: value: { inherit name; path = value; }) packages);
        };
        devShell = pkgs.mkShell {
          inputsFrom = builtins.attrValues self.packages.${system};
          nativeBuildInputs = with pkgs; [
            (rustVersion.override {
              extensions = [ "rust-src" "rust-analyzer" ];
            })
            cargo
            pkg-config
            openssl
            protobuf
            zstd
            lz4
            rocksdb
          ];
          # Environment variables might be needed for some build scripts
          RUST_SRC_PATH = "${rustVersion}/lib/rustlib/src/rust/library";
        };
      });
}
