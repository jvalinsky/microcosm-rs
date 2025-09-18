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
        rustVersion = pkgs.rust-bin.stable."1.79.0".default;
        craneLib = (crane.mkLib pkgs).overrideToolchain rustVersion;
        src = pkgs.lib.cleanSource ./.;
        cargoArtifacts = craneLib.buildDepsOnly {
          inherit src;
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
          "ufos/fuzz"
          "spacedust"
          "who-am-i"
          "slingshot"
          "quasar"
          "pocket"
          "reflector"
        ];
        buildPackage = member:
          craneLib.buildPackage {
            inherit src cargoArtifacts;
            pname = member;
            # The source for each member is a subdirectory of the workspace root
            src = ./.;
            cargoExtraArgs = "--package ${member}";
            nativeBuildInputs = with pkgs; [
              pkg-config
              openssl
              protobuf # for slingshot
            ];
            buildInputs = with pkgs; [
              # Add member-specific dependencies here.
              # For example, if a member needs a C library.
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
