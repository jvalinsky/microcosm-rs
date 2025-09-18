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

        # Use the latest stable toolchain
        rustVersion = pkgs.rust-bin.stable.latest.default;
        craneLib = (crane.mkLib pkgs).overrideToolchain rustVersion;

        src = pkgs.lib.cleanSource ./.;

        commonEnv = {
          LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
          BINDGEN_EXTRA_CLANG_ARGS = "-I${pkgs.glibc.dev}/include -I${pkgs.llvmPackages.libclang.lib}/lib/clang/${pkgs.llvmPackages.libclang.version}/include";
        };

        nativeInputs = with pkgs; [
          pkg-config
          openssl
          protobuf
          perl
          llvmPackages.libclang
          clang
          glibc.dev
        ];

        cargoArtifacts = craneLib.buildDepsOnly {
          inherit src;
          pname = "microcosm-rs-deps";
          nativeBuildInputs = nativeInputs;
          env = commonEnv;
        };

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
          let
            packageName = if member == "ufos/fuzz" then "ufos-fuzz" else member;
          in
          craneLib.buildPackage {
            inherit src cargoArtifacts;
            pname = packageName;
            version = "0.1.0";
            cargoExtraArgs = "--package ${packageName}";
            nativeBuildInputs = nativeInputs;
            buildInputs = with pkgs; [
              zstd
              lz4
              rocksdb
            ] ++ (pkgs.lib.optional (member == "pocket") sqlite);
            env = commonEnv;
          };

        packages = pkgs.lib.genAttrs members (member: buildPackage member);
      in
      {
        packages = packages // {
          default = pkgs.linkFarm "microcosm-rs" (pkgs.lib.mapAttrsToList (name: value:
            let
              linkName = if name == "ufos/fuzz" then "ufos-fuzz" else name;
            in
            { name = linkName; path = value; }
          ) packages);
        };

        devShell = pkgs.mkShell {
          inputsFrom = builtins.attrValues self.packages.${system};
          nativeBuildInputs = nativeInputs ++ [
            (rustVersion.override {
              extensions = [ "rust-src" "rust-analyzer" ];
            })
            cargo
            zstd
            lz4
            rocksdb
            sqlite
          ];
          RUST_SRC_PATH = "${rustVersion}/lib/rustlib/src/rust/library";
          env = commonEnv;
        };
      });
}
