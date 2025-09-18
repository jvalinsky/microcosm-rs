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

        # Latest stable Rust toolchain
        rustVersion = pkgs.rust-bin.stable.latest.default;
        craneLib = (crane.mkLib pkgs).overrideToolchain rustVersion;

        src = pkgs.lib.cleanSource ./.;

        # Common environment variables for bindgen + zstd-sys fix
        commonEnv = {
  LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
  BINDGEN_EXTRA_CLANG_ARGS = [
    "-I${pkgs.glibc.dev}/include"
    "-I${pkgs.llvmPackages.libclang.lib}/lib/clang/${pkgs.llvmPackages.libclang.version}/include"
  ];
  ZSTD_SYS_USE_PKG_CONFIG = "1";
        };

        # Native build dependencies
        nativeInputs = [
          pkgs.pkg-config
          pkgs.openssl
          pkgs.protobuf
          pkgs.perl
          pkgs.llvmPackages.libclang
          pkgs.clang
          pkgs.glibc.dev
        ];

        # Prebuild cargo dependencies
        cargoArtifacts = craneLib.buildDepsOnly {
          inherit src;
          pname = "microcosm-rs-deps";
          nativeBuildInputs = nativeInputs;
          env = commonEnv;
        };

        # Workspace members
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

        # Function to build each member
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
            buildInputs = [
              pkgs.zstd.dev
              pkgs.lz4.dev
              pkgs.rocksdb
            ] ++ (pkgs.lib.optional (member == "pocket") pkgs.sqlite);
            env = commonEnv;
          };

        # Build all members into packages
        packages = pkgs.lib.genAttrs members (member: buildPackage member);

      in {
        packages = packages // {
          default = pkgs.linkFarm "microcosm-rs" (pkgs.lib.mapAttrsToList (name: value:
            let
              linkName = if name == "ufos/fuzz" then "ufos-fuzz" else name;
            in
            { name = linkName; path = value; }
          ) packages);
        };

        # Development shell with Rust + tools
        devShell = pkgs.mkShell {
          inputsFrom = builtins.attrValues self.packages.${system};
          nativeBuildInputs = nativeInputs ++ [
            (rustVersion.override {
              extensions = [ "rust-src" "rust-analyzer" ];
            })
            pkgs.zstd
            pkgs.lz4
            pkgs.rocksdb
            pkgs.sqlite
          ];
          RUST_SRC_PATH = "${rustVersion}/lib/rustlib/src/rust/library";
          env = commonEnv;
        };
      });
}
