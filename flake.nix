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
        rustVersion = pkgs.rust-bin.stable.latest.default;
        craneLib = (crane.mkLib pkgs).overrideToolchain rustVersion;
        src = pkgs.lib.cleanSource ./.;
        # Enhanced environment variables for bindgen + zstd-sys fix
        commonEnv = {
  LIBCLANG_PATH = lib.makeLibraryPath [ pkgs.llvmPackages.libclang.lib ];
  OPENSSL_NO_VENDOR = "1";
  OPENSSL_LIB_DIR = "${lib.getLib pkgs.openssl}/lib";
  OPENSSL_INCLUDE_DIR = "${lib.getDev pkgs.openssl}/include";

  BINDGEN_EXTRA_CLANG_ARGS = lib.concatStringsSep " " (
    (map (a: ''-I"${a}/include"'') [
      pkgs.glibc.dev
    ])
    ++ [
      ''-I"${pkgs.llvmPackages.libclang.lib}/lib/clang/${pkgs.llvmPackages.libclang.version}/include"''
    ]
  );

          ZSTD_SYS_USE_PKG_CONFIG = "1";
          # Additional environment variables for C/C++ compilation

          CC = "${pkgs.gcc}/bin/gcc";
          CXX = "${pkgs.gcc}/bin/g++";
          # Set compiler flags directly - this should be picked up by cc-rs
          CFLAGS = "-I${pkgs.glibc.dev}/include";
          CXXFLAGS = builtins.concatStringsSep " " [
            "-I${pkgs.glibc.dev}/include"
            "-I${pkgs.gcc.cc}/include/c++/${pkgs.gcc.version}"
            "-I${pkgs.gcc.cc}/include/c++/${pkgs.gcc.version}/${pkgs.stdenv.targetPlatform.config}"
          ];
          # Alternative: Set include paths for direct compiler invocation
          CPATH = "${pkgs.glibc.dev}/include";
          CPLUS_INCLUDE_PATH = builtins.concatStringsSep ":" [
            "${pkgs.glibc.dev}/include"
            "${pkgs.gcc.cc}/include/c++/${pkgs.gcc.version}"
            "${pkgs.gcc.cc}/include/c++/${pkgs.gcc.version}/${pkgs.stdenv.targetPlatform.config}"
          ];
          # Library paths
          LIBRARY_PATH = "${pkgs.glibc}/lib:${pkgs.gcc.cc.lib}/lib";
          # Make sure pkg-config can find zstd
          PKG_CONFIG_PATH = "${pkgs.zstd.dev}/lib/pkgconfig:${pkgs.lz4.dev}/lib/pkgconfig";
        };
        nativeInputs = [
          pkgs.pkg-config
          pkgs.openssl.dev
          pkgs.protobuf
          pkgs.perl
          pkgs.llvmPackages.libclang
          pkgs.clang
          pkgs.gcc
          pkgs.glibc.dev
          pkgs.zstd.dev
          pkgs.lz4.dev
        ];
        buildInputs = [
          pkgs.zstd
          pkgs.lz4
          pkgs.rocksdb
          pkgs.openssl
        ];
        cargoArtifacts = craneLib.buildDepsOnly {
          inherit src;
          pname = "microcosm-rs-deps";
          nativeBuildInputs = nativeInputs;
          buildInputs = buildInputs;
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
            buildInputs = buildInputs ++ (pkgs.lib.optional (member == "pocket") pkgs.sqlite);
            env = commonEnv;
          };
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
          buildInputs = buildInputs;
          RUST_SRC_PATH = "${rustVersion}/lib/rustlib/src/rust/library";
          env = commonEnv;
        };
      });
}
