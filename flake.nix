{
  description = "sql made easier";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/25.05";

    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    crane.url = "github:ipetkov/crane";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      nixpkgs,
      fenix,
      crane,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        rustToolchain = fenix.packages.${system}.stable.toolchain;
        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;
      in
      {
        devShells.default = craneLib.devShell {
          packages = with pkgs; [
            rustToolchain
            cargo-hakari
            cargo-expand
          ];
        };
      }
    );
}
