{
  description = "LilLil LogLog - simple replicated binary log";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
    crane.inputs.nixpkgs.follows = "nixpkgs";

  };

  outputs = { self, nixpkgs, flake-utils, crane }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };

        craneLib = (crane.mkLib pkgs);

        commonArgs = {
          src = craneLib.cleanCargoSource (craneLib.path ./.);
          buildInputs = [
          ];
          nativeBuildInputs = [
            pkgs.pkgconfig
          ];
        };


        workspaceDeps = craneLib.buildDepsOnly (commonArgs // {
          pname = "loglog-workspace-deps";
        });

        workspaceAll = craneLib.cargoBuild (commonArgs // {
          cargoArtifacts = workspaceDeps;
          doCheck = true;
        });

        # a function to define both package and container build for a given binary
        pkg = { bin, cargoToml }: rec {
          package =
            let
              meta = craneLib.crateNameFromCargoToml { inherit cargoToml; };
            in
            craneLib.buildPackage (commonArgs // {
              pname = builtins.trace meta.pname meta.pname;
              version = meta.version;


              cargoExtraArgs = "--bin ${bin}";
              doCheck = false;
            });

          container = pkgs.dockerTools.buildLayeredImage {
            name = bin;
            contents = [ package ];
            config = {
              Cmd = [
                "${package}/bin/${bin}"
              ];
              ExposedPorts = {
                "8000/tcp" = { };
              };
            };
          };
        };

        loglogd = pkg { bin = "loglogd"; cargoToml = ./server/Cargo.toml; };

      in
      {
        packages = {
          default = loglogd.package;
          loglogd = loglogd.package;

          deps = workspaceDeps;
          ci = workspaceAll;
        };

        devShells = {
          default =
            pkgs.mkShell {
              buildInputs = workspaceDeps.buildInputs;
              nativeBuildInputs = workspaceDeps.nativeBuildInputs ++ [

                # extra binaries here
                pkgs.rust-analyzer
                pkgs.rustc
                pkgs.cargo
                pkgs.clippy

                # Bench
                pkgs.gnuplot

                # Lints
                pkgs.rustfmt
                pkgs.rnix-lsp
                pkgs.nodePackages.bash-language-server

                # Nix
                pkgs.nixpkgs-fmt
                pkgs.shellcheck

                # Utils
                pkgs.git
                pkgs.gh
                pkgs.cargo-udeps
              ];

              shellHook = ''
                # auto-install git hooks
                dot_git="$(git rev-parse --git-common-dir)"
                if [[ ! -d "$dot_git/hooks" ]]; then mkdir "$dot_git/hooks"; fi
                for hook in misc/git-hooks/* ; do ln -sf "$(pwd)/$hook" "$dot_git/hooks/" ; done
                ${pkgs.git}/bin/git config commit.template misc/git-hooks/commit-template.txt
              '';
            };

          # this shell is used only in CI lints, so it should contain minimum amount
          # of stuff to avoid building and caching things we don't need
          lint = pkgs.mkShell {
            nativeBuildInputs = [
              pkgs.rustfmt
              pkgs.nixpkgs-fmt
              pkgs.shellcheck
              pkgs.git
            ];
          };
        };
      });
}
