{
  description = "Obsrvr Bronze Copier - Stellar blockchain bronze layer data copier";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        # Development tools
        devTools = with pkgs; [
          # Container tools
          docker
          skopeo

          # Development utilities
          yq-go
          jq
          curl
          git
          vim

          # Go tools
          gotools
          gopls
          delve
        ];

        # Build the Bronze Copier application
        bronze-copier = pkgs.buildGoModule rec {
          pname = "obsrvr-bronze-copier";
          version = "0.1.0";

          src = ./.;

          # IMPORTANT: Update this hash after first build attempt
          # Run: nix build 2>&1 | grep "got:" | awk '{print $2}'
          vendorHash = null; # No external dependencies yet

          # Build flags
          ldflags = [
            "-s"
            "-w"
          ];

          # Skip tests for now
          doCheck = false;

          meta = with pkgs.lib; {
            description = "Stellar blockchain bronze layer data copier";
            homepage = "https://github.com/withObsrvr/obsrvr-bronze-copier";
            license = licenses.mit;
            maintainers = [ ];
            platforms = platforms.linux ++ platforms.darwin;
          };
        };

        # Docker push scripts
        pushToProd = pkgs.writeShellScriptBin "push-to-dockerhub-prod" ''
          set -e

          IMAGE_NAME="obsrvr-bronze-copier"
          TAG="''${1:-latest}"
          USERNAME="''${2:-withobsrvr}"

          echo "Building production container with Docker..."
          ${pkgs.docker}/bin/docker build -f Dockerfile.nix -t "docker.io/$USERNAME/$IMAGE_NAME:$TAG" .

          echo "Pushing to DockerHub..."
          ${pkgs.docker}/bin/docker push "docker.io/$USERNAME/$IMAGE_NAME:$TAG"

          echo "Successfully pushed docker.io/$USERNAME/$IMAGE_NAME:$TAG"
        '';

        pushToDev = pkgs.writeShellScriptBin "push-to-dockerhub-dev" ''
          set -e

          IMAGE_NAME="obsrvr-bronze-copier-dev"
          TAG="''${1:-latest}"
          USERNAME="''${2:-withobsrvr}"

          echo "Building development container with Docker..."
          ${pkgs.docker}/bin/docker build -f Dockerfile.nix -t "docker.io/$USERNAME/$IMAGE_NAME:$TAG" .

          echo "Pushing to DockerHub..."
          ${pkgs.docker}/bin/docker push "docker.io/$USERNAME/$IMAGE_NAME:$TAG"

          echo "Successfully pushed docker.io/$USERNAME/$IMAGE_NAME:$TAG"
        '';

      in
      {
        # Development shell
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            # Go development
            go_1_24
          ] ++ devTools;

          shellHook = ''
            echo "🚀 Obsrvr Bronze Copier Development Environment"
            echo ""
            echo "Available commands:"
            echo "  go build ./cmd/bronze-copier           - Build Go application locally"
            echo "  nix build                              - Build the Go application with Nix"
            echo "  docker build -f Dockerfile.nix .       - Build Docker container"
            echo ""
            echo "Environment setup:"
            echo "  Go version: $(go version)"
            echo ""

            # Set custom prompt to indicate Nix development environment
            export PS1="\[\033[1;34m\][nix-bronze-copier]\[\033[0m\] \[\033[1;32m\]\u@\h\[\033[0m\]:\[\033[1;34m\]\w\[\033[0m\]\$ "
          '';
        };

        # Package outputs
        packages = {
          default = bronze-copier;
          bronze-copier = bronze-copier;
          push-to-dockerhub-prod = pushToProd;
          push-to-dockerhub-dev = pushToDev;
        };

        # Application output
        apps = {
          default = flake-utils.lib.mkApp {
            drv = bronze-copier;
            exePath = "/bin/bronze-copier";
          };
        };

        # Formatter for `nix fmt`
        formatter = pkgs.nixpkgs-fmt;
      });
}
