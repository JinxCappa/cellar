{
  description = "Cellar dev test flake";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f {
        pkgs = nixpkgs.legacyPackages.${system};
      });
    in
    {
      packages = forAllSystems ({ pkgs }:
        let
          # Shared custom base (~5 MB) - appears in all closures
          base = pkgs.runCommand "cellar-base-0.1.0" {} ''
            mkdir -p $out/share/cellar
            head -c 5242880 /dev/urandom > $out/share/cellar/base.bin
            echo "cellar-base-0.1.0" > $out/share/cellar/version
          '';

          # Custom libs (~3 MB each)
          lib-a = pkgs.runCommand "cellar-lib-a-0.1.0" {} ''
            mkdir -p $out/lib $out/share
            head -c 3145728 /dev/urandom > $out/lib/a.bin
            cp ${base}/share/cellar/version $out/share/base-version
          '';

          lib-b = pkgs.runCommand "cellar-lib-b-0.1.0" {} ''
            mkdir -p $out/lib $out/share
            head -c 3145728 /dev/urandom > $out/lib/b.bin
            cp ${base}/share/cellar/version $out/share/base-version
          '';

          lib-c = pkgs.runCommand "cellar-lib-c-0.1.0" {} ''
            mkdir -p $out/lib
            head -c 3145728 /dev/urandom > $out/lib/c.bin
            cat ${lib-a}/share/base-version > $out/lib/lineage
          '';
        in
        {
          # Small reference - already on cache.nixos.org
          hello = pkgs.hello;

          # ~10 MB output - exercises chunked uploads
          big-data = pkgs.runCommand "cellar-big-data-0.1.0" {} ''
            mkdir -p $out/bin $out/share/cellar-test
            for i in $(seq 0 9); do
              head -c 1048576 /dev/urandom > "$out/share/cellar-test/block-$i.bin"
            done
            cat > $out/bin/cellar-big-data <<'SCRIPT'
            #!/bin/sh
            dir="$(dirname "$(readlink -f "$0")")/../share/cellar-test"
            echo "cellar-big-data: $(ls "$dir" | wc -l) blocks, $(du -sh "$dir" | cut -f1)"
            SCRIPT
            chmod +x $out/bin/cellar-big-data
          '';

          # Closure: 4 custom paths (base -> lib-a -> lib-c -> app-alpha)
          app-alpha = pkgs.runCommand "cellar-app-alpha-0.1.0" {} ''
            mkdir -p $out/bin
            cat > $out/bin/cellar-alpha <<EOF
            #!/bin/sh
            echo "alpha"
            cat ${base}/share/cellar/version
            cat ${lib-a}/share/base-version
            cat ${lib-c}/lib/lineage
            EOF
            chmod +x $out/bin/cellar-alpha
          '';

          # Closure: 3 custom paths (base -> lib-b -> app-beta)
          app-beta = pkgs.runCommand "cellar-app-beta-0.1.0" {} ''
            mkdir -p $out/bin
            cat > $out/bin/cellar-beta <<EOF
            #!/bin/sh
            echo "beta"
            cat ${base}/share/cellar/version
            cat ${lib-b}/share/base-version
            EOF
            chmod +x $out/bin/cellar-beta
          '';

          # Closure: 5 custom paths (base -> lib-a + lib-b -> lib-c -> app-gamma)
          app-gamma = pkgs.runCommand "cellar-app-gamma-0.1.0" {} ''
            mkdir -p $out/bin
            cat > $out/bin/cellar-gamma <<EOF
            #!/bin/sh
            echo "gamma"
            cat ${base}/share/cellar/version
            cat ${lib-a}/share/base-version
            cat ${lib-b}/share/base-version
            cat ${lib-c}/lib/lineage
            EOF
            chmod +x $out/bin/cellar-gamma
          '';
        });
    };
}
