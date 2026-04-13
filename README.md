# pretix-nocodb

Pretix data sync provider for NocoDB

## Installation

### PyPI

```bash
pip install pretix-nocodb
```

### NixOS

For NixOS users, the plugin can be installed using the flake:

```nix
{ inputs, pkgs, ... }:
{
  services.pretix = {
    enable = true;
    plugins = [
      inputs.pretix-nocodb.packages.${pkgs.stdenv.hostPlatform.system}.default
    ];
  };
}
```

## Development

### Setup with uv

```bash
# Create virtual environment
uv venv

# Install with development dependencies
uv pip install -e ".[dev]"
```

### Setup with Nix

```bash
# Enter development shell
nix develop

# Or use direnv
direnv allow
```

### Running checks

```bash
# Run linting
uv run ruff check .

# Run type checking
uv run ty check pretix_nocodb/

# Run tests with coverage
uv run pytest tests/ --cov=pretix_nocodb --cov-report=term-missing -v
```

## License

GNU Affero General Public License v3.0 (AGPLv3)
