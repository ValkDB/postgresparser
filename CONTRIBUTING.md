# Contributing to postgresparser

Thank you for your interest in contributing to postgresparser! This document provides guidelines and instructions for contributing.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/<your-username>/postgresparser.git`
3. Create a feature branch: `git checkout -b my-feature`
4. Make your changes
5. Run tests: `go test -race ./...`
6. Commit and push your branch
7. Open a pull request against `main`

## Development

### Prerequisites

- Go 1.25.6 or later
- ANTLR4 (only if modifying grammar files)

### Running Tests

You can use the provided `Makefile` or run commands manually:

```bash
# Using Makefile
make test

# Manually
go test -v -race -count=1 ./...
```

### Linting and Vetting

To run static analysis and linting:

```bash
# Using Makefile
make vet
make lint

# Manually (vetting hand-written code only)
go vet $(go list ./... | grep -v /gen)
golangci-lint run
```

### Regenerating the Parser

If you modify the grammar files in `grammar/`, regenerate the lexer and parser:

```bash
antlr4 -Dlanguage=Go -visitor -listener -package gen -o gen grammar/PostgreSQLLexer.g4 grammar/PostgreSQLParser.g4
```

If `antlr4` is not installed locally, you can regenerate with Docker (ANTLR 4.13.1):

```bash
docker run --rm --user "$(id -u)":"$(id -g)" \
  -v "$PWD":/work -w /work \
  --entrypoint java any0ne22/antlr4 \
  -jar /usr/local/lib/antlr4-tool.jar \
  -Dlanguage=Go -visitor -listener -package gen -Xexact-output-dir \
  -o gen grammar/PostgreSQLLexer.g4 grammar/PostgreSQLParser.g4
```

Do not manually edit files in the `gen/` directory.

## Guidelines

- Follow standard Go conventions (`gofmt`, `go vet`)
- Add tests for new functionality
- Keep commits focused and write clear commit messages
- Update documentation if your change affects the public API
- Do not add dependencies without discussion in an issue first

## Reporting Issues

- Use GitHub Issues to report bugs or request features
- Include a minimal SQL example that reproduces the problem
- Include the Go version and OS you are using

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
