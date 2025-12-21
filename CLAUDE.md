# AI Context File - mssql-pg-migrate

This file provides context for AI assistants working on this project. Read this before making changes.

## Project Overview

**mssql-pg-migrate** is a high-performance CLI tool for bidirectional database migration between Microsoft SQL Server and PostgreSQL. Written in Go, it achieves 575K+ rows/sec for MSSQL→PG and 419K+ rows/sec for PG→MSSQL.

It features a modern **Terminal User Interface (TUI)** powered by Bubble Tea, offering an interactive wizard, real-time monitoring, and easy configuration management.

**Repository**: https://github.com/johndauphine/mssql-pg-migrate

## Architecture

```
cmd/migrate/main.go          # CLI entry point (urfave/cli + TUI trigger)
internal/
├── config/                  # Configuration loading and validation
│   ├── config.go           # Config structs, YAML parsing, DSN building
│   ├── permissions_unix.go # File permission check (Linux/macOS)
│   └── permissions_windows.go # File permission check (Windows)
├── tui/                     # Interactive Terminal User Interface (Bubble Tea)
│   ├── model.go            # Main TUI loop, command handling, wizard logic
│   ├── view.go             # (Logic integrated into model.go)
│   ├── styles.go           # Lip Gloss styles (colors, borders, layout)
│   ├── capture.go          # Output redirection (io.Writer -> TUI)
│   └── git.go              # Git status bar integration
├── orchestrator/           # Migration coordinator
│   └── orchestrator.go     # Main workflow: extract → create → transfer → validate
├── checkpoint/             # State persistence (SQLite)
│   └── state.go           # Run/task tracking, progress saving, resume support
├── source/                 # Source database abstraction
├── target/                 # Target database abstraction
├── transfer/              # Data transfer engine
├── pool/                  # Connection pool factory
├── progress/              # Progress bar display
└── notify/                # Slack notifications
examples/                   # Example configuration files
```

## Key Concepts

### Interactive Mode (TUI)
- Launched when no arguments are provided.
- **Framework**: Bubble Tea + Lip Gloss + Bubbles.
- **Commands**: Slash commands (e.g., `/run`, `/wizard`, `/status`) with tab completion.
- **Output Capture**: Redirects `stdout`/`stderr` to a persistent, scrollable viewport.
- **File Picker**: Dynamic `@filename` completion for config files.
- **Wizard**: Step-by-step interactive configuration generator.

### Transfer Pipeline
1. **Read-ahead**: Async goroutines pre-fetch chunks into buffered channel.
2. **Write-ahead**: Multiple parallel writers consume from channel.
3. **Chunk-level checkpointing**: Progress saved every 10 chunks for resume.

## Current State (v1.10.0+ - December 2025)

### Major Feature: Interactive TUI
- **Gemini-Style Interface**: Purple theme, rounded borders, real-time streaming logs.
- **Unified Command Bar**: Supports command execution and file selection in one input.
- **Git Integration**: Status bar shows current branch and uncommitted changes.
- **Robust Output**: Line-buffered log capture handles progress bars (`\r`) and preserves formatting.

### Recent Security Fixes
- Credentials are now sanitized before storing in SQLite state database.
- Automatic migration cleans up any previously stored passwords.
- Config file permission warnings added (chmod 600 recommended).

### Recent Features
- **Configuration Wizard**: `/wizard` command to create/edit configs interactively with SSL support.
- **Slash Commands**: `/run`, `/validate`, `/history --run <id>`, `/logs`, `/about`.
- **Auto-Completion**: Tab completion for commands and `@` for files.
- **SSL/TLS**: Full support for `ssl_mode`, `encrypt`, and `trust_server_cert`.

## Configuration

Config files use YAML with environment variable support (`${VAR_NAME}`).
See `examples/` directory for complete configurations.

## Building

```bash
# Build with TUI support
go build -o mssql-pg-migrate ./cmd/migrate
```

## Dependencies
- `github.com/charmbracelet/bubbletea`: TUI runtime.
- `github.com/charmbracelet/lipgloss`: Styling and layout.
- `github.com/charmbracelet/bubbles`: UI components (viewport, textinput).
- `github.com/urfave/cli/v2`: Command-line argument parsing.

## Session History (December 20, 2025)

### Session 2: TUI Implementation
1.  **Architecture**: Integrated `bubbletea` into `cmd/migrate/main.go` to launch TUI when no args provided.
2.  **UI Design**:
    *   Implemented "Gemini CLI" aesthetic (Purple/Dark theme).
    *   Added scrollable viewport with scrollbar.
    *   Added bordered command input at bottom.
    *   Added Git status bar (Branch + Clean/Dirty state).
3.  **Features**:
    *   **Slash Commands**: `/run`, `/validate`, `/status`, `/history`, `/wizard`, `/logs`, `/about`, `/help`.
    *   **Auto-Completion**: Tab completion for commands.
    *   **File Picker**: Type `@` to get a popup list of files in CWD, navigate with Arrows, select with Enter.
    *   **Wizard**: Interactive step-by-step config builder (supports editing existing configs).
    *   **Output Capture**: Real-time redirection of application logs to the TUI viewport with visual cues (Red X / Green Check).
    *   **Scrolling**: Mouse wheel support and PageUp/PageDown.
4.  **Fixes**:
    *   Fixed viewport jitter during typing by stabilizing layout height.
    *   Fixed "missing output" by implementing raw byte reader + line buffering.
    *   Fixed formatting alignment in validation results.
    *   Fixed wizard state transitions and defaults handling.

### Files Modified
- `internal/tui/*` - NEW: Complete TUI package.
- `cmd/migrate/main.go` - Updated to launch TUI.
- `README.md` - Added "Interactive Mode" documentation.
- `go.mod` - Added Charm libraries.

## Contact

Project maintainer: John Dauphine (jdauphine@gmail.com)