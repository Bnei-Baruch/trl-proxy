// Package logx wires up the structured logger (log/slog) for the whole app.
//
// Logs go either into {LogDir}/trlproxy.log or to stdout (when LogDir is empty).
// Level is controlled via LOG_LEVEL: debug|info|warn|error.
package logx

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// Init builds the root slog logger and installs it as the default.
//
// Behaviour:
//   - logDir == ""  → write to stdout (good for dev/foreground runs);
//   - logDir != ""  → write ONLY to {logDir}/trlproxy.log,
//     no duplicate to stdout (so systemd does not also store it in journald).
//
// Returns a closer that must be invoked via defer.
func Init(logDir, level string) (func(), error) {
	var writer io.Writer = os.Stdout
	var closer func()

	if logDir != "" {
		if err := os.MkdirAll(logDir, 0o755); err != nil {
			return nil, fmt.Errorf("create log dir %s: %w", logDir, err)
		}
		path := filepath.Join(logDir, "trlproxy.log")
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, fmt.Errorf("open log file %s: %w", path, err)
		}
		writer = f
		closer = func() { _ = f.Close() }
	}

	logger := slog.New(slog.NewJSONHandler(writer, &slog.HandlerOptions{
		Level: parseLevel(level),
	}))
	slog.SetDefault(logger)

	if closer == nil {
		closer = func() {}
	}
	return closer, nil
}

// WorkerFileLogger builds a slog logger that writes to a per-language file
// {LogDir}/{lang}.log (mirrors the legacy per-language log layout).
// If LogDir is empty — returns the default logger with a "lang" attribute.
func WorkerFileLogger(logDir, lang, level string) (*slog.Logger, func(), error) {
	if logDir == "" {
		return slog.Default().With("lang", lang), func() {}, nil
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create log dir %s: %w", logDir, err)
	}
	path := filepath.Join(logDir, lang+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("open log file %s: %w", path, err)
	}
	h := slog.NewJSONHandler(f, &slog.HandlerOptions{Level: parseLevel(level)})
	logger := slog.New(h).With("lang", lang)
	return logger, func() { _ = f.Close() }, nil
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error", "err":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
