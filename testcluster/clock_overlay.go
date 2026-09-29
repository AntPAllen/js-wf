package testcluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// WriteClockOverlay writes a test-only Go build overlay that shifts time.Now's
// wall clock while preserving its monotonic component. It fails when the Go
// source shape changes, so a test cannot silently run with an unshifted clock.
func WriteClockOverlay(root string, offset time.Duration) (string, error) {
	if offset == 0 || offset%time.Second != 0 || offset < -60*time.Second || offset > 60*time.Second {
		return "", fmt.Errorf("clock overlay offset must be whole seconds within ±60s: %s", offset)
	}
	if err := os.MkdirAll(root, 0755); err != nil {
		return "", err
	}
	source := filepath.Join(runtime.GOROOT(), "src", "time", "time.go")
	content, err := os.ReadFile(source)
	if err != nil {
		return "", fmt.Errorf("read Go clock source: %w", err)
	}
	const original = "\tsec, nsec, mono := runtimeNow()\n"
	if strings.Count(string(content), original) != 1 {
		return "", fmt.Errorf("Go clock source does not contain exactly one runtimeNow call")
	}
	replacement := original + fmt.Sprintf("\tsec += %d // test-only wall-clock skew\n", int64(offset/time.Second))
	patched := filepath.Join(root, "skew-time.go")
	if err := os.WriteFile(patched, []byte(strings.Replace(string(content), original, replacement, 1)), 0644); err != nil {
		return "", err
	}
	overlay, err := json.Marshal(struct {
		Replace map[string]string `json:"Replace"`
	}{Replace: map[string]string{source: patched}})
	if err != nil {
		return "", err
	}
	overlayPath := filepath.Join(root, "skew-overlay.json")
	if err := os.WriteFile(overlayPath, overlay, 0644); err != nil {
		return "", err
	}
	return overlayPath, nil
}
