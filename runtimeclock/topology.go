package runtimeclock

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

const DeadlineDomain = "utc-quorum-v1"

// Probe describes operator-controlled physical topology, not discovered aliases.
// Tag must select this server alone in the configured NATS cluster.
type Probe struct {
	Name     string `json:"name"`
	Server   string `json:"server"`
	Identity string `json:"identity"`
	Tag      string `json:"tag"`
}

type Config struct {
	Probes       []Probe `json:"probes"`
	MaxSkewed    int     `json:"max_skewed"`
	SampleBudget string  `json:"sample_budget"`
	HealthyError string  `json:"healthy_error"`
	ReadingAge   string  `json:"reading_age"`
	Refresh      string  `json:"refresh"`
}

func DefaultConfig() Config {
	return Config{MaxSkewed: 1, SampleBudget: "250ms", HealthyError: "20ms", ReadingAge: "1s", Refresh: "100ms"}
}

// ReadConfig rejects unknown fields, trailing values and files over64KiB.
func ReadConfig(r io.Reader) (Config, error) {
	b, err := io.ReadAll(io.LimitReader(r, 64*1024+1))
	if err != nil {
		return Config{}, err
	}
	if len(b) > 64*1024 {
		return Config{}, fmt.Errorf("clock topology exceeds64KiB")
	}
	c := DefaultConfig()
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return Config{}, err
	}
	var trailing any
	if err := d.Decode(&trailing); err != io.EOF {
		return Config{}, fmt.Errorf("trailing clock topology data")
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c Config) limits() (budget, healthy, age, refresh time.Duration, err error) {
	values := []*time.Duration{&budget, &healthy, &age, &refresh}
	for i, s := range []string{c.SampleBudget, c.HealthyError, c.ReadingAge, c.Refresh} {
		*values[i], err = time.ParseDuration(s)
		if err != nil {
			return
		}
	}
	if budget <= 0 || budget > time.Second || healthy < 0 || healthy > time.Second || age <= budget || age > 5*time.Second || refresh <= 0 || refresh >= age {
		err = fmt.Errorf("invalid clock sampling/age limits")
	}
	return
}

func (c Config) Validate() error {
	if c.MaxSkewed < 0 || c.MaxSkewed > 2 || len(c.Probes) < 2*c.MaxSkewed+1 || len(c.Probes) > 5 {
		return fmt.Errorf("clock topology cannot tolerate configured skew count")
	}
	if _, _, _, _, err := c.limits(); err != nil {
		return err
	}
	seen := []map[string]bool{{}, {}, {}, {}}
	for _, p := range c.Probes {
		if !strings.HasPrefix(p.Name, "WF_CLOCK_") || len(p.Name) > 64 || strings.Trim(p.Name, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_") != "" {
			return fmt.Errorf("invalid clock probe name %q", p.Name)
		}
		for i, value := range []string{p.Name, p.Server, p.Identity, p.Tag} {
			if value == "" || len(value) > 256 || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\r\n") || seen[i][value] {
				return fmt.Errorf("empty, invalid or duplicate physical clock topology")
			}
			seen[i][value] = true
		}
	}
	return nil
}
