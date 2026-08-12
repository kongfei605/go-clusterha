package clusterha

import (
	"fmt"
	"strconv"
	"time"
)

type Duration time.Duration

func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

func (d Duration) String() string {
	return time.Duration(d).String()
}

func (d *Duration) UnmarshalText(text []byte) error {
	value, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = Duration(value)
	return nil
}

func (d *Duration) UnmarshalTOML(value any) error {
	switch v := value.(type) {
	case string:
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return err
		}
		*d = Duration(parsed)
		return nil
	case int64:
		*d = Duration(time.Duration(v) * time.Second)
		return nil
	case int:
		*d = Duration(time.Duration(v) * time.Second)
		return nil
	case float64:
		*d = Duration(time.Duration(v) * time.Second)
		return nil
	default:
		return fmt.Errorf("invalid duration value %T", value)
	}
}

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

func ParseDuration(raw string) (Duration, error) {
	if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return Duration(time.Duration(seconds) * time.Second), nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, err
	}
	return Duration(parsed), nil
}
