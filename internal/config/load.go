package config

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

var envVarPattern = regexp.MustCompile(`\$\{([A-Z_][A-Z0-9_]*)\}`)

// Load reads config from (in order, last wins): defaults -> file -> env vars.
// If explicitPath is "", viper searches ./config.yaml then /etc/rss2msg/config.yaml.
func Load(explicitPath string) (Config, error) {
	v := viper.New()
	applyDefaults(v, Defaults())

	v.SetEnvPrefix("RSS2MSG")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "__"))
	v.AutomaticEnv()

	if explicitPath != "" {
		v.SetConfigFile(explicitPath)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("/etc/rss2msg")
	}

	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if explicitPath != "" || !errors.As(err, &notFound) {
			return Config{}, fmt.Errorf("read config: %w", err)
		}
	}

	var cfg Config
	hook := viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		expandEnvHook(),
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
	))
	if err := v.Unmarshal(&cfg, hook); err != nil {
		return Config{}, fmt.Errorf("unmarshal config: %w", err)
	}
	return cfg, nil
}

func applyDefaults(v *viper.Viper, d Config) {
	v.SetDefault("log.level", d.Log.Level)
	v.SetDefault("log.format", d.Log.Format)
	v.SetDefault("telemetry.service_name", d.Telemetry.ServiceName)
	v.SetDefault("telemetry.traces.enabled", d.Telemetry.Traces.Enabled)
	v.SetDefault("telemetry.metrics.enabled", d.Telemetry.Metrics.Enabled)
	v.SetDefault("telemetry.logs.enabled", d.Telemetry.Logs.Enabled)
	v.SetDefault("telemetry.prometheus.enabled", d.Telemetry.Prometheus.Enabled)
	v.SetDefault("telemetry.prometheus.listen", d.Telemetry.Prometheus.Listen)
	v.SetDefault("telemetry.graphite.enabled", d.Telemetry.Graphite.Enabled)
	v.SetDefault("telemetry.graphite.address", d.Telemetry.Graphite.Address)
	v.SetDefault("telemetry.graphite.prefix", d.Telemetry.Graphite.Prefix)
	v.SetDefault("telemetry.graphite.interval", d.Telemetry.Graphite.Interval)
	v.SetDefault("telemetry.sentry.enabled", d.Telemetry.Sentry.Enabled)
	v.SetDefault("telemetry.sentry.level", d.Telemetry.Sentry.Level)
	v.SetDefault("telemetry.sentry.sample_rate", d.Telemetry.Sentry.SampleRate)
	v.SetDefault("telemetry.sentry.traces_sample_rate", d.Telemetry.Sentry.TracesSampleRate)
	v.SetDefault("http.user_agent", d.HTTP.UserAgent)
	v.SetDefault("http.timeout", d.HTTP.Timeout)
	v.SetDefault("retry.max_attempts", d.Retry.MaxAttempts)
	v.SetDefault("retry.base_delay", d.Retry.BaseDelay)
	v.SetDefault("retry.max_delay", d.Retry.MaxDelay)
	v.SetDefault("runtime.shutdown_drain_timeout", d.Runtime.ShutdownDrainTimeout)
	v.SetDefault("runtime.run_once_concurrency", d.Runtime.RunOnceConcurrency)
	v.SetDefault("coordination.driver", d.Coordination.Driver)
	v.SetDefault("health.enabled", d.Health.Enabled)
	v.SetDefault("health.listen", d.Health.Listen)
	v.SetDefault("health.liveness_path", d.Health.LivenessPath)
	v.SetDefault("health.readiness_path", d.Health.ReadinessPath)
	v.SetDefault("health.startup_path", d.Health.StartupPath)
}

// expandEnvHook returns a mapstructure DecodeHookFunc that substitutes
// ${VAR} occurrences in any string field with os.Getenv(VAR). Empty when unset.
func expandEnvHook() mapstructure.DecodeHookFuncType {
	return func(from reflect.Type, to reflect.Type, data interface{}) (interface{}, error) {
		if from.Kind() != reflect.String || to.Kind() != reflect.String {
			return data, nil
		}
		s, ok := data.(string)
		if !ok {
			return data, nil
		}
		return envVarPattern.ReplaceAllStringFunc(s, func(m string) string {
			return os.Getenv(m[2 : len(m)-1])
		}), nil
	}
}
