package config

import (
	"strings"
	"testing"
	"time"
)

func goodCfg() Config {
	c := Defaults()
	c.State.Driver = "postgres"
	c.State.Postgres.DSN = "postgres://x"
	c.Sinks = []SinkConfig{
		{Name: "default", Driver: "kafka", Kafka: KafkaSinkConfig{Brokers: []string{"k:9092"}, Topic: "t"}},
		{Name: "pg-main", Driver: "postgres", Postgres: PostgresSinkConfig{DSN: "postgres://x", Table: "feed_changes"}},
		{Name: "dlq-main", Driver: "postgres", Postgres: PostgresSinkConfig{DSN: "postgres://x", Table: "feed_changes_dlq"}},
	}
	c.Sinks[1].DeadLetter = "dlq-main"
	c.Feeds = []FeedConfig{
		{URL: "https://e/1", Interval: 5 * time.Minute, Sinks: []string{"pg-main"}},
		{URL: "https://e/2", Interval: 15 * time.Minute},
	}
	return c
}

func TestValidateAcceptsGoodConfig(t *testing.T) {
	t.Parallel()
	if _, err := Validate(goodCfg()); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsUnknownSinkName(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Feeds[0].Sinks = []string{"does-not-exist"}
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "does-not-exist") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRequiresDefaultSinkWhenFeedOmitsSinks(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = c.Sinks[1:] // remove "default"
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), `"default"`) {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsSinkAsItsOwnDLQ(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks[1].DeadLetter = c.Sinks[1].Name
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "dead_letter") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateUnknownDLQTargetNamesTheTarget(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks[1].DeadLetter = "does-not-exist"
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "does-not-exist") {
		t.Fatalf("error must name the unknown dead_letter target, got %v", err)
	}
}

func TestValidateRejectsIntervalBelowOneSecond(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Feeds[0].Interval = 500 * time.Millisecond
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "interval") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsReservedHeaderOverrides(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Feeds[1].HTTP.Headers = map[string]string{"If-Modified-Since": "now"}
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "If-Modified-Since") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsDuplicateSinkNames(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{Name: "pg-main", Driver: "postgres", Postgres: PostgresSinkConfig{DSN: "x", Table: "y"}})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsUnknownStateDriver(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.State.Driver = "redis"
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "state.driver") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsCoordinationPostgres(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination = CoordinationConfig{Driver: "postgres", Postgres: CoordinationPGConfig{DSN: "postgres://x"}}
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateCoordinationPostgresFallsBackToStateDSN(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination = CoordinationConfig{Driver: "postgres"} // empty DSN
	// goodCfg sets c.State.Postgres.DSN = "postgres://x" — fallback should succeed.
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected fallback to state DSN, got %v", err)
	}
}

func TestValidateRejectsUnknownCoordinationDriver(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination = CoordinationConfig{Driver: "memcached"}
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "coordination.driver") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsCoordinationPostgresMissingDSN(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.State.Postgres.DSN = ""   // strip state DSN too
	c.State.Driver = "postgres" // keep driver
	c.Coordination = CoordinationConfig{Driver: "postgres"}
	_, err := Validate(c)
	if err == nil {
		t.Fatal("expected error when both coord and state DSNs empty")
	}
}

func TestValidateAcceptsSQSSink(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "sqs-x", Driver: "sqs",
		SQS: SQSSinkConfig{QueueURL: "https://sqs.us-east-1.amazonaws.com/123/q"},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsSQSWithoutQueueURL(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{Name: "sqs-x", Driver: "sqs"})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "queue_url") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsFIFOSQSURL(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "sqs-x", Driver: "sqs",
		SQS: SQSSinkConfig{
			QueueURL:     "https://sqs.us-east-1.amazonaws.com/123/q.fifo",
			MessageGroup: "feed_url",
		},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateAcceptsFIFOSQSURLWithoutExplicitMessageGroup(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "sqs-x", Driver: "sqs",
		SQS: SQSSinkConfig{QueueURL: "https://sqs.us-east-1.amazonaws.com/123/q.fifo"},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsSQSUnknownMessageGroup(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "sqs-x", Driver: "sqs",
		SQS: SQSSinkConfig{
			QueueURL:     "https://sqs.us-east-1.amazonaws.com/123/q.fifo",
			MessageGroup: "broadcast",
		},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "message_group") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsSQSMessageGroupOnStandardQueue(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "sqs-x", Driver: "sqs",
		SQS: SQSSinkConfig{
			QueueURL:     "https://sqs.us-east-1.amazonaws.com/123/q",
			MessageGroup: "feed_url",
		},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "FIFO") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsSNSSink(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "sns-x", Driver: "sns",
		SNS: SNSSinkConfig{TopicARN: "arn:aws:sns:us-east-1:123:t"},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsSNSWithoutTopicARN(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{Name: "sns-x", Driver: "sns"})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "topic_arn") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsFIFOSNSTopicARN(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "sns-x", Driver: "sns",
		SNS: SNSSinkConfig{
			TopicARN:     "arn:aws:sns:us-east-1:123:t.fifo",
			MessageGroup: "feed_url",
		},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateAcceptsFIFOSNSTopicARNWithoutExplicitMessageGroup(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "sns-x", Driver: "sns",
		SNS: SNSSinkConfig{TopicARN: "arn:aws:sns:us-east-1:123:t.fifo"},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsSNSUnknownMessageGroup(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "sns-x", Driver: "sns",
		SNS: SNSSinkConfig{
			TopicARN:     "arn:aws:sns:us-east-1:123:t.fifo",
			MessageGroup: "broadcast",
		},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "message_group") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsSNSMessageGroupOnStandardTopic(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "sns-x", Driver: "sns",
		SNS: SNSSinkConfig{
			TopicARN:     "arn:aws:sns:us-east-1:123:t",
			MessageGroup: "feed_url",
		},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "FIFO") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsCoordinationRedis(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination.Driver = "redis"
	c.Coordination.Redis.URL = "redis://localhost:6379/0"
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsRedisWithoutURL(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination.Driver = "redis"
	c.Coordination.Redis.URL = ""
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "coordination.redis.url") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsRedisWithUnparseableURL(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination.Driver = "redis"
	c.Coordination.Redis.URL = "not a url"
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "coordination.redis.url") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsRedisURLWithNegativeDBIndex(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination.Driver = "redis"
	c.Coordination.Redis.URL = "redis://localhost:6379/-1"
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "db index") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsRedisLockTTLBelowOneSecond(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination.Driver = "redis"
	c.Coordination.Redis.URL = "redis://localhost:6379/0"
	c.Coordination.Redis.LockTTL = 500 * time.Millisecond
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "lock_ttl") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsRedisRenewalAtOrAboveTTL(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination.Driver = "redis"
	c.Coordination.Redis.URL = "redis://localhost:6379/0"
	c.Coordination.Redis.LockTTL = 5 * time.Second
	c.Coordination.Redis.RenewalInterval = 5 * time.Second
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "renewal_interval") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsStatePGWithTLSBlock(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.State = StateConfig{
		Driver: "postgres",
		Postgres: PostgresStateConfig{
			DSN: "postgres://pg.internal:5432/db?sslmode=require",
			TLS: StatePGTLSConfig{
				CAFile:     "/etc/ssl/pg-ca.pem",
				CertFile:   "/etc/ssl/pg-client.pem",
				KeyFile:    "/etc/ssl/pg-client.key",
				ServerName: "pg.internal",
			},
		},
	}
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsStatePGTLSWithSSLModeDisable(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.State = StateConfig{
		Driver: "postgres",
		Postgres: PostgresStateConfig{
			DSN: "postgres://pg.internal:5432/db?sslmode=disable",
			TLS: StatePGTLSConfig{CAFile: "/etc/ssl/pg-ca.pem"},
		},
	}
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "state.postgres.tls") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsStatePGTLSCertWithoutKey(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.State = StateConfig{
		Driver: "postgres",
		Postgres: PostgresStateConfig{
			DSN: "postgres://pg.internal:5432/db?sslmode=require",
			TLS: StatePGTLSConfig{CertFile: "/etc/ssl/pg-client.pem"},
		},
	}
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "state.postgres.tls.cert_file and key_file") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsCoordinationPGWithTLSBlock(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination = CoordinationConfig{
		Driver: "postgres",
		Postgres: CoordinationPGConfig{
			DSN: "postgres://pg.internal:5432/db?sslmode=require",
			TLS: CoordinationPGTLSConfig{
				CAFile:     "/etc/ssl/pg-ca.pem",
				CertFile:   "/etc/ssl/pg-client.pem",
				KeyFile:    "/etc/ssl/pg-client.key",
				ServerName: "pg.internal",
			},
		},
	}
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsCoordinationPGTLSWithSSLModeDisable(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination = CoordinationConfig{
		Driver: "postgres",
		Postgres: CoordinationPGConfig{
			DSN: "postgres://pg.internal:5432/db?sslmode=disable",
			TLS: CoordinationPGTLSConfig{CAFile: "/etc/ssl/pg-ca.pem"},
		},
	}
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "sslmode=disable") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsCoordinationPGTLSWithKeywordSSLModeDisable(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination = CoordinationConfig{
		Driver: "postgres",
		Postgres: CoordinationPGConfig{
			DSN: "host=pg.internal port=5432 dbname=db sslmode=disable",
			TLS: CoordinationPGTLSConfig{InsecureSkipVerify: true},
		},
	}
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "sslmode=disable") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsCoordinationPGTLSCertWithoutKey(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination = CoordinationConfig{
		Driver: "postgres",
		Postgres: CoordinationPGConfig{
			DSN: "postgres://pg.internal:5432/db?sslmode=require",
			TLS: CoordinationPGTLSConfig{CertFile: "/etc/ssl/pg-client.pem"},
		},
	}
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "cert_file and key_file") {
		t.Fatalf("got %v", err)
	}
}

func TestPgSSLModeIsDisable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		dsn  string
		want bool
	}{
		{"postgres://pg/db?sslmode=disable", true},
		{"postgres://pg/db?sslmode=DISABLE", true},
		{"postgres://pg/db?sslmode=require", false},
		{"postgres://pg/db", false},
		{"host=pg dbname=d sslmode=disable", true},
		{`host=pg dbname=d sslmode="disable"`, true},
		{"host=pg dbname=d sslmode = disable", true}, // pgx tolerates whitespace around `=`
		{"host=pg dbname=d  sslmode=disable", true},  // double-space separator
		{"host=pg dbname=d sslmode=verify-full", false},
		{"host=pg password=mysslmodepw dbname=d", false}, // word boundary prevents substring match
		{"host=pg dbname=d", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := pgSSLModeIsDisable(tc.dsn); got != tc.want {
			t.Errorf("pgSSLModeIsDisable(%q) = %v, want %v", tc.dsn, got, tc.want)
		}
	}
}

func TestValidateAcceptsRedisTLSWithRediss(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination.Driver = "redis"
	c.Coordination.Redis.URL = "rediss://localhost:6379/0"
	c.Coordination.Redis.TLS = CoordinationRedisTLSConfig{
		CAFile:     "/etc/ssl/ca.pem",
		CertFile:   "/etc/ssl/client.pem",
		KeyFile:    "/etc/ssl/client.key",
		ServerName: "redis.internal",
	}
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsRedisTLSWithPlainScheme(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination.Driver = "redis"
	c.Coordination.Redis.URL = "redis://localhost:6379/0"
	c.Coordination.Redis.TLS = CoordinationRedisTLSConfig{CAFile: "/etc/ssl/ca.pem"}
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "rediss://") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsRedisTLSCertWithoutKey(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination.Driver = "redis"
	c.Coordination.Redis.URL = "rediss://localhost:6379/0"
	c.Coordination.Redis.TLS = CoordinationRedisTLSConfig{CertFile: "/etc/ssl/client.pem"}
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "cert_file and key_file") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsRedisTLSKeyWithoutCert(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination.Driver = "redis"
	c.Coordination.Redis.URL = "rediss://localhost:6379/0"
	c.Coordination.Redis.TLS = CoordinationRedisTLSConfig{KeyFile: "/etc/ssl/client.key"}
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "cert_file and key_file") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsStateSQLite(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.State = StateConfig{Driver: "sqlite", SQLite: SQLiteStateConfig{Path: "./rss2msg.db"}}
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsStateSQLiteMissingPath(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.State = StateConfig{Driver: "sqlite"}
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "state.sqlite.path") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsHTTPSinkWithDefaults(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "hook", Driver: "http",
		HTTP: HTTPSinkConfig{URL: "https://example.com/hook"},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateAcceptsHTTPSinkWithFullConfig(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "hook", Driver: "http",
		HTTP: HTTPSinkConfig{
			URL:          "https://example.com/hook",
			Method:       "PUT",
			Headers:      map[string]string{"Authorization": "Bearer x"},
			Timeout:      10 * time.Second,
			SuccessCodes: []int{200, 202, 418},
		},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsHTTPSinkMissingURL(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{Name: "hook", Driver: "http"})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "http.url") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsHTTPSinkBadURLScheme(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "hook", Driver: "http",
		HTTP: HTTPSinkConfig{URL: "ftp://example/h"},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "valid http(s) URL") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsHTTPSinkBadMethod(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "hook", Driver: "http",
		HTTP: HTTPSinkConfig{URL: "https://example/h", Method: "DELETE"},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "method") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsHTTPSinkBadSuccessCode(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "hook", Driver: "http",
		HTTP: HTTPSinkConfig{URL: "https://example/h", SuccessCodes: []int{600}},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "success_code") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsStdoutSinkWithDefaults(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{Name: "out", Driver: "stdout"})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateAcceptsStdoutSinkWithExplicitTargetAndFormat(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "err", Driver: "stdout",
		Stdout: StdoutSinkConfig{Target: "stderr", Format: "pretty"},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsStdoutSinkUnknownTarget(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "x", Driver: "stdout",
		Stdout: StdoutSinkConfig{Target: "syslog"},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "target") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsStdoutSinkUnknownFormat(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "x", Driver: "stdout",
		Stdout: StdoutSinkConfig{Format: "yaml"},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "format") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsRabbitMQSink(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name:   "rmq-x",
		Driver: "rabbitmq",
		RabbitMQ: RabbitMQSinkConfig{
			URL:          "amqp://guest:guest@localhost:5672/",
			Exchange:     "feed.changes",
			ExchangeType: "topic",
			RoutingKey:   "feed.changes",
			Declare:      true,
			Durable:      true,
		},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsRabbitMQWithoutURL(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{Name: "rmq-x", Driver: "rabbitmq"})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "rabbitmq.url") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsRabbitMQUnknownExchangeType(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name:   "rmq-x",
		Driver: "rabbitmq",
		RabbitMQ: RabbitMQSinkConfig{
			URL:          "amqp://localhost",
			ExchangeType: "broadcast",
		},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "exchange_type") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsRabbitMQDeclareWithoutExchange(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name:   "rmq-x",
		Driver: "rabbitmq",
		RabbitMQ: RabbitMQSinkConfig{
			URL:     "amqp://localhost",
			Declare: true,
		},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "declare=true") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsRabbitMQWithoutOptionalFields(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name:   "rmq-x",
		Driver: "rabbitmq",
		RabbitMQ: RabbitMQSinkConfig{
			URL:        "amqp://localhost",
			RoutingKey: "rss2msg",
		},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil with only url+routing_key, got %v", err)
	}
}

func TestValidateAllowsEmptyFeedsWithSources(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	cfg.State = StateConfig{Driver: "sqlite", SQLite: SQLiteStateConfig{Path: "x.db"}}
	cfg.Sinks = []SinkConfig{{Name: "default", Driver: "stdout"}}
	cfg.Feeds = nil
	cfg.FeedSources = []FeedSourceConfig{{Type: "file", Path: "/tmp/feeds.json"}}
	if _, err := Validate(cfg); err != nil {
		t.Fatalf("expected valid, got %v", err)
	}
}

func TestValidateRejectsFileSourceWithoutPath(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	cfg.State = StateConfig{Driver: "sqlite", SQLite: SQLiteStateConfig{Path: "x.db"}}
	cfg.Sinks = []SinkConfig{{Name: "default", Driver: "stdout"}}
	cfg.FeedSources = []FeedSourceConfig{{Type: "file"}}
	if _, err := Validate(cfg); err == nil {
		t.Fatal("expected error for file source without path")
	}
}

func TestValidateRejectsNoFeedsAndNoSources(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	cfg.State = StateConfig{Driver: "sqlite", SQLite: SQLiteStateConfig{Path: "x.db"}}
	cfg.Sinks = []SinkConfig{{Name: "default", Driver: "stdout"}}
	cfg.Feeds = nil
	cfg.FeedSources = nil
	if _, err := Validate(cfg); err == nil {
		t.Fatal("expected error when neither feeds nor feed_sources is set")
	}
}

func TestValidate_NoWarningsForSingleInstance(t *testing.T) {
	cfg := Defaults()
	cfg.State = StateConfig{Driver: "sqlite", SQLite: SQLiteStateConfig{Path: "/tmp/s.db"}}
	cfg.Feeds = []FeedConfig{{URL: "https://example.com/f.xml", Interval: 5 * time.Minute}}
	cfg.Sinks = []SinkConfig{{Name: "default", Driver: "stdout", Stdout: StdoutSinkConfig{Target: "stdout", Format: "json"}}}
	warnings, err := Validate(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings, got %v", warnings)
	}
}
