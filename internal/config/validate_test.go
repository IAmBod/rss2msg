package config

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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

func TestValidateRejectsNegativeDeliverTimeout(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Runtime.DeliverTimeout = -1 * time.Second
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "deliver_timeout") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsZeroAndPositiveDeliverTimeout(t *testing.T) {
	t.Parallel()
	for _, d := range []time.Duration{0, 30 * time.Second} {
		c := goodCfg()
		c.Runtime.DeliverTimeout = d
		if _, err := Validate(c); err != nil {
			t.Fatalf("deliver_timeout=%v: unexpected error %v", d, err)
		}
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

func TestValidateAcceptsDynamoDBSink(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "ddb-x", Driver: "dynamodb",
		DynamoDB: DynamoDBSinkConfig{Table: "rss2msg-changes"},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsDynamoDBWithoutTable(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{Name: "ddb-x", Driver: "dynamodb"})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "table") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsDynamoDBItemTTLWithoutAttribute(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "ddb-x", Driver: "dynamodb",
		DynamoDB: DynamoDBSinkConfig{Table: "t", ItemTTL: time.Hour},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "ttl_attribute") {
		t.Fatalf("got %v", err)
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

func TestValidateAcceptsNATSSink(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "nats-x", Driver: "nats",
		NATS: NATSSinkConfig{URL: "nats://localhost:4222", Subject: "feed.changes"},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsNATSWithoutURL(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "nats-x", Driver: "nats",
		NATS: NATSSinkConfig{Subject: "feed.changes"},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "nats.url") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsNATSWithoutSubject(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "nats-x", Driver: "nats",
		NATS: NATSSinkConfig{URL: "nats://localhost:4222"},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "nats.subject") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsNATSConflictingAuth(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "nats-x", Driver: "nats",
		NATS: NATSSinkConfig{
			URL: "nats://localhost:4222", Subject: "feed.changes",
			Token: "tok", Username: "u", Password: "p",
		},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "at most one of token") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsNATSPartialUserPassword(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "nats-x", Driver: "nats",
		NATS: NATSSinkConfig{URL: "nats://localhost:4222", Subject: "feed.changes", Username: "u"},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "username and password") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsNATSTLSCertWithoutKey(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "nats-x", Driver: "nats",
		NATS: NATSSinkConfig{
			URL: "nats://localhost:4222", Subject: "feed.changes",
			TLS: SinkTLSConfig{CertFile: "/tmp/c.pem"},
		},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "cert_file and key_file") {
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

func TestValidateAcceptsCoordinationDynamoDB(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination.Driver = "dynamodb"
	c.Coordination.DynamoDB.Table = "rss2msg-locks"
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsCoordinationDynamoDBWithoutTable(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination.Driver = "dynamodb"
	c.Coordination.DynamoDB.Table = ""
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "coordination.dynamodb.table") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsDynamoDBSubSecondLease(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Coordination.Driver = "dynamodb"
	c.Coordination.DynamoDB.Table = "rss2msg-locks"
	c.Coordination.DynamoDB.LeaseDuration = 500 * time.Millisecond
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "lease_duration") {
		t.Fatalf("got %v", err)
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

func TestValidateAcceptsStateDynamoDB(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.State = StateConfig{Driver: "dynamodb", DynamoDB: DynamoDBStateConfig{Table: "rss2msg-state"}}
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateAcceptsStateDynamoDBWithTTL(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.State = StateConfig{Driver: "dynamodb", DynamoDB: DynamoDBStateConfig{
		Table: "t", TTLAttribute: "expires_at", ItemTTL: 720 * time.Hour,
	}}
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsStateDynamoDBMissingTable(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.State = StateConfig{Driver: "dynamodb"}
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "state.dynamodb.table") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsStateDynamoDBHalfTTL(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.State = StateConfig{Driver: "dynamodb", DynamoDB: DynamoDBStateConfig{Table: "t", TTLAttribute: "expires_at"}}
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "ttl_attribute and item_ttl") {
		t.Fatalf("got %v", err)
	}
}

func TestValidate_NoSQLiteStateWarningWithDynamoDBState(t *testing.T) {
	// DynamoDB state is a shared, distributed-safe store, so pairing it with a
	// distributed coordinator must not trip the single-instance warning.
	c := sqliteStateScaleBase()
	c.State = StateConfig{Driver: "dynamodb", DynamoDB: DynamoDBStateConfig{Table: "t"}}
	c.Coordination = CoordinationConfig{Driver: "redis", Redis: CoordinationRedisConfig{URL: "redis://localhost:6379"}}
	warnings, err := Validate(c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hasSQLiteStateScaleWarning(warnings) {
		t.Fatalf("did not expect sqlite-state warning for dynamodb state, got %v", warnings)
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

func TestValidateAllowsPostgresSource(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	cfg.State = StateConfig{Driver: "sqlite", SQLite: SQLiteStateConfig{Path: "x.db"}}
	cfg.Sinks = []SinkConfig{{Name: "default", Driver: "stdout"}}
	cfg.Feeds = nil
	cfg.FeedSources = []FeedSourceConfig{{
		Type:     "postgres",
		Postgres: PostgresFeedSourceConfig{DSN: "postgres://u@h/db"},
	}}
	if _, err := Validate(cfg); err != nil {
		t.Fatalf("expected valid, got %v", err)
	}
}

func TestValidateRejectsPostgresSourceWithoutDSN(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	cfg.State = StateConfig{Driver: "sqlite", SQLite: SQLiteStateConfig{Path: "x.db"}}
	cfg.Sinks = []SinkConfig{{Name: "default", Driver: "stdout"}}
	cfg.FeedSources = []FeedSourceConfig{{Type: "postgres"}}
	if _, err := Validate(cfg); err == nil {
		t.Fatal("expected error for postgres source without dsn")
	}
}

func TestValidateRejectsPostgresSourceTableAndQuery(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	cfg.State = StateConfig{Driver: "sqlite", SQLite: SQLiteStateConfig{Path: "x.db"}}
	cfg.Sinks = []SinkConfig{{Name: "default", Driver: "stdout"}}
	cfg.FeedSources = []FeedSourceConfig{{
		Type:     "postgres",
		Postgres: PostgresFeedSourceConfig{DSN: "postgres://u@h/db", Table: "feeds", Query: "SELECT 1"},
	}}
	if _, err := Validate(cfg); err == nil {
		t.Fatal("expected error when table and query both set")
	}
}

func TestValidateRejectsPostgresSourceBadTable(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	cfg.State = StateConfig{Driver: "sqlite", SQLite: SQLiteStateConfig{Path: "x.db"}}
	cfg.Sinks = []SinkConfig{{Name: "default", Driver: "stdout"}}
	cfg.FeedSources = []FeedSourceConfig{{
		Type:     "postgres",
		Postgres: PostgresFeedSourceConfig{DSN: "postgres://u@h/db", Table: "bad table!"},
	}}
	if _, err := Validate(cfg); err == nil {
		t.Fatal("expected error for invalid table identifier")
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

func feedSinkBase() Config {
	c := Defaults()
	c.State = StateConfig{Driver: "sqlite", SQLite: SQLiteStateConfig{Path: "/tmp/s.db"}}
	c.Feeds = []FeedConfig{{URL: "https://example.com/f.xml", Interval: 5 * time.Minute, Sinks: []string{"out"}}}
	c.Sinks = []SinkConfig{{Name: "out", Driver: "feed", Feed: FeedSinkConfig{
		Listen: ":8088", Link: "https://example.com/", MaxItems: 10,
		Store: FeedStoreConfig{Driver: "memory"},
	}}}
	return c
}

func TestValidate_FeedRequiresListen(t *testing.T) {
	c := feedSinkBase()
	c.Sinks[0].Feed.Listen = ""
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error for missing listen")
	}
}

func TestValidate_FeedMaxItemsOmittedIsAllowed(t *testing.T) {
	// max_items omitted (0) is valid — the sink applies the default (50).
	c := feedSinkBase()
	c.Sinks[0].Feed.MaxItems = 0
	if _, err := Validate(c); err != nil {
		t.Fatalf("omitted max_items must be allowed (defaults later), got %v", err)
	}
}

func TestValidate_FeedRejectsNegativeMaxItems(t *testing.T) {
	c := feedSinkBase()
	c.Sinks[0].Feed.MaxItems = -1
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error for negative max_items")
	}
}

func TestValidate_FeedRejectsBadStoreDriver(t *testing.T) {
	c := feedSinkBase()
	c.Sinks[0].Feed.Store.Driver = "redis"
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error for unknown store driver")
	}
}

func TestValidate_FeedSqliteRequiresPath(t *testing.T) {
	c := feedSinkBase()
	c.Sinks[0].Feed.Store = FeedStoreConfig{Driver: "sqlite"}
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error for missing sqlite path")
	}
}

func TestValidate_FeedAuthExactlyOne(t *testing.T) {
	c := feedSinkBase()
	c.Sinks[0].Feed.Auth = FeedAuthConfig{Basic: FeedBasicAuthConfig{Username: "u", Password: "p"}, BearerToken: "t"}
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error for both basic and bearer set")
	}
}

func TestValidate_FeedRejectsMCPPathNotAbsolute(t *testing.T) {
	c := feedSinkBase()
	on := true
	c.Sinks[0].Feed.MCP = FeedSurfaceConfig{Enabled: &on, Path: "mcp"}
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error for mcp.path missing leading /")
	}
}

func TestValidate_FeedRejectsSurfacePathCollision(t *testing.T) {
	c := feedSinkBase()
	on := true
	// mcp enabled on the same path as the (default-enabled) rss surface.
	c.Sinks[0].Feed.MCP = FeedSurfaceConfig{Enabled: &on, Path: "/rss"}
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error for mcp.path colliding with rss.path")
	}
}

func TestValidate_FeedMCPEnabledDefaultPathIsValid(t *testing.T) {
	c := feedSinkBase()
	on := true
	c.Sinks[0].Feed.MCP = FeedSurfaceConfig{Enabled: &on} // path defaults to /mcp, distinct
	if _, err := Validate(c); err != nil {
		t.Fatalf("mcp enabled with default path should be valid, got %v", err)
	}
}

func TestValidate_FeedRejectsAllSurfacesDisabled(t *testing.T) {
	c := feedSinkBase()
	off := false
	c.Sinks[0].Feed.RSS = FeedSurfaceConfig{Enabled: &off}
	c.Sinks[0].Feed.Atom = FeedSurfaceConfig{Enabled: &off}
	// mcp is off by default → nothing is served.
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error when no feed surface is enabled")
	}
}

func TestValidate_FeedCannotBeDeadLetter(t *testing.T) {
	c := feedSinkBase()
	c.Sinks = append(c.Sinks, SinkConfig{Name: "p", Driver: "stdout",
		Stdout: StdoutSinkConfig{Target: "stdout", Format: "json"}, DeadLetter: "out"})
	if _, err := Validate(c); err == nil {
		t.Fatal("expected error: feed sink used as dead_letter")
	}
}

func TestValidateAcceptsComposite(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name:      "fanout",
		Driver:    "composite",
		Composite: CompositeSinkConfig{Children: []string{"pg-main", "dlq-main"}},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func compositeCfg(children []string) Config {
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "fanout", Driver: "composite",
		Composite: CompositeSinkConfig{Children: children},
	})
	return c
}

func TestValidateRejectsCompositeEmptyChildren(t *testing.T) {
	t.Parallel()
	_, err := Validate(compositeCfg(nil))
	if err == nil || !strings.Contains(err.Error(), "children") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsCompositeUnknownChild(t *testing.T) {
	t.Parallel()
	_, err := Validate(compositeCfg([]string{"nope"}))
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsCompositeSelfReference(t *testing.T) {
	t.Parallel()
	_, err := Validate(compositeCfg([]string{"fanout"}))
	if err == nil || !strings.Contains(err.Error(), "its own sink") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsCompositeDuplicateChild(t *testing.T) {
	t.Parallel()
	_, err := Validate(compositeCfg([]string{"pg-main", "pg-main"}))
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsCompositeWithDeadLetter(t *testing.T) {
	t.Parallel()
	c := compositeCfg([]string{"pg-main"})
	c.Sinks[len(c.Sinks)-1].DeadLetter = "dlq-main"
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "dead_letter") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsCompositeCycle(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks,
		SinkConfig{Name: "a", Driver: "composite", Composite: CompositeSinkConfig{Children: []string{"b"}}},
		SinkConfig{Name: "b", Driver: "composite", Composite: CompositeSinkConfig{Children: []string{"a"}}},
	)
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "cyclic") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsCompositeChain(t *testing.T) {
	t.Parallel()
	// A cycle-free composite DAG (a -> b -> pg-main) must validate: the cycle
	// detector should return no error for a multi-hop, nested-but-acyclic graph.
	c := goodCfg()
	c.Sinks = append(c.Sinks,
		SinkConfig{Name: "a", Driver: "composite", Composite: CompositeSinkConfig{Children: []string{"b"}}},
		SinkConfig{Name: "b", Driver: "composite", Composite: CompositeSinkConfig{Children: []string{"pg-main"}}},
	)
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil for acyclic composite chain, got %v", err)
	}
}

func TestValidate_FeedMultiInstanceWarning(t *testing.T) {
	c := feedSinkBase()
	c.Coordination = CoordinationConfig{Driver: "redis", Redis: CoordinationRedisConfig{URL: "redis://localhost:6379"}}
	warnings, err := Validate(c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "out") && strings.Contains(w, "partial") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected multi-instance partial-feed warning, got %v", warnings)
	}
}

// sqliteStateScaleBase returns a config with a per-instance SQLite state store
// and a single stdout sink — the caller sets Coordination to exercise the
// distributed-coordinator-plus-local-state guardrail.
func sqliteStateScaleBase() Config {
	c := Defaults()
	c.State = StateConfig{Driver: "sqlite", SQLite: SQLiteStateConfig{Path: "/tmp/s.db"}}
	c.Sinks = []SinkConfig{{Name: "default", Driver: "stdout", Stdout: StdoutSinkConfig{Target: "stdout", Format: "json"}}}
	c.Feeds = []FeedConfig{{URL: "https://example.com/f.xml", Interval: 5 * time.Minute}}
	return c
}

func hasSQLiteStateScaleWarning(warnings []string) bool {
	for _, w := range warnings {
		if strings.Contains(w, "state.driver") && strings.Contains(w, "sqlite") && strings.Contains(w, "postgres") {
			return true
		}
	}
	return false
}

func TestValidate_WarnsRedisCoordWithSQLiteState(t *testing.T) {
	c := sqliteStateScaleBase()
	c.Coordination = CoordinationConfig{Driver: "redis", Redis: CoordinationRedisConfig{URL: "redis://localhost:6379"}}
	warnings, err := Validate(c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hasSQLiteStateScaleWarning(warnings) {
		t.Fatalf("expected distributed-coordinator + sqlite-state warning, got %v", warnings)
	}
}

func TestValidate_WarnsPostgresCoordWithSQLiteState(t *testing.T) {
	c := sqliteStateScaleBase()
	c.Coordination = CoordinationConfig{Driver: "postgres", Postgres: CoordinationPGConfig{DSN: "postgres://x"}}
	warnings, err := Validate(c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hasSQLiteStateScaleWarning(warnings) {
		t.Fatalf("expected distributed-coordinator + sqlite-state warning, got %v", warnings)
	}
}

func TestValidate_NoSQLiteStateWarningWithPostgresState(t *testing.T) {
	// Postgres state store is shared across instances, so a distributed
	// coordinator is the correct, warning-free pairing.
	c := goodCfg() // postgres state
	c.Coordination = CoordinationConfig{Driver: "redis", Redis: CoordinationRedisConfig{URL: "redis://localhost:6379"}}
	warnings, err := Validate(c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hasSQLiteStateScaleWarning(warnings) {
		t.Fatalf("did not expect sqlite-state warning for postgres state, got %v", warnings)
	}
}

func TestValidate_NoSQLiteStateWarningWithMemoryCoord(t *testing.T) {
	// SQLite state + memory coordinator is the supported single-instance
	// default and must stay warning-free.
	c := sqliteStateScaleBase()
	c.Coordination = CoordinationConfig{Driver: "memory"}
	warnings, err := Validate(c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hasSQLiteStateScaleWarning(warnings) {
		t.Fatalf("did not expect sqlite-state warning for memory coordinator, got %v", warnings)
	}
}

// base returns a minimal valid Config with Coordination.Driver="redis",
// one valid sink named "default", and one valid feed referencing it.
func base() Config {
	c := Defaults()
	c.State = StateConfig{Driver: "sqlite", SQLite: SQLiteStateConfig{Path: "/tmp/s.db"}}
	c.Sinks = []SinkConfig{{Name: "default", Driver: "stdout"}}
	c.Feeds = []FeedConfig{{URL: "https://example.com/f.xml", Interval: 5 * time.Minute}}
	c.Coordination = CoordinationConfig{Driver: "redis"}
	return c
}

func TestValidateRedisCoordinationModes(t *testing.T) {
	rc := func(mut func(*CoordinationRedisConfig)) Config {
		c := base()
		mut(&c.Coordination.Redis)
		return c
	}
	cases := []struct {
		name    string
		cfg     Config
		wantErr string // "" => no error
	}{
		{"single ok", rc(func(r *CoordinationRedisConfig) { r.Mode = "single"; r.URL = "redis://h:6379" }), ""},
		{"empty mode legacy ok", rc(func(r *CoordinationRedisConfig) { r.URL = "redis://h:6379" }), ""},
		{"single missing url", rc(func(r *CoordinationRedisConfig) { r.Mode = "single" }), "url is required"},
		{"single rejects sentinel block", rc(func(r *CoordinationRedisConfig) {
			r.Mode = "single"
			r.URL = "redis://h:6379"
			r.Sentinel.MasterName = "m"
		}), "sentinel"},
		{"sentinel ok", rc(func(r *CoordinationRedisConfig) {
			r.Mode = "sentinel"
			r.Sentinel.MasterName = "m"
			r.Sentinel.Addrs = []string{"a:26379"}
		}), ""},
		{"sentinel missing master", rc(func(r *CoordinationRedisConfig) { r.Mode = "sentinel"; r.Sentinel.Addrs = []string{"a:26379"} }), "master_name"},
		{"sentinel missing addrs", rc(func(r *CoordinationRedisConfig) { r.Mode = "sentinel"; r.Sentinel.MasterName = "m" }), "addrs"},
		{"sentinel rejects url", rc(func(r *CoordinationRedisConfig) {
			r.Mode = "sentinel"
			r.Sentinel.MasterName = "m"
			r.Sentinel.Addrs = []string{"a:26379"}
			r.URL = "redis://h:6379"
		}), "url"},
		{"cluster ok", rc(func(r *CoordinationRedisConfig) { r.Mode = "cluster"; r.Cluster.Addrs = []string{"n:6379"} }), ""},
		{"cluster missing addrs", rc(func(r *CoordinationRedisConfig) { r.Mode = "cluster" }), "addrs"},
		{"bad mode", rc(func(r *CoordinationRedisConfig) { r.Mode = "galaxy" }), "mode"},
		{"sentinel tls ok (no rediss)", rc(func(r *CoordinationRedisConfig) {
			r.Mode = "sentinel"
			r.Sentinel.MasterName = "m"
			r.Sentinel.Addrs = []string{"a:26379"}
			r.TLS.CAFile = "/tmp/ca.pem"
		}), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Validate(tc.cfg)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantErr)
			}
		})
	}
}

func TestValidateHealthAcceptsDefaults(t *testing.T) {
	t.Parallel()
	if _, err := Validate(goodCfg()); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateHealthRejectsPathWithoutLeadingSlash(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Health.LivenessPath = "healthz"
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "liveness_path") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateHealthRejectsDuplicatePaths(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Health.ReadinessPath = c.Health.LivenessPath
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "distinct") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateHealthRequiresListen(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Health.Listen = ""
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "health.listen") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateHealthSharedListenerWarns(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.Prometheus.Enabled = true
	c.Telemetry.Prometheus.Listen = ":9090"
	c.Health.Listen = ":9090"
	warnings, err := Validate(c)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	var found bool
	for _, w := range warnings {
		if strings.Contains(w, "prometheus.listen") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a warning mentioning prometheus.listen, got %v", warnings)
	}
}

func TestValidateHealthDisabledSkipsChecks(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Health.Enabled = false
	c.Health.LivenessPath = ""
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateAcceptsGCPPubSubSink(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "ps-x", Driver: "gcp_pubsub",
		GCPPubSub: GCPPubSubSinkConfig{ProjectID: "proj", TopicID: "feed-changes"},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateAcceptsGCPPubSubOrderingKey(t *testing.T) {
	t.Parallel()
	for _, k := range []string{"feed_url", "item_id", "sink"} {
		c := goodCfg()
		c.Sinks = append(c.Sinks, SinkConfig{
			Name: "ps-x", Driver: "gcp_pubsub",
			GCPPubSub: GCPPubSubSinkConfig{ProjectID: "proj", TopicID: "t", OrderingKey: k},
		})
		if _, err := Validate(c); err != nil {
			t.Fatalf("ordering_key %q: expected nil, got %v", k, err)
		}
	}
}

func TestValidateAcceptsDaprPubSubSink(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "dapr-x", Driver: "dapr_pubsub",
		DaprPubSub: DaprPubSubSinkConfig{PubsubName: "rss-pubsub", Topic: "rss.changes"},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsDaprPubSubWithoutPubsubName(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "dapr-x", Driver: "dapr_pubsub",
		DaprPubSub: DaprPubSubSinkConfig{Topic: "t"},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "pubsub_name") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsDaprPubSubWithoutTopic(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "dapr-x", Driver: "dapr_pubsub",
		DaprPubSub: DaprPubSubSinkConfig{PubsubName: "ps"},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "topic") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsGCPPubSubWithoutProjectID(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "ps-x", Driver: "gcp_pubsub",
		GCPPubSub: GCPPubSubSinkConfig{TopicID: "t"},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "project_id") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsGCPPubSubWithoutTopicID(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "ps-x", Driver: "gcp_pubsub",
		GCPPubSub: GCPPubSubSinkConfig{ProjectID: "proj"},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "topic_id") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsGCPPubSubUnknownOrderingKey(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "ps-x", Driver: "gcp_pubsub",
		GCPPubSub: GCPPubSubSinkConfig{ProjectID: "proj", TopicID: "t", OrderingKey: "bogus"},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "ordering_key") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsAzureServiceBusSink(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "asb-x", Driver: "azureservicebus",
		AzureServiceBus: AzureServiceBusSinkConfig{
			ConnectionString: "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=v",
			Queue:            "feed-changes",
		},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateAcceptsAzureServiceBusNamespaceTopic(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "asb-x", Driver: "azureservicebus",
		AzureServiceBus: AzureServiceBusSinkConfig{
			Namespace: "my-bus.servicebus.windows.net",
			Topic:     "feed-changes",
		},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsAzureServiceBusMissingAuth(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "asb-x", Driver: "azureservicebus",
		AzureServiceBus: AzureServiceBusSinkConfig{Queue: "feed-changes"},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "connection_string or namespace") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsAzureServiceBusBothAuth(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "asb-x", Driver: "azureservicebus",
		AzureServiceBus: AzureServiceBusSinkConfig{
			ConnectionString: "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=v",
			Namespace:        "my-bus.servicebus.windows.net",
			Queue:            "feed-changes",
		},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsAzureServiceBusMissingEntity(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "asb-x", Driver: "azureservicebus",
		AzureServiceBus: AzureServiceBusSinkConfig{
			ConnectionString: "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=v",
		},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "queue or topic") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsAzureServiceBusBothEntities(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "asb-x", Driver: "azureservicebus",
		AzureServiceBus: AzureServiceBusSinkConfig{
			ConnectionString: "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=v",
			Queue:            "q",
			Topic:            "t",
		},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "queue and topic are mutually exclusive") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsCosmosDBSink(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "cosmos-x", Driver: "cosmosdb",
		CosmosDB: CosmosDBSinkConfig{
			ConnectionString: "AccountEndpoint=https://x.documents.azure.com:443/;AccountKey=k;",
			Database:         "rss2msg",
		},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateAcceptsCosmosDBSinkEndpoint(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "cosmos-x", Driver: "cosmosdb",
		CosmosDB: CosmosDBSinkConfig{
			Endpoint:  "https://x.documents.azure.com:443/",
			Database:  "rss2msg",
			Container: "feed_changes",
		},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsCosmosDBMissingAuth(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "cosmos-x", Driver: "cosmosdb",
		CosmosDB: CosmosDBSinkConfig{Database: "rss2msg"},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "endpoint or connection_string") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsCosmosDBBothAuth(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "cosmos-x", Driver: "cosmosdb",
		CosmosDB: CosmosDBSinkConfig{
			Endpoint:         "https://x.documents.azure.com:443/",
			ConnectionString: "AccountEndpoint=https://x.documents.azure.com:443/;AccountKey=k;",
			Database:         "rss2msg",
		},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsCosmosDBMissingDatabase(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "cosmos-x", Driver: "cosmosdb",
		CosmosDB: CosmosDBSinkConfig{
			Endpoint: "https://x.documents.azure.com:443/",
		},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "cosmosdb.database is required") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsCosmosDBNegativeThroughput(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "cosmos-x", Driver: "cosmosdb",
		CosmosDB: CosmosDBSinkConfig{
			Endpoint:   "https://x.documents.azure.com:443/",
			Database:   "rss2msg",
			Throughput: -1,
		},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "throughput must not be negative") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsGraphiteWithoutAddress(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.Graphite.Enabled = true
	c.Telemetry.Graphite.Address = ""
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "telemetry.graphite.address is required") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsNegativeGraphiteInterval(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.Graphite.Enabled = true
	c.Telemetry.Graphite.Address = "localhost:2003"
	c.Telemetry.Graphite.Interval = -1 * time.Second
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "interval must not be negative") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateWarnsGraphiteEnabledWithoutMetrics(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.Graphite.Enabled = true
	c.Telemetry.Graphite.Address = "localhost:2003"
	c.Telemetry.Metrics.Enabled = false
	warnings, err := Validate(c)
	require.NoError(t, err)
	var found bool
	for _, w := range warnings {
		if strings.Contains(w, "telemetry.graphite is enabled but telemetry.metrics.enabled=false") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected metrics-disabled warning, got %v", warnings)
	}
}

func TestValidateAcceptsGraphiteEnabled(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Telemetry.Graphite.Enabled = true
	c.Telemetry.Graphite.Address = "localhost:2003"
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateSchemaRegistry(t *testing.T) {
	t.Parallel()
	base := func(sr SchemaRegistryConfig) Config {
		c := goodCfg()
		c.Sinks[0].Kafka.SchemaRegistry = sr
		return c
	}
	if _, err := Validate(base(SchemaRegistryConfig{URL: "http://sr:8081", Format: "json"})); err != nil {
		t.Fatalf("valid json registry rejected: %v", err)
	}
	if _, err := Validate(base(SchemaRegistryConfig{URL: "http://sr:8081", Format: "bogus"})); err == nil {
		t.Fatal("expected error for unknown format")
	}
	if _, err := Validate(base(SchemaRegistryConfig{URL: "http://sr:8081", Format: "avro"})); err == nil {
		t.Fatal("expected error for not-yet-supported avro")
	}
	if _, err := Validate(base(SchemaRegistryConfig{URL: "http://sr:8081"})); err == nil {
		t.Fatal("expected error for missing format")
	}
	if _, err := Validate(base(SchemaRegistryConfig{Format: "json"})); err == nil {
		t.Fatal("expected error for format without url")
	}
	if _, err := Validate(base(SchemaRegistryConfig{URL: "http://sr:8081", Format: "json", SchemaFile: "/no/such/file"})); err == nil {
		t.Fatal("expected error for missing schema_file")
	}
}
