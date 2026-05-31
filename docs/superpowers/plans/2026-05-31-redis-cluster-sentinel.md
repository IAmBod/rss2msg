# Redis Cluster & Sentinel Coordination Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the Redis coordinator (`internal/coord/redis`) connect to Redis **Sentinel** (failover/HA) and **Cluster** topologies, not just a single standalone node, selected by an explicit `mode` config field.

**Architecture:** Swap `Coordinator.client` from `*redis.Client` to `redis.UniversalClient` and construct the concrete client per `mode` (`redis.NewClient` / `NewFailoverClient` / `NewClusterClient` — all satisfy `UniversalClient` and `Scripter`). Config gains `mode` + one self-contained block per topology (`sentinel:` / `cluster:`); `single` keeps the existing `url`, so existing configs are unchanged. The lease lock logic (`SetNX` + single-key Lua) is already topology-agnostic and is not touched.

**Tech Stack:** Go 1.25, `github.com/redis/go-redis/v9`, Viper (mapstructure), testify, testcontainers-go.

Tracks issue #2.

---

## Research Findings (grounded in current code)

- **Lock logic is topology-agnostic — do NOT change it.** `TryAcquire` (`internal/coord/redis/redis.go:178-230`) uses `c.client.SetNX`; `renewLoop` (`:236-269`) and `casDelete` (`:274-291`) call `renewScript`/`releaseScript` via `.Run(ctx, c.client, ...)`. Both scripts touch a single `KEYS[1]` (`:80-95`) → already Cluster-safe. Key = `sha256(feed_url)` hex (`:294-297`).
- **`New` dials on construction.** `New(ctx, opts)` (`:115-149`) parses the URL, builds the client, then `client.Ping(ctx)` (`:140`). ⇒ Extract a **no-dial** `buildClient(opts) (redis.UniversalClient, error)` so the per-mode option mapping is unit-testable without a server; `New` calls `buildClient` then pings.
- **`Coordinator.client` is `*redis.Client`** (`:105`). Change to `redis.UniversalClient`. `SetNX`, `Ping`, `Close`, and `*redis.Script.Run` all exist on `UniversalClient`, so the rest of the file is untouched. There is **no `newWithClient`** today; the struct is built inline at `:144-148`.
- **TLS is custom, not `*tls.Config`.** `Options.TLS` is `*TLSOptions` (`:37`, def `:43-56`); `buildTLSConfig(opts TLSOptions, defaultServerName string) (*tls.Config, error)` (`:307-339`) converts it. Single mode derives `defaultServerName` from `redis.ParseURL`'s `cfg.TLSConfig.ServerName` (`:128`). Sentinel/cluster have no URL → pass `defaultServerName == ""` (user sets `server_name` if cert verification needs it).
- **Config today is flat.** `CoordinationRedisConfig{URL, LockTTL, RenewalInterval, TLS}` (`internal/config/config.go:75-80`); shared TLS struct is `CoordinationRedisTLSConfig` (`:82-88`).
- **Validation today requires `url` unconditionally** (`internal/config/validate.go:142-179`): `case Driver=="redis"` requires `Redis.URL`, parses via `redisparseURL`, checks ttl/renewal, and (`:170-175`) rejects TLS unless the URL scheme is `rediss://`. The rediss-only rule must be scoped to **single mode** (sentinel/cluster have no URL).
- **Wiring** (`cmd/rss2msg/wire.go:172-178`): `case "redis"` maps config → `coordredis.Options{URL, LockTTL, RenewalInterval, TLS: redisTLSFromConfig(cc.Redis.TLS)}`. `redisTLSFromConfig` (`:219-`) returns `*coordredis.TLSOptions` (nil when unset).
- **go-redis constructors:** `NewClient(*Options)→*redis.Client`; `NewFailoverClient(*FailoverOptions)→*redis.Client`; `NewClusterClient(*ClusterOptions)→*redis.ClusterClient`. Data-node creds are `Username`/`Password`; Sentinel-node creds are `SentinelUsername`/`SentinelPassword`.

**Target config:**
```yaml
coordination:
  driver: redis
  redis:
    mode: single            # single | sentinel | cluster (default: single)
    lock_ttl: 30s           # shared
    renewal_interval: 10s   # shared
    tls: { ca_file: ..., cert_file: ..., key_file: ..., server_name: ..., insecure_skip_verify: false }  # shared
    url: redis://:pass@host:6379/0     # mode=single (existing)
    sentinel:                          # mode=sentinel
      master_name: mymaster
      addrs: [sentinel-a:26379, sentinel-b:26379]
      username: ...; password: ...
      sentinel_username: ...; sentinel_password: ...
      db: 0
    cluster:                           # mode=cluster
      addrs: [node-a:6379, node-b:6379]
      username: ...; password: ...
```

---

## Task 1: Config types for per-topology blocks

**Files:**
- Modify: `internal/config/config.go:75-80` (extend `CoordinationRedisConfig`, add two sub-structs)
- Test: `internal/config/config_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestRedisCoordinationModeBlocksParse(t *testing.T) {
	cfg := mustLoadYAML(t, `
coordination:
  driver: redis
  redis:
    mode: sentinel
    lock_ttl: 30s
    sentinel:
      master_name: mymaster
      addrs: [a:26379, b:26379]
      password: secret
      sentinel_password: spass
      db: 2
    cluster:
      addrs: [n1:6379, n2:6379]
      username: u
`)
	r := cfg.Coordination.Redis
	require.Equal(t, "sentinel", r.Mode)
	require.Equal(t, "mymaster", r.Sentinel.MasterName)
	require.Equal(t, []string{"a:26379", "b:26379"}, r.Sentinel.Addrs)
	require.Equal(t, "secret", r.Sentinel.Password)
	require.Equal(t, "spass", r.Sentinel.SentinelPassword)
	require.Equal(t, 2, r.Sentinel.DB)
	require.Equal(t, []string{"n1:6379", "n2:6379"}, r.Cluster.Addrs)
	require.Equal(t, "u", r.Cluster.Username)
}

func TestRedisCoordinationLegacyURLStillParses(t *testing.T) {
	cfg := mustLoadYAML(t, `
coordination:
  driver: redis
  redis:
    url: redis://localhost:6379
`)
	require.Equal(t, "", cfg.Coordination.Redis.Mode)
	require.Equal(t, "redis://localhost:6379", cfg.Coordination.Redis.URL)
}
```

If `config_test.go` has no YAML-loading helper, mirror the loader used by existing tests in `internal/config/load_test.go` (Viper unmarshal into `Config`); name it `mustLoadYAML(t, s string) Config`. Reuse it — do not duplicate if one already exists.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestRedisCoordination -v`
Expected: compile error / FAIL — `Mode`, `Sentinel`, `Cluster` undefined.

- [ ] **Step 3: Implement** — replace `CoordinationRedisConfig` at `internal/config/config.go:75-80` with:

```go
type CoordinationRedisConfig struct {
	Mode            string                     `mapstructure:"mode"` // single|sentinel|cluster (default single)
	URL             string                     `mapstructure:"url"`  // single mode
	LockTTL         time.Duration              `mapstructure:"lock_ttl"`
	RenewalInterval time.Duration              `mapstructure:"renewal_interval"`
	TLS             CoordinationRedisTLSConfig `mapstructure:"tls"`
	Sentinel        CoordinationRedisSentinelConfig `mapstructure:"sentinel"`
	Cluster         CoordinationRedisClusterConfig  `mapstructure:"cluster"`
}

type CoordinationRedisSentinelConfig struct {
	MasterName       string   `mapstructure:"master_name"`
	Addrs            []string `mapstructure:"addrs"`
	Username         string   `mapstructure:"username"`
	Password         string   `mapstructure:"password"`
	SentinelUsername string   `mapstructure:"sentinel_username"`
	SentinelPassword string   `mapstructure:"sentinel_password"`
	DB               int      `mapstructure:"db"`
}

type CoordinationRedisClusterConfig struct {
	Addrs    []string `mapstructure:"addrs"`
	Username string   `mapstructure:"username"`
	Password string   `mapstructure:"password"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/config/ -run TestRedisCoordination -v`
Expected: PASS

- [ ] **Step 5: Commit** (pathspec only — the repo is an Obsidian vault with obsidian-git auto-staging; never `git add -A`)

```bash
git add internal/config/config.go internal/config/config_test.go
git commit -m "feat(config): add redis coordination mode + sentinel/cluster blocks"
```

---

## Task 2: Mode-aware validation

**Files:**
- Modify: `internal/config/validate.go:142-179` (the `if c.Coordination.Driver == "redis"` block)
- Test: `internal/config/validate_test.go`

- [ ] **Step 1: Write the failing test** (table-driven; `base()` returns a minimal valid Config with one sink + one feed so only the redis block is under test)

```go
func TestValidateRedisCoordinationModes(t *testing.T) {
	rc := func(mut func(*CoordinationRedisConfig)) Config {
		c := base() // helper: valid Config, Coordination.Driver="redis"
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
		{"single rejects sentinel block", rc(func(r *CoordinationRedisConfig) { r.Mode = "single"; r.URL = "redis://h:6379"; r.Sentinel.MasterName = "m" }), "sentinel"},
		{"sentinel ok", rc(func(r *CoordinationRedisConfig) { r.Mode = "sentinel"; r.Sentinel.MasterName = "m"; r.Sentinel.Addrs = []string{"a:26379"} }), ""},
		{"sentinel missing master", rc(func(r *CoordinationRedisConfig) { r.Mode = "sentinel"; r.Sentinel.Addrs = []string{"a:26379"} }), "master_name"},
		{"sentinel missing addrs", rc(func(r *CoordinationRedisConfig) { r.Mode = "sentinel"; r.Sentinel.MasterName = "m" }), "addrs"},
		{"sentinel rejects url", rc(func(r *CoordinationRedisConfig) { r.Mode = "sentinel"; r.Sentinel.MasterName = "m"; r.Sentinel.Addrs = []string{"a:26379"}; r.URL = "redis://h:6379" }), "url"},
		{"cluster ok", rc(func(r *CoordinationRedisConfig) { r.Mode = "cluster"; r.Cluster.Addrs = []string{"n:6379"} }), ""},
		{"cluster missing addrs", rc(func(r *CoordinationRedisConfig) { r.Mode = "cluster" }), "addrs"},
		{"bad mode", rc(func(r *CoordinationRedisConfig) { r.Mode = "galaxy" }), "mode"},
		{"sentinel tls ok (no rediss)", rc(func(r *CoordinationRedisConfig) { r.Mode = "sentinel"; r.Sentinel.MasterName = "m"; r.Sentinel.Addrs = []string{"a:26379"}; r.TLS.CAFile = "/tmp/ca.pem" }), ""},
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
```

If a `base()` helper doesn't already exist in `validate_test.go`, add one returning a Config with `Coordination.Driver: "redis"`, one valid sink named `default`, and one valid feed referencing it.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestValidateRedisCoordinationModes -v`
Expected: FAIL (current code requires `url` for every mode, so the sentinel/cluster rows fail).

- [ ] **Step 3: Implement** — replace the whole `if c.Coordination.Driver == "redis" { ... }` block (`validate.go:142-179`) with mode dispatch:

```go
	if c.Coordination.Driver == "redis" {
		r := c.Coordination.Redis
		mode := r.Mode
		if mode == "" {
			mode = "single"
		}
		sentinelSet := r.Sentinel.MasterName != "" || len(r.Sentinel.Addrs) > 0
		clusterSet := len(r.Cluster.Addrs) > 0

		switch mode {
		case "single":
			if sentinelSet {
				return *warnings, fmt.Errorf("coordination.redis.sentinel must not be set when mode=single")
			}
			if clusterSet {
				return *warnings, fmt.Errorf("coordination.redis.cluster must not be set when mode=single")
			}
			raw := strings.TrimSpace(r.URL)
			if raw == "" {
				return *warnings, fmt.Errorf("coordination.redis.url is required when coordination.driver=redis and mode=single")
			}
			if _, err := redisparseURL(raw); err != nil {
				safe := raw
				if u, perr := url.Parse(raw); perr == nil {
					safe = u.Redacted()
				}
				return *warnings, fmt.Errorf("coordination.redis.url %q is not parseable: %w", safe, err)
			}
		case "sentinel":
			if strings.TrimSpace(r.URL) != "" {
				return *warnings, fmt.Errorf("coordination.redis.url must not be set when mode=sentinel (use sentinel.addrs)")
			}
			if clusterSet {
				return *warnings, fmt.Errorf("coordination.redis.cluster must not be set when mode=sentinel")
			}
			if strings.TrimSpace(r.Sentinel.MasterName) == "" {
				return *warnings, fmt.Errorf("coordination.redis.sentinel.master_name is required when mode=sentinel")
			}
			if len(r.Sentinel.Addrs) == 0 {
				return *warnings, fmt.Errorf("coordination.redis.sentinel.addrs is required when mode=sentinel")
			}
		case "cluster":
			if strings.TrimSpace(r.URL) != "" {
				return *warnings, fmt.Errorf("coordination.redis.url must not be set when mode=cluster (use cluster.addrs)")
			}
			if sentinelSet {
				return *warnings, fmt.Errorf("coordination.redis.sentinel must not be set when mode=cluster")
			}
			if len(r.Cluster.Addrs) == 0 {
				return *warnings, fmt.Errorf("coordination.redis.cluster.addrs is required when mode=cluster")
			}
		default:
			return *warnings, fmt.Errorf("coordination.redis.mode %q is not supported (want single, sentinel, or cluster)", r.Mode)
		}

		// Shared TTL / renewal checks.
		if ttl := r.LockTTL; ttl != 0 && ttl < time.Second {
			return *warnings, fmt.Errorf("coordination.redis.lock_ttl %v is below the 1s minimum", ttl)
		}
		if ri := r.RenewalInterval; ri != 0 {
			ttl := r.LockTTL
			if ttl == 0 {
				ttl = 30 * time.Second
			}
			if ri >= ttl {
				return *warnings, fmt.Errorf("coordination.redis.renewal_interval %v must be less than lock_ttl %v", ri, ttl)
			}
		}

		// Shared TLS checks.
		tls := r.TLS
		tlsConfigured := tls.CAFile != "" || tls.CertFile != "" || tls.KeyFile != "" ||
			tls.ServerName != "" || tls.InsecureSkipVerify
		if tlsConfigured && mode == "single" {
			// Single mode draws TLS from the rediss:// URL scheme.
			u, _ := url.Parse(strings.TrimSpace(r.URL))
			if u == nil || u.Scheme != "rediss" {
				return *warnings, fmt.Errorf("coordination.redis.tls is only valid when coordination.redis.url uses the rediss:// scheme")
			}
		}
		if (tls.CertFile == "") != (tls.KeyFile == "") {
			return *warnings, fmt.Errorf("coordination.redis.tls.cert_file and key_file must both be set or both empty")
		}
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/config/ -run TestValidateRedisCoordinationModes -v && go test ./internal/config/`
Expected: PASS (new + all existing config tests).

- [ ] **Step 5: Commit**

```bash
git add internal/config/validate.go internal/config/validate_test.go
git commit -m "feat(config): mode-aware redis coordination validation"
```

---

## Task 3: Coordinator — UniversalClient + per-mode buildClient

**Files:**
- Modify: `internal/coord/redis/redis.go` (`Options`, `Coordinator.client`, `New`, new `buildClient`/`tlsConfigOrNil`)
- Test: `internal/coord/redis/redis_unit_test.go`

- [ ] **Step 1: Write the failing test** (`buildClient` does NOT dial, so no server needed)

```go
func TestBuildClientByMode(t *testing.T) {
	c, err := buildClient(Options{Mode: "single", URL: "redis://localhost:6379"})
	require.NoError(t, err)
	require.NotNil(t, c)

	c, err = buildClient(Options{Mode: "sentinel", Sentinel: SentinelOptions{MasterName: "m", Addrs: []string{"a:26379"}}})
	require.NoError(t, err)
	require.NotNil(t, c)

	c, err = buildClient(Options{Mode: "cluster", Cluster: ClusterOptions{Addrs: []string{"n:6379"}}})
	require.NoError(t, err)
	require.NotNil(t, c)

	// Empty mode == single.
	_, err = buildClient(Options{URL: "redis://localhost:6379"})
	require.NoError(t, err)

	// Errors.
	_, err = buildClient(Options{Mode: "single"})
	require.ErrorContains(t, err, "url is required")
	_, err = buildClient(Options{Mode: "galaxy"})
	require.ErrorContains(t, err, "unsupported mode")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/coord/redis/ -run TestBuildClientByMode -v`
Expected: FAIL — `buildClient`, `SentinelOptions`, `ClusterOptions` undefined.

- [ ] **Step 3: Implement**

3a. Add option sub-structs and extend `Options` (after the `TLSOptions` block, ~`redis.go:56`):

```go
// SentinelOptions configures Redis Sentinel (failover) connections.
type SentinelOptions struct {
	MasterName       string
	Addrs            []string
	Username         string // data-node (master/replica) auth
	Password         string
	SentinelUsername string // sentinel-node auth
	SentinelPassword string
	DB               int
}

// ClusterOptions configures Redis Cluster connections.
type ClusterOptions struct {
	Addrs    []string
	Username string
	Password string
}
```

Add three fields to `Options` (`redis.go:29-38`):

```go
	Mode     string // "" or "single" | "sentinel" | "cluster"
	Sentinel SentinelOptions
	Cluster  ClusterOptions
```

3b. Change `Coordinator.client` (`redis.go:105`):

```go
	client redis.UniversalClient
```

3c. Add `buildClient` + `tlsConfigOrNil`, and rewrite `New` to use them (replace `redis.go:114-149`):

```go
// buildClient constructs the topology-appropriate client WITHOUT dialing.
func buildClient(opts Options) (redis.UniversalClient, error) {
	switch opts.Mode {
	case "", "single":
		if opts.URL == "" {
			return nil, fmt.Errorf("coord/redis: url is required for single mode")
		}
		cfg, err := redis.ParseURL(opts.URL)
		if err != nil {
			return nil, fmt.Errorf("coord/redis: parse url: %w", err)
		}
		if opts.TLS != nil {
			if cfg.TLSConfig == nil {
				return nil, fmt.Errorf("coord/redis: TLS options provided but URL scheme is not rediss://")
			}
			tc, err := buildTLSConfig(*opts.TLS, cfg.TLSConfig.ServerName)
			if err != nil {
				return nil, fmt.Errorf("coord/redis: build TLS config: %w", err)
			}
			cfg.TLSConfig = tc
			warnInsecureTLS(opts.TLS)
		}
		return redis.NewClient(cfg), nil
	case "sentinel":
		tc, err := tlsConfigOrNil(opts.TLS)
		if err != nil {
			return nil, err
		}
		return redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:       opts.Sentinel.MasterName,
			SentinelAddrs:    opts.Sentinel.Addrs,
			Username:         opts.Sentinel.Username,
			Password:         opts.Sentinel.Password,
			SentinelUsername: opts.Sentinel.SentinelUsername,
			SentinelPassword: opts.Sentinel.SentinelPassword,
			DB:               opts.Sentinel.DB,
			TLSConfig:        tc,
		}), nil
	case "cluster":
		tc, err := tlsConfigOrNil(opts.TLS)
		if err != nil {
			return nil, err
		}
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:     opts.Cluster.Addrs,
			Username:  opts.Cluster.Username,
			Password:  opts.Cluster.Password,
			TLSConfig: tc,
		}), nil
	default:
		return nil, fmt.Errorf("coord/redis: unsupported mode %q", opts.Mode)
	}
}

// tlsConfigOrNil builds a *tls.Config for sentinel/cluster modes (no URL to
// derive an SNI host from, so defaultServerName is empty; set TLSOptions.ServerName
// if certificate verification needs a specific host).
func tlsConfigOrNil(t *TLSOptions) (*tls.Config, error) {
	if t == nil {
		return nil, nil
	}
	tc, err := buildTLSConfig(*t, "")
	if err != nil {
		return nil, fmt.Errorf("coord/redis: build TLS config: %w", err)
	}
	warnInsecureTLS(t)
	return tc, nil
}

func warnInsecureTLS(t *TLSOptions) {
	if t != nil && t.InsecureSkipVerify {
		log.Warn().
			Str("coord_driver", "redis").
			Msg("coord/redis: TLS verification disabled (insecure_skip_verify=true)")
	}
}

// New builds the client for opts.Mode, dials it, and returns a ready Coordinator.
func New(ctx context.Context, opts Options) (*Coordinator, error) {
	ro := opts.resolved()
	client, err := buildClient(opts)
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("coord/redis: ping: %w", err)
	}
	return &Coordinator{
		client: client,
		opts:   ro,
		held:   make(map[*lease]struct{}),
	}, nil
}
```

Note: `TryAcquire`, `renewLoop`, `casDelete`, `Close` are unchanged — they already operate through `c.client`, which now satisfies `redis.UniversalClient`. The doc comment on `New` (`redis.go:114`) is replaced above.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/coord/redis/ -run TestBuildClientByMode -v && go build ./... && go vet ./internal/coord/redis/`
Expected: PASS + clean build (confirms `redis.UniversalClient` satisfies every existing call site).

- [ ] **Step 5: Run the existing single-node redis tests to confirm no regression**

Run: `go test ./internal/coord/redis/`
Expected: PASS (container-gated tests skip when Docker is absent; unit tests pass).

- [ ] **Step 6: Commit**

```bash
git add internal/coord/redis/redis.go internal/coord/redis/redis_unit_test.go
git commit -m "feat(coord/redis): UniversalClient with per-mode client construction"
```

---

## Task 4: Wire config → coordinator

**Files:**
- Modify: `cmd/rss2msg/wire.go:172-178` (`case "redis"`)
- Test: `cmd/rss2msg/wire_test.go`

- [ ] **Step 1: Write the failing test** — assert the redis `case` maps each mode into `coordredis.Options` (test the mapping, not a live dial). If `openCoordinator` always dials, extract the option mapping into a small pure helper `redisCoordOptions(cc config.CoordinationConfig) coordredis.Options` and test that:

```go
func TestRedisCoordOptionsByMode(t *testing.T) {
	o := redisCoordOptions(config.CoordinationConfig{Redis: config.CoordinationRedisConfig{
		Mode: "sentinel",
		Sentinel: config.CoordinationRedisSentinelConfig{
			MasterName: "m", Addrs: []string{"a:26379"}, Password: "p", SentinelPassword: "sp", DB: 1,
		},
	}})
	require.Equal(t, "sentinel", o.Mode)
	require.Equal(t, "m", o.Sentinel.MasterName)
	require.Equal(t, []string{"a:26379"}, o.Sentinel.Addrs)
	require.Equal(t, "p", o.Sentinel.Password)
	require.Equal(t, "sp", o.Sentinel.SentinelPassword)
	require.Equal(t, 1, o.Sentinel.DB)

	o = redisCoordOptions(config.CoordinationConfig{Redis: config.CoordinationRedisConfig{
		Mode: "cluster", Cluster: config.CoordinationRedisClusterConfig{Addrs: []string{"n:6379"}, Username: "u"},
	}})
	require.Equal(t, "cluster", o.Mode)
	require.Equal(t, []string{"n:6379"}, o.Cluster.Addrs)
	require.Equal(t, "u", o.Cluster.Username)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/rss2msg/ -run TestRedisCoordOptionsByMode -v`
Expected: FAIL — `redisCoordOptions` undefined / new `Options` fields absent.

- [ ] **Step 3: Implement** — add the helper and call it from `case "redis"` (`wire.go:172-178`):

```go
	case "redis":
		return coordredis.New(ctx, redisCoordOptions(cc))
```

```go
// redisCoordOptions maps coordination config into coordredis.Options for all modes.
func redisCoordOptions(cc config.CoordinationConfig) coordredis.Options {
	r := cc.Redis
	return coordredis.Options{
		Mode:            r.Mode,
		URL:             r.URL,
		LockTTL:         r.LockTTL,
		RenewalInterval: r.RenewalInterval,
		TLS:             redisTLSFromConfig(r.TLS),
		Sentinel: coordredis.SentinelOptions{
			MasterName:       r.Sentinel.MasterName,
			Addrs:            r.Sentinel.Addrs,
			Username:         r.Sentinel.Username,
			Password:         r.Sentinel.Password,
			SentinelUsername: r.Sentinel.SentinelUsername,
			SentinelPassword: r.Sentinel.SentinelPassword,
			DB:               r.Sentinel.DB,
		},
		Cluster: coordredis.ClusterOptions{
			Addrs:    r.Cluster.Addrs,
			Username: r.Cluster.Username,
			Password: r.Cluster.Password,
		},
	}
}
```

(`cc` here is the `config.CoordinationConfig` already in scope in `openCoordinator`; confirm the variable name in `wire.go` and match it.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/rss2msg/ -run TestRedisCoordOptions -v && go build ./...`
Expected: PASS + clean build.

- [ ] **Step 5: Commit**

```bash
git add cmd/rss2msg/wire.go cmd/rss2msg/wire_test.go
git commit -m "feat(cmd): wire redis coordination mode/sentinel/cluster options"
```

---

## Task 5: Sentinel integration test (the tested path)

**Files:**
- Modify: `internal/coord/redis/redis_test.go` (add a Sentinel testcontainers case)

- [ ] **Step 1: Write the test** — mirror the existing single-node container test in this file (reuse its helpers/skip-guards). Bring up a master + one Sentinel, then exercise the full lease lifecycle through a sentinel-mode coordinator:

```go
func TestCoordinator_Sentinel_AcquireRenewRelease(t *testing.T) {
	requireDocker(t) // reuse whatever guard the existing container tests use

	masterName, sentinelAddrs := startRedisSentinel(t) // see Step 1a

	c, err := New(context.Background(), Options{
		Mode:    "sentinel",
		LockTTL: 2 * time.Second,
		Sentinel: SentinelOptions{
			MasterName: masterName,
			Addrs:      sentinelAddrs,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	const feed = "https://example.com/feed.xml"
	rel, ok, err := c.TryAcquire(context.Background(), feed)
	require.NoError(t, err)
	require.True(t, ok)

	// A second coordinator cannot acquire the same lease.
	c2, err := New(context.Background(), Options{Mode: "sentinel", LockTTL: 2 * time.Second,
		Sentinel: SentinelOptions{MasterName: masterName, Addrs: sentinelAddrs}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c2.Close() })
	_, ok2, err := c2.TryAcquire(context.Background(), feed)
	require.NoError(t, err)
	require.False(t, ok2)

	// Lease survives past one TTL via the renewal loop.
	time.Sleep(3 * time.Second)
	_, ok3, err := c2.TryAcquire(context.Background(), feed)
	require.NoError(t, err)
	require.False(t, ok3, "renewal should keep the lease held")

	// After release, the lease frees.
	require.NoError(t, rel(context.Background()))
	rel2, ok4, err := c2.TryAcquire(context.Background(), feed)
	require.NoError(t, err)
	require.True(t, ok4)
	_ = rel2(context.Background())
}
```

- [ ] **Step 1a: Implement `startRedisSentinel(t)`** — use `testcontainers-go` (already a dep): a `redis:7` master container and a `redis:7` sentinel container (entrypoint `redis-sentinel` with a generated `sentinel.conf` pointing at the master, `down-after-milliseconds` low). Return the master name (e.g. `"mymaster"`) and the sentinel host:port slice. Keep it in `redis_test.go` next to the existing container helpers; match their network/cleanup pattern.

- [ ] **Step 2: Run it**

Run: `go test ./internal/coord/redis/ -run TestCoordinator_Sentinel -v`
Expected: PASS with Docker available; SKIP without.

- [ ] **Step 3: Commit**

```bash
git add internal/coord/redis/redis_test.go
git commit -m "test(coord/redis): sentinel lease lifecycle integration test"
```

> Cluster is best-effort: NOT covered by an integration test (hard to containerize in CI). It is reachable through `buildClient`'s cluster branch and documented as best-effort in Task 6.

---

## Task 6: Docs & config example

**Files:**
- Modify: the Redis coordination docs page under `docs/` (find it: `grep -rl "coordination.redis" docs/` — likely `docs/reference/` or `docs/how-to/`). Follow the repo's verbatim-docs convention.
- Modify: `config.example.full.yaml` (add commented sentinel + cluster blocks under `coordination.redis`)

- [ ] **Step 1: Update the docs page** — document `mode: single | sentinel | cluster`, the `sentinel:`/`cluster:` blocks (fields from Task 1), that single mode is unchanged (existing `url`), the data-node vs sentinel-node credential split, and that **Sentinel is the tested/supported path while Cluster is best-effort**. Note TLS is the shared `tls:` block; for sentinel/cluster set `server_name` if cert verification needs a specific host.

- [ ] **Step 2: Update `config.example.full.yaml`** — add a commented example:

```yaml
  # redis:
  #   mode: sentinel              # single | sentinel | cluster (default: single)
  #   lock_ttl: 30s
  #   renewal_interval: 10s
  #   sentinel:
  #     master_name: mymaster
  #     addrs: [sentinel-a:26379, sentinel-b:26379]
  #     # username/password authenticate to the data nodes (master/replicas)
  #     # sentinel_username/sentinel_password authenticate to the sentinels
  #   # cluster:
  #   #   addrs: [node-a:6379, node-b:6379]   # best-effort (not CI-tested)
```

- [ ] **Step 3: Verify** — run the docs link checker (per repo convention; e.g. the task in `taskfile.yaml`), then the full build + test:

Run: `task docs:links 2>/dev/null || true && go build ./... && go test ./...`
Expected: link checker passes; build + full test suite green (container tests skip without Docker).

- [ ] **Step 4: Commit**

```bash
git add docs config.example.full.yaml
git commit -m "docs: document redis sentinel/cluster coordination modes"
```

---

## Resolved Questions

- **Topology scope?** All three via one `UniversalClient` path; Sentinel tested, Cluster best-effort.
- **Config shape?** Explicit `mode` + one self-contained block per topology.
- **Data-node credentials in sentinel/cluster?** Each block carries its own `username`/`password`; sentinel adds `sentinel_username`/`sentinel_password`.
- **`addrs` vs `url`?** Single keeps `url` (backward compatible); `addrs` lives in the sentinel/cluster blocks.
- **DB index?** `db` in the sentinel block (and via the `url` DSN for single); Cluster omits it (always DB 0).

## Executor Notes

- TDD strictly: red → green → commit per task. Pathspec commits only (Obsidian vault auto-stages; never `git add -A`); verify `git status` before each commit.
- Treat empty `Mode` as `single` everywhere (config default, validation, `buildClient`).
- `coordredis.Options` stays decoupled from the `config` package — map in `wire.go` (Task 4).
- `buildClient` must not dial; only `New` pings. This is what makes Tasks 3/4 unit-testable without Docker.
