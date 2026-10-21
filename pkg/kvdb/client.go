package kvdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	ErrKeyNotFound      = errors.New("key not found")
	ErrNotMaster        = errors.New("not master for partition")
	ErrPartitionMoved   = errors.New("partition moved")
	ErrServerError      = errors.New("server error")
	ErrNetworkError     = errors.New("network error")
	ErrInvalidResponse  = errors.New("invalid response")
	ErrRetriesExhausted = errors.New("retries exhausted")
	ErrContextCanceled  = errors.New("context canceled")
)

// RetryConfig configures the retry behavior for client operations.
type RetryConfig struct {
	// MaxRetries is the maximum number of retry attempts (0 = no retries).
	MaxRetries int

	// InitialBackoff is the initial backoff duration before the first retry.
	InitialBackoff time.Duration

	// MaxBackoff is the maximum backoff duration between retries.
	MaxBackoff time.Duration

	// BackoffMultiplier is the factor by which backoff increases after each retry.
	BackoffMultiplier float64

	// JitterFraction adds randomness to backoff (0.0-1.0, e.g., 0.2 = ±20%).
	JitterFraction float64

	// RetryableStatusCodes lists HTTP status codes that trigger a retry.
	RetryableStatusCodes []int
}

// DefaultRetryConfig returns sensible defaults for retry behavior.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:        3,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        5 * time.Second,
		BackoffMultiplier: 2.0,
		JitterFraction:    0.2,
		RetryableStatusCodes: []int{
			http.StatusBadGateway,         // 502
			http.StatusServiceUnavailable, // 503
			http.StatusGatewayTimeout,     // 504
			http.StatusConflict,           // 409 (NOT_MASTER)
			http.StatusMisdirectedRequest, // 421 (partition moved)
		},
	}
}

// NoRetryConfig returns a config that disables retries.
func NoRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries: 0,
	}
}

type Client struct {
	baseURL     string
	httpClient  *http.Client
	retryConfig RetryConfig
	// rngMu guards rng: math/rand.Rand is not safe for concurrent use and
	// one Client is routinely shared by dozens of load-test goroutines.
	rngMu sync.Mutex
	rng   *rand.Rand
}

func drainAndCloseResponse(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

type Config struct {
	LoadBalancerURL string
	Timeout         time.Duration
	Retry           RetryConfig
}

func DefaultConfig(loadBalancerURL string) Config {
	return Config{
		LoadBalancerURL: loadBalancerURL,
		Timeout:         10 * time.Second,
		Retry:           DefaultRetryConfig(),
	}
}

func NewClient(cfg Config) *Client {
	return &Client{
		baseURL: cfg.LoadBalancerURL,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          512,
				MaxIdleConnsPerHost:   128,
				MaxConnsPerHost:       16,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		},
		retryConfig: cfg.Retry,
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

type GetResponse struct {
	Key     string `json:"key"`
	Value   []byte `json:"value"`
	Version uint64 `json:"version"`
}

type SetResponse struct {
	Status string `json:"status"`
	Key    string `json:"key"`
}

type DeleteResponse struct {
	Status string `json:"status"`
	Key    string `json:"key"`
}

type ErrorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// retryableError wraps an error and indicates whether it should be retried.
type retryableError struct {
	err        error
	retryable  bool
	statusCode int
}

func (e *retryableError) Error() string {
	return e.err.Error()
}

func (e *retryableError) Unwrap() error {
	return e.err
}

// Set stores a key-value pair with automatic retry on transient failures.
func (c *Client) Set(ctx context.Context, key string, value []byte) error {
	return c.doWithRetry(ctx, "Set", func(ctx context.Context) error {
		return c.doSet(ctx, key, value)
	})
}

func (c *Client) doSet(ctx context.Context, key string, value []byte) error {
	url := fmt.Sprintf("%s/kv/%s", c.baseURL, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(value))
	if err != nil {
		return &retryableError{err: fmt.Errorf("%w: %v", ErrNetworkError, err), retryable: false}
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return c.classifyNetworkError(err)
	}
	defer drainAndCloseResponse(resp)

	if resp.StatusCode != http.StatusOK {
		return c.parseErrorWithRetry(resp)
	}

	return nil
}

// Get retrieves a value by key with automatic retry on transient failures.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	var result []byte
	err := c.doWithRetry(ctx, "Get", func(ctx context.Context) error {
		val, err := c.doGet(ctx, key)
		if err != nil {
			return err
		}
		result = val
		return nil
	})
	return result, err
}

func (c *Client) doGet(ctx context.Context, key string) ([]byte, error) {
	url := fmt.Sprintf("%s/kv/%s", c.baseURL, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, &retryableError{err: fmt.Errorf("%w: %v", ErrNetworkError, err), retryable: false}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, c.classifyNetworkError(err)
	}
	defer drainAndCloseResponse(resp)

	if resp.StatusCode == http.StatusNotFound {
		// Key not found is not retryable
		return nil, &retryableError{err: ErrKeyNotFound, retryable: false, statusCode: resp.StatusCode}
	}

	if resp.StatusCode != http.StatusOK {
		return nil, c.parseErrorWithRetry(resp)
	}

	var getResp GetResponse
	if err := json.NewDecoder(resp.Body).Decode(&getResp); err != nil {
		return nil, &retryableError{err: fmt.Errorf("%w: %v", ErrInvalidResponse, err), retryable: false}
	}

	return getResp.Value, nil
}

// GetWithVersion retrieves a value and its version with automatic retry.
func (c *Client) GetWithVersion(ctx context.Context, key string) ([]byte, uint64, error) {
	var result []byte
	var version uint64
	err := c.doWithRetry(ctx, "GetWithVersion", func(ctx context.Context) error {
		val, ver, err := c.doGetWithVersion(ctx, key)
		if err != nil {
			return err
		}
		result = val
		version = ver
		return nil
	})
	return result, version, err
}

func (c *Client) doGetWithVersion(ctx context.Context, key string) ([]byte, uint64, error) {
	url := fmt.Sprintf("%s/kv/%s", c.baseURL, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, &retryableError{err: fmt.Errorf("%w: %v", ErrNetworkError, err), retryable: false}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, c.classifyNetworkError(err)
	}
	defer drainAndCloseResponse(resp)

	if resp.StatusCode == http.StatusNotFound {
		return nil, 0, &retryableError{err: ErrKeyNotFound, retryable: false, statusCode: resp.StatusCode}
	}

	if resp.StatusCode != http.StatusOK {
		return nil, 0, c.parseErrorWithRetry(resp)
	}

	var getResp GetResponse
	if err := json.NewDecoder(resp.Body).Decode(&getResp); err != nil {
		return nil, 0, &retryableError{err: fmt.Errorf("%w: %v", ErrInvalidResponse, err), retryable: false}
	}

	return getResp.Value, getResp.Version, nil
}

// Delete removes a key with automatic retry on transient failures.
func (c *Client) Delete(ctx context.Context, key string) error {
	return c.doWithRetry(ctx, "Delete", func(ctx context.Context) error {
		return c.doDelete(ctx, key)
	})
}

func (c *Client) doDelete(ctx context.Context, key string) error {
	url := fmt.Sprintf("%s/kv/%s", c.baseURL, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return &retryableError{err: fmt.Errorf("%w: %v", ErrNetworkError, err), retryable: false}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return c.classifyNetworkError(err)
	}
	defer drainAndCloseResponse(resp)

	if resp.StatusCode != http.StatusOK {
		return c.parseErrorWithRetry(resp)
	}

	return nil
}

// doWithRetry executes an operation with retry logic.
func (c *Client) doWithRetry(ctx context.Context, opName string, op func(context.Context) error) error {
	cfg := c.retryConfig

	var lastErr error
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		// Check context before attempting
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return fmt.Errorf("%w: %v (last error: %v)", ErrContextCanceled, err, lastErr)
			}
			return fmt.Errorf("%w: %v", ErrContextCanceled, err)
		}

		err := op(ctx)
		if err == nil {
			return nil
		}

		// Unwrap retryable error
		var retryErr *retryableError
		if errors.As(err, &retryErr) {
			if !retryErr.retryable {
				return retryErr.err
			}
			lastErr = retryErr.err
		} else {
			// Unknown error type, don't retry
			return err
		}

		// Don't sleep after the last attempt
		if attempt < cfg.MaxRetries {
			backoff := c.calculateBackoff(attempt, cfg)

			select {
			case <-ctx.Done():
				return fmt.Errorf("%w: %v (last error: %v)", ErrContextCanceled, ctx.Err(), lastErr)
			case <-time.After(backoff):
				// Continue to next attempt
			}
		}
	}

	return fmt.Errorf("%w after %d attempts: %v", ErrRetriesExhausted, cfg.MaxRetries+1, lastErr)
}

// calculateBackoff computes the backoff duration for a given attempt.
func (c *Client) calculateBackoff(attempt int, cfg RetryConfig) time.Duration {
	// Exponential backoff
	backoff := float64(cfg.InitialBackoff) * math.Pow(cfg.BackoffMultiplier, float64(attempt))

	// Cap at max backoff
	if backoff > float64(cfg.MaxBackoff) {
		backoff = float64(cfg.MaxBackoff)
	}

	// Add jitter
	if cfg.JitterFraction > 0 {
		c.rngMu.Lock()
		roll := c.rng.Float64()
		c.rngMu.Unlock()
		jitter := backoff * cfg.JitterFraction * (roll*2 - 1)
		backoff += jitter
	}

	// Ensure non-negative
	if backoff < 0 {
		backoff = 0
	}

	return time.Duration(backoff)
}

// classifyNetworkError determines if a network error is retryable.
func (c *Client) classifyNetworkError(err error) *retryableError {
	if err == nil {
		return nil
	}

	// Context errors are not retryable
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &retryableError{err: fmt.Errorf("%w: %v", ErrNetworkError, err), retryable: false}
	}

	// Check for retryable network errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		// Timeouts and temporary errors are retryable
		if netErr.Timeout() {
			return &retryableError{err: fmt.Errorf("%w: timeout: %v", ErrNetworkError, err), retryable: true}
		}
	}

	// Connection refused, reset, etc. are retryable
	errStr := err.Error()
	retryablePatterns := []string{
		"connection refused",
		"connection reset",
		"no such host",
		"network is unreachable",
		"i/o timeout",
		"EOF",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(strings.ToLower(errStr), strings.ToLower(pattern)) {
			return &retryableError{err: fmt.Errorf("%w: %v", ErrNetworkError, err), retryable: true}
		}
	}

	// Default: assume network errors are retryable
	return &retryableError{err: fmt.Errorf("%w: %v", ErrNetworkError, err), retryable: true}
}

// parseErrorWithRetry parses an error response and determines if it's retryable.
func (c *Client) parseErrorWithRetry(resp *http.Response) *retryableError {
	body, _ := io.ReadAll(resp.Body)

	var errResp ErrorResponse
	if err := json.Unmarshal(body, &errResp); err == nil && errResp.Code != "" {
		switch errResp.Code {
		case "KEY_NOT_FOUND":
			return &retryableError{err: ErrKeyNotFound, retryable: false, statusCode: resp.StatusCode}
		case "NOT_MASTER":
			return &retryableError{err: ErrNotMaster, retryable: true, statusCode: resp.StatusCode}
		case "PARTITION_MOVING", "PARTITION_NOT_OWNED":
			return &retryableError{err: ErrPartitionMoved, retryable: true, statusCode: resp.StatusCode}
		default:
			retryable := c.isRetryableStatusCode(resp.StatusCode)
			return &retryableError{
				err:        fmt.Errorf("%w: %s - %s", ErrServerError, errResp.Code, errResp.Error),
				retryable:  retryable,
				statusCode: resp.StatusCode,
			}
		}
	}

	retryable := c.isRetryableStatusCode(resp.StatusCode)
	return &retryableError{
		err:        fmt.Errorf("%w: status %d", ErrServerError, resp.StatusCode),
		retryable:  retryable,
		statusCode: resp.StatusCode,
	}
}

// isRetryableStatusCode checks if an HTTP status code should trigger a retry.
func (c *Client) isRetryableStatusCode(statusCode int) bool {
	for _, code := range c.retryConfig.RetryableStatusCodes {
		if statusCode == code {
			return true
		}
	}
	return false
}

// Convenience methods

func (c *Client) SetString(ctx context.Context, key string, value string) error {
	return c.Set(ctx, key, []byte(value))
}

func (c *Client) GetString(ctx context.Context, key string) (string, error) {
	data, err := c.Get(ctx, key)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (c *Client) SetJSON(ctx context.Context, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}
	return c.Set(ctx, key, data)
}

func (c *Client) GetJSON(ctx context.Context, key string, dest any) error {
	data, err := c.Get(ctx, key)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, dest)
}

func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	_, err := c.Get(ctx, key)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, ErrKeyNotFound) {
		return false, nil
	}
	return false, err
}

func (c *Client) Health(ctx context.Context) (bool, error) {
	url := fmt.Sprintf("%s/health", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer drainAndCloseResponse(resp)

	return resp.StatusCode == http.StatusOK, nil
}

// parseError is kept for backward compatibility.
func (c *Client) parseError(resp *http.Response) error {
	retryErr := c.parseErrorWithRetry(resp)
	return retryErr.err
}

func (c *Client) Close() error {
	c.httpClient.CloseIdleConnections()
	return nil
}

// RetryStats provides visibility into retry behavior for testing/monitoring.
type RetryStats struct {
	Attempts   int
	TotalTime  time.Duration
	LastError  error
	Successful bool
}

// SetWithStats performs a Set operation and returns retry statistics.
func (c *Client) SetWithStats(ctx context.Context, key string, value []byte) (RetryStats, error) {
	return c.doWithRetryStats(ctx, "Set", func(ctx context.Context) error {
		return c.doSet(ctx, key, value)
	})
}

// GetWithStats performs a Get operation and returns retry statistics.
func (c *Client) GetWithStats(ctx context.Context, key string) ([]byte, RetryStats, error) {
	var result []byte
	stats, err := c.doWithRetryStats(ctx, "Get", func(ctx context.Context) error {
		val, err := c.doGet(ctx, key)
		if err != nil {
			return err
		}
		result = val
		return nil
	})
	return result, stats, err
}

// DeleteWithStats performs a Delete operation and returns retry statistics.
func (c *Client) DeleteWithStats(ctx context.Context, key string) (RetryStats, error) {
	return c.doWithRetryStats(ctx, "Delete", func(ctx context.Context) error {
		return c.doDelete(ctx, key)
	})
}

// doWithRetryStats is like doWithRetry but also returns statistics.
func (c *Client) doWithRetryStats(ctx context.Context, opName string, op func(context.Context) error) (RetryStats, error) {
	cfg := c.retryConfig
	stats := RetryStats{}
	start := time.Now()

	var lastErr error
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		stats.Attempts = attempt + 1

		if err := ctx.Err(); err != nil {
			stats.TotalTime = time.Since(start)
			stats.LastError = lastErr
			if lastErr != nil {
				return stats, fmt.Errorf("%w: %v (last error: %v)", ErrContextCanceled, err, lastErr)
			}
			return stats, fmt.Errorf("%w: %v", ErrContextCanceled, err)
		}

		err := op(ctx)
		if err == nil {
			stats.TotalTime = time.Since(start)
			stats.Successful = true
			return stats, nil
		}

		var retryErr *retryableError
		if errors.As(err, &retryErr) {
			if !retryErr.retryable {
				stats.TotalTime = time.Since(start)
				stats.LastError = retryErr.err
				return stats, retryErr.err
			}
			lastErr = retryErr.err
		} else {
			stats.TotalTime = time.Since(start)
			stats.LastError = err
			return stats, err
		}

		if attempt < cfg.MaxRetries {
			backoff := c.calculateBackoff(attempt, cfg)
			select {
			case <-ctx.Done():
				stats.TotalTime = time.Since(start)
				stats.LastError = lastErr
				return stats, fmt.Errorf("%w: %v (last error: %v)", ErrContextCanceled, ctx.Err(), lastErr)
			case <-time.After(backoff):
			}
		}
	}

	stats.TotalTime = time.Since(start)
	stats.LastError = lastErr
	return stats, fmt.Errorf("%w after %d attempts: %v", ErrRetriesExhausted, cfg.MaxRetries+1, lastErr)
}
