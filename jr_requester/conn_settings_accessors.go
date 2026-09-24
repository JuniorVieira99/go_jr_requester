package jr_requester

import (
	"maps"
	"net/http"
	"slices"
	"time"
)

// Locking accessors for ConnSettings.
//
// Every getter takes the read lock and every setter the write lock, so a
// settings value can be shared between goroutines and returned while it is
// shared. Note that a Connection copies its settings at construction: changing
// them afterwards configures nothing that is already built.
//
// The map and slice accessors copy in both directions, so a caller can neither
// read nor hold a reference that escapes the lock.

type SettingsAccessor struct {
	settings *ConnSettings
}

func NewSettingsAccessor(settings *ConnSettings) *SettingsAccessor {
	return &SettingsAccessor{
		settings: settings,
	}
}

// MaxIdleConns reports the global idle-connection ceiling across all hosts.
func (cs *ConnSettings) MaxIdleConns() uint64 {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.maxIdleConns
}

// SetMaxIdleConns sets the global idle-connection ceiling across all hosts.
func (cs *ConnSettings) SetMaxIdleConns(value uint64) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.maxIdleConns = value
}

// MaxIdleConnsPerHost reports how many idle connections are kept per host.
func (cs *ConnSettings) MaxIdleConnsPerHost() uint64 {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.maxIdleConnsPerHost
}

// SetMaxIdleConnsPerHost sets how many idle connections are kept per host.
func (cs *ConnSettings) SetMaxIdleConnsPerHost(value uint64) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.maxIdleConnsPerHost = value
}

// MaxConnsPerHost reports the total connection cap per host.
func (cs *ConnSettings) MaxConnsPerHost() uint64 {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.maxConnsPerHost
}

// SetMaxConnsPerHost sets the total connection cap per host.
func (cs *ConnSettings) SetMaxConnsPerHost(value uint64) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.maxConnsPerHost = value
}

// MaxRedirects reports how many redirects are followed before erroring.
func (cs *ConnSettings) MaxRedirects() uint64 {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.maxRedirects
}

// SetMaxRedirects sets how many redirects are followed before erroring.
func (cs *ConnSettings) SetMaxRedirects(value uint64) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.maxRedirects = value
}

// MaxRetries reports how many times a failed transport attempt is replayed.
func (cs *ConnSettings) MaxRetries() uint64 {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.maxRetries
}

// SetMaxRetries sets how many times a failed transport attempt is replayed.
func (cs *ConnSettings) SetMaxRetries(value uint64) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.maxRetries = value
}

// RetryDelay reports the pause between retry attempts.
func (cs *ConnSettings) RetryDelay() time.Duration {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.retryDelay
}

// SetRetryDelay sets the pause between retry attempts.
func (cs *ConnSettings) SetRetryDelay(value time.Duration) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.retryDelay = value
}

// Timeout reports the overall per-request budget.
func (cs *ConnSettings) Timeout() time.Duration {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.timeout
}

// SetTimeout sets the overall per-request budget.
func (cs *ConnSettings) SetTimeout(value time.Duration) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.timeout = value
}

// HandShakeTimeout reports the TLS handshake cap.
func (cs *ConnSettings) HandShakeTimeout() time.Duration {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.handShakeTimeout
}

// SetHandShakeTimeout sets the TLS handshake cap.
func (cs *ConnSettings) SetHandShakeTimeout(value time.Duration) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.handShakeTimeout = value
}

// MaxResponseBodySize reports the response-body buffering cap in bytes.
func (cs *ConnSettings) MaxResponseBodySize() int64 {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.maxResponseBodySize
}

// SetMaxResponseBodySize sets the response-body buffering cap in bytes.
func (cs *ConnSettings) SetMaxResponseBodySize(value int64) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.maxResponseBodySize = value
}

// UseProxy reports whether requests go through a proxy.
func (cs *ConnSettings) UseProxy() bool {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.useProxy
}

// SetUseProxy sets whether requests go through a proxy.
func (cs *ConnSettings) SetUseProxy(value bool) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.useProxy = value
}

// UseCookies reports whether a cookie jar is installed.
func (cs *ConnSettings) UseCookies() bool {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.useCookies
}

// SetUseCookies sets whether a cookie jar is installed.
func (cs *ConnSettings) SetUseCookies(value bool) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.useCookies = value
}

// UseKeepAlive reports whether connections are pooled and reused.
func (cs *ConnSettings) UseKeepAlive() bool {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.useKeepAlive
}

// SetUseKeepAlive sets whether connections are pooled and reused.
func (cs *ConnSettings) SetUseKeepAlive(value bool) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.useKeepAlive = value
}

// UseFailFast reports whether a stalled or failed request gives up early.
func (cs *ConnSettings) UseFailFast() bool {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.useFailFast
}

// SetUseFailFast sets whether a stalled or failed request gives up early.
func (cs *ConnSettings) SetUseFailFast(value bool) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.useFailFast = value
}

// UseHTTP2 reports whether HTTP/2 is forced on.
func (cs *ConnSettings) UseHTTP2() bool {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.useHTTP2
}

// SetUseHTTP2 sets whether HTTP/2 is forced on.
func (cs *ConnSettings) SetUseHTTP2(value bool) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.useHTTP2 = value
}

// UseAsync reports whether batches run concurrently.
func (cs *ConnSettings) UseAsync() bool {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.useAsync
}

// SetUseAsync sets whether batches run concurrently.
func (cs *ConnSettings) SetUseAsync(value bool) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.useAsync = value
}

// UseApiKey reports whether the ApiKey header is sent.
func (cs *ConnSettings) UseApiKey() bool {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.useApiKey
}

// SetUseApiKey sets whether the ApiKey header is sent.
func (cs *ConnSettings) SetUseApiKey(value bool) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.useApiKey = value
}

// UseCachedHeaders reports whether CacheHeaders is applied to every request.
func (cs *ConnSettings) UseCachedHeaders() bool {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.useCachedHeaders
}

// SetUseCachedHeaders sets whether CacheHeaders is applied to every request.
func (cs *ConnSettings) SetUseCachedHeaders(value bool) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.useCachedHeaders = value
}

// UseLogging reports whether a nil logger becomes a real one.
func (cs *ConnSettings) UseLogging() bool {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.useLogging
}

// SetUseLogging sets whether a nil logger becomes a real one.
func (cs *ConnSettings) SetUseLogging(value bool) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.useLogging = value
}

// UseMetrics reports whether ConnMetrics are recorded.
func (cs *ConnSettings) UseMetrics() bool {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.useMetrics
}

// SetUseMetrics sets whether ConnMetrics are recorded.
func (cs *ConnSettings) SetUseMetrics(value bool) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.useMetrics = value
}

// AllowRedirects reports whether redirects are followed at all.
func (cs *ConnSettings) AllowRedirects() bool {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.allowRedirects
}

// SetAllowRedirects sets whether redirects are followed at all.
func (cs *ConnSettings) SetAllowRedirects(value bool) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.allowRedirects = value
}

// CacheCookies reports whether a jar is created when none is supplied.
func (cs *ConnSettings) CacheCookies() bool {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.cacheCookies
}

// SetCacheCookies sets whether a jar is created when none is supplied.
func (cs *ConnSettings) SetCacheCookies(value bool) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.cacheCookies = value
}

// SkipPeerVerification reports whether TLS certificate verification is skipped.
func (cs *ConnSettings) SkipPeerVerification() bool {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.skipPeerVerification
}

// SetSkipPeerVerification sets whether TLS certificate verification is skipped.
func (cs *ConnSettings) SetSkipPeerVerification(value bool) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.skipPeerVerification = value
}

// LogLevel reports the level a default logger is built at.
func (cs *ConnSettings) LogLevel() LogLevel {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.logLevel
}

// SetLogLevel sets the level a default logger is built at.
func (cs *ConnSettings) SetLogLevel(value LogLevel) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.logLevel = value
}

// ProxyURL reports the proxy URL, empty to use the environment.
func (cs *ConnSettings) ProxyURL() string {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.proxyURL
}

// SetProxyURL sets the proxy URL, empty to use the environment.
func (cs *ConnSettings) SetProxyURL(value string) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.proxyURL = value
}

// HttpVersion reports the HTTP version used when Protocols is empty.
func (cs *ConnSettings) HttpVersion() string {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.httpVersion
}

// SetHttpVersion sets the HTTP version used when Protocols is empty.
func (cs *ConnSettings) SetHttpVersion(value string) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.httpVersion = value
}

// ApiKey reports the header and value sent when UseApiKey is on.
func (cs *ConnSettings) ApiKey() ApiKey {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.apiKey
}

// SetApiKey sets the header and value sent when UseApiKey is on. The pair is
// set together so a value can never be left without the header carrying it.
func (cs *ConnSettings) SetApiKey(value ApiKey) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.apiKey = value
}

// CookieJar reports the cookie jar to install.
func (cs *ConnSettings) CookieJar() http.CookieJar {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.cookieJar
}

// SetCookieJar sets the cookie jar to install.
func (cs *ConnSettings) SetCookieJar(value http.CookieJar) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.cookieJar = value
}

// CacheHeaders returns a copy of the headers added to every request.
func (cs *ConnSettings) CacheHeaders() map[string]string {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return maps.Clone(cs.cacheHeaders)
}

// SetCacheHeaders stores a copy of the headers added to every request.
func (cs *ConnSettings) SetCacheHeaders(value map[string]string) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.cacheHeaders = maps.Clone(value)
}

// Protocols returns a copy of the ALPN protocol preferences.
func (cs *ConnSettings) Protocols() []string {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return slices.Clone(cs.protocols)
}

// SetProtocols stores a copy of the ALPN protocol preferences.
func (cs *ConnSettings) SetProtocols(value []string) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.protocols = slices.Clone(value)
}
