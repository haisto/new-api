package service

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/pkg/cachex"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/gin-gonic/gin"
	"github.com/samber/hot"
)

const (
	// ginKeyChannelKeyAffinityRead marks that the session key binding was already
	// consulted for this request. Retries skip the read so a just-failed key is
	// never reused from the binding.
	ginKeyChannelKeyAffinityRead = "channel_key_affinity_read"

	channelKeyAffinityCacheNamespace = "new-api:channel_key_affinity:v1"
)

var (
	channelKeyAffinityCacheOnce sync.Once
	channelKeyAffinityCache     *cachex.HybridCache[string]
)

func getChannelKeyAffinityCache() *cachex.HybridCache[string] {
	channelKeyAffinityCacheOnce.Do(func() {
		capacity := 100_000
		defaultTTLSeconds := 3600
		if setting := operation_setting.GetChannelKeyAffinitySetting(); setting != nil {
			if setting.MaxEntries > 0 {
				capacity = setting.MaxEntries
			}
			if setting.TTLSeconds > 0 {
				defaultTTLSeconds = setting.TTLSeconds
			}
		}

		channelKeyAffinityCache = cachex.NewHybridCache[string](cachex.HybridCacheConfig[string]{
			Namespace: cachex.Namespace(channelKeyAffinityCacheNamespace),
			Redis:     common.RDB,
			RedisEnabled: func() bool {
				return common.RedisEnabled && common.RDB != nil
			},
			RedisCodec: cachex.StringCodec{},
			Memory: func() *hot.HotCache[string, string] {
				return hot.NewHotCache[string, string](hot.LRU, capacity).
					WithTTL(time.Duration(defaultTTLSeconds) * time.Second).
					WithJanitor().
					Build()
			},
		})
	})
	return channelKeyAffinityCache
}

// channelKeyAffinityEnabled reports whether session-to-key binding is active.
// It deliberately follows the existing channel-affinity master switch shown in
// the routing settings UI (channel_affinity_setting.enabled), so enabling
// session affinity turns the key binding on together with it and no separate
// control is needed.
func channelKeyAffinityEnabled() bool {
	setting := operation_setting.GetChannelAffinitySetting()
	return setting != nil && setting.Enabled
}

// SelectMultiKeyForChannel wraps the channel's multi-key picker with session
// stickiness: the first attempt of a request reuses the key bound to the
// session fingerprint, and establishes the binding when none exists yet. Retries
// always pick a fresh key (so a just-failed key is not reused) but never rewrite
// the binding, so the session keeps preferring its original key and switches
// only once that key is disabled or the binding expires. The fingerprint comes
// from the channel-affinity rules when one matched; otherwise it degrades to the
// token-based session identity so the feature also works without any rule
// configured. When session affinity is disabled, has no usable session
// identity, or the channel is not multi-key, it falls straight through to pick.
func SelectMultiKeyForChannel(
	c *gin.Context,
	channel *model.Channel,
	pick func() (string, int, *types.NewAPIError),
) (string, int, *types.NewAPIError) {
	if !channelKeyAffinityEnabled() || channel == nil || !channel.ChannelInfo.IsMultiKey {
		return pick()
	}
	meta, ok := getChannelAffinityMeta(c)
	if !ok || meta.KeyFingerprint == "" {
		// No affinity rule matched (or it produced no key value). Fall back to
		// a request-derived session identity so key affinity still binds
		// without any rule configuration.
		var fallbackOK bool
		meta, fallbackOK = buildChannelKeyAffinityFallbackMeta(c)
		if !fallbackOK {
			return pick()
		}
	}

	bindingKey, ttl := buildChannelKeyAffinityCacheKey(meta)

	// Only the first attempt of a request consults the binding. Retries pick a
	// fresh key so the key that just failed is not selected again, and they
	// never rewrite the binding so the session keeps preferring its original
	// key; it switches only once that key is disabled or the binding expires.
	if _, resolved := c.Get(ginKeyChannelKeyAffinityRead); resolved {
		return pick()
	}
	c.Set(ginKeyChannelKeyAffinityRead, true)
	if key, index, found := resolveChannelKeyAffinity(channel, bindingKey); found {
		return key, index, nil
	}

	key, index, newAPIError := pick()
	if newAPIError != nil {
		return key, index, newAPIError
	}
	value := strconv.Itoa(channel.Id) + ":" + strconv.Itoa(index) + ":" + affinityFingerprint(key)
	_ = getChannelKeyAffinityCache().SetWithTTL(bindingKey, value, ttl)
	return key, index, nil
}

// buildChannelKeyAffinityCacheKey derives the per-session binding key and TTL
// from the affinity meta. It deliberately excludes the channel id so a session
// keeps a single binding instead of accumulating one per channel.
func buildChannelKeyAffinityCacheKey(meta channelAffinityMeta) (string, time.Duration) {
	parts := make([]string, 0, 4)
	if ruleName := strings.TrimSpace(meta.RuleName); ruleName != "" {
		parts = append(parts, ruleName)
	}
	if usingGroup := strings.TrimSpace(meta.UsingGroup); usingGroup != "" {
		parts = append(parts, usingGroup)
	}
	if modelName := strings.TrimSpace(meta.ModelName); modelName != "" {
		parts = append(parts, modelName)
	}
	parts = append(parts, meta.KeyFingerprint)

	ttlSeconds := meta.TTLSeconds
	if ttlSeconds <= 0 {
		if setting := operation_setting.GetChannelKeyAffinitySetting(); setting != nil && setting.TTLSeconds > 0 {
			ttlSeconds = setting.TTLSeconds
		} else {
			ttlSeconds = 3600
		}
	}
	return strings.Join(parts, ":"), time.Duration(ttlSeconds) * time.Second
}

// buildChannelKeyAffinityFallbackMeta derives a session identity for key
// affinity when no channel-affinity rule produced a fingerprint. The API token
// is stable across all requests of one consumer, so the same token keeps using
// the same upstream key without needing any rule configuration. The using group
// and model name ride along into the cache key so the binding is scoped per
// token/group/model instead of one binding per token. It reports ok=false when
// the request carries no usable token identity, in which case the caller keeps
// the plain pick.
func buildChannelKeyAffinityFallbackMeta(c *gin.Context) (channelAffinityMeta, bool) {
	tokenID := common.GetContextKeyInt(c, constant.ContextKeyTokenId)
	if tokenID <= 0 {
		return channelAffinityMeta{}, false
	}
	return channelAffinityMeta{
		KeyFingerprint: affinityFingerprint("token:" + strconv.Itoa(tokenID)),
		UsingGroup:     strings.TrimSpace(common.GetContextKeyString(c, constant.ContextKeyUsingGroup)),
		ModelName:      strings.TrimSpace(common.GetContextKeyString(c, constant.ContextKeyOriginalModel)),
	}, true
}

// RecordChannelKeyAffinitySuccess rebinds the session to the key that actually
// succeeded on the request. Selection prefers the bound key; when it failed and
// a retry succeeded on another key, this hook moves the binding to the
// successful key so the next request starts there instead of returning to the
// failed one. Reproducing an existing binding is a no-op write. The call is
// cheap and safe on paths that carry no key-affinity session identity (session
// affinity off, single-key channel, or no fingerprint): they return immediately.
func RecordChannelKeyAffinitySuccess(c *gin.Context) {
	if c == nil {
		return
	}
	if !channelKeyAffinityEnabled() {
		return
	}
	if !common.GetContextKeyBool(c, constant.ContextKeyChannelIsMultiKey) {
		return
	}
	channelID := common.GetContextKeyInt(c, constant.ContextKeyChannelId)
	index := common.GetContextKeyInt(c, constant.ContextKeyChannelMultiKeyIndex)
	key := common.GetContextKeyString(c, constant.ContextKeyChannelKey)
	if channelID <= 0 || index < 0 || key == "" {
		return
	}
	meta, ok := getChannelAffinityMeta(c)
	if !ok || meta.KeyFingerprint == "" {
		meta, ok = buildChannelKeyAffinityFallbackMeta(c)
		if !ok {
			return
		}
	}
	bindingKey, ttl := buildChannelKeyAffinityCacheKey(meta)
	value := strconv.Itoa(channelID) + ":" + strconv.Itoa(index) + ":" + affinityFingerprint(key)
	_ = getChannelKeyAffinityCache().SetWithTTL(bindingKey, value, ttl)
}

// ClearChannelKeyAffinityCacheAll deletes every session key binding and returns
// the number removed. The existing affinity cache clear-all action calls it so
// the session key bindings are cleared together with the channel affinity
// caches; otherwise stale bindings would survive and keep sessions pinned to
// old keys.
func ClearChannelKeyAffinityCacheAll() int {
	cache := getChannelKeyAffinityCache()
	keys, err := cache.Keys()
	if err != nil {
		common.SysError(fmt.Sprintf("channel key affinity cache list keys failed: err=%v", err))
		keys = nil
	}
	if len(keys) > 0 {
		if _, err := cache.DeleteMany(keys); err != nil {
			common.SysError(fmt.Sprintf("channel key affinity cache delete many failed: err=%v", err))
		}
	}
	return len(keys)
}

// ClearChannelKeyAffinityCacheByRuleName deletes session key bindings created
// from the given channel-affinity rule (their cache keys start with the rule
// name) and returns the number removed. Bindings from the token fallback are
// left untouched because they are not rule-scoped.
func ClearChannelKeyAffinityCacheByRuleName(ruleName string) (int, error) {
	if ruleName = strings.TrimSpace(ruleName); ruleName == "" {
		return 0, fmt.Errorf("rule_name 不能为空")
	}
	deleted, err := getChannelKeyAffinityCache().DeleteByPrefix(ruleName)
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

// resolveChannelKeyAffinity returns the bound key when it still belongs to the
// channel, is within range, is enabled, and matches the recorded key
// fingerprint. A stale or invalid binding is reported as not found so the
// caller repicks and rebinds.
func resolveChannelKeyAffinity(channel *model.Channel, bindingKey string) (string, int, bool) {
	raw, found, err := getChannelKeyAffinityCache().Get(bindingKey)
	if err != nil || !found {
		return "", 0, false
	}
	parts := strings.SplitN(raw, ":", 3)
	if len(parts) != 3 {
		return "", 0, false
	}
	channelID, err := strconv.Atoi(parts[0])
	if err != nil {
		return "", 0, false
	}
	keyIndex, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, false
	}
	if channelID != channel.Id {
		return "", 0, false
	}
	keys := channel.GetKeys()
	if keyIndex < 0 || keyIndex >= len(keys) {
		return "", 0, false
	}

	// Read the status list under the same lock the picker uses; the lock is
	// released before the caller repicks to avoid re-entering it.
	lock := model.GetChannelPollingLock(channel.Id)
	lock.Lock()
	enabled := true
	if statusList := channel.ChannelInfo.MultiKeyStatusList; statusList != nil {
		if status, ok := statusList[keyIndex]; ok {
			enabled = status == common.ChannelStatusEnabled
		}
	}
	lock.Unlock()
	if !enabled {
		return "", 0, false
	}

	key := keys[keyIndex]
	if affinityFingerprint(key) != parts[2] {
		return "", 0, false
	}
	return key, keyIndex, true
}
