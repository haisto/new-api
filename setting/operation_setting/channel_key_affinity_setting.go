package operation_setting

import "github.com/QuantumNous/new-api/setting/config"

// ChannelKeyAffinitySetting tunes the session-to-key binding within a selected
// channel, so the same session keeps using the same upstream key and hits the
// upstream prompt cache more often.
//
// There is deliberately no independent enable switch: the feature follows the
// existing channel-affinity master switch (channel_affinity_setting.enabled)
// shown in the routing settings UI, so enabling session affinity turns the key
// binding on together with it. These fields only tune the binding cache.
//
// It reuses the session fingerprint produced by the channel affinity rules
// (channel_affinity_setting). When no affinity rule matches a request, the
// fingerprint degrades to the API token identity; requests without either keep
// the plain random/polling pick.
type ChannelKeyAffinitySetting struct {
	// TTLSeconds overrides the affinity meta TTL when > 0.
	TTLSeconds int `json:"ttl_seconds"`
	MaxEntries int `json:"max_entries"`
}

var channelKeyAffinitySetting = ChannelKeyAffinitySetting{
	TTLSeconds: 0,
	MaxEntries: 100_000,
}

func init() {
	config.GlobalConfig.Register("channel_key_affinity_setting", &channelKeyAffinitySetting)
}

func GetChannelKeyAffinitySetting() *ChannelKeyAffinitySetting {
	return &channelKeyAffinitySetting
}
