package operation_setting

import "github.com/QuantumNous/new-api/setting/config"

// ChannelKeyAffinitySetting binds a session to a stable multi-key index within
// the selected channel, so the same session keeps using the same upstream key
// and hits the upstream prompt cache more often.
//
// It reuses the session fingerprint produced by the channel affinity rules
// (channel_affinity_setting). When no affinity rule matches a request there is
// no fingerprint, and key affinity degrades to the plain random/polling pick.
type ChannelKeyAffinitySetting struct {
	Enabled bool `json:"enabled"`
	// TTLSeconds overrides the affinity meta TTL when > 0.
	TTLSeconds int `json:"ttl_seconds"`
	MaxEntries int `json:"max_entries"`
}

var channelKeyAffinitySetting = ChannelKeyAffinitySetting{
	Enabled:    false,
	TTLSeconds: 0,
	MaxEntries: 100_000,
}

func init() {
	config.GlobalConfig.Register("channel_key_affinity_setting", &channelKeyAffinitySetting)
}

func GetChannelKeyAffinitySetting() *ChannelKeyAffinitySetting {
	return &channelKeyAffinitySetting
}
