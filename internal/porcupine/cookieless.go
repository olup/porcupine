package porcupine

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"strings"
)

const cookielessSentinel = "$posthog_cookieless"

func parseTrustedProxies(values []string) ([]netip.Prefix, error) {
	prefixes := make([]netip.Prefix, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if ip, err := netip.ParseAddr(value); err == nil && ip.Zone() == "" {
			ip = ip.Unmap()
			prefixes = append(prefixes, netip.PrefixFrom(ip, ip.BitLen()))
			continue
		}
		prefix, err := netip.ParsePrefix(value)
		if err != nil || prefix.Addr().Zone() != "" || prefix.Addr().Is4In6() {
			return nil, errors.New("trusted proxies must be IP addresses or CIDR prefixes")
		}
		prefixes = append(prefixes, prefix.Masked())
	}
	return prefixes, nil
}

func (s *Service) trustedProxy(ip netip.Addr) bool {
	for _, prefix := range s.trustedProxies {
		if prefix.Contains(ip) {
			return true
		}
	}
	return false
}

func (s *Service) clientIP(r *http.Request) (netip.Addr, error) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return netip.Addr{}, errors.New("invalid peer address")
	}
	peer, err := netip.ParseAddr(host)
	if err != nil || peer.Zone() != "" {
		return netip.Addr{}, errors.New("invalid peer IP")
	}
	peer = peer.Unmap()
	forwarded := r.Header.Values("X-Forwarded-For")
	if !s.trustedProxy(peer) || len(forwarded) == 0 {
		return peer, nil
	}
	hops := strings.Split(strings.Join(forwarded, ","), ",")
	if len(hops) > 16 {
		return netip.Addr{}, errors.New("too many forwarded hops")
	}
	// Validate the entire supplied chain, including entries beyond the trust boundary.
	ips := make([]netip.Addr, len(hops))
	for i, hop := range hops {
		ip, err := netip.ParseAddr(strings.TrimSpace(hop))
		if err != nil || ip.Zone() != "" {
			return netip.Addr{}, errors.New("invalid forwarded IP")
		}
		ips[i] = ip.Unmap()
	}
	for i := len(ips) - 1; i >= 0; i-- {
		peer = ips[i]
		if !s.trustedProxy(peer) {
			return peer, nil
		}
	}
	return peer, nil
}

func dailyCookielessKey(tx *sql.Tx, day string) ([]byte, error) {
	var latest sql.NullString
	if err := tx.QueryRow(`SELECT MAX(day) FROM cookieless_keys`).Scan(&latest); err != nil {
		return nil, err
	}
	if latest.Valid && latest.String > day {
		return nil, errors.New("cookieless clock moved before the active identity day")
	}
	var secret []byte
	err := tx.QueryRow(`SELECT secret FROM cookieless_keys WHERE day=?`, day).Scan(&secret)
	if errors.Is(err, sql.ErrNoRows) {
		secret = make([]byte, 32)
		if _, err = rand.Read(secret); err != nil {
			return nil, err
		}
		if _, err = tx.Exec(`INSERT INTO cookieless_keys(day,secret) VALUES(?,?)`, day, secret); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(`DELETE FROM cookieless_keys WHERE day<?`, day); err != nil {
		return nil, err
	}
	return secret, nil
}

func cookielessDistinctID(secret []byte, project string, ip netip.Addr, userAgent string) string {
	mac := hmac.New(sha256.New, secret)
	for _, value := range []string{project, ip.String(), userAgent} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		mac.Write(length[:])
		mac.Write([]byte(value))
	}
	return "cookieless_" + hex.EncodeToString(mac.Sum(nil))
}

func cookielessProperties(raw json.RawMessage, id, day string) json.RawMessage {
	var properties map[string]any
	// These properties were decoded and marshaled during request validation.
	if err := json.Unmarshal(raw, &properties); err != nil {
		panic(err)
	}
	for _, key := range []string{"$device_id", "$user_id", "$anon_distinct_id", "$session_id", "$window_id", "$ip", "ip", "ip_address", "client_ip", "remote_addr", "x-forwarded-for"} {
		delete(properties, key)
	}
	properties["distinct_id"] = id
	properties["cookieless"] = true
	properties["identity_day"] = day
	result, err := json.Marshal(properties)
	if err != nil {
		panic(err)
	}
	return result
}
