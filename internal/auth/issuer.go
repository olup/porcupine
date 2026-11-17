package auth

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
)

func NormalizeIssuer(raw string, interactive bool) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u == nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Path != "" && u.Path != "/" || u.RawPath != "" {
		return "", errors.New("invalid OAuth issuer")
	}
	if u.Scheme != "https" {
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "http" || !(u.Hostname() == "localhost" || ip != nil && ip.IsLoopback()) {
			return "", errors.New("OAuth issuer requires HTTPS except on loopback")
		}
	}
	raw = strings.TrimSuffix(raw, "/")
	if interactive {
		if err := validateBrowserIssuer(u, raw); err != nil {
			return "", err
		}
	}
	return raw, nil
}

// A consent form compares the browser Origin byte for byte. Machine clients
// retain their configured issuer and resource strings across upgrades.
func validateBrowserIssuer(u *url.URL, issuer string) error {
	host := strings.ToLower(u.Hostname())
	if ip := net.ParseIP(host); ip != nil {
		host = ip.String()
	}
	port := u.Port()
	if port != "" {
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return errors.New("invalid OAuth issuer port")
		}
		port = strconv.Itoa(number)
		if u.Scheme == "https" && port == "443" || u.Scheme == "http" && port == "80" {
			port = ""
		}
	}
	if port != "" {
		host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	canonical := u.Scheme + "://" + host
	if issuer != canonical {
		return errors.New("OAuth issuer must use a canonical origin: lowercase scheme and host, canonical IP and port, no default port")
	}
	return nil
}
