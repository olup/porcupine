package main

import (
	"errors"
	"net"
	"net/netip"
	"strconv"
)

const defaultMetricsAddress = "127.0.0.1:9090"

func (s settings) metricsListenAddress() (string, error) {
	raw := s.get("PORCUPINE_METRICS_ENABLED")
	if raw == "" {
		return "", nil
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		return "", errors.New("PORCUPINE_METRICS_ENABLED must be true or false")
	}
	if !enabled {
		return "", nil
	}
	address := s.get("PORCUPINE_METRICS_LISTEN")
	if address == "" {
		return defaultMetricsAddress, nil
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", errors.New("PORCUPINE_METRICS_LISTEN must be an IP address and port")
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || ip.Zone() != "" || !ip.Unmap().IsLoopback() && !ip.Unmap().IsPrivate() {
		return "", errors.New("PORCUPINE_METRICS_LISTEN must bind to a loopback or private IP address")
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return "", errors.New("PORCUPINE_METRICS_LISTEN port must be between 0 and 65535")
	}
	return address, nil
}
