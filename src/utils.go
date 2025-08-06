package main

import "strings"

// mapSSLMode maps PostgreSQL standard SSL modes to lib/pq compatible modes
func mapSSLMode(sslMode string) string {
	switch strings.ToLower(sslMode) {
	case "disable":
		return "disable"
	case "allow":
		// lib/pq doesn't support "allow", fallback to disable
		// "allow" means: try non-SSL first, fallback to SSL
		return "disable"
	case "prefer":
		// lib/pq doesn't support "prefer", fallback to disable for local dev
		// "prefer" means: try SSL first, fallback to non-SSL
		// For production, you might want to use "require" instead
		return "disable"
	case "require":
		return "require"
	case "verify-ca":
		return "verify-ca"
	case "verify-full":
		return "verify-full"
	default:
		// Default to disable for safety in development
		return "disable"
	}
}

// getSSLModeDescription returns a description of what the SSL mode does
func getSSLModeDescription(sslMode string) string {
	switch strings.ToLower(sslMode) {
	case "disable":
		return "SSL is disabled"
	case "allow":
		return "Try non-SSL first, fallback to SSL (mapped to disable)"
	case "prefer":
		return "Try SSL first, fallback to non-SSL (mapped to disable)"
	case "require":
		return "SSL is required"
	case "verify-ca":
		return "SSL required with CA verification"
	case "verify-full":
		return "SSL required with full certificate verification"
	default:
		return "Unknown SSL mode (mapped to disable)"
	}
}

// validateSSLMode checks if an SSL mode is valid for lib/pq
func validateSSLMode(sslMode string) bool {
	validModes := map[string]bool{
		"disable":     true,
		"require":     true,
		"verify-ca":   true,
		"verify-full": true,
		// These are PostgreSQL standard but not lib/pq compatible
		"allow":  false,
		"prefer": false,
	}

	_, exists := validModes[strings.ToLower(sslMode)]
	return exists
}
