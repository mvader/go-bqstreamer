// Package lib provides helper functions for all bqstreamer sub-packages.
package lib

import (
	"io/ioutil"

	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
)

// NewJWTConfig returns a new JWT configuration from a JSON key,
// acquired via https://console.developers.google.com.
//
// A config is used to authenticate with Google OAuth2.
func NewJWTConfig(keyPath string) (c *jwt.Config, err error) {
	keyBytes, err := ioutil.ReadFile(keyPath)
	if err != nil {
		return
	}

	c, err = google.JWTConfigFromJSON(
		keyBytes,
		"https://www.googleapis.com/auth/bigquery")

	// No need to check if err != nil since we return anyways.
	return
}
