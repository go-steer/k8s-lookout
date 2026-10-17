// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-steer/steward/authserver"
	"github.com/go-steer/steward/seal"
	"github.com/go-steer/steward/seal/secretmanager"

	"github.com/go-steer/k8s-lookout/internal/mcpserver"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// oauthFlags are the `lookout mcp` flags of Path B (#702): lookout's own
// OAuth authorization server, for generic MCP clients.
type oauthFlags struct {
	issuer, clientSecretFile, custody, vaultAuthProvider, keysFile, keysSecret, clientsFile *string
	openRegistration                                                                        *bool
}

func registerOAuthFlags(fs *flag.FlagSet) oauthFlags {
	return oauthFlags{
		issuer:            fs.String("oauth-issuer", "", "serve lookout's own OAuth authorization server for generic MCP clients at this public base URL"),
		clientSecretFile:  fs.String("google-client-secret-file", "", "authorization server: the Google OAuth client's JSON file from the Cloud console"),
		custody:           fs.String("oauth-custody", "", "authorization server: who holds each person's Google grant: vault or sealed"),
		vaultAuthProvider: fs.String("oauth-vault-auth-provider", "", "authorization server: the Agent Identity auth provider (projects/P/locations/L/authProviders/N) for --oauth-custody=vault"),
		keysFile:          fs.String("oauth-keys-file", "", "authorization server: keyring file that seals tokens (re-read as it changes)"),
		keysSecret:        fs.String("oauth-keys-secret", "", "authorization server: Secret Manager version holding the keyring (projects/P/secrets/S/versions/V)"),
		clientsFile:       fs.String("oauth-clients-file", "", "authorization server: JSON array of pre-registered clients"),
		openRegistration:  fs.Bool("oauth-open-registration", false, "authorization server: let clients register themselves (DCR and Client ID Metadata Documents)"),
	}
}

func (o oauthFlags) enabled() bool { return *o.issuer != "" }

// problem checks the Path B flags against the rest, returning a usage
// problem or "".
func (o oauthFlags) problem(googleMode bool) string {
	if !o.enabled() {
		for _, f := range []struct {
			name string
			set  bool
		}{
			{"--google-client-secret-file", *o.clientSecretFile != ""},
			{"--oauth-custody", *o.custody != ""},
			{"--oauth-vault-auth-provider", *o.vaultAuthProvider != ""},
			{"--oauth-keys-file", *o.keysFile != ""},
			{"--oauth-keys-secret", *o.keysSecret != ""},
			{"--oauth-clients-file", *o.clientsFile != ""},
			{"--oauth-open-registration", *o.openRegistration},
		} {
			if f.set {
				return f.name + " configures lookout's authorization server for generic MCP clients; pass --oauth-issuer=<url> as well." + usageHint
			}
		}
		return ""
	}
	switch {
	case !googleMode:
		return "--oauth-issuer needs Google-token mode (--google-client-id, --google-allow, --resource-url): people who sign in through it are admitted by the same --google-allow list" + usageHint
	case *o.clientSecretFile == "":
		return "--oauth-issuer needs --google-client-secret-file: the JSON file the Cloud console downloads for the Google OAuth client people sign in through" + usageHint
	case *o.custody == "":
		return "--oauth-issuer needs --oauth-custody: vault (the Agent Identity credential vault holds each person's Google grant) or sealed (it travels, sealed, inside lookout's refresh token)" + usageHint
	case *o.custody != "vault" && *o.custody != "sealed":
		return fmt.Sprintf("--oauth-custody=%s: want vault or sealed", *o.custody) + usageHint
	case *o.custody == "vault" && *o.vaultAuthProvider == "":
		return "--oauth-custody=vault needs --oauth-vault-auth-provider=projects/<p>/locations/<l>/authProviders/<name>" + usageHint
	case *o.custody == "sealed" && *o.vaultAuthProvider != "":
		return "--oauth-vault-auth-provider only applies to --oauth-custody=vault" + usageHint
	case (*o.keysFile == "") == (*o.keysSecret == ""):
		return "--oauth-issuer needs exactly one of --oauth-keys-file and --oauth-keys-secret: the keyring that seals every token it hands out" + usageHint
	case *o.clientsFile == "" && !*o.openRegistration:
		return "--oauth-issuer: no client could sign in: pre-register clients with --oauth-clients-file, or pass --oauth-open-registration" + usageHint
	}
	return ""
}

// startupTimeout bounds the first key load, so a misconfigured key source
// fails at startup rather than at the first sign-in.
const startupTimeout = 30 * time.Second

// build loads the Google client, the clients file and the keys, and picks
// the custody. It returns nil, nil when Path B is off.
func (o oauthFlags) build(ctx context.Context, logger *slog.Logger) (*mcpserver.AuthServerOptions, error) {
	if !o.enabled() {
		return nil, nil
	}
	google, err := authserver.LoadGoogleClient(*o.clientSecretFile)
	if err != nil {
		return nil, fmt.Errorf("--google-client-secret-file=%s: %v", *o.clientSecretFile, err)
	}
	var clients []authserver.StaticClient
	if *o.clientsFile != "" {
		if clients, err = authserver.LoadStaticClients(*o.clientsFile); err != nil {
			return nil, fmt.Errorf("--oauth-clients-file=%s: %v", *o.clientsFile, err)
		}
	}
	var custody authserver.Custody = authserver.SealedCustody{}
	if *o.custody == "vault" {
		hc, err := cloud.ServiceClient(ctx, authserver.ScopeCloudPlatform)
		if err != nil {
			return nil, fmt.Errorf("--oauth-custody=vault: %v", err)
		}
		custody = &authserver.VaultCustody{AuthProvider: *o.vaultAuthProvider, Client: hc}
	}
	var keys *seal.Reloader
	if *o.keysFile != "" {
		keys = seal.NewReloader(seal.FileLoader(*o.keysFile), 0, logger)
	} else {
		hc, err := cloud.ServiceClient(ctx, secretmanager.Scope)
		if err != nil {
			return nil, fmt.Errorf("--oauth-keys-secret: %v", err)
		}
		if keys, err = secretmanager.NewProvider(secretmanager.Source{Client: hc, Version: *o.keysSecret}, 0, logger); err != nil {
			return nil, fmt.Errorf("--oauth-keys-secret=%s: %v", *o.keysSecret, err)
		}
	}
	kctx, cancel := context.WithTimeout(ctx, startupTimeout)
	defer cancel()
	if _, err := keys.Keyring(kctx); err != nil {
		src := "--oauth-keys-file=" + *o.keysFile
		if *o.keysFile == "" {
			src = "--oauth-keys-secret=" + *o.keysSecret
		}
		return nil, fmt.Errorf("%s: %v", src, err)
	}
	return &mcpserver.AuthServerOptions{
		Issuer:           *o.issuer,
		Google:           google,
		Custody:          custody,
		Keys:             keys,
		Clients:          clients,
		OpenRegistration: *o.openRegistration,
	}, nil
}

// hasClientCredentials reports whether any pre-registered client is a
// workload (client credentials).
func hasClientCredentials(clients []authserver.StaticClient) bool {
	for _, c := range clients {
		if c.ClientCredentials || c.ServiceAccount != "" {
			return true
		}
	}
	return false
}
