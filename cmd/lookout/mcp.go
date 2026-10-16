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
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/go-steer/k8s-lookout/internal/fleet"
	"github.com/go-steer/k8s-lookout/internal/mcpserver"
	"github.com/go-steer/k8s-lookout/internal/version"
	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/netprobe"
)

// lookout mcp registers in the multicall root like watch — it is a
// server, not a check, so it does not belong in the pkg/checks
// registry it serves. Every non-hidden registered command becomes an
// MCP tool automatically (§4.3); commands added later appear here
// with no changes to this file.
func init() {
	// Every probe container `net probe-from` adds records the release
	// that added it (LOOKOUT_PROBE_CLIENT); pkg/ cannot read
	// internal/version, so the binary hands it over.
	netprobe.ClientVersion = "lookout/" + version.Semver()
	register(command{
		name:    "mcp",
		summary: "serve the read-path checks as MCP tools (stdio, or --listen for localhost HTTP)",
		run:     runMCP,
	})
}

const mcpHelp = `Usage: lookout mcp [flags]

Serve every registered read-path check as an MCP tool (§4.3): tool
names are the commands' MCP names (e.g. triage delta →
k8s_triage_delta), input schemas mirror the CLI flags plus a "target"
property where a command takes a positional argument, and tool
results carry the same payload the CLI prints — logfmt findings
terminated by the scanned=/findings=/elapsed= summary line, sanitized
by §6.5. Host file paths and cluster credentials are server-scoped and
never accepted from MCP clients.

Flags:
  --listen=<host:port>  serve streamable HTTP on a loopback address
                        (e.g. 127.0.0.1:8383) instead of stdio.
                        Serves plain HTTP (no TLS) — off-host exposure
                        requires a TLS-terminating reverse proxy in
                        front of lookout mcp. Non-loopback binds are
                        refused unless all three of --allow-non-loopback,
                        --access-log, and an authentication mode
                        (--auth-token-file or --google-client-id) are
                        given (§4.3).
  --allow-non-loopback  opt in to a routable bind. Not enough on its
                        own: a bind flag must not open an
                        unauthenticated cluster-read API.
  --auth-token-file=<p> require this bearer token on every HTTP
                        request; 401 otherwise. One shared token, no
                        authorization — every caller presenting it
                        gets the full advertised surface. Compared in
                        constant time. mTLS is out of scope.
  --google-client-id=<ids>
                        Google-token mode (#699): every tool call reads
                        as the person who made it, with their own Google
                        access token, so what they see is what their own
                        Kubernetes RBAC and Cloud IAM allow; lookout's
                        identity reads nothing on their behalf. For a
                        Gemini Enterprise app calling lookout as a Custom
                        MCP Server. The value is the OAuth client ID(s)
                        the data store is configured with; tokens issued
                        to any other client are refused, as are tokens
                        without the cloud-platform scope. Requires
                        --listen, --google-allow and --resource-url;
                        exclusive with --auth-token-file. Serves
                        stateless HTTP and RFC 9728 metadata at
                        /.well-known/oauth-protected-resource/<path>.
                        The cluster must be GKE and reached over https:
                        the caller's token is never sent over plaintext
                        or unverified TLS. Tools whose answers are rows
                        lookout reads as itself (triage status, findings
                        diff/ack, history via --at) show a row only if
                        the caller may list pods in its namespace (all
                        namespaces for a cluster-scoped row), and count
                        the rest as hidden=<n>; live reads are already
                        the caller's own. Callers cannot choose
                        store_cluster, a caller's findings diff never
                        persists, and an ack is recorded as the caller.
                        Upgrade the sentinel first: a write migrates
                        the store, which older binaries then refuse.
  --google-allow=<list> who is let in, comma-separated: exact emails, or
                        "@domain" for every account Google says belongs
                        to that Workspace domain (never matched on the
                        email suffix alone).
  --google-allow-write=<list>
                        who may call tools that change state, same
                        syntax: the triage-status write and findings
                        diff/ack (they update the store; the gate also
                        covers their read modes), and k8s_net_probe_from
                        when served. Empty: no caller may. A caller
                        writes triage records only for namespaces they
                        can see, and each record names them
                        (recorded_by).
  --resource-url=<url>  this server's MCP URL exactly as clients reach
                        it (e.g. https://lookout.example.com/mcp); named
                        in the RFC 9728 metadata and every 401.
  --kubeconfig=<path>   path to a kubeconfig file pinned for all MCP
                        tool invocations (instead of $KUBECONFIG /
                        ~/.kube/config).
  --context=<name>      kubeconfig context pinned for all MCP tool
                        invocations.
  --clusters=<pairs>    serve several clusters (#700): comma-separated
                        name=endpoint pairs, reached with the GKE
                        provider's credentials (-tags gke). Every tool
                        then takes a required "cluster" argument, and
                        k8s_list_clusters lists the names a caller may
                        pass. Exclusive with --clusters-from,
                        --kubeconfig and --context.
  --clusters-from=<scopes>
                        serve several clusters, discovered: the same
                        scopes as lookout watch, comma-separated and
                        mixable — project[/location], fleet:<host>,
                        folders/<id>, organizations/<id> — or alone,
                        kubeconfig[:<path>] (one cluster per context).
                        With --google-client-id, discovery runs as each
                        caller, with their token, cached five minutes:
                        a caller sees only the clusters they can read,
                        and lookout needs no IAM; GKE clusters only, so
                        no kubeconfig fleet and only *.gke.goog
                        --clusters endpoints. Otherwise it runs as
                        lookout at startup and every --rediscover. A
                        name unique in the caller's set is used bare;
                        a repeated one is name@project (then
                        name@project/location); project/location/name
                        always works. --store and --report become stems:
                        each call reads its cluster's own file, named
                        the way a multi-cluster watch names it (not in
                        Google-token mode, where file names cannot be
                        proven unique). net probe, which measures from
                        lookout's own pod, is refused for every
                        cluster but the one lookout runs in. Serves
                        stateless HTTP, so replicas need no sticky
                        sessions.
  --rediscover=<dur>    how often the shared cluster set is refreshed
                        without Google-token mode (default 10m).
  --default-cluster=<name>
                        on a multi-cluster endpoint, the cluster a call
                        that names none reads, when the caller can read
                        it. Without it, such a call reads the caller's
                        only readable cluster in --default-project, or
                        their only readable cluster at all, and
                        otherwise is answered with candidates (or, for a
                        client that supports elicitation, the person is
                        asked). Discovery checks each cluster once per
                        caller: listing a cluster is not reading it.
  --default-project=<project>
                        the project preferred when a call names no
                        cluster, or a bare name that repeats across
                        projects; its clusters are listed first.
  --folder-discovery=auto|asset|resourcemanager
                        how folders/<id> and organizations/<id> scopes
                        are searched: Cloud Asset Inventory, falling
                        back to a Resource Manager walk (auto, the
                        default); Cloud Asset Inventory only; or never
                        Cloud Asset Inventory.
  --max-concurrency-per-cluster=<n>
                        cap each cluster's calls in flight, so one slow
                        API server cannot hold every slot; a call past
                        it is turned away at once. Default: the global
                        limit (8) divided by the number of clusters, at
                        least 2.
  --exemptions=<path>   path to an exemption YAML file pinned for all
                        MCP tool invocations.
  --store=<path>        path to a sentinel SQLite store pinned for MCP
                        tool invocations that read historical state.
  --report=<path>       path to a findings report pinned for MCP tool
                        invocations that read one.
  --profile=<name>      advertise only one curated tool surface
                        instead of all of them (see below).
  --tools=<selection>   adjust the surface tool by tool, left to
                        right: "all" adds every tool, a profile name
                        adds its members, a tool name adds one, and a
                        "-" prefix removes. Combines with --profile,
                        which is evaluated first — so
                        "--profile=triage --tools=-k8s_triage_logs"
                        is the triage surface minus the logs tool.
  --list-tools          print the tools this selection would
                        advertise, with the JSON schema bytes each
                        costs on every model call, and exit.
  --probe-from-image=<repo>@sha256:<digest>
                        serve k8s_net_probe_from (lookout net
                        probe-from), the one tool that CHANGES a
                        workload: it adds an ephemeral probe
                        container to a named pod, which stays in the
                        pod's spec. Never served without this flag;
                        --tools and profiles cannot add it. The value
                        fixes the image the probe container runs
                        (digest-pinned; tags refused), so the tool
                        takes no image argument. The identity also
                        needs the deploy-probe/ grant
                        (docs/in-pod-probe-design.md).
  --access-log=<path>   append one logfmt line per tool call:
                        ts, tool, exit code, duration, response
                        bytes (plus remote peer address on HTTP).
                        Created if absent, appended if not, mode 0600.
                        Deliberately not the arguments or the response
                        body — the log is an operational record, not a
                        second copy of cluster data. Optional on
                        loopback, mandatory off-host.

Profiles:
%s
Every tool advertised is paid for on every model call, in tokens and
in the model's accuracy at choosing among near-identical options — the
full surface is well over a hundred kilobytes of JSON schema. Serve
the smallest surface that answers the questions the agent asks.

With no flags the transport is stdio: JSON-RPC on stdin/stdout, the
transport a daemon uses when it spawns "lookout mcp" as a child
process. Diagnostics go to stderr only.
`

// splitList splits a comma-separated flag value, dropping blanks.
func splitList(s string) []string {
	var out []string
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// enableProbeFrom swaps the CLI build of `net probe-from` (which
// takes --image) for one with the operator's image fixed and no
// image argument in its schema, and adds the tool to the selection.
// The image is the operator's decision, made where the server is
// configured; a model calling the tool cannot choose what runs in the
// pod (docs/in-pod-probe-design.md, "MCP exposure").
func enableProbeFrom(base *checks.Registry, selected map[string]bool, image string) (*checks.Registry, map[string]bool, error) {
	if err := netprobe.ValidImage(image); err != nil {
		return nil, nil, err
	}
	served := netprobe.NewFrom(netprobe.FromDeps{}, netprobe.FromConfig{Image: image})
	reg := checks.NewRegistry()
	for _, c := range base.All() {
		if c.Name == served.Name {
			c = served
		}
		reg.Register(c)
	}
	sel, err := mcpserver.EnablePrivileged(reg, selected, served.MCPName)
	if err != nil {
		return nil, nil, err
	}
	return reg, sel, nil
}

func runMCP(ctx context.Context, args []string) int {
	return mcpMain(ctx, args, os.Stdout, os.Stderr)
}

// mcpMain is runMCP with injectable streams for tests.
func mcpMain(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("lookout mcp", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	listen := fs.String("listen", "", "loopback host:port for streamable HTTP; empty for stdio")
	profile := fs.String("profile", "", "advertise one curated tool surface instead of all of them")
	tools := fs.String("tools", "", "adjust the advertised tools: all,<profile>,<tool>,-<tool>, left to right")
	listTools := fs.Bool("list-tools", false, "print the selected tools and their schema cost, then exit")
	accessLog := fs.String("access-log", "", "append one line per tool call to this path")
	allowNonLoopback := fs.Bool("allow-non-loopback", false, "permit a routable --listen address (requires --auth-token-file and --access-log)")
	authTokenFile := fs.String("auth-token-file", "", "require this bearer token on every HTTP request")
	probeFromImage := fs.String("probe-from-image", "", "serve the privileged k8s_net_probe_from tool, running this digest-pinned image")
	kubeconfig := fs.String("kubeconfig", "", "path to a kubeconfig file pinned for all MCP tool calls")
	contextName := fs.String("context", "", "kubeconfig context pinned for all MCP tool calls")
	exemptions := fs.String("exemptions", "", "path to an exemption YAML file pinned for all MCP tool calls")
	store := fs.String("store", "", "path to a sentinel SQLite store pinned for MCP tool calls that read one")
	report := fs.String("report", "", "path to a findings report pinned for MCP tool calls that read one")
	googleClientID := fs.String("google-client-id", "", "Google-token mode: the OAuth client ID(s) callers' tokens must be issued to (comma-separated)")
	googleAllow := fs.String("google-allow", "", "Google-token mode: who is let in (comma-separated emails or @domain)")
	googleAllowWrite := fs.String("google-allow-write", "", "Google-token mode: who may call tools that change state (comma-separated emails or @domain)")
	resourceURL := fs.String("resource-url", "", "Google-token mode: this server's MCP URL as clients reach it")
	clustersFlag := fs.String("clusters", "", "serve several clusters: comma-separated name=endpoint pairs")
	clustersFrom := fs.String("clusters-from", "", "serve several clusters, discovered from these scopes")
	rediscover := fs.Duration("rediscover", mcpserver.DefaultRediscover, "how often the shared cluster set is refreshed")
	maxPerCluster := fs.Int("max-concurrency-per-cluster", 0, "cap each cluster's calls in flight on a multi-cluster endpoint")
	defaultCluster := fs.String("default-cluster", "", "the cluster a call that names none reads, when the caller can read it")
	defaultProject := fs.String("default-project", "", "the project preferred when a call names no cluster or a repeated name")
	folderDiscovery := fs.String("folder-discovery", "", "how folder and organization scopes are searched: auto, asset or resourcemanager")
	reg := checks.Default()
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			fmt.Fprintf(stdout, mcpHelp, mcpserver.ProfileHelp(reg))
			return 0
		}
		fmt.Fprintf(stderr, "lookout mcp: %v\nRun 'lookout mcp --help' for usage.\n", err)
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "lookout mcp: unexpected argument %q\nRun 'lookout mcp --help' for usage.\n", fs.Arg(0))
		return 2
	}
	// The HTTP-only flags are rejected on stdio rather than ignored: a
	// token that authenticates nothing reads, to whoever wrote the
	// config, exactly like a token that does.
	googleMode := *googleClientID != ""
	if !googleMode {
		for _, f := range []struct {
			name string
			set  bool
		}{
			{"--google-allow", *googleAllow != ""},
			{"--google-allow-write", *googleAllowWrite != ""},
			{"--resource-url", *resourceURL != ""},
		} {
			if f.set {
				fmt.Fprintf(stderr, "lookout mcp: %s only applies in Google-token mode; pass --google-client-id as well.\n"+
					"Run 'lookout mcp --help' for usage.\n", f.name)
				return 2
			}
		}
	} else {
		var problem string
		switch {
		case *authTokenFile != "":
			problem = "--google-client-id and --auth-token-file are exclusive: pick one way to authenticate callers"
		case *googleAllow == "":
			problem = "--google-client-id needs --google-allow: an empty allow-list would admit no one"
		case *resourceURL == "":
			problem = "--google-client-id needs --resource-url, this server's MCP URL as clients reach it (e.g. https://lookout.example.com/mcp)"
		}
		if problem != "" {
			fmt.Fprintf(stderr, "lookout mcp: %s\nRun 'lookout mcp --help' for usage.\n", problem)
			return 2
		}
	}
	if *listen == "" {
		for _, f := range []struct {
			name string
			set  bool
		}{
			{"--auth-token-file", *authTokenFile != ""},
			{"--allow-non-loopback", *allowNonLoopback},
			{"--google-client-id", googleMode},
		} {
			if f.set {
				fmt.Fprintf(stderr, "lookout mcp: %s has no effect on the stdio transport "+
					"(there is no HTTP request to authenticate); pass --listen=<host:port> as well.\n"+
					"Run 'lookout mcp --help' for usage.\n", f.name)
				return 2
			}
		}
	} else if err := (mcpserver.BindPolicy{
		Listen:           *listen,
		AllowNonLoopback: *allowNonLoopback,
		HasAuthToken:     *authTokenFile != "" || googleMode,
		HasAccessLog:     *accessLog != "",
	}).Validate(); err != nil {
		fmt.Fprintf(stderr, "lookout mcp: %v\n", err)
		return 2
	}
	multiCluster := *clustersFlag != "" || *clustersFrom != ""
	if !multiCluster {
		for _, f := range []struct {
			name string
			set  bool
		}{
			{"--default-cluster", *defaultCluster != ""},
			{"--default-project", *defaultProject != ""},
			{"--folder-discovery", *folderDiscovery != ""},
		} {
			if f.set {
				fmt.Fprintf(stderr, "lookout mcp: %s only applies to a multi-cluster endpoint; pass --clusters or --clusters-from as well\nRun 'lookout mcp --help' for usage.\n", f.name)
				return 2
			}
		}
	}
	if err := fleet.ValidFolderSearch(*folderDiscovery); err != nil {
		fmt.Fprintf(stderr, "lookout mcp: %v\nRun 'lookout mcp --help' for usage.\n", err)
		return 2
	}
	if !multiCluster && *maxPerCluster != 0 {
		fmt.Fprintf(stderr, "lookout mcp: --max-concurrency-per-cluster only applies to a multi-cluster endpoint; pass --clusters or --clusters-from as well\nRun 'lookout mcp --help' for usage.\n")
		return 2
	}
	if *maxPerCluster < 0 {
		fmt.Fprintf(stderr, "lookout mcp: --max-concurrency-per-cluster must not be negative\nRun 'lookout mcp --help' for usage.\n")
		return 2
	}
	if problem := multiClusterProblem(multiCluster, googleMode, *clustersFlag, *clustersFrom, *kubeconfig, *contextName, *store, *report, *rediscover, fs); problem != "" {
		fmt.Fprintf(stderr, "lookout mcp: %s\nRun 'lookout mcp --help' for usage.\n", problem)
		return 2
	}
	selected, err := mcpserver.ResolveTools(reg, *profile, *tools)
	if err != nil {
		fmt.Fprintf(stderr, "lookout mcp: %v\nRun 'lookout mcp --help' for usage.\n", err)
		return 2
	}
	if *probeFromImage != "" {
		reg, selected, err = enableProbeFrom(reg, selected, *probeFromImage)
		if err != nil {
			fmt.Fprintf(stderr, "lookout mcp: --probe-from-image: %v\n", err)
			return 2
		}
	}
	if *listTools {
		if multiCluster {
			fmt.Fprint(stdout, mcpserver.MultiClusterToolListing(reg, selected))
		} else {
			fmt.Fprint(stdout, mcpserver.ToolListing(reg, selected))
		}
		return 0
	}

	var log *mcpserver.AccessLog
	if *accessLog != "" {
		l, closer, err := mcpserver.OpenAccessLog(*accessLog)
		if err != nil {
			// A log that cannot be opened is a usage error, not a
			// degraded mode: an operator who asked to be able to
			// answer "what did the agent call" must not find out
			// afterwards that nothing was recorded.
			fmt.Fprintf(stderr, "lookout mcp: %v\n", err)
			return 2
		}
		defer func() { _ = closer.Close() }()
		log = l
	}

	var auth *mcpserver.BearerAuth
	if *authTokenFile != "" {
		a, err := mcpserver.LoadAuthToken(*authTokenFile)
		if err != nil {
			fmt.Fprintf(stderr, "lookout mcp: %v\n", err)
			return 2
		}
		auth = a
	}

	var google *mcpserver.GoogleAuth
	if googleMode {
		g, err := mcpserver.NewGoogleAuth(mcpserver.GoogleOptions{
			ClientIDs:  splitList(*googleClientID),
			Allow:      splitList(*googleAllow),
			AllowWrite: splitList(*googleAllowWrite),
			Resource:   *resourceURL,
			Logger:     slog.New(slog.NewTextHandler(stderr, nil)),
		})
		if err != nil {
			fmt.Fprintf(stderr, "lookout mcp: %v\n", err)
			return 2
		}
		google = g
	}

	var (
		clusters *mcpserver.Clusters
		local    string
	)
	if multiCluster {
		c, code := startClusters(ctx, fleet.Spec{Clusters: *clustersFlag, ClustersFrom: *clustersFrom, FolderSearch: *folderDiscovery},
			googleMode, *rediscover, *defaultCluster, *defaultProject, stderr)
		if c == nil {
			return code
		}
		clusters = c
		local = localClusterName(ctx)
	}

	server := mcpserver.New(reg, version.Semver(),
		mcpserver.WithClusters(clusters),
		mcpserver.WithLocalCluster(local),
		mcpserver.WithMaxConcurrencyPerCluster(*maxPerCluster),
		mcpserver.WithTools(selected),
		mcpserver.WithAccessLog(log),
		mcpserver.WithSelection(*kubeconfig, *contextName),
		mcpserver.WithExemptions(*exemptions),
		mcpserver.WithStore(*store),
		mcpserver.WithReport(*report),
		mcpserver.WithGoogleAuth(google),
	)
	if err := mcpserver.Serve(ctx, server, mcpserver.ServeOptions{
		Listen:           *listen,
		AllowNonLoopback: *allowNonLoopback,
		Auth:             auth,
		Google:           google,
		Announce:         stderr,
		AccessLogPath:    *accessLog,
		Stateless:        multiCluster,
	}); err != nil {
		fmt.Fprintf(stderr, "lookout mcp: %v\n", err)
		return 1
	}
	return 0
}
