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

package findings

import "testing"

func TestNormalizeName(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
		why  string
	}{
		// The case the whole package exists for.
		{"deployment pod", "payment-backend-7d9f8-x9k2l", "payment-backend",
			"ReplicaSet hash + pod suffix both stripped"},
		{"rescheduled sibling", "payment-backend-7d9f8-q4m7p", "payment-backend",
			"a rescheduled pod must normalize to the same subject as the pod it replaced"},
		{"new replicaset", "payment-backend-9c4bx-t2v6n", "payment-backend",
			"a rollout changes the RS hash; still the same subject"},

		// Partial and absent suffixes.
		{"replicaset itself", "payment-backend-7d9f8", "payment-backend", ""},
		{"bare deployment", "payment-backend", "payment-backend", ""},
		{"single segment", "nginx", "nginx", ""},

		// StatefulSet ordinals must survive: db-0 and db-1 are
		// individually-addressable instances, and collapsing them would
		// hide a single-replica failure inside a healthy set.
		{"statefulset ordinal 0", "db-0", "db-0", ""},
		{"statefulset ordinal 2", "db-2", "db-2",
			"2 is in the alphabet but one character is below the length floor"},
		{"statefulset ordinal 10", "kafka-10", "kafka-10", ""},
		{"statefulset longer", "postgres-primary-0", "postgres-primary-0", ""},

		// Vowels are the load-bearing protection: ordinary name
		// segments are never mistaken for generated suffixes.
		{"vowel-bearing segment", "cert-manager-webhook", "cert-manager-webhook", ""},
		{"vowel-bearing segment 2", "redis-master", "redis-master", ""},
		{"vowel-bearing segment 3", "my-app-frontend", "my-app-frontend", ""},
		{"vowel-bearing segment 4", "kube-state-metrics", "kube-state-metrics", ""},

		// Short vowel-free segments survive the length floor.
		{"short consonant segment", "kube-dns", "kube-dns", ""},
		{"short consonant segment 2", "my-svc", "my-svc", ""},
		{"three chars", "prometheus-k8s", "prometheus-k8s", ""},

		// CronJob stamps normally contain 0/1/3, which are not in the
		// alphabet — documented under-normalization, not a bug.
		{"cronjob job", "backup-28472940", "backup-28472940",
			"unix-minute stamps contain 0/1/3, outside the generated alphabet"},

		// Never strip the whole name.
		{"entirely generated", "x9k2l", "x9k2l", ""},
		{"two generated segments only", "x9k2l-q4m7p", "x9k2l",
			"strips down to one segment but never to nothing"},

		// At most two suffixes, so a deeply-hyphenated real name keeps
		// its identity even if three tail segments look generated.
		{"three generated segments", "svc-b2ndf-x9k2l-q4m7p", "svc-b2ndf",
			"maxGeneratedSuffixes caps the strip at the real chain depth"},

		// Boundaries of the length window.
		{"four chars is too short", "app-bcdf", "app-bcdf", ""},
		{"five chars strips", "app-bcdfg", "app", ""},
		{"ten chars strips", "app-bcdfghjklm"[:len("app-bcdfghjklm")-1], "app", ""},
		{"eleven chars is too long", "app-bcdfghjklmn", "app-bcdfghjklmn", ""},

		{"empty", "", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := NormalizeName(tc.in); got != tc.want {
				t.Errorf("NormalizeName(%q) = %q, want %q%s", tc.in, got, tc.want, note(tc.why))
			}
		})
	}
}

// TestNormalizeNameIsIdempotentBelowTheCap: for every name whose
// generated tail is within the real owner-chain depth — which is every
// name the sentinel actually sees — normalizing twice is normalizing
// once. Callers compose subject keys from RAW names, so this is a
// belt-and-braces guard on the common path, not a load-bearing
// contract.
func TestNormalizeNameIsIdempotentBelowTheCap(t *testing.T) {
	for _, in := range []string{
		"payment-backend-7d9f8-x9k2l", "db-0", "nginx", "x9k2l-q4m7p",
		"cert-manager-webhook", "backup-28472940", "",
	} {
		once := NormalizeName(in)
		if twice := NormalizeName(once); twice != once {
			t.Errorf("NormalizeName not idempotent for %q: %q then %q", in, once, twice)
		}
	}
}

// TestNormalizeNameCapIsNotIdempotent documents the deliberate
// exception, because a reader will otherwise find it and file a bug.
//
// Above maxGeneratedSuffixes, normalizing twice strips more:
//
//	svc-b2ndf-x9k2l-q4m7p → svc-b2ndf → svc
//
// The cap is the correct behavior and idempotence is the wrong
// property to want here. A user's OWN name may legitimately end in a
// hash-shaped segment — a Helm release installed as `myapp-7f8bd`
// produces pods `myapp-7f8bd-<rs>-<pod>`. Capping at the real chain
// depth (Deployment → ReplicaSet hash → pod suffix) normalizes that to
// `myapp-7f8bd`, keeping two releases of the same chart distinct. An
// uncapped strip would fold them both to `myapp` and silently merge
// two releases' findings into one subject — a far worse failure than
// the non-idempotence, and one an operator would never see coming.
//
// Safe in practice because subject keys are composed from raw observed
// names exactly once (see ObservationOf); nothing re-normalizes a
// stored key.
func TestNormalizeNameCapIsNotIdempotent(t *testing.T) {
	const in = "svc-b2ndf-x9k2l-q4m7p"
	once := NormalizeName(in)
	if once != "svc-b2ndf" {
		t.Fatalf("NormalizeName(%q) = %q, want %q", in, once, "svc-b2ndf")
	}
	if twice := NormalizeName(once); twice != "svc" {
		t.Fatalf("NormalizeName(%q) = %q, want %q — if this changed, the cap's "+
			"trade-off was revisited and the doc comment needs to say why", once, twice, "svc")
	}
}

// TestGeneratedAlphabetMatchesKubernetes pins the charset to
// Kubernetes' own. If apimachinery ever changes `alphanums`, this test
// is the reminder that the normalizer's safety argument — no vowels,
// no 0/1/3 — has to be re-made rather than assumed.
func TestGeneratedAlphabetMatchesKubernetes(t *testing.T) {
	const upstream = "bcdfghjklmnpqrstvwxz2456789"
	if generatedAlphabet != upstream {
		t.Fatalf("generatedAlphabet = %q, want apimachinery's %q", generatedAlphabet, upstream)
	}
	for _, vowel := range "aeiou" {
		if _, bad := generatedSet[vowel]; bad {
			t.Errorf("vowel %q is in the generated alphabet — the normalizer's false-positive protection is gone", vowel)
		}
	}
	for _, digit := range "013" {
		if _, bad := generatedSet[digit]; bad {
			t.Errorf("digit %q is in the generated alphabet — StatefulSet ordinals are no longer safe", digit)
		}
	}
}

func note(why string) string {
	if why == "" {
		return ""
	}
	return " — " + why
}
