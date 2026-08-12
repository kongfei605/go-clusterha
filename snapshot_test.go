package clusterha

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBlobStorePutReadHas(t *testing.T) {
	store := NewBlobStore(t.TempDir())
	hash, err := store.PutBytes([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(hash, HashPrefixSHA256) {
		t.Fatalf("hash=%q", hash)
	}
	if !store.Has(hash) {
		t.Fatalf("expected store to have %s", hash)
	}
	data, err := store.Read(hash)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Fatalf("data=%q", string(data))
	}
	again, err := store.PutBytes([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if again != hash {
		t.Fatalf("repeat hash=%q want %q", again, hash)
	}
}

func TestBlobStorePutReader(t *testing.T) {
	store := NewBlobStore(t.TempDir())
	hash, err := store.Put(strings.NewReader("streamed"))
	if err != nil {
		t.Fatal(err)
	}
	data, err := store.Read(hash)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "streamed" {
		t.Fatalf("data=%q", string(data))
	}
}

func TestBlobStoreHasVerifiesChecksum(t *testing.T) {
	store := NewBlobStore(t.TempDir())
	hash, err := store.PutBytes([]byte("valid"))
	if err != nil {
		t.Fatal(err)
	}
	path, err := store.pathFor(hash)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("corrupt"), 0644); err != nil {
		t.Fatal(err)
	}
	if store.Has(hash) {
		t.Fatal("Has must return false for a corrupt blob")
	}
	if err := store.Verify(hash); err == nil {
		t.Fatal("expected Verify to reject corrupt blob")
	}
}

func TestBlobStorePutRepairsCorruptBlob(t *testing.T) {
	root := t.TempDir()
	store := NewBlobStore(root)
	hash, err := store.PutBytes([]byte("valid"))
	if err != nil {
		t.Fatal(err)
	}
	path, err := store.pathFor(hash)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("corrupt"), 0644); err != nil {
		t.Fatal(err)
	}
	if repaired, err := store.PutBytes([]byte("valid")); err != nil || repaired != hash {
		t.Fatalf("repair hash=%q err=%v", repaired, err)
	}
	if err := store.Verify(hash); err != nil {
		t.Fatalf("repaired blob is invalid: %v", err)
	}
	quarantined, err := filepath.Glob(filepath.Join(root, "quarantine", "*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(quarantined) != 1 {
		t.Fatalf("quarantined files=%d want 1", len(quarantined))
	}
}

func TestBlobStoreConcurrentRepair(t *testing.T) {
	store := NewBlobStore(t.TempDir())
	hash, err := store.PutBytes([]byte("valid"))
	if err != nil {
		t.Fatal(err)
	}
	path, _ := store.pathFor(hash)
	if err := os.WriteFile(path, []byte("corrupt"), 0644); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := store.PutBytes([]byte("valid"))
			if err != nil {
				errs <- err
				return
			}
			if got != hash {
				errs <- fmt.Errorf("hash=%q want %q", got, hash)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if err := store.Verify(hash); err != nil {
		t.Fatal(err)
	}
}

func TestBlobStoreRejectsInvalidHash(t *testing.T) {
	store := NewBlobStore(t.TempDir())
	if store.Has("../bad") {
		t.Fatal("invalid hash must not map to a blob")
	}
	if _, err := store.Read("../bad"); err == nil {
		t.Fatal("expected invalid hash read error")
	}
}

func TestBlobStoreGCRetainsReferencedHashes(t *testing.T) {
	store := NewBlobStore(t.TempDir())
	keep, err := store.PutBytes([]byte("keep"))
	if err != nil {
		t.Fatal(err)
	}
	remove, err := store.PutBytes([]byte("remove"))
	if err != nil {
		t.Fatal(err)
	}
	count, err := store.GC(map[string]struct{}{keep: {}})
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || !store.Has(keep) || store.Has(remove) {
		t.Fatalf("count=%d keep=%t remove=%t", count, store.Has(keep), store.Has(remove))
	}
}

func TestComputeManifestHashIgnoresExistingManifestHash(t *testing.T) {
	manifest := Manifest{
		SchemaVersion:           1,
		VoterConfigurationIndex: 7,
		CreatedAt:               time.Unix(100, 0).UTC(),
		Generation: Generation{
			ClusterID:    "meraki-prod",
			LeaderEpoch:  2,
			Sequence:     3,
			ManifestHash: "sha256:old",
		},
		Datasets: map[string]DatasetRef{
			"wan": {
				Name:          "wan",
				Scope:         "global",
				BlobHash:      HashPrefixSHA256 + strings.Repeat("a", 64),
				Encoding:      "json",
				RecordCount:   1,
				CollectedAt:   time.Unix(99, 0).UTC(),
				SourceSuccess: true,
			},
		},
	}
	first, err := ComputeManifestHash(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Generation.ManifestHash = "sha256:different"
	second, err := ComputeManifestHash(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("manifest hash changed: %q != %q", first, second)
	}
}

func TestManifestCapabilityTreatsZeroSchemaAsLegacyV1(t *testing.T) {
	manifest := Manifest{Datasets: map[string]DatasetRef{"wan": {Name: "wan"}}}
	if err := validateManifestCapability(manifest, LegacyCapabilityGate()); err != nil {
		t.Fatalf("legacy zero schema rejected: %v", err)
	}
	manifest.Datasets["wan"] = DatasetRef{Name: "wan", SchemaVersion: 2}
	if err := validateManifestCapability(manifest, LegacyCapabilityGate()); err == nil {
		t.Fatal("inactive dataset schema was accepted")
	}
}

func TestCloneManifestCopiesShardMetadata(t *testing.T) {
	manifest := Manifest{Datasets: map[string]DatasetRef{"metrics/wan": {Shards: []DatasetShardRef{{Key: "org-a", BlobHash: "sha256:a"}}}}}
	cloned := cloneManifest(manifest)
	ref := cloned.Datasets["metrics/wan"]
	ref.Shards[0].Key = "changed"
	cloned.Datasets["metrics/wan"] = ref
	if manifest.Datasets["metrics/wan"].Shards[0].Key != "org-a" {
		t.Fatal("cloneManifest aliased shard metadata")
	}
	if hashes := datasetBlobHashes(manifest.Datasets["metrics/wan"]); len(hashes) != 1 || hashes[0] != "sha256:a" {
		t.Fatalf("dataset shard hashes=%v", hashes)
	}
}
