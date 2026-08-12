package clusterha

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const HashPrefixSHA256 = "sha256:"

type Generation struct {
	ClusterID    string `json:"cluster_id"`
	LeaderEpoch  uint64 `json:"leader_epoch"`
	Sequence     uint64 `json:"sequence"`
	ManifestHash string `json:"manifest_hash"`
}

func (g Generation) Equal(other Generation) bool {
	return g.ClusterID == other.ClusterID && g.LeaderEpoch == other.LeaderEpoch &&
		g.Sequence == other.Sequence && g.ManifestHash == other.ManifestHash
}

func (g Generation) IsZero() bool {
	return g.ClusterID == "" && g.LeaderEpoch == 0 && g.Sequence == 0 && g.ManifestHash == ""
}

func (g Generation) String() string {
	return fmt.Sprintf("%s/%d/%d/%s", g.ClusterID, g.LeaderEpoch, g.Sequence, g.ManifestHash)
}

type Manifest struct {
	SchemaVersion           uint32                `json:"schema_version"`
	Generation              Generation            `json:"generation"`
	VoterConfigurationIndex uint64                `json:"voter_configuration_index"`
	CreatedAt               time.Time             `json:"created_at"`
	Datasets                map[string]DatasetRef `json:"datasets"`
}

type DatasetRef struct {
	Name              string            `json:"name"`
	SchemaVersion     uint32            `json:"schema_version"`
	Scope             string            `json:"scope"`
	BlobHash          string            `json:"blob_hash"`
	Encoding          string            `json:"encoding"`
	RecordCount       int64             `json:"record_count"`
	CollectedAt       time.Time         `json:"collected_at"`
	SourceSuccess     bool              `json:"source_success"`
	SourceError       string            `json:"source_error,omitempty"`
	PreviousBlobHash  string            `json:"previous_blob_hash,omitempty"`
	Required          bool              `json:"required"`
	Stale             bool              `json:"stale"`
	UncompressedBytes int64             `json:"uncompressed_bytes,omitempty"`
	Shards            []DatasetShardRef `json:"shards,omitempty"`
}

type DatasetShardRef struct {
	Key               string `json:"key"`
	BlobHash          string `json:"blob_hash"`
	RecordCount       int64  `json:"record_count"`
	UncompressedBytes int64  `json:"uncompressed_bytes"`
}

func datasetBlobHashes(dataset DatasetRef) []string {
	if len(dataset.Shards) > 0 {
		hashes := make([]string, 0, len(dataset.Shards))
		for _, shard := range dataset.Shards {
			if shard.BlobHash != "" {
				hashes = append(hashes, shard.BlobHash)
			}
		}
		return hashes
	}
	if dataset.BlobHash == "" {
		return nil
	}
	return []string{dataset.BlobHash}
}

func validateManifestCapability(manifest Manifest, gate CapabilityGate) error {
	gate = normalizeCapabilityGate(gate)
	manifestVersion := manifest.SchemaVersion
	if manifestVersion == 0 {
		manifestVersion = LegacyManifestSchemaVersion
	}
	if manifestVersion != gate.ManifestSchemaVersion {
		return fmt.Errorf("manifest schema version %d is not active; active version is %d", manifestVersion, gate.ManifestSchemaVersion)
	}
	for name, dataset := range manifest.Datasets {
		version := dataset.SchemaVersion
		if version == 0 {
			version = LegacyDatasetSchemaVersion
		}
		if wanted := gate.DatasetSchemaVersion(name); version != wanted {
			return fmt.Errorf("dataset %q schema version %d is not active; active version is %d", name, version, wanted)
		}
	}
	return nil
}

// ValidateManifest validates the storage shape of every dataset before any
// snapshot blobs are replicated. Capability compatibility is validated
// separately because it is controlled by the active cluster gate.
func ValidateManifest(manifest Manifest) error {
	for name, dataset := range manifest.Datasets {
		if name == "" {
			return fmt.Errorf("dataset name is empty")
		}
		if dataset.Name != name {
			return fmt.Errorf("dataset %q embeds mismatched name %q", name, dataset.Name)
		}
		if dataset.RecordCount < 0 || dataset.UncompressedBytes < 0 {
			return fmt.Errorf("dataset %q has negative record or byte count", name)
		}
		if dataset.PreviousBlobHash != "" {
			if _, err := normalizeSHA256Hash(dataset.PreviousBlobHash); err != nil {
				return fmt.Errorf("dataset %q previous_blob_hash: %w", name, err)
			}
		}
		hasBlob := dataset.BlobHash != ""
		hasShards := len(dataset.Shards) > 0
		if hasBlob == hasShards {
			return fmt.Errorf("dataset %q must contain exactly one of blob_hash or shards", name)
		}
		if hasBlob {
			if _, err := normalizeSHA256Hash(dataset.BlobHash); err != nil {
				return fmt.Errorf("dataset %q blob_hash: %w", name, err)
			}
			continue
		}
		seen := make(map[string]struct{}, len(dataset.Shards))
		var records, uncompressedBytes int64
		for index, shard := range dataset.Shards {
			if shard.Key == "" {
				return fmt.Errorf("dataset %q shard %d has empty key", name, index)
			}
			if _, exists := seen[shard.Key]; exists {
				return fmt.Errorf("dataset %q contains duplicate shard key %q", name, shard.Key)
			}
			seen[shard.Key] = struct{}{}
			if _, err := normalizeSHA256Hash(shard.BlobHash); err != nil {
				return fmt.Errorf("dataset %q shard %q blob_hash: %w", name, shard.Key, err)
			}
			if shard.RecordCount < 0 || shard.UncompressedBytes < 0 {
				return fmt.Errorf("dataset %q shard %q has negative record or byte count", name, shard.Key)
			}
			records += shard.RecordCount
			uncompressedBytes += shard.UncompressedBytes
		}
		if records != dataset.RecordCount || uncompressedBytes != dataset.UncompressedBytes {
			return fmt.Errorf("dataset %q shard totals records=%d bytes=%d do not match dataset records=%d bytes=%d",
				name, records, uncompressedBytes, dataset.RecordCount, dataset.UncompressedBytes)
		}
	}
	return nil
}

func cloneManifest(manifest Manifest) Manifest {
	copyManifest := manifest
	copyManifest.Datasets = make(map[string]DatasetRef, len(manifest.Datasets))
	for key, value := range manifest.Datasets {
		value.Shards = append([]DatasetShardRef(nil), value.Shards...)
		copyManifest.Datasets[key] = value
	}
	return copyManifest
}

func ComputeManifestHash(manifest Manifest) (string, error) {
	manifest.Generation.ManifestHash = ""
	data, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return HashPrefixSHA256 + hex.EncodeToString(sum[:]), nil
}

type BlobStore struct {
	root    string
	locksMu sync.Mutex
	locks   map[string]*sync.Mutex
}

func NewBlobStore(root string) *BlobStore {
	return &BlobStore{root: root, locks: make(map[string]*sync.Mutex)}
}

func (s *BlobStore) PutBytes(data []byte) (string, error) {
	return s.Put(bytes.NewReader(data))
}

func (s *BlobStore) Put(r io.Reader) (string, error) {
	stagingDir := filepath.Join(s.root, "staging")
	if err := os.MkdirAll(stagingDir, 0755); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(stagingDir, ".blob-*.tmp")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	removeTmp := true
	defer func() {
		if removeTmp {
			_ = os.Remove(tmpName)
		}
	}()

	hasher := sha256.New()
	if _, err := io.Copy(tmp, io.TeeReader(r, hasher)); err != nil {
		_ = tmp.Close()
		return "", err
	}
	hash := hashString(hasher)
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}

	path, err := s.pathFor(hash)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return "", err
	}
	unlock := s.lockHash(hash)
	defer unlock()
	if err := s.Verify(hash); err == nil {
		return hash, nil
	} else if !os.IsNotExist(err) && fileExists(path) {
		if err := s.quarantine(path, hash); err != nil {
			return "", fmt.Errorf("quarantine invalid blob %s: %w", hash, err)
		}
	}

	if err := os.Rename(tmpName, path); err != nil {
		return "", err
	}
	removeTmp = false
	if err := syncDir(filepath.Dir(path)); err != nil {
		return "", err
	}
	if err := syncDir(stagingDir); err != nil {
		return "", err
	}
	return hash, nil
}

func (s *BlobStore) quarantine(path, hash string) error {
	quarantineDir := filepath.Join(s.root, "quarantine")
	if err := os.MkdirAll(quarantineDir, 0755); err != nil {
		return err
	}
	hexHash, err := normalizeSHA256Hash(hash)
	if err != nil {
		return err
	}
	destination := filepath.Join(quarantineDir, fmt.Sprintf("%s-%d", hexHash, time.Now().UnixNano()))
	if err := os.Rename(path, destination); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if err := syncDir(quarantineDir); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func (s *BlobStore) lockHash(hash string) func() {
	s.locksMu.Lock()
	lock := s.locks[hash]
	if lock == nil {
		lock = &sync.Mutex{}
		s.locks[hash] = lock
	}
	s.locksMu.Unlock()
	lock.Lock()
	return lock.Unlock
}

func (s *BlobStore) Verify(hash string) error {
	path, err := s.pathFor(hash)
	if err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return err
	}
	actual := hashString(hasher)
	if actual != hash {
		return fmt.Errorf("blob checksum mismatch: expected %s got %s", hash, actual)
	}
	return nil
}

func (s *BlobStore) Open(hash string) (*os.File, error) {
	if err := s.Verify(hash); err != nil {
		return nil, err
	}
	path, err := s.pathFor(hash)
	if err != nil {
		return nil, err
	}
	return os.Open(path)
}

func (s *BlobStore) PutHash(hash string, r io.Reader) error {
	expected, err := normalizeSHA256Hash(hash)
	if err != nil {
		return err
	}
	stagingDir := filepath.Join(s.root, "staging")
	if err := os.MkdirAll(stagingDir, 0755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(stagingDir, ".blob-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	removeTmp := true
	defer func() {
		if removeTmp {
			_ = os.Remove(tmpName)
		}
	}()
	hasher := sha256.New()
	if _, err := io.Copy(tmp, io.TeeReader(r, hasher)); err != nil {
		_ = tmp.Close()
		return err
	}
	actual := hex.EncodeToString(hasher.Sum(nil))
	if actual != expected {
		_ = tmp.Close()
		return fmt.Errorf("blob checksum mismatch: expected %s got %s", hash, HashPrefixSHA256+actual)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	path, err := s.pathFor(hash)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	unlock := s.lockHash(hash)
	defer unlock()
	if err := s.Verify(hash); err == nil {
		return nil
	} else if !os.IsNotExist(err) && fileExists(path) {
		if err := s.quarantine(path, hash); err != nil {
			return fmt.Errorf("quarantine invalid blob %s: %w", hash, err)
		}
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	removeTmp = false
	if err := syncDir(filepath.Dir(path)); err != nil {
		return err
	}
	return syncDir(stagingDir)
}

func (s *BlobStore) Read(hash string) ([]byte, error) {
	f, err := s.Open(hash)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

func (s *BlobStore) Has(hash string) bool {
	return s.Verify(hash) == nil
}

// GC removes verified CAS blobs that are not present in the retained set.
// Invalid files are quarantined so a later synchronization can repair them.
func (s *BlobStore) GC(retained map[string]struct{}) (int, error) {
	root := filepath.Join(s.root, "blobs", "sha256")
	removed := 0
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return nil
			}
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		hexHash := strings.ReplaceAll(relative, string(filepath.Separator), "")
		if len(hexHash) != sha256.Size*2 {
			return nil
		}
		hash := HashPrefixSHA256 + hexHash
		if _, ok := retained[hash]; ok {
			return nil
		}
		unlock := s.lockHash(hash)
		defer unlock()
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		removed++
		return nil
	})
	return removed, err
}

func (s *BlobStore) pathFor(hash string) (string, error) {
	hexHash, err := normalizeSHA256Hash(hash)
	if err != nil {
		return "", err
	}
	return filepath.Join(s.root, "blobs", "sha256", hexHash[:2], hexHash[2:]), nil
}

func normalizeSHA256Hash(hash string) (string, error) {
	hexHash := strings.TrimPrefix(hash, HashPrefixSHA256)
	if len(hexHash) != sha256.Size*2 {
		return "", fmt.Errorf("invalid sha256 hash length for %q", hash)
	}
	if _, err := hex.DecodeString(hexHash); err != nil {
		return "", fmt.Errorf("invalid sha256 hash %q: %w", hash, err)
	}
	return hexHash, nil
}

func hashString(h hash.Hash) string {
	return HashPrefixSHA256 + hex.EncodeToString(h.Sum(nil))
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
