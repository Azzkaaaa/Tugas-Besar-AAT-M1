package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/google/uuid"
)

const archiveChunkSize = 64 << 10

var errUncertainProgress = errors.New("ingest response progress could not be established")

type Pipeline struct {
	inbox       storage.IngestInbox
	hazards     storage.HazardStore
	adapt       bool
	maxResponse int64
}

func NewPipeline(inbox storage.IngestInbox, hazards storage.HazardStore, adapt bool, maxResponse int64) *Pipeline {
	return &Pipeline{inbox: inbox, hazards: hazards, adapt: adapt, maxResponse: maxResponse}
}

type incoming struct {
	storage.IngestMessage
	items []json.RawMessage
}

func fetchSource(ctx context.Context, client *http.Client, base, path, header, credential string, since time.Time, corrID string) (*http.Response, error) {
	endpoint := base + path
	if !since.IsZero() {
		endpoint += "?since=" + url.QueryEscape(since.Format(time.RFC3339Nano))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(header, credential)
	req.Header.Set("X-Correlation-ID", corrID)
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		_ = response.Body.Close()
		return nil, fmt.Errorf("%s returned HTTP %d", path, response.StatusCode)
	}
	return response, nil
}

func (p *Pipeline) receive(ctx context.Context, response *http.Response, stream domain.SourceKey, format, corrID string) (incoming, error) {
	defer response.Body.Close()
	message := storage.IngestMessage{Source: domain.SourceOf(stream), Stream: string(stream),
		Format: format, ReceivedAt: time.Now().UTC(), CorrelationID: corrID,
		SchemaVersion: response.Header.Get("X-PVMBG-Schema-Version")}
	prefix, readErr := io.ReadAll(io.LimitReader(response.Body, p.maxResponse+1))
	if int64(len(prefix)) > p.maxResponse {
		message.Reason = "response_too_large"
		if err := p.spool(ctx, message, prefix, response.Body, readErr); err != nil {
			return incoming{}, err
		}
		return incoming{}, errUncertainProgress
	}
	var envelope struct {
		Items []json.RawMessage `json:"items"`
	}
	if readErr != nil || json.Unmarshal(prefix, &envelope) != nil || envelope.Items == nil {
		message.Reason = "malformed_envelope"
		if readErr != nil {
			message.Reason = "incomplete_response"
		}
		if err := p.storeEnvelope(ctx, message, prefix); err != nil {
			return incoming{}, err
		}
		return incoming{}, errUncertainProgress
	}
	return incoming{IngestMessage: message, items: envelope.Items}, nil
}

func messageID(m storage.IngestMessage, raw []byte) string {
	hash := sha256.New()
	_, _ = io.WriteString(hash, m.Stream+"\x00"+m.Format+"\x00"+m.SchemaVersion+"\x00")
	_, _ = hash.Write(raw)
	return shardedMessageID(m, hash.Sum(nil))
}

func shardedMessageID(m storage.IngestMessage, digest []byte) string {
	shard := int(digest[0]) % storage.IngestShardCount
	if m.SourceRefID != "" {
		shard = storage.IngestShard(m.Stream + "\x00" + m.SourceRefID)
	}
	return fmt.Sprintf("msg-%02x%s", shard, hex.EncodeToString(digest[1:]))
}

func (p *Pipeline) storeEnvelope(ctx context.Context, m storage.IngestMessage, raw []byte) error {
	if len(raw) > maxItemBytes {
		return p.spool(ctx, m, raw, nil, nil)
	}
	m.ID = messageID(m, raw)
	m.Format, m.State, m.Attempts, m.Raw = formatEnvelope, storage.IngestParked, 1, raw
	return p.put(ctx, m)
}

// Spool limits memory and hashes before writing chunks. A repeated oversized poll uses
// the same chunk partition; the inbox row is committed after all bytes are durable.
func (p *Pipeline) spool(ctx context.Context, m storage.IngestMessage, prefix []byte, rest io.Reader, readErr error) error {
	file, err := os.CreateTemp("", "ingest-capture-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	hash := sha256.New()
	_, _ = io.WriteString(hash, m.Stream+"\x00"+m.Format+"\x00"+m.SchemaVersion+"\x00")
	out := io.MultiWriter(file, hash)
	if _, err := out.Write(prefix); err != nil {
		return err
	}
	if rest != nil {
		if _, err := io.CopyBuffer(out, rest, make([]byte, archiveChunkSize)); err != nil {
			var pathErr *os.PathError
			if errors.As(err, &pathErr) {
				return err
			}
			readErr = err
		}
	}
	if readErr != nil {
		m.Reason = "incomplete_response"
	}
	digest := hash.Sum(nil)
	m.ID = shardedMessageID(m, digest)
	m.ArchiveID = uuid.NewSHA1(uuid.NameSpaceOID, digest).String()
	m.State, m.Attempts = storage.IngestParked, 1
	prior, err := p.inbox.GetMessage(ctx, m.ID)
	if err != nil || prior != nil {
		return err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	chunk := make([]byte, archiveChunkSize)
	for {
		n, err := io.ReadFull(file, chunk)
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return err
		}
		if n == 0 {
			break
		}
		if err := p.inbox.ArchiveChunk(ctx, m.ArchiveID, m.ArchiveChunks, chunk[:n]); err != nil {
			return err
		}
		m.ArchiveChunks++
	}
	return p.put(ctx, m)
}

func (p *Pipeline) put(ctx context.Context, m storage.IngestMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.inbox.Put(ctx, m); err != nil {
		return err
	}
	if m.State == storage.IngestParked {
		slog.WarnContext(ctx, "upstream message parked", "source", m.Source, "message_id", m.ID,
			"reason", m.Reason, "correlation_id", m.CorrelationID)
	}
	return nil
}

func (p *Pipeline) stage(ctx context.Context, response incoming, companions map[string][]json.RawMessage) (time.Time, error) {
	var latest time.Time
	uncertain := false
	for _, raw := range response.items {
		m := response.IngestMessage
		meta := itemMetadata(raw, m.Format)
		m.SourceRefID = meta.ref
		if m.SourceRefID == "" || (m.Format != formatWarning && meta.at.IsZero()) {
			uncertain = true
		}
		if meta.at.After(latest) {
			latest = meta.at
		}
		m.ID, m.Raw, m.State = messageID(m, raw), raw, storage.IngestPending
		if _, err := validateItem(raw, m.Format, m.SchemaVersion, p.adapt); err != nil {
			m.State, m.Reason, m.Attempts = storage.IngestParked, err.Error(), 1
		}
		if len(raw) > maxItemBytes {
			m.Raw = nil
			if err := p.spool(ctx, m, raw, nil, nil); err != nil {
				return time.Time{}, err
			}
			continue
		}
		if related := companions[meta.ref]; len(related) > 0 {
			companion, _ := json.Marshal(related)
			if len(companion) <= maxItemBytes {
				m.Companion = companion
			}
		}
		if err := p.put(ctx, m); err != nil {
			return time.Time{}, err
		}
	}
	if uncertain {
		return latest, errUncertainProgress
	}
	return latest, nil
}

// process runs under the same source lease as staging. It considers the oldest
// unresolved item per key first. Subsequent heads can run in the same poll.
func (p *Pipeline) process(ctx context.Context, stream domain.SourceKey) (int, error) {
	byKey := make(map[string][]storage.IngestMessage)
	unavailable := make(map[string]bool)
	for cursor := (storage.IngestCursor{}); ; {
		page, err := p.inbox.ListMessages(ctx, domain.SourceOf(stream), cursor, 100)
		if err != nil {
			return 0, err
		}
		for _, unit := range page.UnavailableUnits {
			unavailable[unit] = true
		}
		for _, m := range page.Items {
			if m.Stream == string(stream) && m.SourceRefID != "" {
				byKey[m.SourceRefID] = append(byKey[m.SourceRefID], m)
			}
		}
		if page.Next == nil {
			break
		}
		cursor = *page.Next
	}
	count := 0
	for ref, group := range byKey {
		// An unavailable shard can hide an older item for this key.
		if unavailable[fmt.Sprintf("ingest-inbox-%02d", storage.IngestShard(string(stream)+"\x00"+ref))] {
			continue
		}
		slices.SortFunc(group, func(a, b storage.IngestMessage) int {
			switch {
			case a.Sequence < b.Sequence:
				return -1
			case a.Sequence > b.Sequence:
				return 1
			default:
				return strings.Compare(a.ID, b.ID)
			}
		})
		for i, m := range group {
			if err := ctx.Err(); err != nil {
				return count, err
			}
			if m.State == storage.IngestParked || m.NextAttempt.After(time.Now()) {
				break
			}
			applied, resolved, err := p.processOne(ctx, m, group[i+1:])
			if err != nil {
				return count, err
			}
			if !resolved {
				break
			}
			if applied {
				count++
			}
		}
	}
	if len(unavailable) > 0 {
		return count, errors.New("ingest inbox is partially unavailable")
	}
	return count, nil
}

func (p *Pipeline) processOne(ctx context.Context, m storage.IngestMessage, later []storage.IngestMessage) (bool, bool, error) {
	stored, err := p.inbox.GetMessage(ctx, m.ID)
	if err != nil || stored == nil || stored.State != m.State || stored.Attempts != m.Attempts {
		return false, false, err
	}
	item, err := validateItem(stored.Raw, m.Format, m.SchemaVersion, p.adapt)
	if err != nil {
		_, updateErr := p.inbox.ChangeState(ctx, m, storage.IngestParked, err.Error(), m.Attempts+1, time.Time{})
		return false, false, updateErr
	}
	hazard, err := p.mapItem(ctx, *stored, item, later)
	var changed bool
	if err == nil {
		changed, err = applySequencedRecord(ctx, p.hazards, hazard, m.Sequence, m.CorrelationID)
	}
	if err != nil {
		delay := min(time.Second<<min(m.Attempts, 5), 30*time.Second)
		_, updateErr := p.inbox.ChangeState(ctx, m, storage.IngestRetrying, "temporary_processing_failure", m.Attempts+1, time.Now().Add(delay))
		slog.WarnContext(ctx, "upstream message retry scheduled", "message_id", m.ID, "correlation_id", m.CorrelationID)
		return false, false, updateErr
	}
	resolved, err := p.inbox.ChangeState(ctx, m, storage.IngestResolved, "", m.Attempts+1, time.Time{})
	if resolved {
		slog.InfoContext(ctx, "upstream message processed", "message_id", m.ID, "correlation_id", m.CorrelationID, "sequence", m.Sequence)
	}
	return changed, resolved, err
}

func (p *Pipeline) mapItem(ctx context.Context, m storage.IngestMessage, item any, later []storage.IngestMessage) (*domain.HazardRecord, error) {
	if volcanic, ok := item.(*PVMBGVolcanicRaw); ok {
		return MapPVMBGToHazard(volcanic), nil
	}
	prior, err := p.hazards.Get(ctx, domain.HazardID(domain.SourceBMKG, m.SourceRefID))
	if err != nil {
		return nil, err
	}
	warnings := make(map[string]*BMKGTsunamiWarningRaw)
	if prior != nil {
		b, err := json.Marshal(prior.Attributes["tsunami_warnings"])
		if err != nil {
			return nil, err
		}
		var saved []*BMKGTsunamiWarningRaw
		if err := json.Unmarshal(b, &saved); err != nil {
			return nil, err
		}
		for _, warning := range saved {
			warnings[warning.WarningID] = warning
		}
	}
	var seismic *BMKGSeismicRaw
	switch source := item.(type) {
	case *BMKGSeismicRaw:
		seismic = source
		var companion []*BMKGTsunamiWarningRaw
		if len(m.Companion) != 0 {
			if err := json.Unmarshal(m.Companion, &companion); err != nil {
				return nil, err
			}
		}
		for _, warning := range companion {
			warnings[warning.WarningID] = warning
		}
	case *BMKGTsunamiWarningRaw:
		if prior == nil {
			// A warning can be durably received before its earthquake. Use the
			// first applicable seismic message as context for this earlier revision.
			seismic, err = p.pendingSeismic(ctx, later)
			if err != nil {
				return nil, err
			}
		} else {
			seismic = seismicFromHazard(prior)
		}
		warnings[source.WarningID] = source
	}
	ordered := make([]*BMKGTsunamiWarningRaw, 0, len(warnings))
	for _, warning := range warnings {
		ordered = append(ordered, warning)
	}
	slices.SortFunc(ordered, func(a, b *BMKGTsunamiWarningRaw) int { return strings.Compare(a.WarningID, b.WarningID) })
	return MapBMKGToHazard(seismic, ordered), nil
}

func (p *Pipeline) pendingSeismic(ctx context.Context, later []storage.IngestMessage) (*BMKGSeismicRaw, error) {
	for _, candidate := range later {
		if candidate.Format != formatSeismic {
			continue
		}
		if candidate.State == storage.IngestParked {
			break
		}
		record, err := p.inbox.GetMessage(ctx, candidate.ID)
		if err != nil {
			return nil, err
		}
		if record != nil {
			if item, err := validateItem(record.Raw, formatSeismic, "", false); err == nil {
				return item.(*BMKGSeismicRaw), nil
			}
		}
		break
	}
	return nil, errors.New("related earthquake is unavailable")
}

func seismicFromHazard(prior *domain.HazardRecord) *BMKGSeismicRaw {
	raw := &BMKGSeismicRaw{EventID: prior.SourceRefID, RegionName: prior.AreaName,
		EpicenterLat: prior.Latitude, EpicenterLon: prior.Longitude, OccurredAt: prior.OccurredAt,
		RawFields: make(map[string]any)}
	raw.Magnitude, _ = prior.Attributes["magnitude"].(float64)
	raw.DepthKM, _ = prior.Attributes["depth_km"].(float64)
	raw.PotentialTsunami, _ = prior.Attributes["potential_tsunami"].(bool)
	for key, value := range prior.Attributes {
		if key != "tsunami_warnings" {
			raw.RawFields[key] = value
		}
	}
	return raw
}

// Known-good warnings are attached to their first earthquake revision as well as
// journaled separately so an update to an existing warning can be replayed.
func validWarnings(result incoming) map[string][]json.RawMessage {
	warnings := make(map[string][]json.RawMessage)
	for _, raw := range result.items {
		if item, err := validateItem(raw, formatWarning, "", false); err == nil {
			warning := item.(*BMKGTsunamiWarningRaw)
			warnings[warning.RelatedEventID] = append(warnings[warning.RelatedEventID], raw)
		}
	}
	return warnings
}
