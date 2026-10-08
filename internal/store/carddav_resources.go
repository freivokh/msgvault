package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.kenn.io/msgvault/internal/vcard"
)

type CardDAVMappingStatus string

const (
	CardDAVMappingMapped    CardDAVMappingStatus = "mapped"
	CardDAVMappingUnbound   CardDAVMappingStatus = "unbound"
	CardDAVMappingAmbiguous CardDAVMappingStatus = "ambiguous"
)

type CardDAVGovernance string

const (
	CardDAVGovernanceRemote CardDAVGovernance = "remote"
	CardDAVGovernanceLocal  CardDAVGovernance = "local"
	CardDAVGovernanceNone   CardDAVGovernance = "none"
)

var (
	ErrCardDAVResourceNotFound = errors.New("CardDAV resource not found")
	ErrCardDAVStalePlan        = errors.New("CardDAV sync plan is stale")
	ErrCardDAVInvalidPlan      = errors.New("invalid CardDAV sync plan")
)

type CardDAVResource struct {
	ID                   int64
	AddressBookID        int64
	Href                 string
	RemoteUID            string
	RemoteETag           string
	RemoteBody           []byte
	RemoteSemanticHash   string
	LocalHash            string
	MappingStatus        CardDAVMappingStatus
	MappingRevision      int64
	Governance           CardDAVGovernance
	PersonID             *int64
	PersonRevisionAtBind *int64
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// CardDAVRemoteResource is one complete network result. Contact values are
// already decoded from the same body and are used only for safe person
// binding; RemoteBody remains the authoritative lossless payload.
type CardDAVRemoteResource struct {
	Href                  string
	PreviousHref          string
	RemoteUID             string
	RemoteETag            string
	RemoteBody            []byte
	SemanticHash          string
	DisplayName           string
	DisplayNameIdentity   VCardIdentity
	Emails                []string
	EmailIdentities       []VCardIdentity
	Phones                []string
	PhoneIdentities       []VCardIdentity
	DisplayNameOccurrence vcard.PropertyIdentity
	EmailOccurrences      []vcard.PropertyIdentity
	PhoneOccurrences      []vcard.PropertyIdentity
	// EquivalentLocalHash is sync-plan evidence that the current local
	// projection and this remote body have the same CardDAV semantic hash.
	EquivalentLocalHash string
}

type CardDAVSyncPlan struct {
	AddressBookID          int64
	ConnectionGeneration   int64
	SyncRevision           int64
	ReplaceAll             bool
	NextSyncToken          string
	Upserts                []CardDAVRemoteResource
	RemovedHrefs           []string
	Conflicts              []CardDAVConflictCapture
	CompletesFullReconcile bool
}

type CardDAVApplyResult struct {
	Created int
	Updated int
	Removed int
}

// ApplyCardDAVSyncPlanContext applies a fully fetched network plan in one
// transaction. Both account and book fences are checked before ledger or
// person state is read, and the network client is intentionally absent from
// this API.
func (s *Store) ApplyCardDAVSyncPlanContext(
	ctx context.Context, plan CardDAVSyncPlan,
) (*CardDAVApplyResult, error) {
	if err := validateCardDAVSyncPlan(plan); err != nil {
		return nil, err
	}
	result := &CardDAVApplyResult{}
	err := s.withTxContext(ctx, func(tx *loggedTx) error {
		var generation int64
		if err := tx.QueryRowContext(ctx, `SELECT connection_generation
			FROM carddav_accounts WHERE id = (SELECT account_id FROM carddav_address_books WHERE id = ?)`+s.dialect.SelectForUpdate(), plan.AddressBookID).Scan(&generation); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrCardDAVStalePlan
			}
			return fmt.Errorf("lock CardDAV account sync fence: %w", err)
		}
		if generation != plan.ConnectionGeneration {
			return ErrCardDAVStalePlan
		}
		var book CardDAVAddressBook
		if err := tx.QueryRowContext(ctx, `SELECT id, account_id, canonical_url,
			is_subscribed, is_lookup_source, sync_token, sync_revision
			FROM carddav_address_books WHERE id = ?`+s.dialect.SelectForUpdate(),
			plan.AddressBookID,
		).Scan(&book.ID, &book.AccountID, &book.CanonicalURL,
			&book.IsSubscribed, &book.IsLookupSource, &book.SyncToken, &book.SyncRevision); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrCardDAVStalePlan
			}
			return fmt.Errorf("lock CardDAV address book sync fence: %w", err)
		}
		if book.SyncRevision != plan.SyncRevision ||
			(!book.IsSubscribed && !book.IsLookupSource) {
			return ErrCardDAVStalePlan
		}
		if err := s.lockIdentityMutationTxContext(ctx, tx); err != nil {
			return err
		}

		seen := make(map[string]bool, len(plan.Upserts))
		conflicts := make(map[string]CardDAVConflictCapture, len(plan.Conflicts))
		for _, capture := range plan.Conflicts {
			conflicts[capture.Href] = capture
		}
		for _, input := range plan.Upserts {
			var moveHashes cardDAVHrefMoveHashes
			if input.PreviousHref != "" {
				var moveErr error
				moveHashes, moveErr = s.moveCardDAVResourceHrefTx(ctx, tx, book.ID, input)
				if moveErr != nil {
					return moveErr
				}
				if input.EquivalentLocalHash != "" && moveHashes.hasPerson {
					if input.EquivalentLocalHash != moveHashes.currentBefore {
						return ErrCardDAVStalePlan
					}
					input.EquivalentLocalHash = moveHashes.currentAfter
				}
			}
			seen[input.Href] = true
			capture, supplied := conflicts[input.Href]
			if supplied && input.PreviousHref != "" && moveHashes.hasPerson {
				if capture.BaseLocalHash != moveHashes.baseBefore {
					return ErrCardDAVStalePlan
				}
				capture.BaseLocalHash = moveHashes.baseAfter
				if capture.LocalTombstone {
					if capture.LocalHash != moveHashes.baseBefore {
						return ErrCardDAVStalePlan
					}
					capture.LocalHash = moveHashes.baseAfter
				} else {
					if capture.LocalHash != moveHashes.currentBefore {
						return ErrCardDAVStalePlan
					}
					capture.LocalHash = moveHashes.currentAfter
				}
			}
			needsConflict, err := s.cardDAVResourceNeedsConflictTx(ctx, tx, book.ID, input.Href,
				input.SemanticHash, input.EquivalentLocalHash, input.RemoteBody)
			if err != nil {
				return err
			}
			if supplied != needsConflict {
				return ErrCardDAVStalePlan
			}
			if supplied {
				if capture.AddressBookID != book.ID || capture.RemoteTombstone ||
					capture.RemoteETag != input.RemoteETag || !bytes.Equal(capture.RemoteBody, input.RemoteBody) {
					return ErrCardDAVInvalidPlan
				}
				if _, err := s.recordCardDAVConflictTx(ctx, tx, capture, cardDAVConflictRecordOptions{
					supersedePendingIntent:         true,
					preserveExistingLocalTombstone: true,
				}); err != nil {
					return err
				}
				delete(conflicts, input.Href)
				continue
			}
			created, changed, err := s.applyCardDAVResourceTx(
				ctx, tx, book, input, plan.CompletesFullReconcile,
			)
			if err != nil {
				return err
			}
			if created {
				result.Created++
			} else if changed {
				result.Updated++
			}
		}

		removed := make(map[string]bool, len(plan.RemovedHrefs))
		for _, href := range plan.RemovedHrefs {
			removed[href] = true
		}
		if plan.ReplaceAll {
			rows, err := tx.QueryContext(ctx, `SELECT href FROM carddav_resources
				WHERE address_book_id = ? ORDER BY href`, book.ID)
			if err != nil {
				return fmt.Errorf("list CardDAV snapshot tombstones: %w", err)
			}
			for rows.Next() {
				var href string
				if err := rows.Scan(&href); err != nil {
					_ = rows.Close()
					return fmt.Errorf("scan CardDAV snapshot tombstone: %w", err)
				}
				if !seen[href] {
					removed[href] = true
				}
			}
			if err := rows.Close(); err != nil {
				return fmt.Errorf("close CardDAV snapshot tombstones: %w", err)
			}
		}
		for href := range removed {
			capture, supplied := conflicts[href]
			needsConflict, err := s.cardDAVResourceNeedsConflictTx(ctx, tx, book.ID, href, "", "", nil)
			if err != nil {
				return err
			}
			if supplied != needsConflict {
				return ErrCardDAVStalePlan
			}
			if supplied {
				if capture.AddressBookID != book.ID || !capture.RemoteTombstone {
					return ErrCardDAVInvalidPlan
				}
				completed, err := s.completePendingCardDAVConflictTombstoneFromPullTx(
					ctx, tx, book, generation, capture)
				if err != nil {
					return err
				}
				if completed {
					result.Removed++
					delete(conflicts, href)
					continue
				}
				if _, err := s.recordCardDAVConflictTx(ctx, tx, capture, cardDAVConflictRecordOptions{}); err != nil {
					return err
				}
				delete(conflicts, href)
				continue
			}
			resource, err := s.findCardDAVResourceTx(ctx, tx, book.ID, href)
			if err != nil && !errors.Is(err, ErrCardDAVResourceNotFound) {
				return err
			}
			wasRemoved, err := s.removeCardDAVResourceTx(ctx, tx, book.ID, href)
			if err != nil {
				return err
			}
			if wasRemoved {
				result.Removed++
				if resource.PersonID != nil {
					// A remote-only delete of a settled publication is accepted like keep_remote.
					if err := s.cancelCardDAVPublicationForRemoteDeleteTx(
						ctx, tx, *resource.PersonID, book.ID, href, true,
					); err != nil {
						return err
					}
				}
			}
		}
		if len(conflicts) != 0 {
			return ErrCardDAVInvalidPlan
		}

		updated, err := tx.ExecContext(ctx, `UPDATE carddav_address_books SET
			sync_token = ?, sync_revision = sync_revision + 1,
			needs_full_reconcile = CASE WHEN ? THEN FALSE ELSE needs_full_reconcile END,
			updated_at = `+s.dialect.Now()+`
			WHERE id = ? AND sync_revision = ?`,
			plan.NextSyncToken, plan.CompletesFullReconcile, book.ID, plan.SyncRevision)
		if err != nil {
			return fmt.Errorf("advance CardDAV address book sync fence: %w", err)
		}
		affected, err := updated.RowsAffected()
		if err != nil {
			return fmt.Errorf("count CardDAV address book fence update: %w", err)
		}
		if affected != 1 {
			return ErrCardDAVStalePlan
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func validateCardDAVSyncPlan(plan CardDAVSyncPlan) error {
	if plan.AddressBookID <= 0 || plan.ConnectionGeneration <= 0 || plan.SyncRevision <= 0 {
		return ErrCardDAVInvalidPlan
	}
	if plan.CompletesFullReconcile && !plan.ReplaceAll {
		return ErrCardDAVInvalidPlan
	}
	seen := make(map[string]bool, len(plan.Upserts))
	for _, resource := range plan.Upserts {
		if strings.TrimSpace(resource.Href) == "" || strings.TrimSpace(resource.RemoteETag) == "" ||
			len(resource.RemoteBody) == 0 || strings.TrimSpace(resource.SemanticHash) == "" {
			return ErrCardDAVInvalidPlan
		}
		if seen[resource.Href] {
			return fmt.Errorf("%w: duplicate href", ErrCardDAVInvalidPlan)
		}
		if resource.PreviousHref != "" && (resource.PreviousHref == resource.Href ||
			strings.TrimSpace(resource.RemoteUID) == "") {
			return ErrCardDAVInvalidPlan
		}
		seen[resource.Href] = true
	}
	for _, href := range plan.RemovedHrefs {
		if strings.TrimSpace(href) == "" || seen[href] {
			return fmt.Errorf("%w: contradictory removal", ErrCardDAVInvalidPlan)
		}
	}
	conflicts := make(map[string]bool, len(plan.Conflicts))
	for _, capture := range plan.Conflicts {
		if err := validateCardDAVConflictCapture(capture); err != nil {
			return err
		}
		if capture.AddressBookID != plan.AddressBookID || conflicts[capture.Href] {
			return ErrCardDAVInvalidPlan
		}
		conflicts[capture.Href] = true
	}
	return nil
}

type cardDAVHrefMoveHashes struct {
	hasPerson     bool
	baseBefore    string
	baseAfter     string
	currentBefore string
	currentAfter  string
}

func (s *Store) moveCardDAVResourceHrefTx(
	ctx context.Context, tx *loggedTx, bookID int64, input CardDAVRemoteResource,
) (cardDAVHrefMoveHashes, error) {
	hashes := cardDAVHrefMoveHashes{}
	resource, err := s.findCardDAVResourceTx(ctx, tx, bookID, input.PreviousHref)
	if err != nil || resource.RemoteUID != input.RemoteUID {
		return hashes, ErrCardDAVStalePlan
	}
	var pendingConflict bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM carddav_conflicts WHERE address_book_id=? AND href=? AND status='unresolved' AND local_mutation_intent IS NOT NULL)`, bookID, input.PreviousHref).Scan(&pendingConflict); err != nil {
		return hashes, err
	}
	if pendingConflict {
		return hashes, ErrCardDAVPublicationPending
	}
	if _, err := s.findCardDAVResourceTx(ctx, tx, bookID, input.Href); err == nil {
		return hashes, ErrCardDAVStalePlan
	} else if !errors.Is(err, ErrCardDAVResourceNotFound) {
		return hashes, err
	}
	if resource.PersonID != nil {
		snapshot, err := s.loadPersonVCardSnapshotTx(ctx, tx, *resource.PersonID)
		if err != nil {
			return hashes, err
		}
		hashes = cardDAVHrefMoveHashes{
			hasPerson: true, baseBefore: resource.LocalHash, baseAfter: resource.LocalHash,
			currentBefore: snapshot.Fingerprint, currentAfter: snapshot.Fingerprint,
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE carddav_resources SET href = ?, updated_at = `+
		s.dialect.Now()+` WHERE id = ? AND href = ?`, input.Href, resource.ID, input.PreviousHref)
	if err != nil {
		return hashes, fmt.Errorf("move CardDAV resource href: %w", err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return hashes, ErrCardDAVStalePlan
	}
	sourceRef := fmt.Sprintf("carddav:%d", bookID)
	if err := s.rewriteVCardSourceResourceProvenanceTx(
		ctx, tx, sourceRef, input.PreviousHref, input.Href,
	); err != nil {
		return hashes, err
	}
	if resource.PersonID != nil {
		snapshot, err := s.loadPersonVCardSnapshotTx(ctx, tx, *resource.PersonID)
		if err != nil {
			return hashes, err
		}
		hashes.currentAfter = snapshot.Fingerprint
		if hashes.currentBefore == hashes.baseBefore {
			hashes.baseAfter = snapshot.Fingerprint
			if _, err := tx.ExecContext(ctx, `UPDATE carddav_resources
				SET local_hash = ?, updated_at = `+s.dialect.Now()+` WHERE id = ?`,
				snapshot.Fingerprint, resource.ID); err != nil {
				return hashes, fmt.Errorf("rebase moved CardDAV resource local hash: %w", err)
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE vcard_resource_envelopes SET
		source_resource_uid = ?, href = ?, revision = revision + 1,
		updated_at = `+s.dialect.Now()+`
		WHERE source_ref = ? AND source_resource_uid = ?`,
		input.Href, input.Href, sourceRef, input.PreviousHref); err != nil {
		return hashes, fmt.Errorf("move CardDAV resource envelope href: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE carddav_publications SET href = ?, updated_at = `+
		s.dialect.Now()+` WHERE address_book_id = ? AND href = ?`,
		input.Href, bookID, input.PreviousHref); err != nil {
		return hashes, fmt.Errorf("move CardDAV publication href: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE carddav_conflicts SET review_revision = review_revision + 1, approved_local_body_sha256 = NULL, approved_local_inference_revision = NULL, approved_conflict_revision = NULL, local_envelope_metadata = NULL, href = ?, updated_at = `+
		s.dialect.Now()+` WHERE address_book_id = ? AND href = ?`,
		input.Href, bookID, input.PreviousHref); err != nil {
		return hashes, fmt.Errorf("move CardDAV conflict href: %w", err)
	}
	return hashes, nil
}

func (s *Store) cardDAVResourceNeedsConflictTx(
	ctx context.Context, tx *loggedTx, bookID int64, href, remoteHash, equivalentLocalHash string,
	remoteBody []byte,
) (bool, error) {
	resource, err := s.findCardDAVResourceTx(ctx, tx, bookID, href)
	if errors.Is(err, ErrCardDAVResourceNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var unresolved int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM carddav_conflicts
		WHERE address_book_id = ? AND href = ? AND status = 'unresolved'`, bookID, href).Scan(&unresolved); err != nil {
		return false, fmt.Errorf("check unresolved CardDAV mapping conflict: %w", err)
	}
	if unresolved > 0 {
		return true, nil
	}
	if resource.MappingStatus != CardDAVMappingMapped {
		return false, nil
	}
	localChanged := resource.PersonID == nil
	localHash := ""
	var snapshot *PersonVCardSnapshot
	if resource.PersonID != nil {
		snapshot, err = s.loadPersonVCardSnapshotTx(ctx, tx, *resource.PersonID)
		if err != nil {
			return false, err
		}
		localHash = snapshot.Fingerprint
		localChanged = localHash != resource.LocalHash
	}
	remoteChanged := remoteHash == "" || remoteHash != resource.RemoteSemanticHash
	if !localChanged && remoteChanged && remoteHash != "" && resource.PersonID != nil {
		return s.cardDAVPublishedRemoteNeedsConflictTx(ctx, tx, bookID, resource, remoteBody, snapshot)
	}
	if localChanged && remoteChanged {
		if resource.PersonID == nil && remoteHash == "" {
			return false, nil
		}
		if equivalentLocalHash != "" && localHash == equivalentLocalHash {
			return false, nil
		}
	}
	return localChanged && remoteChanged, nil
}

// CardDAVPublishedRemoteNeedsConflictContext reports remote edits a settled, locally unchanged publication cannot import.
func (s *Store) CardDAVPublishedRemoteNeedsConflictContext(ctx context.Context, bookID int64, href string, remoteBody []byte) (bool, error) {
	var conflict bool
	err := s.withTxContext(ctx, func(tx *loggedTx) error {
		resource, err := s.findCardDAVResourceTx(ctx, tx, bookID, href)
		if errors.Is(err, ErrCardDAVResourceNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		conflict, err = s.cardDAVPublishedRemoteNeedsConflictTx(ctx, tx, bookID, resource, remoteBody, nil)
		return err
	})
	return conflict, err
}

func (s *Store) cardDAVPublishedRemoteNeedsConflictTx(ctx context.Context, tx *loggedTx, bookID int64, resource *CardDAVResource, remoteBody []byte, snapshot *PersonVCardSnapshot) (bool, error) {
	if resource.MappingStatus != CardDAVMappingMapped || resource.PersonID == nil {
		return false, nil
	}
	published, err := s.cardDAVPublishedPersonUnchangedLocallyTx(ctx, tx, bookID, *resource.PersonID, resource, snapshot)
	if err != nil || !published {
		return false, err
	}
	stored, err := s.findVCardResourceEnvelopeTx(ctx, tx, fmt.Sprintf("carddav:%d", bookID), resource.Href)
	if errors.Is(err, ErrVCardResourceNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	incoming, err := vcard.ParseResourceEnvelope(remoteBody)
	if err != nil {
		return false, err
	}
	owners, err := cardDAVRebindOwners(stored.ResourceEnvelope, incoming)
	if err != nil {
		return false, err
	}
	if cardDAVContactValuesNeedConflict(stored.ResourceEnvelope, incoming) {
		return true, nil
	}
	for _, occurrence := range incoming.PropertyTree {
		if !strings.EqualFold(occurrence.Property.Name, "FN") || slices.ContainsFunc(stored.PropertyTree, func(prior vcard.PropertyOccurrence) bool {
			return vcard.CompareSemanticProperties(
				vcard.NormalizeSemanticProperty(stored.RenderMetadata.StoredVersion, prior.Property),
				vcard.NormalizeSemanticProperty(incoming.RenderMetadata.StoredVersion, occurrence.Property),
			) == 0
		}) {
			continue
		}
		if len(occurrence.Property.Parameters) > 0 {
			return true, nil
		}
		names, err := s.listPersonNamesTx(ctx, tx, *resource.PersonID, true)
		if err != nil {
			return false, err
		}
		for _, name := range names {
			if name.NameKind == PersonNameStructured {
				return true, nil
			}
		}
	}
	for _, owner := range owners {
		if owner.kept {
			continue
		}
		mapping := owner.mapping
		switch owner.property.Name {
		case "EMAIL", "TEL":
			if mapping.Table == personContactPointsTableName {
				point, err := getPersonContactPointTx(ctx, tx, *resource.PersonID, mapping.RowID)
				if errors.Is(err, ErrProfileValueNotFound) {
					continue
				}
				if err != nil {
					return false, err
				}
				if point.Envelope.IsCurrent() && !cardDAVOwnerIsReplaceable(owner, point.Envelope) {
					return true, nil
				}
				continue
			}
		case "FN":
			if cardDAVOwnerIsReplaceable(owner, ValueEnvelope{}) {
				continue
			}
		}
		return true, nil
	}
	return false, nil
}

func cardDAVContactValuesNeedConflict(stored, incoming vcard.ResourceEnvelope) bool {
	prior := make([]vcard.SemanticProperty, 0)
	changed := make([]vcard.SemanticProperty, 0)
	for _, occurrence := range stored.PropertyTree {
		property := vcard.NormalizeSemanticProperty(stored.RenderMetadata.StoredVersion, occurrence.Property)
		if property.Name == "EMAIL" || property.Name == "TEL" {
			prior = append(prior, property)
		}
	}
	for _, occurrence := range incoming.PropertyTree {
		property := vcard.NormalizeSemanticProperty(incoming.RenderMetadata.StoredVersion, occurrence.Property)
		if property.Name != "EMAIL" && property.Name != "TEL" {
			continue
		}
		index := slices.IndexFunc(prior, func(old vcard.SemanticProperty) bool {
			return vcard.CompareSemanticProperties(old, property) == 0
		})
		if index >= 0 {
			prior = slices.Delete(prior, index, index+1)
		} else {
			changed = append(changed, property)
		}
	}
	for _, property := range changed {
		if strings.TrimSpace(property.RawValue) == "" || property.RawValue == `\n` {
			continue
		}
		if !cardDAVContactValueIsPlain(property) {
			return true
		}
		index := slices.IndexFunc(prior, func(old vcard.SemanticProperty) bool {
			old.RawValue = property.RawValue
			return vcard.CompareSemanticProperties(old, property) == 0
		})
		if index >= 0 {
			if !cardDAVContactValueIsPlain(prior[index]) {
				return true
			}
			prior = slices.Delete(prior, index, index+1)
		} else if len(property.Parameters) > 0 || slices.ContainsFunc(prior, func(old vcard.SemanticProperty) bool {
			return old.Name == property.Name && old.Group == property.Group
		}) {
			return true
		}
	}
	return slices.ContainsFunc(prior, func(property vcard.SemanticProperty) bool {
		return !cardDAVContactValueIsPlain(property)
	})
}

func cardDAVContactValue(kind ContactAddressKind, value string) string {
	prefix := "mailto:"
	if kind == ContactAddressPhone {
		prefix = "tel:"
	}
	if len(value) >= len(prefix) && strings.EqualFold(value[:len(prefix)], prefix) {
		return value[len(prefix):]
	}
	return value
}

func cardDAVContactValueIsPlain(property vcard.SemanticProperty) bool {
	value := property.RawValue
	if property.ValueType == "text" {
		var err error
		value, err = vcard.UnescapeText(value)
		if err != nil {
			return false
		}
	}
	kind := ContactAddressEmail
	if property.Name == "TEL" {
		kind = ContactAddressPhone
	}
	value = cardDAVContactValue(kind, value)
	if strings.TrimSpace(value) == "" {
		return true
	}
	if kind != ContactAddressPhone && value != strings.TrimSpace(value) {
		return false
	}
	if kind == ContactAddressPhone {
		digits := strings.NewReplacer(" ", "", "-", "", ".", "", "(", "", ")", "").Replace(value)
		digits = strings.TrimPrefix(digits, "+")
		if digits == "" || strings.ContainsFunc(digits, func(r rune) bool { return r < '0' || r > '9' }) {
			return false
		}
	}
	_, err := NormalizeServiceValue(nil, kind, value)
	return err == nil
}

type cardDAVResourceOwner struct {
	mapping  vcard.NativeMapping
	property vcard.Property
	kept     bool
}

func cardDAVOwnerIsReplaceable(owner cardDAVResourceOwner, envelope ValueEnvelope) bool {
	if envelope.Source == ProvenanceCardDAVImport && envelope.IsCurrent() {
		return true
	}
	switch owner.property.Name {
	case "EMAIL", "TEL":
		return owner.mapping.Table == personContactPointsTableName && len(envelope.TypeTokens) == 0 &&
			envelope.Pref == nil && (envelope.TypeLabel == nil || *envelope.TypeLabel == "")
	case "FN":
		return len(owner.property.Parameters) == 0 && ((owner.mapping.Table == personNamesTableName && owner.mapping.Field == "formatted") ||
			(owner.mapping.Table == "persons" && owner.mapping.Field == "display_name"))
	default:
		return false
	}
}

func cardDAVRebindOwners(stored, incoming vcard.ResourceEnvelope) ([]cardDAVResourceOwner, error) {
	if len(stored.NativeMappings) == 0 {
		return nil, nil
	}
	rebound, err := vcard.RebindResourceOwnership(stored, incoming, false)
	if err != nil {
		return nil, err
	}
	owners := make([]cardDAVResourceOwner, 0, len(stored.NativeMappings))
	for _, mapping := range stored.NativeMappings {
		owner := cardDAVResourceOwner{mapping: mapping}
		for _, occurrence := range stored.PropertyTree {
			if occurrence.Identity.Equal(mapping.Identity) {
				owner.property = occurrence.Property
				owner.property.Name = strings.ToUpper(owner.property.Name)
				break
			}
		}
		for _, kept := range rebound.NativeMappings {
			if kept.Table == mapping.Table && kept.RowID == mapping.RowID && kept.Field == mapping.Field {
				owner.mapping, owner.kept = kept, true
				break
			}
		}
		owners = append(owners, owner)
	}
	return owners, nil
}

// cardDAVPublishedPersonUnchangedLocallyTx reports a settled publication to this card with no local edit since the last sync.
func (s *Store) cardDAVPublishedPersonUnchangedLocallyTx(
	ctx context.Context, tx *loggedTx, bookID, personID int64, resource *CardDAVResource, snapshot *PersonVCardSnapshot,
) (bool, error) {
	publication, err := getCardDAVPublicationFrom(ctx, tx, personID, "")
	if errors.Is(err, ErrCardDAVPublicationNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !publication.Desired || publication.PendingOperation != "" ||
		publication.AddressBookID != bookID || publication.Href != resource.Href {
		return false, nil
	}
	// The equivalent-local path passes the conflict check, so require an unchanged local side here.
	if snapshot == nil {
		snapshot, err = s.loadPersonVCardSnapshotTx(ctx, tx, personID)
		if err != nil {
			return false, fmt.Errorf("hash CardDAV-published person: %w", err)
		}
	}
	return snapshot.Fingerprint == resource.LocalHash, nil
}

func (s *Store) applyCardDAVResourceTx(
	ctx context.Context, tx *loggedTx, book CardDAVAddressBook,
	input CardDAVRemoteResource, reconcileBinding bool,
) (bool, bool, error) {
	current, err := s.findCardDAVResourceTx(ctx, tx, book.ID, input.Href)
	created := errors.Is(err, ErrCardDAVResourceNotFound)
	if err != nil && !created {
		return false, false, err
	}
	unchangedRemote := !created && current.RemoteETag == input.RemoteETag &&
		bytes.Equal(current.RemoteBody, input.RemoteBody)
	bindingNeedsReconcile := !created && (current.MappingStatus == CardDAVMappingUnbound ||
		current.MappingStatus == CardDAVMappingAmbiguous)
	if unchangedRemote && (!reconcileBinding || !bindingNeedsReconcile) {
		return false, false, nil
	}

	var personID *int64
	personRevision := (*int64)(nil)
	status := CardDAVMappingUnbound
	governance := CardDAVGovernanceNone
	var ambiguous []cardDAVPersonMatch
	projectionRebased := false
	remoteOwnsDisplay := false
	preserveLocalTombstone := !created && current.MappingStatus == CardDAVMappingMapped &&
		current.PersonID == nil && current.RemoteSemanticHash == input.SemanticHash
	if preserveLocalTombstone {
		status, governance = current.MappingStatus, current.Governance
	} else if !created && current.PersonID != nil {
		personID = current.PersonID
		personRevision = current.PersonRevisionAtBind
		status, governance = current.MappingStatus, current.Governance
	} else {
		resourceID := int64(0)
		if !created {
			resourceID = current.ID
		}
		personID, ambiguous, err = s.resolveCardDAVPersonTx(ctx, tx, resourceID, input)
		if err != nil {
			return false, false, err
		}
		switch {
		case personID != nil:
			status, governance = CardDAVMappingMapped, CardDAVGovernanceLocal
		case len(ambiguous) > 0:
			status, governance = CardDAVMappingAmbiguous, CardDAVGovernanceNone
		case book.IsSubscribed:
			personID, personRevision, err = s.createCardDAVImportedPersonTx(ctx, tx, book.ID, input)
			if err != nil {
				return false, false, err
			}
			status, governance = CardDAVMappingMapped, CardDAVGovernanceRemote
		}
	}
	semanticRemoteChanged := !created && current.RemoteSemanticHash != input.SemanticHash
	if semanticRemoteChanged && current.MappingStatus == CardDAVMappingMapped &&
		personID != nil {
		rebase := false
		if current.Governance == CardDAVGovernanceRemote && current.PersonRevisionAtBind != nil {
			hasUserOwnedState, err := s.personHasUserOwnedStateTx(
				ctx, tx, *personID, *current.PersonRevisionAtBind,
			)
			if err != nil {
				return false, false, err
			}
			rebase = !hasUserOwnedState
		}
		if !rebase {
			rebase, err = s.cardDAVPublishedPersonUnchangedLocallyTx(
				ctx, tx, book.ID, *personID, current, nil,
			)
			if err != nil {
				return false, false, err
			}
		}
		if rebase {
			remoteOwnsDisplay, err = s.rebaseCardDAVImportedProjectionTx(
				ctx, tx, book.ID, *personID, input, false,
			)
			if err != nil {
				return false, false, err
			}
			projectionRebased = true
			personRevision = nil
		}
	}
	if personID != nil && personRevision == nil {
		var revision int64
		if err := tx.QueryRowContext(ctx, `SELECT revision FROM persons WHERE id = ?`, *personID).Scan(&revision); err != nil {
			return false, false, fmt.Errorf("load CardDAV-bound person revision: %w", err)
		}
		personRevision = &revision
	}
	localHash := input.SemanticHash
	if !created {
		localHash = current.LocalHash
	}
	if personID != nil {
		snapshot, err := s.loadPersonVCardSnapshotTx(ctx, tx, *personID)
		if err != nil {
			return false, false, fmt.Errorf("hash CardDAV-bound local person: %w", err)
		}
		localHash = snapshot.Fingerprint
	}

	var resourceID int64
	if created {
		err = tx.QueryRowContext(ctx, `INSERT INTO carddav_resources (
			address_book_id, href, remote_uid, remote_etag, remote_body,
			remote_semantic_hash, local_hash, mapping_status, mapping_revision,
			governance, person_id, person_revision_at_bind
		) VALUES (?, ?, NULLIF(?, ''), ?, ?, ?, ?, ?, 1, ?, ?, ?) RETURNING id`,
			book.ID, input.Href, input.RemoteUID, input.RemoteETag, input.RemoteBody,
			input.SemanticHash, localHash, status, governance,
			nullableVCardInt64(personID), nullableVCardInt64(personRevision),
		).Scan(&resourceID)
	} else {
		resourceID = current.ID
		_, err = tx.ExecContext(ctx, `UPDATE carddav_resources SET
			remote_uid = NULLIF(?, ''), remote_etag = ?, remote_body = ?,
			remote_semantic_hash = ?, local_hash = ?, mapping_status = ?,
			mapping_revision = mapping_revision + 1, governance = ?, person_id = ?,
			person_revision_at_bind = ?, updated_at = `+s.dialect.Now()+`
			WHERE id = ?`, input.RemoteUID, input.RemoteETag, input.RemoteBody,
			input.SemanticHash, localHash, status, governance,
			nullableVCardInt64(personID), nullableVCardInt64(personRevision), resourceID)
	}
	if err != nil {
		return false, false, fmt.Errorf("save CardDAV resource ledger: %w", err)
	}
	sourceRef := fmt.Sprintf("carddav:%d", book.ID)
	if _, err := tx.ExecContext(ctx, `DELETE FROM identity_match_candidates
		WHERE source = ? AND source_ref = ?
		  AND state IN (?, ?)
		  AND decided_at IS NULL
		  AND ((left_kind = ? AND left_id = ?)
		    OR (right_kind = ? AND right_id = ?))`,
		ProvenanceCardDAVImport, sourceRef,
		IdentityMatchStateCandidate, IdentityMatchStateConflict,
		IdentityMatchCardDAVResource, resourceID,
		IdentityMatchCardDAVResource, resourceID,
	); err != nil {
		return false, false, fmt.Errorf("reconcile CardDAV identity candidates: %w", err)
	}
	if personID != nil {
		if err := s.putCardDAVEnvelopeTx(ctx, tx, book.ID, *personID, input); err != nil {
			return false, false, err
		}
	}
	if projectionRebased {
		// A remote-only rebase settles publication even when rendering would normalize untouched values.
		if _, err := tx.ExecContext(ctx, `UPDATE carddav_publications SET local_hash = ?
			WHERE person_id = ? AND address_book_id = ? AND href = ?
			  AND desired = TRUE AND pending_operation IS NULL`, localHash, *personID, book.ID, input.Href); err != nil {
			return false, false, fmt.Errorf("settle remote-only CardDAV publication: %w", err)
		}
		if err := s.refreshCardDAVImportedPersonBindBaselineTx(
			ctx, tx, resourceID, *personID, remoteOwnsDisplay,
		); err != nil {
			return false, false, err
		}
	}
	for _, match := range ambiguous {
		normalized := match.NormalizedValue
		_, _, err := s.upsertIdentityMatchCandidateTx(ctx, tx, IdentityMatchCandidateInput{
			LeftKind: IdentityMatchCardDAVResource, LeftID: resourceID,
			RightKind: IdentityMatchPerson, RightID: match.PersonID,
			Basis: match.Basis, NormalizedValue: &normalized,
			State: IdentityMatchStateConflict, Source: ProvenanceCardDAVImport,
			SourceRef: &sourceRef,
		}, IdentityMatchCardDAVResource, resourceID, IdentityMatchPerson,
			match.PersonID, nil, false)
		if err != nil {
			return false, false, err
		}
	}
	return created, true, nil
}

// acceptCardDAVIdentityMatchCandidateContext records the explicit review
// decision and binds the resource to the chosen person in one transaction.
// Competing generated candidates are rejected as part of the same decision;
// subsequent reconciliation preserves every accepted or rejected row.
func (s *Store) acceptCardDAVIdentityMatchCandidateContext(
	ctx context.Context, candidateID int64, decidedBy string, notes *string,
) (*IdentityMatchCandidate, int64, error) {
	return s.acceptCardDAVIdentityMatchCandidateReviewedContext(
		ctx, candidateID, decidedBy, notes, nil)
}

func (s *Store) acceptCardDAVIdentityMatchCandidateReviewedContext(
	ctx context.Context, candidateID int64, decidedBy string, notes *string, reviewToken *string,
) (*IdentityMatchCandidate, int64, error) {
	if decidedBy != string(ProvenanceUser) {
		return nil, 0, ErrIdentityMatchNotAcceptable
	}
	var accepted *IdentityMatchCandidate
	err := s.withTxContext(ctx, func(tx *loggedTx) error {
		if err := s.lockIdentityMutationTxContext(ctx, tx); err != nil {
			return err
		}
		candidate, err := getIdentityMatchCandidateTx(ctx, tx, candidateID)
		if err != nil {
			return err
		}
		if candidate.LeftKind != IdentityMatchCardDAVResource ||
			candidate.RightKind != IdentityMatchPerson {
			return ErrIdentityMatchEndpointUnsupported
		}
		if reviewToken != nil {
			actual, fingerprintErr := identityMatchFingerprintContext(ctx, tx, candidate, true)
			if fingerprintErr != nil {
				return fingerprintErr
			}
			if actual != *reviewToken {
				if candidate.State == IdentityMatchStateAccepted && candidate.DecidedBy != nil &&
					*candidate.DecidedBy == string(ProvenanceUser) {
					matches, receiptErr := identityMatchReviewReceiptMatchesTxContext(
						ctx, tx, candidate, IdentityMatchStateAccepted, *reviewToken)
					if receiptErr != nil {
						return receiptErr
					}
					if matches {
						accepted = candidate
						return nil
					}
				}
				return ErrIdentityMatchReviewStale
			}
			if candidate.State == IdentityMatchStateAccepted && candidate.DecidedBy != nil &&
				*candidate.DecidedBy == string(ProvenanceUser) {
				accepted = candidate
				return nil
			}
		}
		resource, err := scanCardDAVResource(tx.QueryRowContext(ctx,
			cardDAVResourceSelect+` WHERE id = ?`+s.dialect.SelectForUpdate(),
			candidate.LeftID,
		))
		if errors.Is(err, sql.ErrNoRows) {
			return ErrIdentityMatchEndpointNotFound
		}
		if err != nil {
			return fmt.Errorf("lock CardDAV candidate resource: %w", err)
		}
		if resource.PersonID != nil && *resource.PersonID != candidate.RightID {
			return ErrIdentityMatchAlreadyApplied
		}
		var personRevision int64
		if err := tx.QueryRowContext(ctx, `SELECT revision FROM persons WHERE id = ?`+
			s.dialect.SelectForUpdate(), candidate.RightID).Scan(&personRevision); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrIdentityMatchEndpointNotFound
			}
			return fmt.Errorf("lock CardDAV candidate person: %w", err)
		}
		snapshot, err := s.loadPersonVCardSnapshotTx(ctx, tx, candidate.RightID)
		if err != nil {
			return fmt.Errorf("hash accepted CardDAV candidate person: %w", err)
		}
		if resource.PersonID == nil {
			if _, err := tx.ExecContext(ctx, `UPDATE carddav_resources SET
				mapping_status = ?, mapping_revision = mapping_revision + 1,
				governance = ?, person_id = ?, person_revision_at_bind = ?,
				local_hash = ?, updated_at = `+s.dialect.Now()+` WHERE id = ?`,
				CardDAVMappingMapped, CardDAVGovernanceLocal, candidate.RightID,
				personRevision, snapshot.Fingerprint, resource.ID,
			); err != nil {
				return fmt.Errorf("bind accepted CardDAV identity candidate: %w", err)
			}
			if err := s.putCardDAVEnvelopeTx(ctx, tx, resource.AddressBookID,
				candidate.RightID, CardDAVRemoteResource{
					Href: resource.Href, RemoteBody: resource.RemoteBody,
				}); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE identity_match_candidates SET
			state = ?, decided_by = ?, decided_at = `+s.dialect.Now()+`, notes = ?,
			pre_conflict_state = NULL, application_pending = FALSE,
			updated_at = `+s.dialect.Now()+` WHERE id = ?`,
			IdentityMatchStateAccepted, decidedBy, stringValue(notes), candidate.ID,
		); err != nil {
			return fmt.Errorf("accept CardDAV identity candidate: %w", err)
		}
		peerNote := "another CardDAV identity candidate was accepted"
		if _, err := tx.ExecContext(ctx, `UPDATE identity_match_candidates SET
			state = ?, decided_by = ?, decided_at = `+s.dialect.Now()+`, notes = ?,
			pre_conflict_state = NULL, application_pending = FALSE,
			updated_at = `+s.dialect.Now()+`
			WHERE id <> ? AND source = ? AND state IN (?, ?)
			  AND left_kind = ? AND left_id = ? AND right_kind = ?`,
			IdentityMatchStateRejected, decidedBy, peerNote, candidate.ID,
			ProvenanceCardDAVImport, IdentityMatchStateCandidate,
			IdentityMatchStateConflict, IdentityMatchCardDAVResource,
			resource.ID, IdentityMatchPerson,
		); err != nil {
			return fmt.Errorf("reject competing CardDAV identity candidates: %w", err)
		}
		if reviewToken != nil {
			if err := recordIdentityMatchReviewReceiptTxContext(
				ctx, tx, candidate.ID, IdentityMatchStateAccepted, *reviewToken); err != nil {
				return err
			}
		}
		accepted, err = getIdentityMatchCandidateTx(ctx, tx, candidate.ID)
		return err
	})
	if err != nil {
		return nil, 0, err
	}
	revision, err := readIdentityRevisionContext(ctx, s.db)
	if err != nil {
		return nil, 0, err
	}
	return accepted, revision, nil
}

type cardDAVPersonMatch struct {
	PersonID        int64
	Basis           IdentityMatchBasis
	NormalizedValue string
}

func (s *Store) resolveCardDAVPersonTx(
	ctx context.Context, tx *loggedTx, resourceID int64, input CardDAVRemoteResource,
) (*int64, []cardDAVPersonMatch, error) {
	rejected := map[int64]struct{}{}
	if resourceID != 0 {
		rows, err := tx.QueryContext(ctx, `SELECT right_id, state FROM identity_match_candidates
			WHERE left_kind = ? AND left_id = ? AND right_kind = ?
			  AND state IN (?, ?) ORDER BY right_id`,
			IdentityMatchCardDAVResource, resourceID, IdentityMatchPerson,
			IdentityMatchStateAccepted, IdentityMatchStateRejected)
		if err != nil {
			return nil, nil, fmt.Errorf("load reviewed CardDAV identity candidates: %w", err)
		}
		var accepted []int64
		for rows.Next() {
			var personID int64
			var state IdentityMatchState
			if err := rows.Scan(&personID, &state); err != nil {
				_ = rows.Close()
				return nil, nil, fmt.Errorf("scan reviewed CardDAV identity candidate: %w", err)
			}
			if state == IdentityMatchStateAccepted {
				accepted = append(accepted, personID)
			} else {
				rejected[personID] = struct{}{}
			}
		}
		if err := rows.Close(); err != nil {
			return nil, nil, fmt.Errorf("close reviewed CardDAV identity candidates: %w", err)
		}
		if len(accepted) > 1 {
			return nil, nil, errors.New("CardDAV resource has multiple accepted identity candidates")
		}
		if len(accepted) == 1 {
			return &accepted[0], nil, nil
		}
	}
	if uid := strings.TrimSpace(input.RemoteUID); uid != "" {
		var id int64
		err := tx.QueryRowContext(ctx, `SELECT id FROM persons WHERE vcard_uid = ?`, uid).Scan(&id)
		if err == nil {
			if _, wasRejected := rejected[id]; !wasRejected {
				return &id, nil, nil
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return nil, nil, fmt.Errorf("match CardDAV canonical UID: %w", err)
		}
		var surviving sql.NullInt64
		err = tx.QueryRowContext(ctx, `SELECT surviving_person_id
			FROM person_uid_aliases WHERE retired_uid = ?`, uid).Scan(&surviving)
		if err == nil && surviving.Valid {
			id = surviving.Int64
			if _, wasRejected := rejected[id]; !wasRejected {
				return &id, nil, nil
			}
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, nil, fmt.Errorf("match CardDAV retired UID: %w", err)
		}
	}

	matches := map[int64]cardDAVPersonMatch{}
	for _, candidate := range []struct {
		kind   ContactAddressKind
		basis  IdentityMatchBasis
		values []string
	}{{ContactAddressEmail, IdentityMatchEmail, input.Emails},
		{ContactAddressPhone, IdentityMatchPhone, input.Phones}} {
		for _, raw := range candidate.values {
			normalized, err := NormalizeServiceValue(nil, candidate.kind, raw)
			if err != nil {
				continue
			}
			rows, err := tx.QueryContext(ctx, `SELECT person_id FROM person_contact_points
				WHERE address_kind = ? AND service_id IS NULL
				  AND normalized_value = ? AND active_until IS NULL AND superseded_at IS NULL
				ORDER BY person_id`, candidate.kind, normalized)
			if err != nil {
				return nil, nil, fmt.Errorf("match CardDAV contact point: %w", err)
			}
			for rows.Next() {
				var personID int64
				if err := rows.Scan(&personID); err != nil {
					_ = rows.Close()
					return nil, nil, fmt.Errorf("scan CardDAV contact match: %w", err)
				}
				if _, wasRejected := rejected[personID]; !wasRejected {
					matches[personID] = cardDAVPersonMatch{PersonID: personID, Basis: candidate.basis, NormalizedValue: normalized}
				}
			}
			if err := rows.Close(); err != nil {
				return nil, nil, fmt.Errorf("close CardDAV contact matches: %w", err)
			}
		}
	}
	if len(matches) == 1 {
		for id := range matches {
			return &id, nil, nil
		}
	}
	ambiguous := make([]cardDAVPersonMatch, 0, len(matches))
	for _, match := range matches {
		ambiguous = append(ambiguous, match)
	}
	slices.SortFunc(ambiguous, func(left, right cardDAVPersonMatch) int {
		return int(left.PersonID - right.PersonID)
	})
	return nil, ambiguous, nil
}

func (s *Store) createCardDAVImportedPersonTx(
	ctx context.Context, tx *loggedTx, bookID int64, input CardDAVRemoteResource,
) (*int64, *int64, error) {
	uid, err := newVCardUID()
	if err != nil {
		return nil, nil, err
	}
	var personID int64
	if err := tx.QueryRowContext(ctx, `INSERT INTO persons (vcard_uid, display_name)
		VALUES (?, NULLIF(?, '')) RETURNING id`, uid, input.DisplayName).Scan(&personID); err != nil {
		return nil, nil, fmt.Errorf("create CardDAV imported person: %w", err)
	}
	if strings.TrimSpace(input.DisplayName) != "" {
		if err := s.bumpPersonDisplayNameRevisionContext(ctx, tx); err != nil {
			return nil, nil, err
		}
	}
	if err := s.addCardDAVImportedProjectionTx(ctx, tx, bookID, personID, input, nil); err != nil {
		return nil, nil, err
	}
	revision := int64(1)
	return &personID, &revision, nil
}

func (s *Store) addCardDAVImportedProjectionTx(
	ctx context.Context, tx *loggedTx, bookID, personID int64, input CardDAVRemoteResource,
	skip map[string]bool,
) error {
	resource, err := vcard.ParseResourceEnvelope(input.RemoteBody)
	if err != nil {
		return err
	}
	properties := make(map[string]vcard.Property, len(resource.PropertyTree))
	for _, occurrence := range resource.PropertyTree {
		properties[occurrence.Identity.Key()] = occurrence.Property
	}
	parameters := func(envelope *ValueEnvelopeInput, identity vcard.PropertyIdentity) *string {
		var language *string
		for _, parameter := range properties[identity.Key()].Parameters {
			for _, value := range parameter.Values {
				switch parameter.Name {
				case "LANGUAGE":
					language = new(value.Decoded)
				case "TYPE":
					if strings.EqualFold(value.Decoded, "pref") {
						if envelope.Pref == nil {
							envelope.Pref = new(1)
						}
					} else {
						envelope.TypeTokens = append(envelope.TypeTokens, value.Decoded)
					}
				case "PREF":
					if pref, err := strconv.Atoi(value.Decoded); err == nil && pref >= 1 && pref <= 100 {
						envelope.Pref = &pref
					}
				}
			}
		}
		return language
	}
	sourceRef := fmt.Sprintf("carddav:%d", bookID)
	baseEnvelope := ValueEnvelopeInput{
		Source: ProvenanceCardDAVImport, SourceRef: &sourceRef,
		SourceResourceUID: &input.Href,
	}
	resource.SourceRef = sourceRef
	resource.SourceResourceUID = input.Href
	resource.Href = input.Href
	resource.CanonicalPersonUID, err = vcardCanonicalUIDTx(ctx, tx, personID)
	if err != nil {
		return err
	}
	stored, err := s.findVCardResourceEnvelopeTx(ctx, tx, sourceRef, input.Href)
	if err != nil && !errors.Is(err, ErrVCardResourceNotFound) {
		return err
	}
	if stored != nil {
		resource, err = vcard.RebindResourceOwnership(stored.ResourceEnvelope, resource, false)
		if err != nil {
			return err
		}
	}
	bind := func(identity vcard.PropertyIdentity, table string, rowID int64, field string) {
		resource.NativeMappings = slices.DeleteFunc(resource.NativeMappings, func(mapping vcard.NativeMapping) bool {
			return mapping.Identity.Equal(identity)
		})
		resource.NativeMappings = append(resource.NativeMappings, vcard.NativeMapping{
			Identity: identity, SourceRef: sourceRef, Table: table, RowID: rowID, Field: field, Kind: vcard.HandlingNative,
		})
	}
	for _, occurrence := range resource.PropertyTree {
		if !strings.EqualFold(occurrence.Property.Name, "FN") || skip[occurrence.Identity.Key()] {
			continue
		}
		value, err := vcard.UnescapeText(occurrence.Property.RawValue)
		if err != nil {
			return err
		}
		if strings.TrimSpace(value) == "" {
			continue
		}
		envelope := baseEnvelope
		envelope.VCard = VCardIdentity{
			Property: "FN", Group: trimmedOrNil(new(occurrence.Identity.Group)), PropID: occurrence.Identity.PropID,
			PID: occurrence.Identity.PID, AltID: occurrence.Identity.AltID,
		}
		language := parameters(&envelope, occurrence.Identity)
		name := PersonNameInput{
			NameKind: PersonNameFormatted, Formatted: &value,
			Language:      language,
			OriginalValue: value, Envelope: envelope,
		}
		representable := true
		seen := make(map[string]bool)
		for _, parameter := range occurrence.Property.Parameters {
			if seen[parameter.Name] || len(parameter.Values) == 0 {
				representable = false
				break
			}
			seen[parameter.Name] = true
			value := parameter.Values[0].Decoded
			switch parameter.Name {
			case "LANGUAGE", "SCRIPT", "PHONETIC":
				if len(parameter.Values) != 1 || strings.TrimSpace(value) != value || value == "" {
					representable = false
				}
				switch parameter.Name {
				case "SCRIPT":
					name.Script = &value
				case "PHONETIC":
					name.PhoneticSystem = &value
				}
			case "SORT-AS":
				values := make([]string, 0, len(parameter.Values))
				for _, value := range parameter.Values {
					if strings.Contains(value.Decoded, ",") || strings.TrimSpace(value.Decoded) != value.Decoded || value.Decoded == "" {
						representable = false
					}
					values = append(values, value.Decoded)
				}
				name.SortAs = new(strings.Join(values, ","))
			case "TYPE", "PREF", "PROP-ID", "PID", "ALTID":
			default:
				representable = false
			}
		}
		if representable {
			row, err := s.addPersonNameTx(ctx, tx, personID, name)
			if err != nil {
				return err
			}
			bind(occurrence.Identity, personNamesTableName, row.Envelope.ID, "formatted")
		}
	}
	for _, point := range []struct {
		kind        ContactAddressKind
		values      []string
		identities  []VCardIdentity
		occurrences []vcard.PropertyIdentity
	}{
		{ContactAddressEmail, input.Emails, input.EmailIdentities, input.EmailOccurrences},
		{ContactAddressPhone, input.Phones, input.PhoneIdentities, input.PhoneOccurrences},
	} {
		for index, value := range point.values {
			if index < len(point.occurrences) && skip[point.occurrences[index].Key()] {
				continue
			}
			envelope := baseEnvelope
			if index < len(point.identities) {
				envelope.VCard = point.identities[index]
			}
			if index < len(point.occurrences) {
				property := vcard.NormalizeSemanticProperty(resource.RenderMetadata.StoredVersion, properties[point.occurrences[index].Key()])
				if !cardDAVContactValueIsPlain(property) {
					continue
				}
				parameters(&envelope, point.occurrences[index])
			}
			row, err := s.addPersonContactPointTx(ctx, tx, personID, PersonContactPointInput{
				AddressKind: point.kind, OriginalValue: value, Envelope: envelope,
			})
			if err != nil {
				if point.kind == ContactAddressPhone && errors.Is(err, ErrNormalizationRejected) {
					continue
				}
				return err
			}
			if index < len(point.occurrences) {
				bind(point.occurrences[index], personContactPointsTableName, row.Envelope.ID, "original_value")
			}
		}
	}
	resource.Residue = vcard.ResidueWithMappings(resource.PropertyTree, resource.NativeMappings)
	return s.putCardDAVPreparedEnvelopeTx(ctx, tx, bookID, personID, input.Href, resource)
}

// rebaseCardDAVImportedProjectionTx replaces imported fields and values the card carried; a local display rename survives.
func (s *Store) rebaseCardDAVImportedProjectionTx(
	ctx context.Context, tx *loggedTx, bookID, personID int64, input CardDAVRemoteResource,
	resolveRemote bool,
) (bool, error) {
	sourceRef := fmt.Sprintf("carddav:%d", bookID)
	envelope, err := s.findVCardResourceEnvelopeTx(ctx, tx, sourceRef, input.Href)
	if err != nil && !errors.Is(err, ErrVCardResourceNotFound) {
		return false, err
	}
	skip := make(map[string]bool)
	keptOwners := make(map[string][]int64)
	if envelope != nil && len(envelope.NativeMappings) > 0 {
		incoming, err := vcard.ParseResourceEnvelope(input.RemoteBody)
		if err != nil {
			return false, err
		}
		owners, err := cardDAVRebindOwners(envelope.ResourceEnvelope, incoming)
		if err != nil {
			return false, err
		}
		for _, owner := range owners {
			mapping, property, kept := owner.mapping, owner.property.Name, owner.kept
			if kept {
				keptOwners[mapping.Table] = append(keptOwners[mapping.Table], mapping.RowID)
			}
			if resolveRemote && !kept {
				// Independent fields keep their ownership when one occurrence is displaced.
				envelope.NativeMappings = slices.DeleteFunc(envelope.NativeMappings, func(candidate vcard.NativeMapping) bool {
					return candidate.Table == mapping.Table && candidate.RowID == mapping.RowID &&
						(mapping.Table != "employments" && mapping.Field != "derived_fn" || candidate.Field == mapping.Field)
				})
				var supersede func(context.Context, *loggedTx, int64, int64, *time.Time) error
				switch mapping.Table {
				case personNamesTableName:
					if mapping.Field == "derived_fn" {
						continue
					}
					supersede = s.supersedePersonNameTx
				case personContactPointsTableName:
					supersede = s.supersedePersonContactPointTx
				case "person_addresses":
					supersede = s.supersedePersonAddressTx
				case "person_dates":
					supersede = s.supersedePersonDateTx
				case "person_categories":
					supersede = s.supersedePersonCategoryTx
				case "person_media":
					supersede = s.supersedePersonMediaTx
				case "person_attribute_values":
					value, err := s.attributeValueByIDTx(ctx, tx, personAttributeOwner, mapping.RowID)
					if errors.Is(err, sql.ErrNoRows) {
						continue
					}
					if err != nil {
						return false, err
					}
					if value.OwnerID == personID && value.ActiveUntil == nil && value.SupersededAt == nil {
						now := time.Now().UTC()
						if _, err := s.closePersonAttributeValueTx(ctx, tx, mapping.RowID, now, now); err != nil {
							return false, err
						}
					}
					continue
				case "employments":
					employment, err := getEmploymentTx(ctx, tx, mapping.RowID)
					if errors.Is(err, ErrEmploymentNotFound) {
						continue
					}
					if err != nil {
						return false, err
					}
					if employment.PersonID != personID {
						continue
					}
					update := EmploymentInput{
						PersonID: employment.PersonID, OrganizationID: employment.OrganizationID,
						Title: employment.Title, Role: employment.Role, Department: employment.Department,
						Location: employment.Location, AddressID: employment.AddressID, Description: employment.Description,
						StartDate: employment.StartDate, EndDate: employment.EndDate,
						IsCurrent: &employment.IsCurrent, IsPrimary: &employment.IsPrimary,
						Source: employment.Source, SourceRef: employment.SourceRef, Confidence: employment.Confidence,
					}
					switch mapping.Field {
					case "title":
						update.Title = nil
					case "role":
						update.Role = nil
					case "organization_id":
						// A required organization stays in history after its card projection is displaced.
						update.IsPrimary = new(false)
						envelope.NativeMappings = slices.DeleteFunc(envelope.NativeMappings, func(candidate vcard.NativeMapping) bool {
							return candidate.Table == mapping.Table && candidate.RowID == mapping.RowID
						})
					}
					if _, err := s.reviseEmploymentTx(ctx, tx, mapping.RowID, employment.Revision, update); err != nil {
						return false, err
					}
					continue
				case "person_relationships":
					var sourceID, targetID int64
					err := tx.QueryRowContext(ctx, `DELETE FROM person_relationships WHERE id = ? AND (source_person_id = ? OR target_person_id = ?)
						RETURNING source_person_id, target_person_id`, mapping.RowID, personID, personID).Scan(&sourceID, &targetID)
					if errors.Is(err, sql.ErrNoRows) {
						continue
					}
					if err != nil {
						return false, fmt.Errorf("retire displaced CardDAV relationship: %w", err)
					}
					if err := s.bumpPersonVCardProjectionsTx(ctx, tx, sourceID, targetID); err != nil {
						return false, err
					}
					continue
				case "person_relationship_reviews":
					if _, err := s.claimRelationshipReviewTx(ctx, tx, mapping.RowID, RelationshipReviewRejected, string(ProvenanceUser)); err != nil &&
						!errors.Is(err, ErrRelationshipReviewNotFound) && !errors.Is(err, ErrRelationshipReviewNotPending) {
						return false, err
					}
					continue
				}
				if supersede != nil {
					if err := supersede(ctx, tx, personID, mapping.RowID, nil); err != nil && !errors.Is(err, ErrProfileValueNotFound) {
						return false, err
					}
					continue
				}
			}
			if property == "FN" && mapping.Table == personNamesTableName && mapping.Field == "formatted" {
				name, err := getPersonNameTx(ctx, tx, personID, mapping.RowID)
				if errors.Is(err, ErrProfileValueNotFound) {
					continue
				}
				if err != nil {
					return false, err
				}
				if !name.Envelope.IsCurrent() {
					continue
				}
				if !kept && cardDAVOwnerIsReplaceable(owner, name.Envelope) {
					if err := s.supersedePersonNameTx(ctx, tx, personID, mapping.RowID, nil); err != nil {
						return false, err
					}
				} else if kept {
					skip[mapping.Identity.Key()] = true
				}
				continue
			}
			if property == "FN" && mapping.Table == "persons" && kept {
				skip[mapping.Identity.Key()] = true
			}
			if mapping.Table != personContactPointsTableName || (property != "EMAIL" && property != "TEL") {
				continue
			}
			point, err := getPersonContactPointTx(ctx, tx, personID, mapping.RowID)
			if errors.Is(err, ErrProfileValueNotFound) {
				continue
			}
			if err != nil {
				return false, err
			}
			if !point.Envelope.IsCurrent() {
				continue
			}
			if !kept && cardDAVOwnerIsReplaceable(owner, point.Envelope) {
				if err := s.supersedePersonContactPointTx(ctx, tx, personID, mapping.RowID, nil); err != nil {
					return false, err
				}
			} else if kept {
				skip[mapping.Identity.Key()] = true
			}
		}
		if resolveRemote {
			envelope.Residue = vcard.ResidueWithMappings(envelope.PropertyTree, envelope.NativeMappings)
			if err := s.putCardDAVPreparedEnvelopeTx(ctx, tx, bookID, personID, input.Href, envelope.ResourceEnvelope); err != nil {
				return false, err
			}
		}
	}
	var currentDisplay sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT display_name FROM persons WHERE id = ?`+
		s.dialect.SelectForUpdate(), personID).Scan(&currentDisplay); err != nil {
		return false, fmt.Errorf("lock CardDAV projection person: %w", err)
	}
	var priorDisplay string
	if envelope != nil {
		for _, occurrence := range envelope.PropertyTree {
			if strings.EqualFold(occurrence.Property.Name, "FN") {
				value, err := vcard.UnescapeText(occurrence.Property.RawValue)
				if err != nil {
					return false, fmt.Errorf("decode prior CardDAV display label: %w", err)
				}
				priorDisplay = strings.TrimSpace(value)
				if priorDisplay != "" {
					break
				}
			}
		}
	}
	remoteOwnsDisplay := !currentDisplay.Valid ||
		(priorDisplay != "" && currentDisplay.String == priorDisplay)

	for _, table := range []string{"person_names", "person_contact_points"} {
		query := `UPDATE ` + table + ` SET
			superseded_at = ` + s.dialect.Now() + `, updated_at = ` + s.dialect.Now() + `
			WHERE person_id = ? AND source = ? AND source_ref = ? AND source_resource_uid = ?
			  AND active_until IS NULL AND superseded_at IS NULL`
		args := []any{personID, ProvenanceCardDAVImport, sourceRef, input.Href}
		if kept := keptOwners[table]; len(kept) > 0 {
			placeholders, ids := sortedIDPlaceholders(kept)
			query += ` AND id NOT IN (` + placeholders + `)`
			args = append(args, ids...)
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return false, fmt.Errorf("supersede %s CardDAV projection: %w", table, err)
		}
	}
	if err := s.addCardDAVImportedProjectionTx(ctx, tx, bookID, personID, input, skip); err != nil {
		return false, err
	}
	displayChanged := false
	if remoteOwnsDisplay {
		result, err := tx.ExecContext(ctx, `UPDATE persons SET display_name = NULLIF(?, '')
			WHERE id = ? AND display_name IS DISTINCT FROM NULLIF(?, '')`,
			strings.TrimSpace(input.DisplayName), personID, strings.TrimSpace(input.DisplayName))
		if err != nil {
			return false, fmt.Errorf("rebase CardDAV display label: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return false, fmt.Errorf("count rebased CardDAV display label: %w", err)
		}
		displayChanged = affected > 0
		if displayChanged {
			if err := s.bumpPersonDisplayNameRevisionContext(ctx, tx); err != nil {
				return false, err
			}
		}
	}
	if err := s.bumpPersonRevisionsTx(ctx, tx, personID); err != nil {
		return false, err
	}
	if err := s.invalidatePersonEnrichmentIdentitiesAfterRevisionTx(ctx, tx, personID); err != nil {
		return false, err
	}
	if displayChanged {
		if err := s.bumpDisplayNameCounterpartVCardProjectionsTx(ctx, tx, personID); err != nil {
			return false, err
		}
	}
	return remoteOwnsDisplay, nil
}

// retireCardDAVImportedProjectionTx removes one resource's current semantic
// projection while retaining its history. The scalar display label is cleared
// only when it still matches the imported formatted name being retired.
func (s *Store) retireCardDAVImportedProjectionTx(
	ctx context.Context, tx *loggedTx, bookID, personID int64, href string,
) error {
	sourceRef := fmt.Sprintf("carddav:%d", bookID)
	var currentDisplay, importedDisplay sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT display_name FROM persons WHERE id = ?`+
		s.dialect.SelectForUpdate(), personID).Scan(&currentDisplay); err != nil {
		return fmt.Errorf("lock retired CardDAV projection person: %w", err)
	}
	err := tx.QueryRowContext(ctx, `SELECT formatted FROM person_names
		WHERE person_id = ? AND source = ? AND source_ref = ? AND source_resource_uid = ?
		  AND name_kind = ? AND active_until IS NULL AND superseded_at IS NULL
		ORDER BY id LIMIT 1`, personID, ProvenanceCardDAVImport, sourceRef, href,
		PersonNameFormatted).Scan(&importedDisplay)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("load retired CardDAV display projection: %w", err)
	}
	remoteOwnsDisplay := currentDisplay.Valid && importedDisplay.Valid &&
		currentDisplay.String == importedDisplay.String

	for _, table := range []string{"person_names", "person_contact_points"} {
		if _, err := tx.ExecContext(ctx, `UPDATE `+table+` SET
			superseded_at = `+s.dialect.Now()+`, updated_at = `+s.dialect.Now()+`
			WHERE person_id = ? AND source = ? AND source_ref = ? AND source_resource_uid = ?
			  AND active_until IS NULL AND superseded_at IS NULL`,
			personID, ProvenanceCardDAVImport, sourceRef, href); err != nil {
			return fmt.Errorf("retire %s CardDAV projection: %w", table, err)
		}
	}
	if remoteOwnsDisplay {
		result, err := tx.ExecContext(ctx, `UPDATE persons SET display_name = NULL
			WHERE id = ? AND display_name = ?`, personID, importedDisplay.String)
		if err != nil {
			return fmt.Errorf("clear retired CardDAV display label: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("count cleared CardDAV display label: %w", err)
		}
		if affected > 0 {
			if err := s.bumpPersonDisplayNameRevisionContext(ctx, tx); err != nil {
				return err
			}
		}
	}
	if err := s.bumpPersonRevisionsTx(ctx, tx, personID); err != nil {
		return err
	}
	if err := s.invalidatePersonEnrichmentIdentitiesAfterRevisionTx(ctx, tx, personID); err != nil {
		return err
	}
	if remoteOwnsDisplay {
		if err := s.bumpDisplayNameCounterpartVCardProjectionsTx(ctx, tx, personID); err != nil {
			return err
		}
	}
	return nil
}

// refreshCardDAVImportedPersonBindBaselineTx records the revision produced by
// a remote-owned rebase only when the rebase left no user-owned state behind.
// A locally changed display label has no separate provenance row, so the
// caller supplies whether the imported display still owned that scalar.
func (s *Store) refreshCardDAVImportedPersonBindBaselineTx(
	ctx context.Context, tx *loggedTx, resourceID, personID int64, remoteOwnsDisplay bool,
) error {
	if !remoteOwnsDisplay {
		return nil
	}
	var revision int64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM persons WHERE id = ?`+
		s.dialect.SelectForUpdate(), personID).Scan(&revision); err != nil {
		return fmt.Errorf("load rebased CardDAV person revision: %w", err)
	}
	hasUserOwnedState, err := s.personHasUserOwnedStateTx(ctx, tx, personID, revision)
	if err != nil {
		return err
	}
	if hasUserOwnedState {
		return nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE carddav_resources
		SET person_revision_at_bind = ?
		WHERE id = ? AND person_id = ? AND governance = ?`,
		revision, resourceID, personID, CardDAVGovernanceRemote)
	if err != nil {
		return fmt.Errorf("refresh rebased CardDAV person revision: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count refreshed CardDAV person revision: %w", err)
	}
	if affected != 1 {
		return ErrCardDAVConflictStale
	}
	return nil
}

func (s *Store) putCardDAVEnvelopeTx(
	ctx context.Context, tx *loggedTx, bookID, personID int64,
	input CardDAVRemoteResource,
) error {
	envelope, err := vcard.ParseResourceEnvelope(input.RemoteBody)
	if err != nil {
		return fmt.Errorf("parse CardDAV resource envelope: %w", err)
	}
	envelope.SourceRef = fmt.Sprintf("carddav:%d", bookID)
	envelope.SourceResourceUID = input.Href
	envelope.Href = input.Href
	canonicalUID, err := vcardCanonicalUIDTx(ctx, tx, personID)
	if err != nil {
		return err
	}
	envelope.CanonicalPersonUID = canonicalUID
	current, loadErr := s.findVCardResourceEnvelopeTx(ctx, tx, envelope.SourceRef, input.Href)
	if loadErr != nil && !errors.Is(loadErr, ErrVCardResourceNotFound) {
		return loadErr
	}
	if current != nil && len(current.NativeMappings) > 0 {
		envelope, err = vcard.RebindResourceOwnership(current.ResourceEnvelope, envelope, false)
		if err != nil {
			return err
		}
	}
	return s.putCardDAVPreparedEnvelopeTx(ctx, tx, bookID, personID, input.Href, envelope)
}

func (s *Store) putCardDAVPreparedEnvelopeTx(ctx context.Context, tx *loggedTx, bookID, personID int64, href string, envelope vcard.ResourceEnvelope) error {
	canonicalUID, err := vcardCanonicalUIDTx(ctx, tx, personID)
	if err != nil {
		return err
	}
	if envelope.SourceRef != fmt.Sprintf("carddav:%d", bookID) || envelope.SourceResourceUID != href || envelope.CanonicalPersonUID != canonicalUID {
		return ErrCardDAVPublicationMismatch
	}
	prepared, err := prepareVCardEnvelope(envelope)
	if err != nil {
		return err
	}
	current, err := s.findVCardResourceEnvelopeTx(ctx, tx, envelope.SourceRef, href)
	if errors.Is(err, ErrVCardResourceNotFound) {
		_, err = s.insertVCardResourceEnvelopeTx(ctx, tx,
			VCardResourceEnvelopeInput{PersonID: personID}, prepared)
		return err
	}
	if err != nil {
		return err
	}
	expected := current.Revision
	_, err = s.updateVCardResourceEnvelopeTx(ctx, tx, VCardResourceEnvelopeInput{
		PersonID: personID, ExpectedRevision: &expected,
	}, prepared, current)
	return err
}

func (s *Store) removeCardDAVResourceTx(
	ctx context.Context, tx *loggedTx, bookID int64, href string,
) (bool, error) {
	return s.removeCardDAVResourceWithPersonRetentionTx(ctx, tx, bookID, href, false)
}

func (s *Store) removeCardDAVResourceWithPersonRetentionTx(
	ctx context.Context, tx *loggedTx, bookID int64, href string, retainPerson bool,
) (bool, error) {
	return s.removeCardDAVResourceWithModeTx(
		ctx, tx, bookID, href, retainPerson, cardDAVRemovalPreserveProjection,
	)
}

type cardDAVResourceRemovalMode uint8

const (
	cardDAVRemovalPreserveProjection cardDAVResourceRemovalMode = iota
	cardDAVRemovalRetireProjection
)

func (s *Store) removeCardDAVResourceWithModeTx(
	ctx context.Context, tx *loggedTx, bookID int64, href string, retainPerson bool,
	mode cardDAVResourceRemovalMode,
) (bool, error) {
	resource, err := s.findCardDAVResourceTx(ctx, tx, bookID, href)
	if errors.Is(err, ErrCardDAVResourceNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	cleanupImportedPerson := false
	personHasUserOwnedState := false
	if !retainPerson && resource.PersonID != nil && resource.PersonRevisionAtBind != nil {
		cleanupImportedPerson, err = s.personHasCardDAVImportedProjectionTx(
			ctx, tx, *resource.PersonID,
		)
		if err != nil {
			return false, err
		}
	}
	if cleanupImportedPerson {
		personHasUserOwnedState, err = s.personHasUserOwnedStateTx(
			ctx, tx, *resource.PersonID, *resource.PersonRevisionAtBind,
		)
		if err != nil {
			return false, err
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM identity_match_candidates
		WHERE left_kind = ? AND left_id = ?`, IdentityMatchCardDAVResource, resource.ID); err != nil {
		return false, fmt.Errorf("delete CardDAV identity candidates: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM vcard_resource_envelopes
		WHERE source_ref = ? AND source_resource_uid = ?`, fmt.Sprintf("carddav:%d", bookID), href); err != nil {
		return false, fmt.Errorf("delete CardDAV resource envelope: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM carddav_resources WHERE id = ?`, resource.ID); err != nil {
		return false, fmt.Errorf("delete CardDAV resource ledger row: %w", err)
	}
	if err := s.transferCardDAVImportedPersonCleanupBaselineTx(
		ctx, tx, resource, personHasUserOwnedState,
	); err != nil {
		return false, err
	}
	if mode == cardDAVRemovalRetireProjection && personHasUserOwnedState {
		if err := s.retireCardDAVImportedProjectionTx(
			ctx, tx, bookID, *resource.PersonID, href,
		); err != nil {
			return false, err
		}
	}
	if !retainPerson {
		if err := s.deleteUntouchedCardDAVImportedPersonTx(
			ctx, tx, resource, cleanupImportedPerson, personHasUserOwnedState,
		); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (s *Store) demoteCardDAVBookResourcesTx(ctx context.Context, tx *loggedTx, bookID int64) error {
	rows, err := tx.QueryContext(ctx, `SELECT href FROM carddav_resources
		WHERE address_book_id = ? ORDER BY href`, bookID)
	if err != nil {
		return fmt.Errorf("list CardDAV resources for demotion: %w", err)
	}
	var hrefs []string
	for rows.Next() {
		var href string
		if err := rows.Scan(&href); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan CardDAV resource for demotion: %w", err)
		}
		hrefs = append(hrefs, href)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterate CardDAV resources for demotion: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close CardDAV resources for demotion: %w", err)
	}
	for _, href := range hrefs {
		resource, err := s.findCardDAVResourceTx(ctx, tx, bookID, href)
		if err != nil {
			return err
		}
		cleanupImportedPerson := false
		personHasUserOwnedState := false
		if resource.PersonID != nil && resource.PersonRevisionAtBind != nil {
			cleanupImportedPerson, err = s.personHasCardDAVImportedProjectionTx(
				ctx, tx, *resource.PersonID,
			)
			if err != nil {
				return err
			}
		}
		if cleanupImportedPerson {
			personHasUserOwnedState, err = s.personHasUserOwnedStateTx(
				ctx, tx, *resource.PersonID, *resource.PersonRevisionAtBind,
			)
			if err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM identity_match_candidates
			WHERE left_kind = ? AND left_id = ? AND source = ?
			  AND state IN (?, ?) AND decided_at IS NULL`,
			IdentityMatchCardDAVResource, resource.ID, ProvenanceCardDAVImport,
			IdentityMatchStateCandidate, IdentityMatchStateConflict); err != nil {
			return fmt.Errorf("delete demoted CardDAV identity candidates: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM vcard_resource_envelopes
			WHERE source_ref = ? AND source_resource_uid = ?`, fmt.Sprintf("carddav:%d", bookID), href); err != nil {
			return fmt.Errorf("delete demoted CardDAV resource envelope: %w", err)
		}
		demotedStatus := CardDAVMappingUnbound
		if resource.MappingStatus == CardDAVMappingMapped && resource.PersonID == nil {
			demotedStatus = CardDAVMappingMapped
		}
		if _, err := tx.ExecContext(ctx, `UPDATE carddav_resources SET
			mapping_status = ?, mapping_revision = mapping_revision + 1,
			governance = ?, person_id = NULL, person_revision_at_bind = NULL,
			updated_at = `+s.dialect.Now()+` WHERE id = ?`,
			demotedStatus, CardDAVGovernanceNone, resource.ID); err != nil {
			return fmt.Errorf("demote CardDAV resource: %w", err)
		}
		if err := s.transferCardDAVImportedPersonCleanupBaselineTx(
			ctx, tx, resource, personHasUserOwnedState,
		); err != nil {
			return err
		}
		if err := s.deleteUntouchedCardDAVImportedPersonTx(
			ctx, tx, resource, cleanupImportedPerson, personHasUserOwnedState,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) dropCardDAVBookResourcesTx(ctx context.Context, tx *loggedTx, bookID int64) error {
	return s.dropCardDAVBookResourcesWithModeTx(
		ctx, tx, bookID, cardDAVRemovalPreserveProjection,
	)
}

func (s *Store) dropCardDAVBookResourcesForIdentityChangeTx(
	ctx context.Context, tx *loggedTx, bookID int64,
) error {
	return s.dropCardDAVBookResourcesWithModeTx(
		ctx, tx, bookID, cardDAVRemovalRetireProjection,
	)
}

func (s *Store) dropCardDAVBookResourcesWithModeTx(
	ctx context.Context, tx *loggedTx, bookID int64, mode cardDAVResourceRemovalMode,
) error {
	rows, err := tx.QueryContext(ctx, `SELECT href FROM carddav_resources
		WHERE address_book_id = ? ORDER BY href`, bookID)
	if err != nil {
		return fmt.Errorf("list ignored CardDAV resources: %w", err)
	}
	var hrefs []string
	for rows.Next() {
		var href string
		if err := rows.Scan(&href); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan ignored CardDAV resource: %w", err)
		}
		hrefs = append(hrefs, href)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterate ignored CardDAV resources: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close ignored CardDAV resources: %w", err)
	}
	for _, href := range hrefs {
		if _, err := s.removeCardDAVResourceWithModeTx(
			ctx, tx, bookID, href, false, mode,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) deleteUntouchedCardDAVImportedPersonTx(
	ctx context.Context, tx *loggedTx, resource *CardDAVResource,
	cleanupImportedPerson, personHasUserOwnedState bool,
) error {
	if resource.PersonID == nil || resource.PersonRevisionAtBind == nil ||
		!cleanupImportedPerson || personHasUserOwnedState {
		return nil
	}
	var revision, otherMappings int64
	err := tx.QueryRowContext(ctx, `SELECT revision,
		(SELECT COUNT(*) FROM carddav_resources WHERE person_id = persons.id)
		FROM persons WHERE id = ?`, *resource.PersonID).Scan(&revision, &otherMappings)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check CardDAV imported person tombstone: %w", err)
	}
	if revision != *resource.PersonRevisionAtBind || otherMappings != 0 {
		return nil
	}
	if err := s.deleteIdentityMatchCandidatesForPersonTx(ctx, tx, *resource.PersonID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM persons WHERE id = ? AND revision = ?`,
		*resource.PersonID, revision); err != nil {
		return fmt.Errorf("delete untouched CardDAV imported person: %w", err)
	}
	return nil
}

func (s *Store) personHasCardDAVImportedProjectionTx(
	ctx context.Context, tx *loggedTx, personID int64,
) (bool, error) {
	var imported bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM person_names WHERE person_id = ? AND source = ?
		UNION ALL
		SELECT 1 FROM person_contact_points WHERE person_id = ? AND source = ?
	)`, personID, ProvenanceCardDAVImport, personID, ProvenanceCardDAVImport).Scan(&imported)
	if err != nil {
		return false, fmt.Errorf("check CardDAV imported person projection: %w", err)
	}
	return imported, nil
}

// When a remotely governed resource disappears before another mapping for the
// same imported person, carry its cleanup baseline forward. A nil baseline is
// deliberately sticky once user-owned state is observed, so removing the last
// duplicate cannot later erase that state using a newer local bind revision.
func (s *Store) transferCardDAVImportedPersonCleanupBaselineTx(
	ctx context.Context, tx *loggedTx, resource *CardDAVResource, personHasUserOwnedState bool,
) error {
	if resource.PersonID == nil || resource.Governance != CardDAVGovernanceRemote ||
		resource.PersonRevisionAtBind == nil {
		return nil
	}
	var baseline any = *resource.PersonRevisionAtBind
	if personHasUserOwnedState {
		baseline = nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE carddav_resources
		SET person_revision_at_bind = ?, updated_at = `+s.dialect.Now()+`
		WHERE person_id = ?`, baseline, *resource.PersonID); err != nil {
		return fmt.Errorf("transfer CardDAV imported person cleanup baseline: %w", err)
	}
	return nil
}

func (s *Store) GetCardDAVResourceContext(
	ctx context.Context, bookID int64, href string,
) (*CardDAVResource, error) {
	resource, err := scanCardDAVResource(s.db.QueryRowContext(ctx,
		cardDAVResourceSelect+` WHERE address_book_id = ? AND href = ?`, bookID, href))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrCardDAVResourceNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get CardDAV resource: %w", err)
	}
	return resource, nil
}

// GetCardDAVResourceForPersonContext returns the person's mapping in one
// address book. Task 6 will tighten role transitions; publication already
// scopes this lookup to the configured write target.
func (s *Store) GetCardDAVResourceForPersonContext(
	ctx context.Context, bookID, personID int64,
) (*CardDAVResource, error) {
	return findCardDAVResourceForPersonTx(ctx, s.db, bookID, personID, "")
}

func (s *Store) ListCardDAVResourcesContext(
	ctx context.Context, bookID int64,
) ([]CardDAVResource, error) {
	rows, err := s.db.QueryContext(ctx, cardDAVResourceSelect+`
		WHERE address_book_id = ? ORDER BY href`, bookID)
	if err != nil {
		return nil, fmt.Errorf("list CardDAV resources: %w", err)
	}
	defer func() { _ = rows.Close() }()
	resources := []CardDAVResource{}
	for rows.Next() {
		resource, err := scanCardDAVResource(rows)
		if err != nil {
			return nil, fmt.Errorf("scan CardDAV resource list: %w", err)
		}
		resources = append(resources, *resource)
	}
	return resources, rows.Err()
}

func (s *Store) findCardDAVResourceTx(
	ctx context.Context, tx *loggedTx, bookID int64, href string,
) (*CardDAVResource, error) {
	resource, err := scanCardDAVResource(tx.QueryRowContext(ctx,
		cardDAVResourceSelect+` WHERE address_book_id = ? AND href = ?`+
			s.dialect.SelectForUpdate(), bookID, href))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrCardDAVResourceNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("find CardDAV resource: %w", err)
	}
	return resource, nil
}

const cardDAVResourceSelect = `SELECT id, address_book_id, href, remote_uid,
	remote_etag, remote_body, remote_semantic_hash, local_hash, mapping_status,
	mapping_revision, governance, person_id, person_revision_at_bind,
	created_at, updated_at FROM carddav_resources`

func scanCardDAVResource(row scanner) (*CardDAVResource, error) {
	var resource CardDAVResource
	var uid sql.NullString
	var personID, personRevision sql.NullInt64
	if err := row.Scan(&resource.ID, &resource.AddressBookID, &resource.Href, &uid,
		&resource.RemoteETag, &resource.RemoteBody, &resource.RemoteSemanticHash,
		&resource.LocalHash, &resource.MappingStatus, &resource.MappingRevision,
		&resource.Governance, &personID, &personRevision,
		&resource.CreatedAt, &resource.UpdatedAt); err != nil {
		return nil, err
	}
	resource.RemoteUID = uid.String
	if personID.Valid {
		resource.PersonID = &personID.Int64
	}
	if personRevision.Valid {
		resource.PersonRevisionAtBind = &personRevision.Int64
	}
	resource.RemoteBody = append([]byte(nil), resource.RemoteBody...)
	return &resource, nil
}
