package carddav

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/vcard"
)

func conflictCard(uid, name string) []byte {
	return []byte("BEGIN:VCARD\r\nVERSION:4.0\r\nUID:" + uid + "\r\nFN:" + name + "\r\nEND:VCARD\r\n")
}

func conflictCardWithEmail(uid, name string, emails ...string) []byte {
	var card strings.Builder
	card.WriteString("BEGIN:VCARD\r\nVERSION:4.0\r\nUID:" + uid + "\r\nFN:" + name + "\r\n")
	for _, email := range emails {
		card.WriteString("EMAIL:" + email + "\r\n")
	}
	card.WriteString("END:VCARD\r\n")
	return []byte(card.String())
}

func escapedCardData(body []byte) string {
	value := strings.ReplaceAll(string(body), "&", "&amp;")
	value = strings.ReplaceAll(value, "<", "&lt;")
	return strings.ReplaceAll(value, ">", "&gt;")
}

func TestPullConflictBlocksOnlyMappingAndAdvancesBookFence(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	var mu sync.Mutex
	cards := map[string]struct {
		body []byte
		etag string
	}{
		"alice": {body: conflictCard("alice", "Alice Base"), etag: `"alice-1"`},
		"bob":   {body: conflictCard("bob", "Bob Base"), etag: `"bob-1"`},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		var events strings.Builder
		for _, uid := range []string{"alice", "bob"} {
			card := cards[uid]
			events.WriteString(cardResponseRaw("/books/personal/"+uid+".vcf", card.etag, escapedCardData(card.body)))
		}
		writeDAVXML(t, w, syncResponse(events.String(), ""))
	}))
	t.Cleanup(server.Close)
	service, st, book := newPullService(t, server, false)

	_, err := service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	books, err := st.ListCardDAVAddressBooksContext(t.Context(), store.AllCardDAVAccounts)
	require.NoError(err)
	beforeRevision := books[0].SyncRevision
	alice, err := st.GetCardDAVResourceContext(t.Context(), book.ID, server.URL+"/books/personal/alice.vcf")
	require.NoError(err)
	require.NotNil(alice.PersonID)
	bob, err := st.GetCardDAVResourceContext(t.Context(), book.ID, server.URL+"/books/personal/bob.vcf")
	require.NoError(err)
	_, err = st.AddPersonContactPointContext(t.Context(), *alice.PersonID, store.PersonContactPointInput{
		AddressKind: store.ContactAddressEmail, OriginalValue: "alice-local@example.test",
		Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
	})
	require.NoError(err)

	mu.Lock()
	cards["alice"] = struct {
		body []byte
		etag string
	}{body: conflictCard("alice", "Alice Remote"), etag: `"alice-2"`}
	cards["bob"] = struct {
		body []byte
		etag string
	}{body: conflictCard("bob", "Bob Remote"), etag: `"bob-2"`}
	mu.Unlock()

	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	books, err = st.ListCardDAVAddressBooksContext(t.Context(), store.AllCardDAVAccounts)
	require.NoError(err)
	assert.Equal(beforeRevision+1, books[0].SyncRevision)

	afterAlice, err := st.GetCardDAVResourceContext(t.Context(), book.ID, alice.Href)
	require.NoError(err)
	assert.Equal(alice.RemoteBody, afterAlice.RemoteBody, "conflicted mapping must retain its last common remote state")
	afterBob, err := st.GetCardDAVResourceContext(t.Context(), book.ID, bob.Href)
	require.NoError(err)
	assert.Equal(cards["bob"].body, afterBob.RemoteBody, "unrelated mapping must continue applying")

	conflicts, err := service.ListConflicts(t.Context())
	require.NoError(err)
	require.Len(conflicts, 1)
	assert.Equal(alice.Href, conflicts[0].Href)
	assert.Contains(string(conflicts[0].LocalBody), "EMAIL:alice-local@example.test")
	assert.Equal(cards["alice"].body, conflicts[0].RemoteBody)

	views, err := service.ListConflictViews(t.Context())
	require.NoError(err)
	require.Len(views, 1)
	assert.Equal(book.ID, views[0].AddressBook.ID)
	assert.NotContains(views[0].AddressBook.Name, "http")
	assert.Equal(ConflictSidePresent, views[0].LocalState)
	assert.Equal(ConflictSidePresent, views[0].RemoteState)
	assert.Equal([]ResolutionChoice{ResolutionKeepLocal, ResolutionKeepRemote}, views[0].AllowedResolutions)

	detail, err := service.GetConflictView(t.Context(), conflicts[0].ID)
	require.NoError(err)
	assert.Equal(ConflictSidePresent, detail.Base.State)
	assert.Equal("Alice Base", detail.Base.DisplayName)
	assert.Equal("Alice Remote", detail.Remote.DisplayName)
	assert.Contains(detail.Local.Emails, "alice-local@example.test")
}

func TestPullConflictCapturesLocalEditAgainstRemoteDelete(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	var mu sync.Mutex
	deleted := false
	body := conflictCard("alice", "Alice Base")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodGet {
			if deleted {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("ETag", `"one"`)
			_, err := w.Write(body)
			assert.NoError(err)
			return
		}
		events := ""
		if !deleted {
			events = cardResponseRaw("/books/personal/alice.vcf", `"one"`, escapedCardData(body))
		}
		writeDAVXML(t, w, syncResponse(events, ""))
	}))
	t.Cleanup(server.Close)
	service, st, book := newPullService(t, server, false)
	_, err := service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	mapping, err := st.GetCardDAVResourceContext(t.Context(), book.ID, server.URL+"/books/personal/alice.vcf")
	require.NoError(err)
	require.NotNil(mapping.PersonID)
	_, err = st.DB().Exec(st.Rebind(`INSERT INTO carddav_publications
		(person_id, desired, address_book_id, href) VALUES (?, TRUE, ?, ?)`),
		*mapping.PersonID, book.ID, mapping.Href)
	require.NoError(err)
	_, err = st.AddPersonContactPointContext(t.Context(), *mapping.PersonID, store.PersonContactPointInput{
		AddressKind: store.ContactAddressEmail, OriginalValue: "alice-local@example.test",
		Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
	})
	require.NoError(err)
	mu.Lock()
	deleted = true
	mu.Unlock()

	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	conflicts, err := service.ListConflicts(t.Context())
	require.NoError(err)
	require.Len(conflicts, 1)
	assert.True(conflicts[0].RemoteTombstone)
	assert.False(conflicts[0].LocalTombstone)
	_, err = st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
	require.NoError(err, "conflict must retain the mapping")
	require.NoError(service.ResolveConflict(t.Context(), conflicts[0].ID, ResolutionKeepRemote))
	_, err = st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
	require.ErrorIs(err, store.ErrCardDAVResourceNotFound)
	_, err = st.GetCardDAVPublicationContext(t.Context(), *mapping.PersonID)
	require.ErrorIs(err, store.ErrCardDAVPublicationNotFound)
}

func TestPullConflictCapturesLocalDeleteAgainstRemoteEdit(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	var mu sync.Mutex
	body := conflictCard("alice", "Alice Base")
	etag := `"one"`
	remoteDeleted := false
	deletes := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case "REPORT":
			events := ""
			if !remoteDeleted {
				events = cardResponseRaw("/books/personal/alice.vcf", etag, escapedCardData(body))
			}
			writeDAVXML(t, w, syncResponse(events, ""))
		case http.MethodGet:
			if remoteDeleted {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("ETag", etag)
			_, err := w.Write(body)
			assert.NoError(err)
		case http.MethodDelete:
			deletes++
			assert.Equal(etag, r.Header.Get("If-Match"))
			remoteDeleted = true
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	service, st, book := newPullService(t, server, false)
	_, err := service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	mapping, err := st.GetCardDAVResourceContext(t.Context(), book.ID, server.URL+"/books/personal/alice.vcf")
	require.NoError(err)
	require.NotNil(mapping.PersonID)
	person, err := st.GetPersonContext(t.Context(), *mapping.PersonID)
	require.NoError(err)
	require.NoError(st.DeletePersonContext(t.Context(), person.ID, person.Revision))
	mu.Lock()
	body = conflictCard("alice", "Alice Remote")
	etag = `"two"`
	mu.Unlock()

	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	conflicts, err := service.ListConflicts(t.Context())
	require.NoError(err)
	require.Len(conflicts, 1)
	assert.True(conflicts[0].LocalTombstone)
	assert.False(conflicts[0].RemoteTombstone)
	assert.Equal(body, conflicts[0].RemoteBody)
	require.NoError(service.ResolveConflict(t.Context(), conflicts[0].ID, ResolutionKeepLocal))
	assert.Equal(1, deletes)
	_, err = st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
	require.ErrorIs(err, store.ErrCardDAVResourceNotFound)
}

func TestPullAppliesEquivalentConcurrentOutcomesWithoutConflict(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	var mu sync.Mutex
	body := conflictCard("alice", "Alice Base")
	etag := `"one"`
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method != "REPORT" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		responses := ""
		if !deleted {
			responses = cardResponseRaw("/books/personal/alice.vcf", etag, escapedCardData(body))
		}
		writeDAVXML(t, w, syncResponse(responses, ""))
	}))
	t.Cleanup(server.Close)
	service, st, book := newPullService(t, server, false)
	_, err := service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	mapping, err := st.GetCardDAVResourceContext(t.Context(), book.ID, server.URL+"/books/personal/alice.vcf")
	require.NoError(err)
	require.NotNil(mapping.PersonID)
	personID := *mapping.PersonID
	_, err = st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
		AddressKind: store.ContactAddressEmail, OriginalValue: "alice@example.test",
		Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
	})
	require.NoError(err)
	person, err := st.GetPersonContext(t.Context(), personID)
	require.NoError(err)
	localBody, _, err := service.renderPublicationCard(t.Context(), *person, book, mapping)
	require.NoError(err)
	mu.Lock()
	body, etag = localBody, `"two"`
	mu.Unlock()

	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	conflicts, err := service.ListConflicts(t.Context())
	require.NoError(err)
	assert.Empty(conflicts)
	mapping, err = st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
	require.NoError(err)
	assert.Equal(`"two"`, mapping.RemoteETag)
	person, err = st.GetPersonContext(t.Context(), personID)
	require.NoError(err)
	require.NoError(st.DeletePersonContext(t.Context(), personID, person.Revision))
	mu.Lock()
	deleted = true
	mu.Unlock()

	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	conflicts, err = service.ListConflicts(t.Context())
	require.NoError(err)
	assert.Empty(conflicts)
	_, err = st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
	require.ErrorIs(err, store.ErrCardDAVResourceNotFound)
}

func TestKeepRemoteReimportsSubscribedCardAfterLocalDeleteWithoutPublicationReplay(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &conflictMutationServer{}
	server := httptest.NewServer(fixture.handler(t))
	t.Cleanup(server.Close)
	service, st, personID, book := seededMutationServiceForServer(t, server)
	fixture.body = conflictCard("person", "Alice Base")
	fixture.etag = `"remote-base"`
	_, err := service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	person, err := st.GetPersonContext(t.Context(), personID)
	require.NoError(err)
	require.NoError(st.DeletePersonContext(t.Context(), personID, person.Revision))
	latest := conflictCard("person", "Alice Retained")
	fixture.mu.Lock()
	fixture.body = latest
	fixture.etag = `"remote-retained"`
	fixture.mu.Unlock()

	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	conflicts, err := service.ListConflicts(t.Context())
	require.NoError(err)
	require.Len(conflicts, 1)
	require.NoError(service.ResolveConflict(t.Context(), conflicts[0].ID, ResolutionKeepRemote))
	mapping, err := st.GetCardDAVResourceContext(t.Context(), book.ID, book.CanonicalURL+"person.vcf")
	require.NoError(err)
	require.NotNil(mapping.PersonID)
	assert.NotEqual(personID, *mapping.PersonID)
	assert.Equal(store.CardDAVGovernanceRemote, mapping.Governance)
	fixture.mu.Lock()
	requestsBeforeReconcile := fixture.puts + fixture.deletes + fixture.gets + fixture.reports
	fixture.mu.Unlock()
	require.NoError(service.ReconcilePublications(t.Context()))
	fixture.mu.Lock()
	assert.Equal(requestsBeforeReconcile, fixture.puts+fixture.deletes+fixture.gets+fixture.reports)
	fixture.mu.Unlock()
}

func TestKeepRemoteRebasesBoundImportedProjectionWithoutPublicationReplay(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &conflictMutationServer{
		body: conflictCardWithEmail(
			"person", "Alice Remote Base", "alice.base@example.test",
		),
		etag: `"remote-1"`,
	}
	server := httptest.NewServer(fixture.handler(t))
	t.Cleanup(server.Close)
	service, st, book := newPullService(t, server, false)

	_, err := service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	mapping, err := st.GetCardDAVResourceContext(
		t.Context(), book.ID, book.CanonicalURL+"person.vcf",
	)
	require.NoError(err)
	require.NotNil(mapping.PersonID)
	personID := *mapping.PersonID
	person, err := st.GetPersonContext(t.Context(), personID)
	require.NoError(err)
	_, err = st.UpdatePersonDisplayNameContext(
		t.Context(), personID, person.Revision, new("Alice Local Label"),
	)
	require.NoError(err)
	_, err = st.AddPersonNameContext(t.Context(), personID, store.PersonNameInput{
		NameKind: store.PersonNameSort, SortAs: new("Local Sort Key"),
		OriginalValue: "Local Sort Key",
		Envelope:      store.ValueEnvelopeInput{Source: store.ProvenanceUser},
	})
	require.NoError(err)
	_, err = st.DB().Exec(st.Rebind(`INSERT INTO carddav_publications
		(person_id, desired, address_book_id, href) VALUES (?, TRUE, ?, ?)`),
		personID, book.ID, mapping.Href)
	require.NoError(err)

	retained := conflictCardWithEmail(
		"person", "Alice Remote Retained", "alice.retained@example.test",
	)
	fixture.mu.Lock()
	fixture.body = retained
	fixture.etag = `"remote-2"`
	fixture.mu.Unlock()
	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	conflicts, err := service.ListConflicts(t.Context())
	require.NoError(err)
	require.Len(conflicts, 1)

	require.NoError(service.ResolveConflict(
		t.Context(), conflicts[0].ID, ResolutionKeepRemote,
	))
	person, err = st.GetPersonContext(t.Context(), personID)
	require.NoError(err)
	require.NotNil(person.DisplayName)
	assert.Equal("Alice Local Label", *person.DisplayName,
		"an explicit local display label must not be overwritten")
	names, err := st.ListPersonNamesContext(t.Context(), personID, true)
	require.NoError(err)
	require.Len(names, 2)
	remoteName, userName := names[0], names[1]
	if remoteName.Envelope.Source == store.ProvenanceUser {
		remoteName, userName = userName, remoteName
	}
	require.NotNil(remoteName.Formatted)
	assert.Equal("Alice Remote Retained", *remoteName.Formatted)
	assert.Equal(store.ProvenanceCardDAVImport, remoteName.Envelope.Source)
	require.NotNil(userName.SortAs)
	assert.Equal("Local Sort Key", *userName.SortAs)
	assert.Equal(store.ProvenanceUser, userName.Envelope.Source)
	points, err := st.ListPersonContactPointsContext(t.Context(), personID, true)
	require.NoError(err)
	require.Len(points, 1)
	assert.Equal("alice.retained@example.test", points[0].OriginalValue)
	assert.Equal(store.ProvenanceCardDAVImport, points[0].Envelope.Source)

	mapping, err = st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
	require.NoError(err)
	snapshot, err := st.LoadPersonVCardSnapshotContext(t.Context(), personID)
	require.NoError(err)
	assert.Equal(snapshot.Fingerprint, mapping.LocalHash)
	publication, err := st.GetCardDAVPublicationContext(t.Context(), personID)
	require.NoError(err)
	assert.True(publication.Desired)
	assert.Empty(publication.PendingOperation)

	fixture.mu.Lock()
	putsBefore, deletesBefore := fixture.puts, fixture.deletes
	getsBefore, reportsBefore := fixture.gets, fixture.reports
	fixture.mu.Unlock()
	require.NoError(service.ReconcilePublications(t.Context()))
	fixture.mu.Lock()
	assert.Equal(putsBefore, fixture.puts)
	assert.Equal(deletesBefore, fixture.deletes)
	assert.Equal(getsBefore, fixture.gets)
	assert.Equal(reportsBefore, fixture.reports)
	fixture.mu.Unlock()
}

func TestKeepRemoteRebasesImportedPersonCleanupBaseline(t *testing.T) {
	tests := []struct {
		name               string
		makeLocalChange    func(t *testing.T, st *store.Store, personID int64)
		wantPersonRetained bool
	}{
		{
			name: "discarded imported projection edit advances cleanup baseline",
			makeLocalChange: func(t *testing.T, st *store.Store, personID int64) {
				t.Helper()
				points, err := st.ListPersonContactPointsContext(t.Context(), personID, true)
				require.NoError(t, err)
				require.Len(t, points, 1)
				require.NoError(t, st.SupersedePersonContactPointContext(
					t.Context(), personID, points[0].Envelope.ID, nil,
				))
			},
		},
		{
			name: "explicit user state keeps cleanup baseline",
			makeLocalChange: func(t *testing.T, st *store.Store, personID int64) {
				t.Helper()
				_, err := st.AddPersonNameContext(t.Context(), personID, store.PersonNameInput{
					NameKind: store.PersonNameSort, SortAs: new("User Sort Key"),
					OriginalValue: "User Sort Key",
					Envelope:      store.ValueEnvelopeInput{Source: store.ProvenanceUser},
				})
				require.NoError(t, err)
			},
			wantPersonRetained: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			fixture := &conflictMutationServer{
				body: conflictCardWithEmail(
					"person", "Alice Remote Base", "alice.base@example.test",
				),
				etag: `"remote-1"`,
			}
			server := httptest.NewServer(fixture.handler(t))
			t.Cleanup(server.Close)
			service, st, book := newPullService(t, server, false)

			_, err := service.Sync(t.Context(), SyncOptions{Full: true})
			require.NoError(err)
			mapping, err := st.GetCardDAVResourceContext(
				t.Context(), book.ID, book.CanonicalURL+"person.vcf",
			)
			require.NoError(err)
			require.NotNil(mapping.PersonID)
			personID := *mapping.PersonID
			tt.makeLocalChange(t, st, personID)

			fixture.mu.Lock()
			fixture.body = conflictCardWithEmail(
				"person", "Alice Remote Retained", "alice.retained@example.test",
			)
			fixture.etag = `"remote-2"`
			fixture.mu.Unlock()
			_, err = service.Sync(t.Context(), SyncOptions{Full: true})
			require.NoError(err)
			conflicts, err := service.ListConflicts(t.Context())
			require.NoError(err)
			require.Len(conflicts, 1)
			require.NoError(service.ResolveConflict(
				t.Context(), conflicts[0].ID, ResolutionKeepRemote,
			))

			fixture.mu.Lock()
			fixture.body = nil
			fixture.etag = ""
			fixture.mu.Unlock()
			_, err = service.Sync(t.Context(), SyncOptions{Full: true})
			require.NoError(err)
			_, err = st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
			require.ErrorIs(err, store.ErrCardDAVResourceNotFound)
			person, err := st.GetPersonContext(t.Context(), personID)
			if tt.wantPersonRetained {
				require.NoError(err)
				assert.Equal(t, personID, person.ID)
				return
			}
			require.ErrorIs(err, store.ErrPersonNotFound)
		})
	}
}

type conflictMutationServer struct {
	mu                  sync.Mutex
	body                []byte
	etag                string
	force412            bool
	delete412           bool
	deleteStatus        int
	timeoutPut          bool
	timeoutDelete       bool
	puts                int
	deletes             int
	gets                int
	reports             int
	lastPutBody         []byte
	lastIfMatch         string
	syncToken           string
	deleteRaceBody      []byte
	deleteRaceETag      string
	deleteRaceTombstone bool
	getFailures         int
	onGet               func(int)
}

func (f *conflictMutationServer) handler(t *testing.T) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			f.puts++
			body, err := io.ReadAll(r.Body)
			assert.NoError(t, err)
			f.lastPutBody = append([]byte(nil), body...)
			f.lastIfMatch = r.Header.Get("If-Match")
			if f.force412 {
				f.force412 = false
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			if len(f.body) == 0 {
				assert.Equal(t, "*", r.Header.Get("If-None-Match"))
			} else {
				assert.Equal(t, f.etag, r.Header.Get("If-Match"))
			}
			f.body = append([]byte(nil), body...)
			f.etag = `"server-` + string(rune('0'+f.puts)) + `"`
			if f.timeoutPut {
				f.timeoutPut = false
				f.mu.Unlock()
				<-r.Context().Done()
				f.mu.Lock()
				return
			}
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			f.gets++
			if f.onGet != nil {
				f.onGet(f.gets)
			}
			if f.getFailures > 0 {
				f.getFailures--
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			if len(f.body) == 0 {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("ETag", f.etag)
			_, err := w.Write(f.body)
			assert.NoError(t, err)
		case http.MethodDelete:
			f.deletes++
			f.lastIfMatch = r.Header.Get("If-Match")
			if f.deleteStatus != 0 {
				if f.deleteStatus == http.StatusTooManyRequests {
					w.Header().Set("Retry-After", "3600")
				}
				w.WriteHeader(f.deleteStatus)
				return
			}
			if f.delete412 {
				f.delete412 = false
				if f.deleteRaceTombstone {
					f.body = nil
					f.etag = ""
				} else if len(f.deleteRaceBody) > 0 {
					f.body = append([]byte(nil), f.deleteRaceBody...)
					f.etag = f.deleteRaceETag
				}
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			assert.Equal(t, f.etag, r.Header.Get("If-Match"))
			f.body = nil
			f.etag = ""
			if f.timeoutDelete {
				f.timeoutDelete = false
				f.mu.Unlock()
				<-r.Context().Done()
				f.mu.Lock()
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case "REPORT":
			f.reports++
			events := ""
			if len(f.body) > 0 {
				events = cardResponseRaw("/books/personal/person.vcf", f.etag, escapedCardData(f.body))
			}
			writeDAVXML(t, w, syncResponse(events, f.syncToken))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func oversizedConflictCard(uid string) []byte {
	prefix := []byte("BEGIN:VCARD\r\nVERSION:4.0\r\nUID:" + uid + "\r\nFN:Oversized\r\n")
	suffix := []byte("END:VCARD\r\n")
	body := make([]byte, 0, store.MaxCardDAVConflictSnapshotBytes)
	body = append(body, prefix...)
	remaining := store.MaxCardDAVConflictSnapshotBytes - len(prefix) - len(suffix)
	for remaining > 0 {
		const overhead = len("NOTE:\r\n")
		chunk := min(1<<20, remaining-overhead)
		body = append(body, "NOTE:"...)
		body = append(body, bytes.Repeat([]byte("x"), chunk)...)
		body = append(body, '\r', '\n')
		remaining -= overhead + chunk
	}
	body = append(body, suffix...)
	return body
}

func TestConditional412CapturesConflictAndKeepLocalRefetchesCurrentETag(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &conflictMutationServer{}
	server := httptest.NewServer(fixture.handler(t))
	t.Cleanup(server.Close)
	service, st, personID, book := seededMutationServiceForServer(t, server)
	service.dav().client.requestTimeout = 250 * time.Millisecond
	require.NoError(service.PublishPerson(t.Context(), personID))
	_, err := st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
		AddressKind: store.ContactAddressEmail, OriginalValue: "alice-local@example.test",
		Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
	})
	require.NoError(err)

	fixture.mu.Lock()
	fixture.body = conflictCard("person", "Alice Remote")
	fixture.etag = `"remote-2"`
	fixture.force412 = true
	fixture.mu.Unlock()

	err = service.PublishPerson(t.Context(), personID)
	var conflictErr *ConflictError
	require.ErrorAs(err, &conflictErr)
	publication, getErr := st.GetCardDAVPublicationContext(t.Context(), personID)
	require.NoError(getErr)
	assert.Empty(publication.PendingOperation)
	conflicts, getErr := service.ListConflicts(t.Context())
	require.NoError(getErr)
	require.Len(conflicts, 1)
	assert.Equal(conflictErr.ID, conflicts[0].ID)
	assert.Contains(string(conflicts[0].LocalBody), "EMAIL:alice-local@example.test")
	assert.Equal(conflictCard("person", "Alice Remote"), conflicts[0].RemoteBody)
	fixture.mu.Lock()
	putsBeforeBlockedRetry := fixture.puts
	fixture.mu.Unlock()
	err = service.PublishPerson(t.Context(), personID)
	require.ErrorIs(err, ErrCardDAVConflictPending)
	fixture.mu.Lock()
	assert.Equal(putsBeforeBlockedRetry, fixture.puts, "unresolved mapping must block before network")
	fixture.mu.Unlock()

	fixture.mu.Lock()
	fixture.body = conflictCard("person", "Alice Even Newer")
	fixture.etag = `"remote-3"`
	fixture.timeoutPut = true
	getsBefore := fixture.gets
	fixture.mu.Unlock()
	_, err = st.DB().Exec(st.Rebind(`UPDATE carddav_address_books SET
		is_write_target = FALSE, is_subscribed = TRUE, can_update = TRUE WHERE id = ?`), book.ID)
	require.NoError(err)

	require.Error(service.ResolveConflict(t.Context(), conflictErr.ID, ResolutionKeepLocal))
	fixture.mu.Lock()
	putsAfterTimeout := fixture.puts
	fixture.mu.Unlock()
	require.NoError(service.ResolveConflict(t.Context(), conflictErr.ID, ResolutionKeepLocal))
	fixture.mu.Lock()
	assert.GreaterOrEqual(fixture.gets, getsBefore+2, "resolution must preflight and recover with canonical GETs")
	assert.Equal(putsAfterTimeout, fixture.puts, "ambiguous resolution recovery must not replay PUT")
	assert.Equal(`"remote-3"`, fixture.lastIfMatch)
	assert.Equal(conflicts[0].LocalBody, fixture.lastPutBody)
	fixture.mu.Unlock()
	resolved, err := st.GetCardDAVConflictContext(t.Context(), conflictErr.ID)
	require.NoError(err)
	assert.Equal(store.CardDAVConflictResolved, resolved.Status)
	assert.Equal(store.CardDAVResolutionKeepLocal, resolved.Resolution)
}

func TestSyncSkipsConflictedPublicationAndStillAdvancesToken(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &conflictMutationServer{}
	server := httptest.NewServer(fixture.handler(t))
	t.Cleanup(server.Close)
	service, st, personID, book := seededMutationServiceForServer(t, server)
	require.NoError(service.PublishPerson(t.Context(), personID))
	_, err := st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
		AddressKind: store.ContactAddressEmail, OriginalValue: "alice-local@example.test",
		Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
	})
	require.NoError(err)

	fixture.mu.Lock()
	fixture.body = conflictCard("person", "Alice Remote")
	fixture.etag = `"remote-2"`
	fixture.force412 = true
	fixture.syncToken = "token-after-conflict"
	fixture.mu.Unlock()
	_, err = st.DB().Exec(st.Rebind(`UPDATE carddav_address_books
		SET supports_sync_collection = TRUE WHERE id = ?`), book.ID)
	require.NoError(err)
	var conflictErr *ConflictError
	require.ErrorAs(service.PublishPerson(t.Context(), personID), &conflictErr)
	fixture.mu.Lock()
	putsBeforeSync := fixture.puts
	deletesBeforeSync := fixture.deletes
	fixture.mu.Unlock()

	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	books, err := st.ListCardDAVAddressBooksContext(t.Context(), store.AllCardDAVAccounts)
	require.NoError(err)
	for _, candidate := range books {
		if candidate.ID == book.ID {
			assert.Equal("token-after-conflict", candidate.SyncToken)
		}
	}
	fixture.mu.Lock()
	assert.Equal(putsBeforeSync, fixture.puts, "reconciliation must not replay a conflicted publication")
	assert.Equal(deletesBeforeSync, fixture.deletes)
	fixture.mu.Unlock()
}

func TestSyncAbortsPullAfterPendingPublicationRecoveryFailure(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &conflictMutationServer{}
	server := httptest.NewServer(fixture.handler(t))
	t.Cleanup(server.Close)
	service, st, personID, book := seededMutationServiceForServer(t, server)
	service.dav().client.requestTimeout = 250 * time.Millisecond
	require.NoError(service.PublishPerson(t.Context(), personID))
	_, err := st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
		AddressKind: store.ContactAddressEmail, OriginalValue: "alice-local@example.test",
		Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
	})
	require.NoError(err)
	fixture.mu.Lock()
	fixture.timeoutPut = true
	fixture.mu.Unlock()
	require.Error(service.PublishPerson(t.Context(), personID))
	pending, err := st.GetCardDAVPublicationContext(t.Context(), personID)
	require.NoError(err)
	require.Equal(store.CardDAVMutationUpdate, pending.PendingOperation)

	fixture.mu.Lock()
	fixture.body = conflictCard("person", "Alice Divergent Remote")
	fixture.etag = `"remote-divergent"`
	fixture.getFailures = 1
	reportsBefore := fixture.reports
	fixture.mu.Unlock()

	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	var status *StatusError
	require.ErrorAs(err, &status)
	assert.Equal(http.StatusServiceUnavailable, status.StatusCode)
	fixture.mu.Lock()
	assert.Equal(reportsBefore, fixture.reports, "pull must not advance a mapping after recovery fails")
	fixture.mu.Unlock()
	stillPending, err := st.GetCardDAVPublicationContext(t.Context(), personID)
	require.NoError(err)
	assert.Equal(pending.MappingRevision, stillPending.MappingRevision)
	assert.Equal(pending.PendingOperation, stillPending.PendingOperation)

	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	fixture.mu.Lock()
	assert.Greater(fixture.reports, reportsBefore, "a later successful recovery may proceed to pull")
	fixture.mu.Unlock()
	conflicts, err := service.ListConflicts(t.Context())
	require.NoError(err)
	require.Len(conflicts, 1)
	require.NoError(service.ResolveConflict(t.Context(), conflicts[0].ID, ResolutionKeepLocal))
	resolved, err := st.GetCardDAVConflictContext(t.Context(), conflicts[0].ID)
	require.NoError(err)
	assert.Equal(store.CardDAVConflictResolved, resolved.Status)
	assert.Equal(store.CardDAVResolutionKeepLocal, resolved.Resolution)
	resource, err := st.GetCardDAVResourceContext(t.Context(), book.ID, book.CanonicalURL+"person.vcf")
	require.NoError(err)
	assert.Contains(string(resource.RemoteBody), "EMAIL:alice-local@example.test")
}

func seedLocalTombstoneConflict(
	t *testing.T, fixture *conflictMutationServer,
) (*Service, *store.Store, store.CardDAVAddressBook, *store.CardDAVResource, *store.CardDAVConflict) {
	t.Helper()
	server := httptest.NewServer(fixture.handler(t))
	t.Cleanup(server.Close)
	service, st, personID, book := seededMutationServiceForServer(t, server)
	fixture.body = conflictCard("person", "Alice Base")
	fixture.etag = `"remote-base"`
	_, err := service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(t, err)
	mapping, err := st.GetCardDAVResourceContext(t.Context(), book.ID, book.CanonicalURL+"person.vcf")
	require.NoError(t, err)
	person, err := st.GetPersonContext(t.Context(), personID)
	require.NoError(t, err)
	require.NoError(t, st.DeletePersonContext(t.Context(), personID, person.Revision))
	remoteBody := conflictCard("person", "Alice Remote")
	fixture.mu.Lock()
	fixture.body = append([]byte(nil), remoteBody...)
	fixture.etag = `"remote-race"`
	fixture.mu.Unlock()
	capture := store.CardDAVConflictCapture{
		AddressBookID: book.ID, Href: mapping.Href,
		ExpectedMappingRevision: mapping.MappingRevision,
		BaseLocalHash:           mapping.LocalHash, LocalHash: mapping.LocalHash,
		BaseRemoteHash: mapping.RemoteSemanticHash, BaseRemoteETag: mapping.RemoteETag,
		RemoteETag: `"remote-race"`, RemoteBody: remoteBody,
		LocalTombstone: true,
	}
	conflict, err := st.RecordCardDAVConflictContext(t.Context(), capture)
	require.NoError(t, err)
	return service, st, book, mapping, conflict
}

func seedUnpublishConflict(
	t *testing.T, fixture *conflictMutationServer,
) (*Service, *store.Store, int64, store.CardDAVAddressBook, *store.CardDAVResource, *store.CardDAVConflict) {
	t.Helper()
	server := httptest.NewServer(fixture.handler(t))
	t.Cleanup(server.Close)
	service, st, personID, book := seededMutationServiceForServer(t, server)
	require.NoError(t, service.PublishPerson(t.Context(), personID))
	_, err := st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
		AddressKind: store.ContactAddressEmail, OriginalValue: "alice-local@example.test",
		Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
	})
	require.NoError(t, err)
	fixture.mu.Lock()
	fixture.body = conflictCard("person", "Alice Remote")
	fixture.etag = `"remote-unpublish"`
	fixture.delete412 = true
	fixture.mu.Unlock()

	var conflictErr *ConflictError
	require.ErrorAs(t, service.UnpublishPerson(t.Context(), personID), &conflictErr)
	mapping, err := st.GetCardDAVResourceContext(t.Context(), book.ID, book.CanonicalURL+"person.vcf")
	require.NoError(t, err)
	require.NotNil(t, mapping.PersonID)
	conflict, err := st.GetCardDAVConflictContext(t.Context(), conflictErr.ID)
	require.NoError(t, err)
	require.True(t, conflict.LocalTombstone)
	return service, st, personID, book, mapping, conflict
}

func TestKeepLocalTombstoneTimeoutPersistsIntentAndRecoversWithoutReplay(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &conflictMutationServer{timeoutDelete: true}
	service, st, book, mapping, conflict := seedLocalTombstoneConflict(t, fixture)
	service.dav().client.requestTimeout = 250 * time.Millisecond

	require.Error(service.ResolveConflict(t.Context(), conflict.ID, ResolutionKeepLocal))
	pending, err := st.GetCardDAVConflictContext(t.Context(), conflict.ID)
	require.NoError(err)
	assert.Equal(store.CardDAVMutationDelete, pending.PendingOperation)
	assert.Equal(conflict.MappingRevision+1, pending.MappingRevision)
	assert.Equal(conflict.MappingRevision, pending.PreviousMappingRevision)
	assert.NotNil(pending.PendingStartedAt)
	fixture.mu.Lock()
	deletesAfterTimeout := fixture.deletes
	fixture.mu.Unlock()

	require.NoError(service.ResolveConflict(t.Context(), conflict.ID, ResolutionKeepLocal))
	fixture.mu.Lock()
	assert.Equal(deletesAfterTimeout, fixture.deletes, "ambiguous tombstone recovery must not replay DELETE")
	fixture.mu.Unlock()
	_, err = st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
	require.ErrorIs(err, store.ErrCardDAVResourceNotFound)
	resolved, err := st.GetCardDAVConflictContext(t.Context(), conflict.ID)
	require.NoError(err)
	assert.Equal(store.CardDAVConflictResolved, resolved.Status)
}

func TestPullTombstoneCompletesTimedOutKeepLocalWithoutDeleteReplay(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &conflictMutationServer{timeoutDelete: true, syncToken: "token-after-tombstone"}
	service, st, book, mapping, conflict := seedLocalTombstoneConflict(t, fixture)
	service.dav().client.requestTimeout = 250 * time.Millisecond
	_, err := st.DB().Exec(st.Rebind(`UPDATE carddav_address_books
		SET supports_sync_collection = TRUE WHERE id = ?`), book.ID)
	require.NoError(err)

	require.Error(service.ResolveConflict(t.Context(), conflict.ID, ResolutionKeepLocal))
	fixture.mu.Lock()
	deletesAfterTimeout := fixture.deletes
	fixture.mu.Unlock()

	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	fixture.mu.Lock()
	assert.Equal(deletesAfterTimeout, fixture.deletes, "pull proof must not replay DELETE")
	fixture.mu.Unlock()
	_, err = st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
	require.ErrorIs(err, store.ErrCardDAVResourceNotFound)
	resolved, err := st.GetCardDAVConflictContext(t.Context(), conflict.ID)
	require.NoError(err)
	assert.Equal(store.CardDAVConflictResolved, resolved.Status)
	assert.Equal(store.CardDAVResolutionKeepLocal, resolved.Resolution)
	books, err := st.ListCardDAVAddressBooksContext(t.Context(), store.AllCardDAVAccounts)
	require.NoError(err)
	require.Len(books, 1)
	assert.Equal("token-after-tombstone", books[0].SyncToken)
}

func TestKeepLocalTombstoneDefinitiveRejectionRestoresFence(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &conflictMutationServer{deleteStatus: http.StatusForbidden}
	service, st, book, mapping, conflict := seedLocalTombstoneConflict(t, fixture)
	before, err := st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
	require.NoError(err)

	err = service.ResolveConflict(t.Context(), conflict.ID, ResolutionKeepLocal)
	var status *StatusError
	require.ErrorAs(err, &status)
	assert.Equal(http.StatusForbidden, status.StatusCode)
	refreshed, getErr := st.GetCardDAVConflictContext(t.Context(), conflict.ID)
	require.NoError(getErr)
	assert.Empty(refreshed.PendingOperation)
	assert.Zero(refreshed.PreviousMappingRevision)
	after, getErr := st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
	require.NoError(getErr)
	assert.Equal(before.MappingRevision, after.MappingRevision)

	fixture.deleteStatus = 0
	require.NoError(service.ResolveConflict(t.Context(), conflict.ID, ResolutionKeepLocal))
}

func TestKeepLocalTombstoneThrottleClearsIntentAndPersistsGate(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &conflictMutationServer{deleteStatus: http.StatusTooManyRequests}
	service, st, book, mapping, conflict := seedLocalTombstoneConflict(t, fixture)
	before, err := st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
	require.NoError(err)
	started := time.Now()

	err = service.ResolveConflict(t.Context(), conflict.ID, ResolutionKeepLocal)
	var status *StatusError
	require.ErrorAs(err, &status)
	assert.Equal(http.StatusTooManyRequests, status.StatusCode)
	refreshed, getErr := st.GetCardDAVConflictContext(t.Context(), conflict.ID)
	require.NoError(getErr)
	assert.Empty(refreshed.PendingOperation)
	after, getErr := st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
	require.NoError(getErr)
	assert.Equal(before.MappingRevision, after.MappingRevision)
	gate, getErr := st.GetCardDAVRetryAfterContext(t.Context(), store.DefaultCardDAVAccountID)
	require.NoError(getErr)
	require.NotNil(gate)
	assert.WithinDuration(started.Add(time.Hour), *gate, 5*time.Second)
}

func TestKeepLocalUnpublishConflictRetainsPersonAndClearsPublication(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &conflictMutationServer{}
	service, st, personID, book, mapping, conflict := seedUnpublishConflict(t, fixture)

	require.NoError(service.ResolveConflict(t.Context(), conflict.ID, ResolutionKeepLocal))
	_, err := st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
	require.ErrorIs(err, store.ErrCardDAVResourceNotFound)
	_, err = st.GetCardDAVPublicationContext(t.Context(), personID)
	require.ErrorIs(err, store.ErrCardDAVPublicationNotFound)
	_, err = st.GetPersonContext(t.Context(), personID)
	require.NoError(err)
	resolved, err := st.GetCardDAVConflictContext(t.Context(), conflict.ID)
	require.NoError(err)
	assert.Equal(store.CardDAVConflictResolved, resolved.Status)
	assert.Equal(store.CardDAVResolutionKeepLocal, resolved.Resolution)
}

func TestPullTombstoneCompletesTimedOutUnpublishAndRetainsPerson(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &conflictMutationServer{timeoutDelete: true, syncToken: "token-after-unpublish"}
	service, st, personID, book, mapping, conflict := seedUnpublishConflict(t, fixture)
	service.dav().client.requestTimeout = 250 * time.Millisecond
	_, err := st.DB().Exec(st.Rebind(`UPDATE carddav_address_books
		SET supports_sync_collection = TRUE WHERE id = ?`), book.ID)
	require.NoError(err)

	require.Error(service.ResolveConflict(t.Context(), conflict.ID, ResolutionKeepLocal))
	pending, err := st.GetCardDAVConflictContext(t.Context(), conflict.ID)
	require.NoError(err)
	require.Equal(store.CardDAVMutationDelete, pending.PendingOperation)
	fixture.mu.Lock()
	deletesAfterTimeout := fixture.deletes
	fixture.mu.Unlock()

	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	fixture.mu.Lock()
	assert.Equal(deletesAfterTimeout, fixture.deletes, "pull proof must not replay DELETE")
	fixture.mu.Unlock()
	_, err = st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
	require.ErrorIs(err, store.ErrCardDAVResourceNotFound)
	_, err = st.GetCardDAVPublicationContext(t.Context(), personID)
	require.ErrorIs(err, store.ErrCardDAVPublicationNotFound)
	_, err = st.GetPersonContext(t.Context(), personID)
	require.NoError(err)
	resolved, err := st.GetCardDAVConflictContext(t.Context(), conflict.ID)
	require.NoError(err)
	assert.Equal(store.CardDAVConflictResolved, resolved.Status)
	assert.Equal(store.CardDAVResolutionKeepLocal, resolved.Resolution)
	books, err := st.ListCardDAVAddressBooksContext(t.Context(), store.AllCardDAVAccounts)
	require.NoError(err)
	require.Len(books, 1)
	assert.Equal("token-after-unpublish", books[0].SyncToken)
}

func TestPullRefreshSupersedesTimedOutTombstoneIntentBeforeFreshDelete(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &conflictMutationServer{timeoutDelete: true}
	service, st, book, mapping, conflict := seedLocalTombstoneConflict(t, fixture)
	service.dav().client.requestTimeout = 250 * time.Millisecond
	require.Error(service.ResolveConflict(t.Context(), conflict.ID, ResolutionKeepLocal))
	pending, err := st.GetCardDAVConflictContext(t.Context(), conflict.ID)
	require.NoError(err)
	require.Equal(store.CardDAVMutationDelete, pending.PendingOperation)

	latest := conflictCard("person", "Alice Updated After Timeout")
	fixture.mu.Lock()
	fixture.body = append([]byte(nil), latest...)
	fixture.etag = `"remote-after-timeout"`
	deletesBeforeSync := fixture.deletes
	fixture.mu.Unlock()
	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	fixture.mu.Lock()
	assert.Equal(deletesBeforeSync, fixture.deletes, "pull must not replay the ambiguous DELETE")
	fixture.mu.Unlock()

	refreshed, err := st.GetCardDAVConflictContext(t.Context(), conflict.ID)
	require.NoError(err)
	assert.Equal(store.CardDAVConflictUnresolved, refreshed.Status)
	assert.Empty(refreshed.PendingOperation)
	assert.Zero(refreshed.PreviousMappingRevision)
	assert.Nil(refreshed.PendingStartedAt)
	assert.Greater(refreshed.MappingRevision, pending.MappingRevision)
	assert.Equal(`"remote-after-timeout"`, refreshed.RemoteETag)
	assert.Equal(latest, refreshed.RemoteBody)
	afterPull, err := st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
	require.NoError(err)
	assert.Equal(refreshed.MappingRevision, afterPull.MappingRevision)

	require.NoError(service.ResolveConflict(t.Context(), conflict.ID, ResolutionKeepLocal))
	fixture.mu.Lock()
	assert.Equal(deletesBeforeSync+1, fixture.deletes, "explicit retry must issue one freshly fenced DELETE")
	assert.Equal(`"remote-after-timeout"`, fixture.lastIfMatch)
	fixture.mu.Unlock()
	_, err = st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
	require.ErrorIs(err, store.ErrCardDAVResourceNotFound)
}

func TestDirectConflictRefreshDoesNotSupersedePendingTombstoneIntent(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &conflictMutationServer{timeoutDelete: true}
	service, st, book, mapping, conflict := seedLocalTombstoneConflict(t, fixture)
	service.dav().client.requestTimeout = 250 * time.Millisecond
	require.Error(service.ResolveConflict(t.Context(), conflict.ID, ResolutionKeepLocal))
	pending, err := st.GetCardDAVConflictContext(t.Context(), conflict.ID)
	require.NoError(err)
	currentMapping, err := st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
	require.NoError(err)
	latest := conflictCard("person", "Direct Refresh")

	refreshed, err := st.RecordCardDAVConflictContext(t.Context(), store.CardDAVConflictCapture{
		AddressBookID: book.ID, Href: mapping.Href,
		ExpectedMappingRevision: currentMapping.MappingRevision,
		BaseLocalHash:           currentMapping.LocalHash, LocalHash: pending.LocalHash,
		BaseRemoteHash: currentMapping.RemoteSemanticHash, BaseRemoteETag: currentMapping.RemoteETag,
		RemoteETag: `"direct-refresh"`, RemoteBody: latest, LocalTombstone: true,
	})
	require.NoError(err)
	assert.Equal(store.CardDAVMutationDelete, refreshed.PendingOperation)
	assert.Equal(pending.PreviousMappingRevision, refreshed.PreviousMappingRevision)
	assert.NotNil(refreshed.PendingStartedAt)
}

func TestKeepLocalTombstone412RefreshesConflictBeforeExplicitRetry(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	latest := conflictCard("person", "Alice Raced Again")
	fixture := &conflictMutationServer{
		delete412: true, deleteRaceBody: latest, deleteRaceETag: `"remote-latest"`,
	}
	service, st, _, _, conflict := seedLocalTombstoneConflict(t, fixture)

	var conflictErr *ConflictError
	require.ErrorAs(service.ResolveConflict(t.Context(), conflict.ID, ResolutionKeepLocal), &conflictErr)
	assert.Equal(conflict.ID, conflictErr.ID)
	refreshed, err := st.GetCardDAVConflictContext(t.Context(), conflict.ID)
	require.NoError(err)
	assert.Equal(store.CardDAVConflictUnresolved, refreshed.Status)
	assert.Empty(refreshed.PendingOperation)
	assert.Equal(`"remote-latest"`, refreshed.RemoteETag)
	assert.Equal(latest, refreshed.RemoteBody)

	require.NoError(service.ResolveConflict(t.Context(), conflict.ID, ResolutionKeepLocal))
	fixture.mu.Lock()
	assert.Equal(2, fixture.deletes)
	assert.Equal(`"remote-latest"`, fixture.lastIfMatch)
	fixture.mu.Unlock()
}

func TestKeepLocalTombstoneCanonicalCommitHonorsBookFence(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &conflictMutationServer{}
	service, st, book, mapping, conflict := seedLocalTombstoneConflict(t, fixture)
	fixture.mu.Lock()
	fencedGet := fixture.gets + 2
	fixture.onGet = func(gets int) {
		if gets != fencedGet {
			return
		}
		_, err := st.DB().Exec(st.Rebind(`UPDATE carddav_address_books
			SET sync_revision = sync_revision + 1 WHERE id = ?`), book.ID)
		require.NoError(err)
	}
	fixture.mu.Unlock()

	err := service.ResolveConflict(t.Context(), conflict.ID, ResolutionKeepLocal)
	require.ErrorIs(err, store.ErrCardDAVStalePlan)
	pending, err := st.GetCardDAVConflictContext(t.Context(), conflict.ID)
	require.NoError(err)
	assert.Equal(store.CardDAVConflictUnresolved, pending.Status)
	assert.Equal(store.CardDAVMutationDelete, pending.PendingOperation)
	_, err = st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
	require.NoError(err)
}

func TestOversized412RestoresMappingFenceAndClearsKnownUnappliedIntent(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &conflictMutationServer{}
	server := httptest.NewServer(fixture.handler(t))
	t.Cleanup(server.Close)
	service, st, personID, book := seededMutationServiceForServer(t, server)
	require.NoError(service.PublishPerson(t.Context(), personID))
	mapping, err := st.GetCardDAVResourceContext(t.Context(), book.ID, book.CanonicalURL+"person.vcf")
	require.NoError(err)
	_, err = st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
		AddressKind: store.ContactAddressEmail, OriginalValue: "oversized-local@example.test",
		Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
	})
	require.NoError(err)
	fixture.mu.Lock()
	fixture.body = oversizedConflictCard("person")
	fixture.etag = `"oversized"`
	fixture.force412 = true
	fixture.mu.Unlock()

	err = service.PublishPerson(t.Context(), personID)
	require.ErrorIs(err, store.ErrCardDAVConflictTooLarge)
	after, err := st.GetCardDAVResourceContext(t.Context(), book.ID, mapping.Href)
	require.NoError(err)
	assert.Equal(mapping.MappingRevision, after.MappingRevision)
	publication, err := st.GetCardDAVPublicationContext(t.Context(), personID)
	require.NoError(err)
	assert.Empty(publication.PendingOperation)
	conflicts, err := service.ListConflicts(t.Context())
	require.NoError(err)
	assert.Empty(conflicts)
}

func TestOversizedCreateCollisionCanBeCanceledWithoutDeletingRemote(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	remoteBody := oversizedConflictCard("remote-owner")
	putCount := 0
	deleteCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putCount++
			w.WriteHeader(http.StatusPreconditionFailed)
		case http.MethodGet:
			w.Header().Set("ETag", `"remote-owner"`)
			_, err := w.Write(remoteBody)
			assert.NoError(err)
		case http.MethodDelete:
			deleteCount++
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	service, st, personID, book := seededMutationServiceForServer(t, server)

	err := service.PublishPerson(t.Context(), personID)
	require.ErrorIs(err, store.ErrCardDAVConflictTooLarge)
	resource, err := st.GetCardDAVResourceContext(t.Context(), book.ID, book.CanonicalURL+"person.vcf")
	require.NoError(err)
	assert.Equal(remoteBody, resource.RemoteBody)
	assert.Nil(resource.PersonID)
	assert.Equal(store.CardDAVMappingUnbound, resource.MappingStatus)
	assert.Equal(store.CardDAVGovernanceNone, resource.Governance)
	err = service.PublishPerson(t.Context(), personID)
	require.ErrorIs(err, store.ErrCardDAVPublicationMismatch)
	assert.Equal(1, putCount)
	require.NoError(service.UnpublishPerson(t.Context(), personID))
	_, err = st.GetCardDAVPublicationContext(t.Context(), personID)
	require.ErrorIs(err, store.ErrCardDAVPublicationNotFound)
	assert.Equal(1, putCount)
	assert.Zero(deleteCount)
}

func TestOversizedAmbiguousRecoveryRebasesAndRetainsReadOnlyIntent(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &conflictMutationServer{}
	server := httptest.NewServer(fixture.handler(t))
	t.Cleanup(server.Close)
	service, st, personID, book := seededMutationServiceForServer(t, server)
	service.dav().client.requestTimeout = 250 * time.Millisecond
	require.NoError(service.PublishPerson(t.Context(), personID))
	_, err := st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
		AddressKind: store.ContactAddressEmail, OriginalValue: "recovery-local@example.test",
		Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
	})
	require.NoError(err)
	fixture.mu.Lock()
	fixture.timeoutPut = true
	fixture.mu.Unlock()
	require.Error(service.PublishPerson(t.Context(), personID))
	// The short deadline above exercises an ambiguous write. Recovery reads a
	// deliberately oversized response and needs the normal request deadline.
	service.dav().client.requestTimeout = 5 * time.Second
	pending, err := st.GetCardDAVPublicationContext(t.Context(), personID)
	require.NoError(err)
	require.Equal(store.CardDAVMutationUpdate, pending.PendingOperation)
	fixture.mu.Lock()
	fixture.body = oversizedConflictCard("person")
	fixture.etag = `"oversized-recovery"`
	putsBeforeRecovery := fixture.puts
	fixture.mu.Unlock()

	err = service.PublishPerson(t.Context(), personID)
	require.ErrorIs(err, store.ErrCardDAVConflictTooLarge)
	after, err := st.GetCardDAVResourceContext(t.Context(), book.ID, book.CanonicalURL+"person.vcf")
	require.NoError(err)
	assert.Equal(pending.PreviousMappingRevision, after.MappingRevision)
	rebased, err := st.GetCardDAVPublicationContext(t.Context(), personID)
	require.NoError(err)
	assert.Equal(pending.PendingOperation, rebased.PendingOperation)
	assert.Equal(pending.OutgoingBody, rebased.OutgoingBody)
	assert.Equal(pending.RemoteETag, rebased.RemoteETag)
	assert.Equal(pending.MutationRevision, rebased.MutationRevision)
	assert.Equal(pending.PreviousMappingRevision, rebased.MappingRevision)
	assert.Equal(pending.PreviousMappingRevision, rebased.PreviousMappingRevision)

	err = service.PublishPerson(t.Context(), personID)
	require.ErrorIs(err, store.ErrCardDAVConflictTooLarge)
	fixture.mu.Lock()
	assert.Equal(putsBeforeRecovery, fixture.puts, "oversized ambiguous recovery must remain read-only")
	fixture.mu.Unlock()
}

func TestResolveConflictRejectsChoicesOutsideExactContract(t *testing.T) {
	service := &Service{}
	err := service.ResolveConflict(t.Context(), 1, ResolutionChoice("merge"))
	require.ErrorIs(t, err, ErrInvalidResolutionChoice)
}

func TestConditionalDelete412KeepRemoteRevalidatesCanonicalCard(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &conflictMutationServer{}
	server := httptest.NewServer(fixture.handler(t))
	t.Cleanup(server.Close)
	service, st, personID, book := seededMutationServiceForServer(t, server)
	require.NoError(service.PublishPerson(t.Context(), personID))
	fixture.mu.Lock()
	fixture.body = conflictCard("person", "Alice Remote")
	fixture.etag = `"remote-2"`
	fixture.delete412 = true
	fixture.mu.Unlock()

	err := service.UnpublishPerson(t.Context(), personID)
	var conflictErr *ConflictError
	require.ErrorAs(err, &conflictErr)
	conflict, err := st.GetCardDAVConflictContext(t.Context(), conflictErr.ID)
	require.NoError(err)
	assert.True(conflict.LocalTombstone)
	assert.False(conflict.RemoteTombstone)
	assert.Equal(conflictCard("person", "Alice Remote"), conflict.RemoteBody)
	fixture.mu.Lock()
	requestsBefore := fixture.puts + fixture.gets + fixture.deletes
	fixture.body = conflictCard("person", "Alice Newer Remote")
	fixture.etag = `"remote-3"`
	fixture.mu.Unlock()

	require.ErrorIs(service.ResolveConflict(t.Context(), conflict.ID, ResolutionKeepRemote),
		store.ErrCardDAVConflictStale)
	unresolved, err := st.GetCardDAVConflictContext(t.Context(), conflict.ID)
	require.NoError(err)
	assert.Equal(store.CardDAVConflictUnresolved, unresolved.Status)
	fixture.mu.Lock()
	fixture.body = conflict.RemoteBody
	fixture.etag = conflict.RemoteETag
	fixture.mu.Unlock()
	require.NoError(service.ResolveConflict(t.Context(), conflict.ID, ResolutionKeepRemote))
	fixture.mu.Lock()
	assert.Equal(requestsBefore+2, fixture.puts+fixture.gets+fixture.deletes)
	fixture.mu.Unlock()
	mapping, err := st.GetCardDAVResourceContext(t.Context(), book.ID, book.CanonicalURL+"person.vcf")
	require.NoError(err)
	assert.Equal(conflict.RemoteBody, mapping.RemoteBody)
	_, err = st.GetPersonContext(t.Context(), personID)
	require.NoError(err)
	resolved, err := st.GetCardDAVConflictContext(t.Context(), conflict.ID)
	require.NoError(err)
	assert.Equal(store.CardDAVResolutionKeepRemote, resolved.Resolution)
	publication, err := st.GetCardDAVPublicationContext(t.Context(), personID)
	require.NoError(err)
	assert.True(publication.Desired)
	assert.Empty(publication.PendingOperation)
	fixture.mu.Lock()
	deletesBeforeReconcile := fixture.deletes
	fixture.mu.Unlock()
	require.NoError(service.ReconcilePublications(t.Context()))
	fixture.mu.Lock()
	assert.Equal(deletesBeforeReconcile, fixture.deletes, "keep-remote must cancel the stale unpublish")
	fixture.mu.Unlock()
}

func TestPullRefreshPreservesUnpublishTombstoneUntilKeepRemoteCancelsIt(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &conflictMutationServer{}
	server := httptest.NewServer(fixture.handler(t))
	t.Cleanup(server.Close)
	service, st, personID, _ := seededMutationServiceForServer(t, server)
	require.NoError(service.PublishPerson(t.Context(), personID))
	fixture.mu.Lock()
	fixture.body = conflictCard("person", "Alice Remote")
	fixture.etag = `"remote-2"`
	fixture.delete412 = true
	fixture.mu.Unlock()

	var conflictErr *ConflictError
	require.ErrorAs(service.UnpublishPerson(t.Context(), personID), &conflictErr)
	person, err := st.GetPersonContext(t.Context(), personID)
	require.NoError(err)
	_, err = st.UpdatePersonDisplayNameContext(t.Context(), personID, person.Revision, new("Alice Local After Unpublish"))
	require.NoError(err)
	latest := conflictCard("person", "Alice Remote Refreshed")
	fixture.mu.Lock()
	fixture.body = latest
	fixture.etag = `"remote-3"`
	fixture.mu.Unlock()
	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)

	refreshed, err := st.GetCardDAVConflictContext(t.Context(), conflictErr.ID)
	require.NoError(err)
	assert.True(refreshed.LocalTombstone)
	assert.Equal(latest, refreshed.RemoteBody)
	require.NoError(service.ResolveConflict(t.Context(), refreshed.ID, ResolutionKeepRemote))
	publication, err := st.GetCardDAVPublicationContext(t.Context(), personID)
	require.NoError(err)
	assert.True(publication.Desired)
	assert.Empty(publication.PendingOperation)
	fixture.mu.Lock()
	deletesBeforeReconcile := fixture.deletes
	fixture.mu.Unlock()
	require.NoError(service.ReconcilePublications(t.Context()))
	fixture.mu.Lock()
	assert.Equal(deletesBeforeReconcile, fixture.deletes)
	fixture.mu.Unlock()
}

func TestConditionalDelete412WithCanonicalTombstoneCommitsCleanup(t *testing.T) {
	require := require.New(t)

	fixture := &conflictMutationServer{}
	server := httptest.NewServer(fixture.handler(t))
	t.Cleanup(server.Close)
	service, st, personID, book := seededMutationServiceForServer(t, server)
	require.NoError(service.PublishPerson(t.Context(), personID))
	fixture.mu.Lock()
	fixture.delete412 = true
	fixture.deleteRaceTombstone = true
	fixture.mu.Unlock()

	require.NoError(service.UnpublishPerson(t.Context(), personID))
	_, err := st.GetCardDAVResourceContext(t.Context(), book.ID, book.CanonicalURL+"person.vcf")
	require.ErrorIs(err, store.ErrCardDAVResourceNotFound)
	_, err = st.GetCardDAVPublicationContext(t.Context(), personID)
	require.ErrorIs(err, store.ErrCardDAVPublicationNotFound)
	conflicts, err := service.ListConflicts(t.Context())
	require.NoError(err)
	assert.Empty(t, conflicts)
}

func TestAmbiguousUpdateRecoveryCapturesConflictWithoutReplay(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &mutationFixture{}
	service, st, personID, _ := seededMutationService(t, fixture)
	require.NoError(service.PublishPerson(t.Context(), personID))
	_, err := st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
		AddressKind: store.ContactAddressEmail, OriginalValue: "alice-local@example.test",
		Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
	})
	require.NoError(err)
	fixture.timeout = true
	require.Error(service.PublishPerson(t.Context(), personID))
	putsBeforeRecovery := fixture.puts
	fixture.mu.Lock()
	fixture.body = conflictCard("person", "Alice Remote After Timeout")
	fixture.etag = `"remote-after-timeout"`
	fixture.mu.Unlock()

	err = service.PublishPerson(t.Context(), personID)
	var conflictErr *ConflictError
	require.ErrorAs(err, &conflictErr)
	assert.Equal(putsBeforeRecovery, fixture.puts, "ambiguous mapped recovery must remain read-only")
	conflict, err := st.GetCardDAVConflictContext(t.Context(), conflictErr.ID)
	require.NoError(err)
	assert.Contains(string(conflict.RemoteBody), "FN:Alice Remote After Timeout")
	assert.Contains(string(conflict.RemoteBody), "PRODID:-//Server//EN")
	assert.Contains(string(conflict.LocalBody), "EMAIL:alice-local@example.test")
}

func TestKeepLocalRecordsLineMappingSoNextSyncDoesNotDuplicateEmail(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := &mutationFixture{}
	service, st, personID, _ := seededMutationService(t, fixture)
	require.NoError(service.PublishPerson(t.Context(), personID))
	_, err := service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)

	addProjectedEmail(t, st, personID)
	fixture.mu.Lock()
	fixture.body = bytes.Replace(fixture.body, []byte("FN:Alice Example"), []byte("FN:Alice Renamed"), 1)
	fixture.etag = `"remote-renamed"`
	fixture.mu.Unlock()

	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	conflicts, err := service.ListConflicts(t.Context())
	require.NoError(err)
	require.Len(conflicts, 1)
	require.NoError(service.ResolveConflict(t.Context(), conflicts[0].ID, ResolutionKeepLocal))
	fixture.mu.Lock()
	assert.Equal(1, strings.Count(string(fixture.body), "alice.updated@example.test"))
	fixture.mu.Unlock()

	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	assert.Equal(1, strings.Count(string(fixture.body), "alice.updated@example.test"), string(fixture.body))
}

func publishedImportFixture(t *testing.T) (*conflictMutationServer, *Service, *store.Store, int64) {
	t.Helper()
	fixture := &conflictMutationServer{body: conflictCardWithEmail("person", "Alice", "e1@example.test", "e2@example.test"), etag: `"remote-1"`}
	server := httptest.NewServer(fixture.handler(t))
	t.Cleanup(server.Close)
	service, st, book := newPullService(t, server, false)
	_, err := service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(t, err)
	mapping, err := st.GetCardDAVResourceContext(t.Context(), book.ID, book.CanonicalURL+"person.vcf")
	require.NoError(t, err)
	require.NotNil(t, mapping.PersonID)
	require.NoError(t, service.PublishPerson(t.Context(), *mapping.PersonID))
	require.Equal(t, 0, fixture.puts)
	return fixture, service, st, *mapping.PersonID
}

func (f *conflictMutationServer) setRemote(body []byte, etag string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.body, f.etag = body, etag
}

func TestRemoteEditToPublishedImportReachesPerson(t *testing.T) {
	for _, tc := range []struct {
		name       string
		localEdit  string
		localKind  store.ContactAddressKind
		remoteEdit string
	}{
		{name: "unchanged since publish"},
		{name: "after a local edit was published", localEdit: "alice-local@example.test"},
		{name: "remote replaces a published local email", localEdit: "e3@example.test", remoteEdit: "e3b@example.test"},
		{name: "remote removes a published local email", localEdit: "e3@example.test", remoteEdit: "remove"},
		{name: "remote replaces a published local phone", localEdit: "+12025550101", localKind: store.ContactAddressPhone, remoteEdit: "+12025550102"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)

			fixture, service, st, personID := publishedImportFixture(t)
			want := []string{"e1@example.test", "e2b@example.test"}
			if tc.localEdit != "" {
				kind := tc.localKind
				if kind == "" {
					kind = store.ContactAddressEmail
				}
				_, err := st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
					AddressKind: kind, OriginalValue: tc.localEdit,
					Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
				})
				require.NoError(err)
				require.NoError(service.ReconcilePublications(t.Context()))
				require.Equal(1, fixture.puts)
				require.Contains(string(fixture.lastPutBody), tc.localEdit)
				if tc.remoteEdit == "" {
					want = append(want, tc.localEdit)
				} else if tc.remoteEdit != "remove" {
					want = append(want, tc.remoteEdit)
				}
			}
			puts := fixture.puts
			fixture.mu.Lock()
			edited := bytes.Replace(fixture.body, []byte("e2@example.test"), []byte("e2b@example.test"), 1)
			fixture.mu.Unlock()
			if tc.remoteEdit == "remove" {
				edited = bytes.Replace(edited, []byte("EMAIL:"+tc.localEdit+"\r\n"), nil, 1)
			} else if tc.remoteEdit != "" {
				edited = bytes.Replace(edited, []byte(tc.localEdit), []byte(tc.remoteEdit), 1)
			}
			fixture.setRemote(edited, `"remote-2"`)
			_, err := service.Sync(t.Context(), SyncOptions{Full: true})
			require.NoError(err)
			require.NoError(service.ReconcilePublications(t.Context()))

			assert.Equal(puts, fixture.puts)
			points, err := st.ListPersonContactPointsContext(t.Context(), personID, true)
			require.NoError(err)
			values := make([]string, 0, len(points))
			for _, point := range points {
				values = append(values, point.OriginalValue)
			}
			assert.ElementsMatch(want, values)
			conflicts, err := service.ListConflicts(t.Context())
			require.NoError(err)
			assert.Empty(conflicts)
		})
	}
}

func TestKeepRemoteRebasesPublishedLocalContactPoint(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	fixture, service, st, personID := publishedImportFixture(t)
	for _, email := range []string{"e3@example.test", "e5@example.test"} {
		_, err := st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
			AddressKind: store.ContactAddressEmail, OriginalValue: email,
			Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
		})
		require.NoError(err)
		if email == "e3@example.test" {
			require.NoError(service.ReconcilePublications(t.Context()))
			require.Equal(1, fixture.puts)
			require.Contains(string(fixture.lastPutBody), email)
		}
	}
	fixture.mu.Lock()
	edited := bytes.Replace(fixture.body, []byte("e3@example.test"), []byte("e3b@example.test"), 1)
	fixture.mu.Unlock()
	fixture.setRemote(edited, `"remote-2"`)
	_, err := service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	conflicts, err := service.ListConflicts(t.Context())
	require.NoError(err)
	require.Len(conflicts, 1)
	require.NoError(service.ResolveConflict(t.Context(), conflicts[0].ID, ResolutionKeepRemote))
	require.NoError(service.ReconcilePublications(t.Context()))
	points, err := st.ListPersonContactPointsContext(t.Context(), personID, true)
	require.NoError(err)
	values := make([]string, 0, len(points))
	for _, point := range points {
		values = append(values, point.OriginalValue)
	}
	assert.ElementsMatch([]string{"e1@example.test", "e2@example.test", "e3b@example.test", "e5@example.test"}, values)
	assert.Equal(2, fixture.puts)
	assert.NotContains(string(fixture.lastPutBody), "EMAIL:e3@example.test")
	assert.Contains(string(fixture.lastPutBody), "e3b@example.test")
	assert.Contains(string(fixture.lastPutBody), "e5@example.test")
}

func TestRemoteDeleteOfPublishedImportStopsPublishing(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture, service, st, personID := publishedImportFixture(t)
	withResidue := bytes.Replace(conflictCardWithEmail("person", "Alice", "e1@example.test", "e2@example.test"), []byte("END:VCARD"),
		[]byte("TITLE:Engineer\r\nPHOTO:data:image/png;base64,iVBORw0KGgo=\r\nEND:VCARD"), 1)
	fixture.setRemote(withResidue, `"remote-2"`)
	_, err := service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	require.NoError(service.ReconcilePublications(t.Context()))
	puts := fixture.puts

	fixture.setRemote(nil, "")
	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	require.NoError(service.ReconcilePublications(t.Context()))

	assert.Equal(puts, fixture.puts, "a deleted card must not be recreated without its residue")
	_, err = st.GetCardDAVPublicationContext(t.Context(), personID)
	require.ErrorIs(err, store.ErrCardDAVPublicationNotFound)
	_, err = st.GetPersonContext(t.Context(), personID)
	require.NoError(err)
	conflicts, err := service.ListConflicts(t.Context())
	require.NoError(err)
	assert.Empty(conflicts)
}

func TestRemoteEditToPublishedImportWithLocalEditConflicts(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture, service, st, personID := publishedImportFixture(t)
	_, err := st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
		AddressKind: store.ContactAddressEmail, OriginalValue: "alice-local@example.test",
		Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
	})
	require.NoError(err)
	fixture.setRemote(conflictCardWithEmail("person", "Alice", "e1@example.test", "e2b@example.test"), `"remote-2"`)
	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)

	conflicts, err := service.ListConflicts(t.Context())
	require.NoError(err)
	assert.Len(conflicts, 1)
	points, err := st.ListPersonContactPointsContext(t.Context(), personID, true)
	require.NoError(err)
	values := make([]string, 0, len(points))
	for _, point := range points {
		values = append(values, point.OriginalValue)
	}
	assert.Contains(values, "e2@example.test")
	assert.NotContains(values, "e2b@example.test")
}

func TestRemoteEditToPublishedContactOccurrences(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		types                  []string
		typeLabel              *string
		pref                   *int
		before                 string
		after                  string
		want                   string
		reorder                bool
		imported               bool
		rename                 bool
		addLanguageAfterRename bool
		fnLanguage             *string
		fnTypes                []string
		fnPref                 *int
		userFN                 bool
		phone                  bool
		phoneTypes             []string
		keepRemote             bool
		conflict               string
	}{
		{name: "label only", types: []string{"home"}, before: "EMAIL;TYPE=home:e1@example.test", after: "EMAIL;TYPE=work:e1@example.test", conflict: "EMAIL"},
		{name: "duplicate removed", types: []string{"home", "work"}, before: "EMAIL;TYPE=home:e1@example.test\r\n", after: "EMAIL:\\n\r\n", conflict: "EMAIL"},
		{name: "created in msgvault", types: []string{""}, before: "e1@example.test", after: "e1b@example.test", want: "e1b@example.test"},
		{name: "created in msgvault with label and preference", types: []string{"home"}, pref: new(1), before: "e1@example.test", after: "e1b@example.test", conflict: "EMAIL", keepRemote: true},
		{name: "created in msgvault with type label only", types: []string{""}, typeLabel: new("Personal"), before: "e1@example.test", after: "e1b@example.test", conflict: "EMAIL", keepRemote: true},
		{name: "reordered v3 occurrences", types: []string{"work"}, reorder: true, imported: true, want: "e1@example.test"},
		{name: "imported cell number change", imported: true, phone: true, phoneTypes: []string{"cell"}, before: "+12025550101", after: "+12025550103", want: "+12025550103"},
		{name: "imported work email change", imported: true, before: "e1@example.test", after: "e1b@example.test", want: "e1b@example.test"},
		{name: "imported label only", imported: true, before: "TYPE=work", after: "TYPE=home", conflict: "EMAIL"},
		{name: "remote rename of msgvault person", types: []string{"home"}, before: "FN:Alice Example", after: "FN:Renamed Example", rename: true, want: "e1@example.test"},
		{name: "remote rename of user FN", types: []string{"home"}, before: "FN:Alice Example", after: "FN:Renamed Example", rename: true, userFN: true, want: "e1@example.test"},
		{name: "remote adds FN language after rename", types: []string{"home"}, before: "FN:Alice Example", after: "FN:Renamed Example", rename: true, userFN: true, addLanguageAfterRename: true, want: "e1@example.test"},
		{name: "user FN with language", before: "Alice Example", after: "Renamed Example", userFN: true, fnLanguage: new("en"), conflict: "FN", keepRemote: true},
		{name: "remote adds FN language", before: "FN:Alice Example", after: "FN;LANGUAGE=en:Alice Example", userFN: true, conflict: "FN", keepRemote: true},
		{name: "remote adds email type and preference", types: []string{""}, before: "EMAIL:e1@example.test", after: "EMAIL;TYPE=work;PREF=2:e1@example.test", conflict: "EMAIL", keepRemote: true},
		{name: "duplicate email parameters", types: []string{"home", "work"}, before: "EMAIL;TYPE=work:e1@example.test", after: "EMAIL;TYPE=cell;PREF=2:e1@example.test", conflict: "EMAIL", keepRemote: true},
		{name: "user FN with type", before: "Alice Example", after: "Renamed Example", userFN: true, fnTypes: []string{"work"}, conflict: "FN"},
		{name: "user FN with preference", before: "Alice Example", after: "Renamed Example", userFN: true, fnPref: new(1), conflict: "FN"},
		{name: "unchanged user FN", types: []string{""}, before: "e1@example.test", after: "e1b@example.test", userFN: true, want: "e1b@example.test"},
		{name: "user URL", before: "https://example.test/before", after: "https://example.test/after", conflict: "URL", keepRemote: true},
		{name: "structured name", before: "N:Example;Alice", after: "N:Changed;Alice", conflict: "N", keepRemote: true},
		{name: "remote rename with structured name", before: "FN:Alice Example", after: "FN:Renamed Example", conflict: "N"},
		{name: "remote phone edit", phone: true, before: "TEL:+12025550102", after: "TEL:tel:+12025550103", want: "+12025550103"},
		{name: "remote phone visual separators", phone: true, before: "TEL:+12025550102", after: "TEL:tel: +1 (202) 555-01.03 ", want: "+1 (202) 555-01.03"},
		{name: "labeled remote phone edit", phone: true, phoneTypes: []string{"work"}, pref: new(1), before: "tel:+12025550102", after: "tel:+12025550103", conflict: "TEL", keepRemote: true},
		{name: "remote phone text", phone: true, before: "TEL:+12025550102", after: "TEL;VALUE=text:call the front desk", conflict: "TEL"},
		{name: "remote email whitespace", types: []string{"home"}, before: "e1@example.test", after: " e1@example.test ", conflict: "EMAIL"},
		{name: "remote adds email whitespace", types: []string{"home"}, before: "END:VCARD", after: "EMAIL: added@example.test \r\nEND:VCARD", conflict: "EMAIL"},
		{name: "address", before: "Example Street", after: "Changed Street", conflict: "ADR", keepRemote: true},
		{name: "photo URI", before: "https://example.test/old.jpg", after: "https://example.test/new.jpg", conflict: "PHOTO", keepRemote: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)
			fixture := &conflictMutationServer{}
			server := httptest.NewServer(fixture.handler(t))
			t.Cleanup(server.Close)
			var service *Service
			var st *store.Store
			var personID int64
			var book store.CardDAVAddressBook
			if tc.imported {
				body := []byte("BEGIN:VCARD\r\nVERSION:3.0\r\nUID:person\r\nFN:Alice Example\r\nEMAIL;TYPE=HOME:e1@example.test\r\nEND:VCARD\r\n")
				if !tc.reorder {
					body = []byte("BEGIN:VCARD\r\nVERSION:3.0\r\nUID:person\r\nFN:Alice Example\r\nEMAIL;TYPE=work:e1@example.test\r\nEND:VCARD\r\n")
					if tc.phone {
						body = bytes.Replace(body, []byte("EMAIL;TYPE=work:e1@example.test"), []byte("TEL;TYPE=cell:+12025550101"), 1)
					}
				}
				if tc.reorder {
					body = bytes.Replace(body, []byte("END:VCARD"), []byte("TEL;TYPE=WORK:+12025550102\r\nEND:VCARD"), 1)
				}
				fixture.setRemote(body, `"remote-1"`)
				service, st, book = newPullService(t, server, false)
				_, err := service.Sync(t.Context(), SyncOptions{Full: true})
				require.NoError(err)
				mapping, err := st.GetCardDAVResourceContext(t.Context(), book.ID, book.CanonicalURL+"person.vcf")
				require.NoError(err)
				require.NotNil(mapping.PersonID)
				personID = *mapping.PersonID
			} else {
				service, st, personID, book = seededMutationServiceForServer(t, server)
			}
			if tc.keepRemote {
				_, err := st.DB().ExecContext(t.Context(), st.Rebind(`UPDATE carddav_address_books SET supported_vcard_versions = '["4.0"]' WHERE id = ?`), book.ID)
				require.NoError(err)
			}
			var userName *store.PersonName
			var phone *store.PersonContactPoint
			if tc.phone && !tc.imported {
				var err error
				phone, err = st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
					AddressKind: store.ContactAddressPhone, OriginalValue: "+12025550102",
					Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser, TypeTokens: tc.phoneTypes, Pref: tc.pref},
				})
				require.NoError(err)
			}
			if tc.userFN {
				var err error
				userName, err = st.AddPersonNameContext(t.Context(), personID, store.PersonNameInput{
					NameKind: store.PersonNameFormatted, Formatted: new("Alice Example"), Language: tc.fnLanguage,
					Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser, TypeTokens: tc.fnTypes, Pref: tc.fnPref},
				})
				require.NoError(err)
			}
			switch tc.conflict {
			case "PHOTO":
				_, err := st.AddPersonMediaContext(t.Context(), personID, store.PersonMediaInput{
					MediaKind: store.PersonMediaPhoto, URI: new(tc.before),
					Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
				})
				require.NoError(err)
			case "URL":
				_, err := st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
					AddressKind: store.ContactAddressURL, OriginalValue: tc.before,
					Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
				})
				require.NoError(err)
			case "N", "ADR":
				_, err := st.AddPersonNameContext(t.Context(), personID, store.PersonNameInput{
					NameKind: store.PersonNameStructured, FamilyName: new("Example"), GivenName: new("Alice"),
					Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
				})
				require.NoError(err)
				_, err = st.AddPersonAddressContext(t.Context(), personID, store.PersonAddressInput{
					AddressKind: store.PersonAddressPostal, StreetAddress: new("Example Street"),
					Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
				})
				require.NoError(err)
			}
			for _, label := range tc.types {
				email := "e1@example.test"
				if tc.reorder {
					email = "e2@example.test"
				}
				_, err := st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
					AddressKind: store.ContactAddressEmail, OriginalValue: email,
					Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser, TypeTokens: strings.Fields(label), TypeLabel: tc.typeLabel, Pref: tc.pref},
				})
				require.NoError(err)
			}
			if tc.reorder {
				_, err := st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
					AddressKind: store.ContactAddressPhone, OriginalValue: "+12025550101",
					Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser, TypeTokens: []string{"cell"}},
				})
				require.NoError(err)
			}
			beforePoints, err := st.ListPersonContactPointsContext(t.Context(), personID, true)
			require.NoError(err)
			require.NoError(service.PublishPerson(t.Context(), personID))
			puts := fixture.puts
			var edited []byte
			if tc.reorder {
				edited = []byte("BEGIN:VCARD\r\nVERSION:3.0\r\nUID:person\r\nFN:Alice Example\r\nitem0.EMAIL:\\n\r\nEMAIL;TYPE=work:e2@example.test\r\nEMAIL;TYPE=HOME:e1@example.test\r\nitem0.TEL:\\n\r\nTEL;TYPE=cell:+12025550101\r\nTEL;TYPE=WORK:+12025550102\r\nEND:VCARD\r\n")
			} else {
				require.Contains(string(fixture.body), tc.before)
				edited = bytes.Replace(fixture.body, []byte(tc.before), []byte(tc.after), 1)
				if tc.conflict == "FN" {
					edited = bytes.ReplaceAll(fixture.body, []byte(tc.before), []byte(tc.after))
				}
				if tc.name == "user FN with language" {
					edited = bytes.Replace(edited, []byte("FN:Renamed Example\r\n"), nil, 1)
				}
			}
			fixture.setRemote(edited, `"remote-2"`)
			_, err = service.Sync(t.Context(), SyncOptions{Full: true})
			require.NoError(err)
			require.NoError(service.ReconcilePublications(t.Context()))
			assert.Equal(puts, fixture.puts)
			points, err := st.ListPersonContactPointsContext(t.Context(), personID, true)
			require.NoError(err)
			if tc.conflict != "" {
				conflicts, err := service.ListConflicts(t.Context())
				require.NoError(err)
				require.Len(conflicts, 1)
				assert.Equal(string(edited), string(fixture.body))
				if tc.conflict == "EMAIL" {
					assert.Equal(beforePoints, points)
				}
				if tc.conflict == "FN" {
					names, err := st.ListPersonNamesContext(t.Context(), personID, true)
					require.NoError(err)
					require.Len(names, 1)
					assert.Equal(*userName, names[0])
				}
				if tc.conflict == "URL" {
					require.Len(points, 1)
					assert.Equal(tc.before, points[0].OriginalValue)
				}
				if tc.conflict == "TEL" {
					require.Len(points, 1)
					assert.Equal(*phone, points[0])
				}
				if tc.keepRemote {
					require.NoError(service.ResolveConflict(t.Context(), conflicts[0].ID, ResolutionKeepRemote))
					require.NoError(service.ReconcilePublications(t.Context()))
					assert.Equal(puts, fixture.puts)
					snapshot, err := st.LoadPersonVCardSnapshotContext(t.Context(), personID)
					require.NoError(err)
					switch tc.conflict {
					case "PHOTO":
						assert.Empty(snapshot.Profile.Media)
					case "ADR":
						assert.Empty(snapshot.Profile.Addresses)
						assert.Len(snapshot.Profile.Names, 2)
					case "N":
						for _, name := range snapshot.Profile.Names {
							assert.NotEqual(store.PersonNameStructured, name.NameKind)
						}
						assert.Len(snapshot.Profile.Addresses, 1)
					case "URL":
						assert.Empty(snapshot.Profile.ContactPoints)
					case "FN":
						count := 1
						if tc.name == "remote adds FN language" {
							count = 2
						}
						require.Len(snapshot.Profile.Names, count)
						for _, name := range snapshot.Profile.Names {
							assert.Equal(new("en"), name.Language)
						}
					case "EMAIL", "TEL":
						if tc.name == "duplicate email parameters" {
							require.Len(snapshot.Profile.ContactPoints, 2)
							assert.Equal([]string{"cell"}, snapshot.Profile.ContactPoints[0].Envelope.TypeTokens)
							assert.Equal(new(2), snapshot.Profile.ContactPoints[0].Envelope.Pref)
							assert.Equal(store.ProvenanceUser, snapshot.Profile.ContactPoints[1].Envelope.Source)
							assert.Equal([]string{"home"}, snapshot.Profile.ContactPoints[1].Envelope.TypeTokens)
						} else {
							require.Len(snapshot.Profile.ContactPoints, 1)
							assert.Equal(store.ProvenanceCardDAVImport, snapshot.Profile.ContactPoints[0].Envelope.Source)
							if tc.name == "remote adds email type and preference" {
								assert.Equal([]string{"work"}, snapshot.Profile.ContactPoints[0].Envelope.TypeTokens)
								assert.Equal(new(2), snapshot.Profile.ContactPoints[0].Envelope.Pref)
							}
							if tc.phone {
								assert.Equal(tc.phoneTypes, snapshot.Profile.ContactPoints[0].Envelope.TypeTokens)
								assert.Equal(tc.pref, snapshot.Profile.ContactPoints[0].Envelope.Pref)
							}
						}
					}
					_, err = st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
						AddressKind: store.ContactAddressEmail, OriginalValue: "unrelated@example.test",
						Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
					})
					require.NoError(err)
					require.NoError(service.ReconcilePublications(t.Context()))
					assert.Equal(puts+1, fixture.puts)
					body := string(fixture.lastPutBody)
					assert.Contains(body, "unrelated@example.test")
					assert.Contains(body, tc.after)
					assert.NotContains(body, tc.before)
					if tc.conflict == "PHOTO" {
						assert.Equal(1, strings.Count(body, "PHOTO"))
					}
					if tc.conflict == "FN" {
						assert.Contains(body, "FN;LANGUAGE=en:")
					}
					if tc.phone {
						assert.Contains(body, "TEL;TYPE=work;PREF=1:tel:+12025550103")
					}
				}
				return
			}
			if tc.reorder {
				want := map[string][]string{"e1@example.test": {"HOME"}, "e2@example.test": {"work"}, "+12025550101": {"cell"}, "+12025550102": {"WORK"}}
				require.Len(points, len(want))
				values := make([]string, 0, len(points))
				for _, point := range points {
					assert.NotContains(values, point.OriginalValue)
					values = append(values, point.OriginalValue)
					assert.Contains(want, point.OriginalValue)
					assert.ElementsMatch(want[point.OriginalValue], point.Envelope.TypeTokens)
				}
			} else {
				require.Len(points, 1)
				assert.Equal(tc.want, points[0].OriginalValue)
				if tc.imported {
					tokens := []string{"work"}
					if tc.phone {
						tokens = tc.phoneTypes
					}
					assert.Equal(tokens, points[0].Envelope.TypeTokens)
					assert.Equal(store.ProvenanceCardDAVImport, points[0].Envelope.Source)
				}
			}
			if tc.rename {
				person, err := st.GetPersonContext(t.Context(), personID)
				require.NoError(err)
				require.NotNil(person.DisplayName)
				assert.Equal("Renamed Example", *person.DisplayName)
			}
			if tc.userFN {
				names, err := st.ListPersonNamesContext(t.Context(), personID, true)
				require.NoError(err)
				count := 1
				if tc.name == "unchanged user FN" || tc.rename {
					count = 2
				}
				require.Len(names, count)
				if count == 2 {
					assert.Equal(store.ProvenanceCardDAVImport, names[1].Envelope.Source)
				}
				if tc.rename {
					assert.ElementsMatch([]*string{new("Renamed Example"), new("Alice Example")}, []*string{names[0].Formatted, names[1].Formatted})
				} else {
					assert.Equal(store.ProvenanceUser, names[0].Envelope.Source)
				}
			}
			conflicts, err := service.ListConflicts(t.Context())
			require.NoError(err)
			assert.Empty(conflicts)
			assert.Equal(string(edited), string(fixture.body))
			if tc.addLanguageAfterRename {
				names, err := st.ListPersonNamesContext(t.Context(), personID, true)
				require.NoError(err)
				require.Contains(string(edited), "FN:Renamed Example")
				edited = bytes.ReplaceAll(edited, []byte("FN:Renamed Example"), []byte("FN;LANGUAGE=en:Renamed Example"))
				fixture.setRemote(edited, `"remote-3"`)
				_, err = service.Sync(t.Context(), SyncOptions{Full: true})
				require.NoError(err)
				require.NoError(service.ReconcilePublications(t.Context()))
				assert.Equal(puts, fixture.puts)
				conflicts, err = service.ListConflicts(t.Context())
				require.NoError(err)
				assert.Len(conflicts, 1)
				afterNames, err := st.ListPersonNamesContext(t.Context(), personID, true)
				require.NoError(err)
				assert.Equal(names, afterNames)
				assert.Equal(string(edited), string(fixture.body))
			}
		})
	}
}

func TestCardDAVKeepRemoteSupersedesDisplacedUserOwner(t *testing.T) {
	for _, field := range []string{"EMAIL TYPE", "EMAIL label", "EMAIL PREF", "TEL TYPE", "TEL label", "TEL PREF", "FN LANGUAGE"} {
		t.Run(field, func(t *testing.T) {
			require := require.New(t)
			assert := assert.New(t)
			fixture := &conflictMutationServer{}
			server := httptest.NewServer(fixture.handler(t))
			t.Cleanup(server.Close)
			service, st, personID, book := seededMutationServiceForServer(t, server)
			_, err := st.DB().ExecContext(t.Context(), st.Rebind(`UPDATE carddav_address_books SET supported_vcard_versions = '["4.0"]' WHERE id = ?`), book.ID)
			require.NoError(err)
			var point *store.PersonContactPoint
			var name *store.PersonName
			before, after := "owner@example.test", "edited@example.test"
			if field == "FN LANGUAGE" {
				var err error
				name, err = st.AddPersonNameContext(t.Context(), personID, store.PersonNameInput{
					NameKind: store.PersonNameFormatted, Formatted: new("Alice Example"), Language: new("en"),
					Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
				})
				require.NoError(err)
				before, after = "Alice Example", "Renamed Owner"
			} else {
				input := store.PersonContactPointInput{
					AddressKind: store.ContactAddressEmail,
					Envelope:    store.ValueEnvelopeInput{Source: store.ProvenanceUser},
				}
				switch field {
				case "EMAIL TYPE", "TEL TYPE":
					input.Envelope.TypeTokens = []string{"work"}
				case "EMAIL label", "TEL label":
					input.Envelope.TypeLabel = new("Office")
				case "EMAIL PREF", "TEL PREF":
					input.Envelope.Pref = new(1)
				}
				if field[:3] == "TEL" {
					input.AddressKind = store.ContactAddressPhone
					before, after = "+12025550101", "+12025550102"
				}
				input.OriginalValue = before
				var err error
				point, err = st.AddPersonContactPointContext(t.Context(), personID, input)
				require.NoError(err)
			}
			require.NoError(service.PublishPerson(t.Context(), personID))
			puts := fixture.puts
			require.Contains(string(fixture.body), before)
			edited := bytes.ReplaceAll(fixture.body, []byte(before), []byte(after))
			if name != nil {
				edited = bytes.Replace(edited, []byte("FN:Renamed Owner\r\n"), nil, 1)
			}
			fixture.setRemote(edited, `"edited"`)
			_, err = service.Sync(t.Context(), SyncOptions{Full: true})
			require.NoError(err)
			conflicts, err := service.ListConflicts(t.Context())
			require.NoError(err)
			require.Len(conflicts, 1)
			require.NoError(service.ResolveConflict(t.Context(), conflicts[0].ID, ResolutionKeepRemote))
			if name != nil {
				names, err := st.ListPersonNamesContext(t.Context(), personID, true)
				require.NoError(err)
				require.Len(names, 1)
				assert.Equal(new(after), names[0].Formatted)
				assert.Equal(new("en"), names[0].Language)
				assert.Equal(store.ProvenanceCardDAVImport, names[0].Envelope.Source)
				assert.NotEqual(name.Envelope.ID, names[0].Envelope.ID)
			} else {
				points, err := st.ListPersonContactPointsContext(t.Context(), personID, true)
				require.NoError(err)
				require.Len(points, 1)
				assert.Equal(after, points[0].OriginalValue)
				assert.Equal(point.Envelope.TypeTokens, points[0].Envelope.TypeTokens)
				assert.Equal(point.Envelope.Pref, points[0].Envelope.Pref)
				assert.Equal(store.ProvenanceCardDAVImport, points[0].Envelope.Source)
				assert.NotEqual(point.Envelope.ID, points[0].Envelope.ID)
			}
			require.NoError(service.ReconcilePublications(t.Context()))
			assert.Equal(puts, fixture.puts)
			_, err = st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
				AddressKind: store.ContactAddressEmail, OriginalValue: "unrelated@example.test",
				Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
			})
			require.NoError(err)
			require.NoError(service.ReconcilePublications(t.Context()))
			assert.Equal(puts+1, fixture.puts)
			assert.Contains(string(fixture.lastPutBody), after)
			assert.NotContains(string(fixture.lastPutBody), before)
			require.NoError(service.ReconcilePublications(t.Context()))
			assert.Equal(puts+1, fixture.puts)
		})
	}
}

func TestCardDAVKeepRemotePreservesAcceptedOccurrences(t *testing.T) {
	for _, tc := range []struct {
		name, before, after string
	}{
		{"TEL extension", "tel:+12025550101", "tel:+12025550101;ext=123"},
		{"TITLE", "TITLE:Engineer", "TITLE:Architect"},
		{"NOTE", "NOTE:Original note", "NOTE:Accepted note"},
		{"RELATED", "RELATED;TYPE=friend:urn:uuid:00000000-0000-4000-8000-000000000001", "RELATED;TYPE=friend:urn:uuid:00000000-0000-4000-8000-000000000002"},
		{"FN SORT-AS", "FN:Alice", "FN;LANGUAGE=en;SCRIPT=Latn;PHONETIC=ipa;SORT-AS=Example,Alice:Alice"},
		{"FN unsupported parameter", "FN:Alice", "FN;X-CUSTOM=keep:Alice"},
		{"FN unrepresentable sort value", "FN:Alice", "FN;SORT-AS=\"Example,Alice\":Alice"},
		{"two FN rename", "FN:Alice", "FN:Renamed Alice"},
		{"derived FN", "FN;DERIVED=true:Alice Example", "FN:Renamed Alice"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			assert := assert.New(t)
			fixture, service, st, personID := publishedImportFixture(t)
			publication, err := st.GetCardDAVPublicationContext(t.Context(), personID)
			require.NoError(err)
			_, err = st.DB().ExecContext(t.Context(), st.Rebind(`UPDATE carddav_address_books SET supported_vcard_versions = '["4.0"]' WHERE id = ?`), publication.AddressBookID)
			require.NoError(err)
			switch tc.name {
			case "TEL extension":
				_, err = st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
					AddressKind: store.ContactAddressPhone, OriginalValue: "+12025550101",
					Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser, TypeTokens: []string{"work"}},
				})
				require.NoError(err)
			case "TITLE":
				organization, err := st.CreateOrganizationContext(t.Context(), store.OrganizationInput{Name: "Example Company", Kind: store.OrganizationKindCompany})
				require.NoError(err)
				for _, title := range []string{"Engineer", "Unchanged title"} {
					_, err = st.AddEmploymentContext(t.Context(), store.EmploymentInput{
						PersonID: personID, OrganizationID: organization.ID, Title: &title, IsPrimary: new(title == "Engineer"), Source: store.ProvenanceUser,
					})
					require.NoError(err)
				}
			case "NOTE":
				for slug, value := range map[string]string{store.AttributeSlugNotes: "Original note", store.AttributeSlugLocation: "Unchanged location"} {
					_, err = st.SetPersonAttributeValueContext(t.Context(), store.PersonAttributeValueInput{
						PersonID: personID, DefinitionSlug: slug, Source: store.ProvenanceUser,
						Value: store.AttributeValue{Type: store.AttributeValueText, Text: &value},
					})
					require.NoError(err)
				}
			case "RELATED":
				var targetID int64
				err = st.DB().QueryRowContext(t.Context(), st.Rebind(`INSERT INTO persons (vcard_uid, display_name) VALUES (?, ?) RETURNING id`),
					"00000000-0000-4000-8000-000000000001", "Example Friend").Scan(&targetID)
				require.NoError(err)
				for _, slug := range []string{"friend", "neighbor"} {
					_, err = st.AddPersonRelationshipContext(t.Context(), store.PersonRelationshipInput{
						SourcePersonID: personID, TargetPersonID: targetID, TypeSlug: slug, Source: store.ProvenanceUser, Actor: "user",
					})
					require.NoError(err)
				}
			case "two FN rename":
				_, err = st.AddPersonNameContext(t.Context(), personID, store.PersonNameInput{
					NameKind: store.PersonNameFormatted, Formatted: new("User name"), Language: new("en"),
					Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
				})
				require.NoError(err)
			case "derived FN":
				names, err := st.ListPersonNamesContext(t.Context(), personID, true)
				require.NoError(err)
				for _, name := range names {
					require.NoError(st.SupersedePersonNameContext(t.Context(), personID, name.Envelope.ID, nil))
				}
				_, err = st.AddPersonNameContext(t.Context(), personID, store.PersonNameInput{
					NameKind: store.PersonNameStructured, GivenName: new("Alice"), FamilyName: new("Example"),
					Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
				})
				require.NoError(err)
			}
			require.NoError(service.ReconcilePublications(t.Context()))
			puts := fixture.puts
			require.Contains(string(fixture.body), tc.before)
			edited := bytes.Replace(fixture.body, []byte(tc.before), []byte(tc.after), 1)
			// An unpublished local edit also makes the plain imported FN rename conflict.
			_, err = st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
				AddressKind: store.ContactAddressEmail, OriginalValue: "pending@example.test",
				Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
			})
			require.NoError(err)
			fixture.setRemote(edited, `"accepted"`)
			_, err = service.Sync(t.Context(), SyncOptions{Full: true})
			require.NoError(err)
			conflicts, err := service.ListConflicts(t.Context())
			require.NoError(err)
			require.Len(conflicts, 1)
			require.NoError(service.ResolveConflict(t.Context(), conflicts[0].ID, ResolutionKeepRemote))
			require.NoError(service.ReconcilePublications(t.Context()))
			assert.Equal(puts+1, fixture.puts)
			assert.Contains(string(fixture.lastPutBody), tc.after)
			assert.NotContains(string(fixture.lastPutBody), tc.before+"\r\n")
			puts = fixture.puts
			switch tc.name {
			case "TEL extension":
				points, err := st.ListPersonContactPointsContext(t.Context(), personID, true)
				require.NoError(err)
				for _, point := range points {
					assert.NotEqual(store.ContactAddressPhone, point.AddressKind)
				}
			case "TITLE":
				employments, err := st.ListEmploymentsContext(t.Context(), store.EmploymentFilter{PersonID: personID})
				require.NoError(err)
				require.Len(employments, 2)
				assert.ElementsMatch([]*string{nil, new("Unchanged title")}, []*string{employments[0].Title, employments[1].Title})
			case "NOTE":
				values, err := st.ListPersonAttributeValuesContext(t.Context(), personID, store.PersonAttributeQuery{})
				require.NoError(err)
				require.Len(values, 1)
				assert.Equal(store.AttributeSlugLocation, values[0].DefinitionSlug)
			case "RELATED":
				relationships, err := st.ListPersonRelationshipsContext(t.Context(), personID, store.PersonRelationshipListOptions{})
				require.NoError(err)
				require.Len(relationships, 1)
				assert.Equal("neighbor", relationships[0].Relationship.TypeSlug)
			case "FN SORT-AS":
				names, err := st.ListPersonNamesContext(t.Context(), personID, true)
				require.NoError(err)
				require.Len(names, 1)
				assert.Equal(new("Latn"), names[0].Script)
				assert.Equal(new("ipa"), names[0].PhoneticSystem)
				assert.Equal(new("Example,Alice"), names[0].SortAs)
			case "FN unsupported parameter", "FN unrepresentable sort value":
				names, err := st.ListPersonNamesContext(t.Context(), personID, true)
				require.NoError(err)
				assert.Empty(names)
			case "two FN rename":
				names, err := st.ListPersonNamesContext(t.Context(), personID, true)
				require.NoError(err)
				require.Len(names, 2)
				assert.ElementsMatch([]*string{new("User name"), new("Renamed Alice")}, []*string{names[0].Formatted, names[1].Formatted})
			case "derived FN":
				names, err := st.ListPersonNamesContext(t.Context(), personID, true)
				require.NoError(err)
				require.Len(names, 2)
				for _, name := range names {
					if name.NameKind == store.PersonNameStructured {
						assert.Equal(new("Alice"), name.GivenName)
						assert.Equal(new("Example"), name.FamilyName)
					}
				}
			}
			_, err = st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
				AddressKind: store.ContactAddressEmail, OriginalValue: "unrelated@example.test",
				Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
			})
			require.NoError(err)
			require.NoError(service.ReconcilePublications(t.Context()))
			assert.Equal(puts+1, fixture.puts)
			body := string(fixture.lastPutBody)
			assert.Contains(body, tc.after)
			assert.NotContains(body, tc.before+"\r\n")
			assert.Contains(body, "unrelated@example.test")
			if tc.name == "TITLE" {
				assert.Contains(body, "ORG:Example Company")
			}
			if tc.name == "two FN rename" {
				assert.Contains(body, "FN;LANGUAGE=en:User name")
			}
			if tc.name == "derived FN" {
				assert.Contains(body, "N:Example;Alice")
			}
			if tc.name == "RELATED" {
				assert.Contains(body, "RELATED;TYPE=neighbor:urn:uuid:00000000-0000-4000-8000-000000000001")
			}
		})
	}
}

func TestKeepRemoteRetiresOnlyDisplacedEmploymentField(t *testing.T) {
	for _, field := range []string{"TITLE", "ROLE", "ORG"} {
		t.Run(field, func(t *testing.T) {
			require := require.New(t)
			assert := assert.New(t)
			fixture, service, st, personID := publishedImportFixture(t)
			organization, err := st.CreateOrganizationContext(t.Context(), store.OrganizationInput{Name: "Example Company", Kind: store.OrganizationKindCompany})
			require.NoError(err)
			employment, err := st.AddEmploymentContext(t.Context(), store.EmploymentInput{
				PersonID: personID, OrganizationID: organization.ID, Title: new("Engineer"), Role: new("Developer"),
				Department: new("Platform"), Location: new("Example City"), Description: new("Maintains archives"),
				StartDate: &store.PartialDate{Year: new(2020), Month: new(3), Day: new(15)},
				IsCurrent: new(true), IsPrimary: new(true), Source: store.ProvenanceUser,
			})
			require.NoError(err)
			require.NoError(service.ReconcilePublications(t.Context()))
			before := map[string]string{"TITLE": "TITLE:Engineer", "ROLE": "ROLE:Developer", "ORG": "ORG:Example Company;Platform"}[field]
			after := field + ":Accepted remote value"
			require.Contains(string(fixture.body), before)
			puts := fixture.puts
			fixture.setRemote(bytes.Replace(fixture.body, []byte(before), []byte(after), 1), `"employment-edit"`)
			_, err = service.Sync(t.Context(), SyncOptions{Full: true})
			require.NoError(err)
			conflicts, err := service.ListConflicts(t.Context())
			require.NoError(err)
			require.Len(conflicts, 1)
			require.NoError(service.ResolveConflict(t.Context(), conflicts[0].ID, ResolutionKeepRemote))
			require.NoError(service.ReconcilePublications(t.Context()))
			assert.Equal(puts, fixture.puts)
			want := *employment
			switch field {
			case "TITLE":
				want.Title = nil
			case "ROLE":
				want.Role = nil
			case "ORG":
				want.IsPrimary = false
			}
			want.Revision++
			for step := range 2 {
				got, err := st.GetEmploymentContext(t.Context(), employment.ID)
				require.NoError(err)
				want.UpdatedAt = got.UpdatedAt
				assert.Equal(want, *got)
				if step == 1 {
					break
				}
				_, err = st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
					AddressKind: store.ContactAddressEmail, OriginalValue: "unrelated@example.test",
					Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
				})
				require.NoError(err)
				require.NoError(service.ReconcilePublications(t.Context()))
			}
			assert.Equal(puts+1, fixture.puts)
			body := string(fixture.lastPutBody)
			assert.Contains(body, after)
			assert.NotContains(body, before)
			assert.Equal(1, strings.Count(body, field+":"))
			assert.Contains(body, "unrelated@example.test")
			for other, value := range map[string]string{"TITLE": "TITLE:Engineer", "ROLE": "ROLE:Developer", "ORG": "ORG:Example Company;Platform"} {
				if other != field {
					assert.Contains(body, value)
				}
			}
		})
	}
}

func TestRemotePhoneDialSuffixConflictsAndKeepRemotePreservesResidue(t *testing.T) {
	for _, suffix := range []string{" x123", " ext. 123", ",123", ";123", "w123", "p123"} {
		t.Run(suffix, func(t *testing.T) {
			require := require.New(t)
			assert := assert.New(t)
			body := []byte("BEGIN:VCARD\r\nVERSION:4.0\r\nUID:person\r\nFN:Alice\r\nTEL;VALUE=text:+12025550101\r\nEND:VCARD\r\n")
			fixture := &conflictMutationServer{body: body, etag: `"remote-1"`}
			server := httptest.NewServer(fixture.handler(t))
			t.Cleanup(server.Close)
			service, st, book := newPullService(t, server, false)
			_, err := service.Sync(t.Context(), SyncOptions{Full: true})
			require.NoError(err)
			mapping, err := st.GetCardDAVResourceContext(t.Context(), book.ID, book.CanonicalURL+"person.vcf")
			require.NoError(err)
			require.NotNil(mapping.PersonID)
			personID := *mapping.PersonID
			_, err = st.DB().ExecContext(t.Context(), st.Rebind(`UPDATE carddav_address_books SET supported_vcard_versions = '["4.0"]' WHERE id = ?`), book.ID)
			require.NoError(err)
			snapshot, err := st.LoadPersonVCardSnapshotContext(t.Context(), personID)
			require.NoError(err)
			publication, err := st.PrepareCardDAVPublicationContext(t.Context(), store.CardDAVPublicationPlan{
				PersonID: personID, Desired: true, AddressBookID: book.ID, Href: mapping.Href,
				OutgoingBody: body, OutgoingSemanticHash: mapping.RemoteSemanticHash, LocalHash: snapshot.Fingerprint,
			})
			require.NoError(err)
			require.True(publication.Noop)
			before := "TEL;VALUE=text:+12025550101"
			after := "TEL;VALUE=text:" + vcard.EscapeText("+12025550101"+suffix)
			require.Contains(string(fixture.body), before)
			puts := fixture.puts
			fixture.setRemote(bytes.Replace(fixture.body, []byte(before), []byte(after), 1), `"phone-edit"`)
			_, err = service.Sync(t.Context(), SyncOptions{Full: true})
			require.NoError(err)
			conflicts, err := service.ListConflicts(t.Context())
			require.NoError(err)
			require.Len(conflicts, 1)
			points, err := st.ListPersonContactPointsContext(t.Context(), personID, true)
			require.NoError(err)
			assert.True(slices.ContainsFunc(points, func(point store.PersonContactPoint) bool {
				return point.AddressKind == store.ContactAddressPhone && point.OriginalValue == "+12025550101"
			}))
			require.NoError(service.ResolveConflict(t.Context(), conflicts[0].ID, ResolutionKeepRemote))
			require.NoError(service.ReconcilePublications(t.Context()))
			assert.Equal(puts, fixture.puts)
			points, err = st.ListPersonContactPointsContext(t.Context(), personID, true)
			require.NoError(err)
			for _, point := range points {
				assert.NotEqual(store.ContactAddressPhone, point.AddressKind)
			}
			_, err = st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
				AddressKind: store.ContactAddressEmail, OriginalValue: "unrelated@example.test",
				Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
			})
			require.NoError(err)
			require.NoError(service.ReconcilePublications(t.Context()))
			assert.Equal(puts+1, fixture.puts)
			assert.Contains(string(fixture.lastPutBody), after)
			assert.Equal(1, strings.Count(string(fixture.lastPutBody), "TEL"))
		})
	}
}

func TestRemoteEmailEditWithUnchangedPhone(t *testing.T) {
	for _, phone := range []string{"+1 202 555 0100", "call the front desk"} {
		t.Run(phone, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)
			fixture := &conflictMutationServer{}
			server := httptest.NewServer(fixture.handler(t))
			t.Cleanup(server.Close)
			body := []byte("BEGIN:VCARD\r\nVERSION:4.0\r\nUID:person\r\nFN:Alice Example\r\nEMAIL:e1@example.test\r\nTEL:" + phone + "\r\nEND:VCARD\r\n")
			fixture.setRemote(body, `"remote-1"`)
			service, st, book := newPullService(t, server, false)
			_, err := service.Sync(t.Context(), SyncOptions{Full: true})
			require.NoError(err)
			mapping, err := st.GetCardDAVResourceContext(t.Context(), book.ID, book.CanonicalURL+"person.vcf")
			require.NoError(err)
			require.NotNil(mapping.PersonID)
			snapshot, err := st.LoadPersonVCardSnapshotContext(t.Context(), *mapping.PersonID)
			require.NoError(err)
			// Settle the original card through the store so its unchanged phone keeps the fixture spelling.
			publication, err := st.PrepareCardDAVPublicationContext(t.Context(), store.CardDAVPublicationPlan{
				PersonID: *mapping.PersonID, Desired: true, AddressBookID: book.ID, Href: mapping.Href,
				OutgoingBody: body, OutgoingSemanticHash: mapping.RemoteSemanticHash, LocalHash: snapshot.Fingerprint,
			})
			require.NoError(err)
			require.True(publication.Noop)
			puts := fixture.puts
			require.Contains(string(fixture.body), "TEL:"+phone)
			edited := bytes.Replace(fixture.body, []byte("e1@example.test"), []byte("e2@example.test"), 1)
			fixture.setRemote(edited, `"remote-2"`)
			_, err = service.Sync(t.Context(), SyncOptions{Full: true})
			require.NoError(err)
			require.NoError(service.ReconcilePublications(t.Context()))
			assert.Equal(puts, fixture.puts)
			conflicts, err := service.ListConflicts(t.Context())
			require.NoError(err)
			assert.Empty(conflicts)
			points, err := st.ListPersonContactPointsContext(t.Context(), *mapping.PersonID, true)
			require.NoError(err)
			emails := make([]string, 0)
			for _, point := range points {
				if point.AddressKind == store.ContactAddressEmail {
					emails = append(emails, point.OriginalValue)
				}
			}
			assert.Equal([]string{"e2@example.test"}, emails)
			assert.Equal(string(edited), string(fixture.body))
		})
	}
}

func TestCardDAVFirstImportKeepsBothEmails(t *testing.T) {
	_, _, st, personID := publishedImportFixture(t)
	points, err := st.ListPersonContactPointsContext(t.Context(), personID, true)
	require.NoError(t, err)
	require.Len(t, points, 2)
	assert.ElementsMatch(t, []string{"e1@example.test", "e2@example.test"}, []string{points[0].OriginalValue, points[1].OriginalValue})
}

func TestRemoteEditToPublishedContactAfterCrossBookImportDelete(t *testing.T) {
	require := require.New(t)
	fixture := &conflictMutationServer{}
	importBody := conflictCardWithEmail("person", "Alice Example", "e1@example.test")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/books/import/" {
			events := ""
			if len(importBody) > 0 {
				events = cardResponseRaw("/books/import/person.vcf", `"import-1"`, escapedCardData(importBody))
			}
			writeDAVXML(t, w, syncResponse(events, ""))
			return
		}
		fixture.handler(t)(w, r)
	}))
	t.Cleanup(server.Close)
	service, st, _ := newPullService(t, server, false)
	_, books, err := st.ReplaceCardDAVDiscoveryContext(t.Context(), store.CardDAVDiscoveryInput{
		BaseURL: server.URL, Username: "alice", PrincipalURL: server.URL + "/principal/", HomeURL: server.URL + "/books/",
		Books: []store.CardDAVDiscoveredBook{
			{CanonicalURL: server.URL + "/books/personal/", CanCreate: new(true), SupportsMultiget: true},
			{CanonicalURL: server.URL + "/books/import/", SupportsMultiget: true},
		},
	})
	require.NoError(err)
	require.NoError(st.SetCardDAVBookRolesContext(t.Context(), books[1].ID, store.CardDAVBookRoles{IsSubscribed: true, IsLookupSource: true}))
	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	imported, err := st.GetCardDAVResourceContext(t.Context(), books[1].ID, books[1].CanonicalURL+"person.vcf")
	require.NoError(err)
	require.NotNil(imported.PersonID)
	personID := *imported.PersonID
	fixture.setRemote(importBody, `"duplicate-1"`)
	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	duplicate, err := st.GetCardDAVResourceContext(t.Context(), books[0].ID, books[0].CanonicalURL+"person.vcf")
	require.NoError(err)
	require.Equal(imported.PersonID, duplicate.PersonID)
	require.NoError(service.PublishPerson(t.Context(), personID))
	puts := fixture.puts
	importBody = nil
	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	duplicate, err = st.GetCardDAVResourceContext(t.Context(), books[0].ID, duplicate.Href)
	require.NoError(err)
	require.Nil(duplicate.PersonRevisionAtBind)
	require.Contains(string(fixture.body), "e1@example.test")
	edited := bytes.ReplaceAll(fixture.body, []byte("e1@example.test"), []byte("e1b@example.test"))
	fixture.setRemote(edited, `"duplicate-2"`)
	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	require.NoError(service.ReconcilePublications(t.Context()))
	assert.Equal(t, puts, fixture.puts)
	points, err := st.ListPersonContactPointsContext(t.Context(), personID, true)
	require.NoError(err)
	require.NotEmpty(points)
	for _, point := range points {
		assert.Equal(t, "e1b@example.test", point.OriginalValue)
	}
}

func TestKeepRemotePreservesTwoImportedPhonesAfterExtension(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	body := []byte("BEGIN:VCARD\r\nVERSION:4.0\r\nUID:person\r\nFN:Alice\r\nTEL:tel:+12025550101\r\nTEL:tel:+12025550102\r\nEND:VCARD\r\n")
	fixture := &conflictMutationServer{body: body, etag: `"remote-1"`}
	server := httptest.NewServer(fixture.handler(t))
	t.Cleanup(server.Close)
	service, st, book := newPullService(t, server, false)
	_, err := service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	mapping, err := st.GetCardDAVResourceContext(t.Context(), book.ID, book.CanonicalURL+"person.vcf")
	require.NoError(err)
	require.NotNil(mapping.PersonID)
	personID := *mapping.PersonID
	_, err = st.DB().ExecContext(t.Context(), st.Rebind(`UPDATE carddav_address_books SET supported_vcard_versions = '["4.0"]' WHERE id = ?`), book.ID)
	require.NoError(err)
	require.NoError(service.PublishPerson(t.Context(), personID))
	_, err = st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
		AddressKind: store.ContactAddressEmail, OriginalValue: "pending@example.test",
		Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
	})
	require.NoError(err)
	edited := bytes.Replace(fixture.body, []byte("tel:+12025550101"), []byte("tel:+12025550101;ext=123"), 1)
	fixture.setRemote(edited, `"remote-2"`)
	_, err = service.Sync(t.Context(), SyncOptions{Full: true})
	require.NoError(err)
	conflicts, err := service.ListConflicts(t.Context())
	require.NoError(err)
	require.Len(conflicts, 1)
	require.NoError(service.ResolveConflict(t.Context(), conflicts[0].ID, ResolutionKeepRemote))
	require.NoError(service.ReconcilePublications(t.Context()))
	assert.Contains(string(fixture.lastPutBody), "TEL:tel:+12025550101;ext=123\r\n")
	assert.Contains(string(fixture.lastPutBody), "TEL:tel:+12025550102\r\n")
	_, err = st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
		AddressKind: store.ContactAddressEmail, OriginalValue: "unrelated@example.test",
		Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
	})
	require.NoError(err)
	require.NoError(service.ReconcilePublications(t.Context()))
	assert.Contains(string(fixture.lastPutBody), "TEL:tel:+12025550101;ext=123\r\n")
	assert.Contains(string(fixture.lastPutBody), "TEL:tel:+12025550102\r\n")
	assert.Contains(string(fixture.lastPutBody), "unrelated@example.test")
}

func TestRemoteNameReorderWithEmailEditPreservesBothNames(t *testing.T) {
	for _, importedName := range []string{"Alice", "Renamed Alice"} {
		t.Run(importedName, func(t *testing.T) {
			require := require.New(t)
			assert := assert.New(t)
			fixture, service, st, personID := publishedImportFixture(t)
			_, err := st.AddPersonNameContext(t.Context(), personID, store.PersonNameInput{
				NameKind: store.PersonNameFormatted, Formatted: new("User name"),
				Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
			})
			require.NoError(err)
			require.NoError(service.ReconcilePublications(t.Context()))
			names, err := st.ListPersonNamesContext(t.Context(), personID, true)
			require.NoError(err)
			require.Len(names, 2)
			var importedID int64
			for _, name := range names {
				if name.Envelope.Source == store.ProvenanceCardDAVImport {
					importedID = name.Envelope.ID
				}
			}
			require.NotZero(importedID)
			require.Contains(string(fixture.body), "FN:User name\r\n")
			edited := bytes.Replace(fixture.body, []byte("FN:User name\r\n"), nil, 1)
			edited = bytes.Replace(edited, []byte("FN:Alice\r\n"), []byte("FN:User name\r\nFN:"+importedName+"\r\n"), 1)
			edited = bytes.Replace(edited, []byte("e1@example.test"), []byte("edited@example.test"), 1)
			fixture.setRemote(edited, `"remote-2"`)
			_, err = service.Sync(t.Context(), SyncOptions{Full: true})
			require.NoError(err)
			names, err = st.ListPersonNamesContext(t.Context(), personID, true)
			require.NoError(err)
			require.Len(names, 2)
			assert.ElementsMatch([]*string{new("User name"), new(importedName)}, []*string{names[0].Formatted, names[1].Formatted})
			if importedName == "Alice" {
				assert.Contains([]int64{names[0].Envelope.ID, names[1].Envelope.ID}, importedID)
			}
			_, err = st.AddPersonContactPointContext(t.Context(), personID, store.PersonContactPointInput{
				AddressKind: store.ContactAddressEmail, OriginalValue: "unrelated@example.test",
				Envelope: store.ValueEnvelopeInput{Source: store.ProvenanceUser},
			})
			require.NoError(err)
			require.NoError(service.ReconcilePublications(t.Context()))
			body := string(fixture.lastPutBody)
			assert.Contains(body, "FN:User name\r\n")
			assert.Contains(body, "FN:"+importedName+"\r\n")
			assert.Contains(body, "edited@example.test")
			assert.NotContains(body, "e1@example.test")
			assert.Contains(body, "unrelated@example.test")
		})
	}
}
