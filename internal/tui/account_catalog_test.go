package tui

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/msgvault/internal/query"
	"go.kenn.io/msgvault/internal/query/querytest"
	"go.kenn.io/msgvault/internal/store"
)

// flakyCatalogEngine reports the virtual account catalog unavailable until
// available is set, as a daemon does while its first catalog read is slow.
type flakyCatalogEngine struct {
	*querytest.MockEngine

	available bool
}

func (e *flakyCatalogEngine) ListVirtualAccounts(ctx context.Context) (map[int64][]store.VirtualAccount, error) {
	if !e.available {
		return nil, errors.New("virtual account catalog unavailable")
	}
	return e.MockEngine.ListVirtualAccounts(ctx)
}

func TestAccountCatalogRecoversWithinSession(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	children := []store.VirtualAccount{
		{Key: "identity:7:work", SourceID: 7, AccountAddress: "work@example.org", MessageCount: 3},
		{Key: "unattributed:7", SourceID: 7, Unattributed: true, MessageCount: 2},
	}
	engine := &flakyCatalogEngine{MockEngine: &querytest.MockEngine{
		Accounts:        []query.AccountInfo{{ID: 7, Identifier: "inbox@example.net"}},
		VirtualAccounts: map[int64][]store.VirtualAccount{7: children},
	}}
	model := New(engine, Options{DataDir: t.TempDir(), Version: "test"})

	// Startup: the catalog is down, so the account has no children yet and a
	// reread is scheduled.
	updated, retry := model.Update(model.loadAccounts()())
	model = asModel(t, updated)
	require.NotNil(retry, "an unavailable catalog schedules a reread")
	require.Len(model.accounts, 1)
	assert.Empty(model.accounts[0].VirtualAccounts)

	// The catalog recovers; the scheduled reread fills in the children.
	engine.available = true
	updated, reload := model.Update(accountCatalogRetryMsg{})
	model = asModel(t, updated)
	require.NotNil(reload)
	updated, next := model.Update(reload())
	model = asModel(t, updated)
	assert.Nil(next, "a readable catalog schedules nothing more")
	assert.Equal(children, model.accounts[0].VirtualAccounts)

	// A later unavailable read keeps the receiving addresses already shown.
	engine.available = false
	updated, _ = model.Update(model.loadAccounts()())
	model = asModel(t, updated)
	assert.Equal(children, model.accounts[0].VirtualAccounts)
}
