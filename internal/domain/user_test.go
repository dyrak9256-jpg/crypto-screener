package domain

import (
	"sync"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserManager_CRUD(t *testing.T) {
	t.Parallel()

	um := NewUserManager()
	require.NotNil(t, um)

	// Initially empty
	assert.Empty(t, um.GetAllUsers())
	_, found := um.GetUser(1001)
	assert.False(t, found)

	// Set user
	user1 := &User{
		ChatID:    1001,
		Username:  "alice",
		MinSpread: decimal.RequireFromString("0.02"),
		MinVolume: decimal.RequireFromString("500000"),
		Timeframe: TF_15m,
	}
	um.SetUser(user1)

	fetched, found := um.GetUser(1001)
	assert.True(t, found)
	assert.Equal(t, user1, fetched)

	all := um.GetAllUsers()
	assert.Len(t, all, 1)
	assert.Equal(t, user1, all[0])

	// Update user
	user1Updated := &User{
		ChatID:    1001,
		Username:  "alice_updated",
		MinSpread: decimal.RequireFromString("0.03"),
		MinVolume: decimal.RequireFromString("600000"),
		Timeframe: TF_1h,
	}
	um.SetUser(user1Updated)

	fetched, found = um.GetUser(1001)
	assert.True(t, found)
	assert.Equal(t, "alice_updated", fetched.Username)
	assert.True(t, fetched.MinSpread.Equal(decimal.RequireFromString("0.03")))

	// Add second user
	user2 := &User{
		ChatID:    1002,
		Username:  "bob",
		MinSpread: decimal.RequireFromString("0.01"),
		MinVolume: decimal.RequireFromString("100000"),
		Timeframe: TF_5m,
	}
	um.SetUser(user2)
	assert.Len(t, um.GetAllUsers(), 2)

	// Remove user
	um.RemoveUser(1001)
	_, found = um.GetUser(1001)
	assert.False(t, found)
	assert.Len(t, um.GetAllUsers(), 1)
	assert.Equal(t, int64(1002), um.GetAllUsers()[0].ChatID)

	// Removing non-existent user should be safe
	um.RemoveUser(9999)
	assert.Len(t, um.GetAllUsers(), 1)
}

func TestUserManager_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	um := NewUserManager()
	const numGoroutines = 50
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 3)

	// Writers: SetUser
	for i := 0; i < numGoroutines; i++ {
		chatID := int64(i)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				um.SetUser(&User{
					ChatID:    chatID,
					Username:  "concurrent_user",
					MinSpread: decimal.NewFromInt(int64(j)),
					MinVolume: decimal.NewFromInt(1000),
					Timeframe: TF_15m,
				})
			}
		}()
	}

	// Readers: GetUser & GetAllUsers
	for i := 0; i < numGoroutines; i++ {
		chatID := int64(i)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				um.GetUser(chatID)
				_ = um.GetAllUsers()
			}
		}()
	}

	// Removers: RemoveUser
	for i := 0; i < numGoroutines; i++ {
		chatID := int64(i)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				um.RemoveUser(chatID)
			}
		}()
	}

	wg.Wait()
}
