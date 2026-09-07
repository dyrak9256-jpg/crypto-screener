package domain

import (
	"sync"

	"github.com/shopspring/decimal"
)

type User struct {
	ChatID            int64
	Username          string
	MinSpread         decimal.Decimal
	MinVolume         decimal.Decimal
	Timeframe         Timeframe
	MinFundingMinutes int
	// BotID identifies which Telegram bot instance serves this user when
	// several bot tokens share the delivery load (TELEGRAM_TOKENS).
	BotID int64
}

type UserManager struct {
	mu    sync.RWMutex
	users map[int64]User
}

func NewUserManager() *UserManager {
	return &UserManager{users: make(map[int64]User)}
}

func cloneUser(u *User) User {
	if u == nil {
		return User{}
	}
	return *u
}

func (um *UserManager) SetUser(u *User) {
	if u == nil {
		return
	}
	um.mu.Lock()
	um.users[u.ChatID] = cloneUser(u)
	um.mu.Unlock()
}

func (um *UserManager) RemoveUser(chatID int64) {
	um.mu.Lock()
	delete(um.users, chatID)
	um.mu.Unlock()
}

func (um *UserManager) GetUser(chatID int64) (*User, bool) {
	um.mu.RLock()
	u, ok := um.users[chatID]
	um.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return &u, true
}

// Range visits a point-in-time snapshot of users without exposing internal map pointers.
// The callback must not retain the supplied pointer after it returns.
func (um *UserManager) Range(fn func(User) bool) {
	if fn == nil {
		return
	}
	um.mu.RLock()
	defer um.mu.RUnlock()
	for _, u := range um.users {
		if !fn(u) {
			return
		}
	}
}
func (um *UserManager) HasUsers() bool {
	um.mu.RLock()
	n := len(um.users)
	um.mu.RUnlock()
	return n > 0
}

func (um *UserManager) GetAllUsers() []*User {
	um.mu.RLock()
	list := make([]*User, 0, len(um.users))
	for _, u := range um.users {
		copy := u
		list = append(list, &copy)
	}
	um.mu.RUnlock()
	return list
}
