package domain

import (
	"sync"

	"github.com/shopspring/decimal"
)

type User struct {
	ChatID    int64
	Username  string
	MinSpread decimal.Decimal
	MinVolume decimal.Decimal
	Timeframe Timeframe
}

// UserManager — потокобезопасный in-memory кэш подписчиков
type UserManager struct {
	mu    sync.RWMutex
	users map[int64]*User
}

func NewUserManager() *UserManager {
	return &UserManager{users: make(map[int64]*User)}
}

func (um *UserManager) SetUser(u *User) {
	um.mu.Lock()
	defer um.mu.Unlock()
	um.users[u.ChatID] = u
}

func (um *UserManager) RemoveUser(chatID int64) {
	um.mu.Lock()
	defer um.mu.Unlock()
	delete(um.users, chatID)
}

func (um *UserManager) GetUser(chatID int64) (*User, bool) {
	um.mu.RLock()
	defer um.mu.RUnlock()
	u, ok := um.users[chatID]
	return u, ok
}

func (um *UserManager) GetAllUsers() []*User {
	um.mu.RLock()
	defer um.mu.RUnlock()
	list := make([]*User, 0, len(um.users))
	for _, u := range um.users {
		list = append(list, u)
	}
	return list
}
