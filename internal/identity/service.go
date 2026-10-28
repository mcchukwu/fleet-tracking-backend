package identity

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidCredentials = errors.New("identity: invalid credentials")
	ErrUnauthorized       = errors.New("identity: unauthorized")
)

type Service struct {
	repo *Repository
	ttl  time.Duration

	dummyOnce sync.Once
	dummyHash string
}

func NewService(repo *Repository, ttl time.Duration) *Service {
	return &Service{repo: repo, ttl: ttl}
}

func (s *Service) Login(ctx context.Context, email, password, userAgent, ip string) (string, *User, error) {
	email = strings.TrimSpace(strings.ToLower(email))

	user, err := s.repo.UserByEmail(ctx, email)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", nil, err
	}

	hash := s.dummy()
	if user != nil {
		hash = user.PasswordHash
	}
	ok, err := VerifyPassword(hash, password)
	if err != nil {
		return "", nil, err
	}
	if user == nil || !ok || user.Status != "active" {
		return "", nil, ErrInvalidCredentials
	}

	raw, tokenHash, err := NewToken()
	if err != nil {
		return "", nil, err
	}
	if err := s.repo.CreateSession(ctx, user.ID, tokenHash, userAgent, ip, time.Now().Add(s.ttl)); err != nil {
		return "", nil, err
	}
	return raw, user, nil
}

func (s *Service) Authenticate(ctx context.Context, raw string) (*User, error) {
	if raw == "" {
		return nil, ErrUnauthorized
	}
	user, err := s.repo.SessionUser(ctx, HashToken(raw))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrUnauthorized
	}
	if err != nil {
		return nil, err
	}
	return user, nil
}

func (s *Service) Logout(ctx context.Context, raw string) error {
	if raw == "" {
		return nil
	}
	return s.repo.RevokeSession(ctx, HashToken(raw))
}

func (s *Service) dummy() string {
	s.dummyOnce.Do(func() {
		s.dummyHash, _ = HashPassword("not-a-real-password")
	})
	return s.dummyHash
}
