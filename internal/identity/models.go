package identity

import "time"

type Role string

const (
	RoleAdmin      Role = "admin"
	RoleDispatcher Role = "dispatcher"
	RoleSecurity   Role = "security"
	RoleViewer     Role = "viewer"
)

type User struct {
	ID           string
	TenantID     string
	Email        string
	Name         string
	Role         Role
	PasswordHash string
	Status       string
}

type Session struct {
	ID        string
	UserID    string
	ExpiresAt time.Time
}
