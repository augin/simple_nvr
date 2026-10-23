// Package auth implements the user store (bcrypt passwords in JSON file),
// signed session cookies and HTTP middleware for role based access control.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// Roles supported by the application.
const (
	RoleAdmin = "admin"
	RoleUser  = "user"
)

const (
	sessionCookieName = "session_token"
	sessionTTL        = 24 * time.Hour
	// kioskProxyHeader marks requests forwarded by the kiosk reverse proxy.
	// It is only trusted from loopback addresses (see isLoopback).
	kioskProxyHeader = "X-Kiosk-Proxy"
	kioskUsername    = "kiosk"
	// anonymousUser is used when the store has no users at all (auth disabled).
	anonymousUsername = "admin"

	permissionsFile = 0600
	secretLength    = 32
)

// bcryptCost is a variable so tests can use a cheaper hash cost.
var bcryptCost = bcrypt.DefaultCost

// publicPaths are always served without a session.
var publicPaths = []string{
	"/",
	"/favicon.ico",
	"/api/auth/login",
	"/api/auth/check",
	"/api/version",
}

// User is a single account. The password hash never leaves the server.
type User struct {
	Password string `yaml:"password" json:"-"`
	Role     string `yaml:"role" json:"role"`
}

type userStoreEntry struct {
	Password string `json:"password"`
	Role     string `json:"role"`
}

// Identity is the authenticated principal attached to the request context.
type Identity struct {
	Username string
	Role     string
}

type contextKey struct{}

var identityKey contextKey

// UserStore keeps accounts in memory and mirrors them into a JSON file.
type UserStore struct {
	mu       sync.RWMutex
	users    map[string]User
	filePath string
	secret   []byte
	loadErr  error
}

// NewUserStore creates a store and loads existing accounts from filePath.
// Loading errors (e.g. a corrupted users file) are remembered and reported by
// Err; the caller is expected to abort startup instead of running with auth
// accidentally disabled.
func NewUserStore(filePath string) *UserStore {
	secret := make([]byte, secretLength)
	if _, err := rand.Read(secret); err != nil {
		log.Fatalf("Failed to generate session secret: %v", err)
	}

	store := &UserStore{
		users:    make(map[string]User),
		filePath: filePath,
		secret:   secret,
	}
	store.loadErr = store.Load()
	return store
}

// Err returns the error (if any) encountered while loading the users file.
func (s *UserStore) Err() error {
	return s.loadErr
}

// Load reads the users file; a missing file is not an error.
func (s *UserStore) Load() error {
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read users file: %w", err)
	}

	var entries map[string]userStoreEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return fmt.Errorf("parse users file: %w", err)
	}

	users := make(map[string]User, len(entries))
	for name, entry := range entries {
		users[name] = User{Password: entry.Password, Role: entry.Role}
	}

	s.mu.Lock()
	s.users = users
	s.mu.Unlock()
	return nil
}

// Save writes the current accounts to disk.
func (s *UserStore) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked()
}

func (s *UserStore) saveLocked() error {
	entries := make(map[string]userStoreEntry, len(s.users))
	for name, user := range s.users {
		entries[name] = userStoreEntry{Password: user.Password, Role: user.Role}
	}

	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.filePath, data, permissionsFile)
}

// HasUsers reports whether authentication is enabled (any account exists).
func (s *UserStore) HasUsers() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.users) > 0
}

// Authenticate verifies credentials and returns the matching account.
func (s *UserStore) Authenticate(username, password string) (User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	user, ok := s.users[username]
	if !ok {
		return User{}, fmt.Errorf("user not found")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(password)); err != nil {
		return User{}, fmt.Errorf("invalid password")
	}
	return user, nil
}

// AddUser creates a new account and persists it.
func (s *UserStore) AddUser(username, password, role string) error {
	if role != RoleAdmin && role != RoleUser {
		return fmt.Errorf("invalid role: must be admin or user")
	}
	if err := validateCredentials(username, password); err != nil {
		return err
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return fmt.Errorf("failed to hash password: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.users[username]; exists {
		return fmt.Errorf("user already exists")
	}
	s.users[username] = User{Password: string(hash), Role: role}
	return s.saveLocked()
}

// DeleteUser removes an account and persists the change.
func (s *UserStore) DeleteUser(username string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.users[username]; !exists {
		return fmt.Errorf("user not found")
	}
	delete(s.users, username)
	return s.saveLocked()
}

// ChangePassword replaces an account password and persists the change.
func (s *UserStore) ChangePassword(username, newPassword string) error {
	if err := validateCredentials(username, newPassword); err != nil {
		return err
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcryptCost)
	if err != nil {
		return fmt.Errorf("failed to hash password: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	user, exists := s.users[username]
	if !exists {
		return fmt.Errorf("user not found")
	}
	user.Password = string(hash)
	s.users[username] = user
	return s.saveLocked()
}

// GetUsers returns usernames with their roles (passwords are omitted).
func (s *UserStore) GetUsers() map[string]User {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]User, len(s.users))
	for name, user := range s.users {
		result[name] = User{Role: user.Role}
	}
	return result
}

// validateCredentials rejects values that would break the session token
// format or leave an account unusable.
func validateCredentials(username, password string) error {
	if username == "" {
		return fmt.Errorf("username required")
	}
	if strings.Contains(username, ":") {
		return fmt.Errorf("username must not contain ':'")
	}
	if password == "" {
		return fmt.Errorf("password required")
	}
	return nil
}

// CreateSessionCookie builds a signed cookie valid for sessionTTL.
func (s *UserStore) CreateSessionCookie(username, role string) *http.Cookie {
	expiry := time.Now().Add(sessionTTL)
	token := s.signSession(username, role, expiry.Unix())

	return &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(sessionTTL / time.Second),
	}
}

// signSession builds "payload.signature" for the given expiry timestamp.
func (s *UserStore) signSession(username, role string, expiryUnix int64) string {
	payload := fmt.Sprintf("%s:%s:%d", username, role, expiryUnix)

	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(payload))
	return base64.URLEncoding.EncodeToString([]byte(payload)) + "." +
		base64.URLEncoding.EncodeToString(mac.Sum(nil))
}

// ParseSessionCookie validates the signature and expiry of a session cookie.
func (s *UserStore) ParseSessionCookie(cookie *http.Cookie) (username, role string, err error) {
	if cookie == nil {
		return "", "", fmt.Errorf("no cookie")
	}

	payloadB64, sigB64, found := strings.Cut(cookie.Value, ".")
	if !found || strings.Contains(sigB64, ".") {
		return "", "", fmt.Errorf("invalid token format")
	}

	payloadBytes, err := base64.URLEncoding.DecodeString(payloadB64)
	if err != nil {
		return "", "", fmt.Errorf("invalid token encoding")
	}
	sigBytes, err := base64.URLEncoding.DecodeString(sigB64)
	if err != nil {
		return "", "", fmt.Errorf("invalid signature encoding")
	}

	mac := hmac.New(sha256.New, s.secret)
	mac.Write(payloadBytes)
	if !hmac.Equal(mac.Sum(nil), sigBytes) {
		return "", "", fmt.Errorf("invalid signature")
	}

	name, rest, ok1 := strings.Cut(string(payloadBytes), ":")
	role, expiryStr, ok2 := strings.Cut(rest, ":")
	if !ok1 || !ok2 {
		return "", "", fmt.Errorf("invalid payload format")
	}

	expiryUnix, convErr := strconv.ParseInt(expiryStr, 10, 64)
	if convErr != nil {
		return "", "", fmt.Errorf("invalid expiry")
	}
	if time.Now().Unix() > expiryUnix {
		return "", "", fmt.Errorf("session expired")
	}
	return name, role, nil
}

// RequireAuth wraps a handler with session based authentication.
func (s *UserStore) RequireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isPublicPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		// The kiosk proxy runs on the same host; only loopback peers may
		// claim the kiosk identity, otherwise the header would be a free
		// authentication bypass.
		if r.Header.Get(kioskProxyHeader) == "1" && isLoopback(r.RemoteAddr) {
			next.ServeHTTP(w, r.WithContext(withIdentity(r, Identity{
				Username: kioskUsername,
				Role:     RoleUser,
			})))
			return
		}

		if !s.HasUsers() {
			next.ServeHTTP(w, r.WithContext(withIdentity(r, Identity{
				Username: anonymousUsername,
				Role:     RoleAdmin,
			})))
			return
		}

		cookie, err := r.Cookie(sessionCookieName)
		if err != nil {
			writeUnauthorized(w)
			return
		}
		username, role, err := s.ParseSessionCookie(cookie)
		if err != nil {
			writeUnauthorized(w)
			return
		}

		next.ServeHTTP(w, r.WithContext(withIdentity(r, Identity{
			Username: username,
			Role:     role,
		})))
	})
}

func isPublicPath(path string) bool {
	if strings.HasPrefix(path, "/static/") {
		return true
	}
	for _, p := range publicPaths {
		if path == p {
			return true
		}
	}
	return false
}

func isLoopback(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	fmt.Fprint(w, `{"error":"unauthorized"}`)
}

func withIdentity(r *http.Request, id Identity) context.Context {
	return context.WithValue(r.Context(), identityKey, id)
}

// WithIdentity returns a copy of r carrying the given identity. Useful for
// tests and for handlers that act on behalf of a known principal.
func WithIdentity(r *http.Request, username, role string) *http.Request {
	return r.WithContext(withIdentity(r, Identity{Username: username, Role: role}))
}

// GetUserFromContext returns the identity attached by RequireAuth.
func GetUserFromContext(r *http.Request) (username, role string, ok bool) {
	id, ok := r.Context().Value(identityKey).(Identity)
	if !ok {
		return "", "", false
	}
	return id.Username, id.Role, true
}

// RequireAdmin rejects requests without the admin role.
func RequireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, role, ok := GetUserFromContext(r); !ok || role != RoleAdmin {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"error":"forbidden"}`)
			return
		}
		next(w, r)
	}
}

// RequireAuthHandler rejects requests without an identity.
func RequireAuthHandler(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, _, ok := GetUserFromContext(r); !ok {
			writeUnauthorized(w)
			return
		}
		next(w, r)
	}
}
