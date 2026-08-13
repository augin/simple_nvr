package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

type User struct {
	Password string `yaml:"password" json:"-"`
	Role     string `yaml:"role" json:"role"`
}

type userStoreEntry struct {
	Password string `json:"password"`
	Role     string `json:"role"`
}

type UserStore struct {
	mu       sync.RWMutex
	users    map[string]User
	filePath string
	secret   []byte
}

type contextKey string

const userContextKey contextKey = "user"

func NewUserStore(filePath string) *UserStore {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		log.Fatalf("Failed to generate session secret: %v", err)
	}

	store := &UserStore{
		users:    make(map[string]User),
		filePath: filePath,
		secret:   secret,
	}

	store.Load()
	return store
}

func (s *UserStore) Load() error {
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var entries map[string]userStoreEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return err
	}

	s.users = make(map[string]User, len(entries))
	for k, v := range entries {
		s.users[k] = User{Password: v.Password, Role: v.Role}
	}
	return nil
}

func (s *UserStore) Save() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.saveLocked()
}

func (s *UserStore) saveLocked() error {
	entries := make(map[string]userStoreEntry, len(s.users))
	for k, v := range s.users {
		entries[k] = userStoreEntry{Password: v.Password, Role: v.Role}
	}

	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.filePath, data, 0600)
}

func (s *UserStore) HasUsers() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.users) > 0
}

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

func (s *UserStore) AddUser(username, password, role string) error {
	if role != "admin" && role != "user" {
		return fmt.Errorf("invalid role: must be admin or user")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.users[username]; exists {
		return fmt.Errorf("user already exists")
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("failed to hash password: %v", err)
	}

	s.users[username] = User{
		Password: string(hash),
		Role:     role,
	}

	return s.saveLocked()
}

func (s *UserStore) DeleteUser(username string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.users[username]; !exists {
		return fmt.Errorf("user not found")
	}

	delete(s.users, username)
	return s.saveLocked()
}

func (s *UserStore) ChangePassword(username, newPassword string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	user, exists := s.users[username]
	if !exists {
		return fmt.Errorf("user not found")
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("failed to hash password: %v", err)
	}

	user.Password = string(hash)
	s.users[username] = user

	return s.saveLocked()
}

func (s *UserStore) GetUsers() map[string]User {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]User, len(s.users))
	for k, v := range s.users {
		result[k] = User{Role: v.Role}
	}
	return result
}

func (s *UserStore) CreateSessionCookie(username, role string) *http.Cookie {
	expiry := time.Now().Add(24 * time.Hour)
	payload := fmt.Sprintf("%s:%s:%d", username, role, expiry.Unix())
	sig := hmac.New(sha256.New, s.secret)
	sig.Write([]byte(payload))
	token := base64.URLEncoding.EncodeToString([]byte(payload)) + "." + base64.URLEncoding.EncodeToString(sig.Sum(nil))

	return &http.Cookie{
		Name:     "session_token",
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   86400,
	}
}

func (s *UserStore) ParseSessionCookie(cookie *http.Cookie) (username, role string, err error) {
	parts := strings.Split(cookie.Value, ".")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid token format")
	}

	payloadBytes, err := base64.URLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", "", fmt.Errorf("invalid token encoding")
	}

	sigBytes, err := base64.URLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", fmt.Errorf("invalid signature encoding")
	}

	sig := hmac.New(sha256.New, s.secret)
	sig.Write(payloadBytes)
	if !hmac.Equal(sig.Sum(nil), sigBytes) {
		return "", "", fmt.Errorf("invalid signature")
	}

	payload := string(payloadBytes)
	parts2 := strings.SplitN(payload, ":", 3)
	if len(parts2) != 3 {
		return "", "", fmt.Errorf("invalid payload format")
	}

	expiryInt, _ := strconv.ParseInt(parts2[2], 10, 64)
	if time.Now().Unix() > expiryInt {
		return "", "", fmt.Errorf("session expired")
	}

	return parts2[0], parts2[1], nil
}

func (s *UserStore) RequireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		if path == "/" || path == "/favicon.ico" || path == "/api/auth/login" || path == "/api/auth/check" || path == "/api/version" || strings.HasPrefix(path, "/static/") {
			next.ServeHTTP(w, r)
			return
		}

		if !s.HasUsers() {
			ctx := context.WithValue(r.Context(), userContextKey, map[string]string{
				"username": "admin",
				"role":     "admin",
			})
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		cookie, err := r.Cookie("session_token")
		if err != nil {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}

		username, role, err := s.ParseSessionCookie(cookie)
		if err != nil {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}

		ctx := context.WithValue(r.Context(), userContextKey, map[string]string{
			"username": username,
			"role":     role,
		})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func GetUserFromContext(r *http.Request) (username, role string, ok bool) {
	val := r.Context().Value(userContextKey)
	if val == nil {
		return "", "", false
	}
	m, ok := val.(map[string]string)
	if !ok {
		return "", "", false
	}
	return m["username"], m["role"], true
}

func RequireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, role, ok := GetUserFromContext(r)
		if !ok || role != "admin" {
			http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

func RequireAuthHandler(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, _, ok := GetUserFromContext(r)
		if !ok {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}
