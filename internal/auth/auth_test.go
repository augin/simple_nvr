package auth

import (
	"encoding/base64"
	"encoding/json"
	"golang.org/x/crypto/bcrypt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestMain keeps bcrypt cheap so the suite stays fast.
func TestMain(m *testing.M) {
	bcryptCost = bcrypt.MinCost
	os.Exit(m.Run())
}

func newTestStore(t *testing.T) *UserStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "users.json")
	store := NewUserStore(path)
	if err := store.Err(); err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	return store
}

func TestNewUserStoreMissingFile(t *testing.T) {
	store := NewUserStore(filepath.Join(t.TempDir(), "absent.json"))
	if err := store.Err(); err != nil {
		t.Fatalf("missing file must not be an error: %v", err)
	}
	if store.HasUsers() {
		t.Error("HasUsers = true for empty store")
	}
}

func TestNewUserStoreCorruptedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "users.json")
	if err := os.WriteFile(path, []byte("{not json"), 0600); err != nil {
		t.Fatal(err)
	}
	store := NewUserStore(path)
	if store.Err() == nil {
		t.Fatal("corrupted users file must be reported")
	}
}

func TestAddUserPersistsAndReloads(t *testing.T) {
	store := newTestStore(t)

	if err := store.AddUser("alice", "secret", RoleAdmin); err != nil {
		t.Fatalf("AddUser: %v", err)
	}
	if !store.HasUsers() {
		t.Error("HasUsers = false after AddUser")
	}

	info, err := os.Stat(store.filePath)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("users file mode = %o, want 600", perm)
	}

	// a fresh store instance sees the same account
	reloaded := NewUserStore(store.filePath)
	if err := reloaded.Err(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if user, err := reloaded.Authenticate("alice", "secret"); err != nil || user.Role != RoleAdmin {
		t.Fatalf("Authenticate after reload = (%v, %v)", user, err)
	}
}

func TestAddUserValidation(t *testing.T) {
	store := newTestStore(t)
	if err := store.AddUser("bob", "pw", RoleUser); err != nil {
		t.Fatalf("AddUser: %v", err)
	}

	cases := []struct {
		name, pass, role string
		wantErr          string
	}{
		{"bob", "pw", RoleUser, "already exists"},
		{"", "pw", RoleUser, "username required"},
		{"carol", "", RoleUser, "password required"},
		{"with:colon", "pw", RoleUser, "must not contain"},
		{"carol", "pw", "root", "invalid role"},
		{"carol", "pw", "", "invalid role"},
	}
	for _, tc := range cases {
		err := store.AddUser(tc.name, tc.pass, tc.role)
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("AddUser(%q,%q,%q) = %v, want error containing %q",
				tc.name, tc.pass, tc.role, err, tc.wantErr)
		}
	}
}

func TestAuthenticate(t *testing.T) {
	store := newTestStore(t)
	if err := store.AddUser("alice", "secret", RoleUser); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Authenticate("alice", "secret"); err != nil {
		t.Errorf("valid credentials rejected: %v", err)
	}
	if _, err := store.Authenticate("alice", "wrong"); err == nil {
		t.Error("wrong password accepted")
	}
	if _, err := store.Authenticate("nobody", "secret"); err == nil {
		t.Error("unknown user accepted")
	}
}

func TestChangePassword(t *testing.T) {
	store := newTestStore(t)
	if err := store.AddUser("alice", "old", RoleUser); err != nil {
		t.Fatal(err)
	}

	if err := store.ChangePassword("alice", "new"); err != nil {
		t.Fatalf("ChangePassword: %v", err)
	}
	if _, err := store.Authenticate("alice", "old"); err == nil {
		t.Error("old password still valid")
	}
	if _, err := store.Authenticate("alice", "new"); err != nil {
		t.Errorf("new password rejected: %v", err)
	}

	if err := store.ChangePassword("ghost", "x"); err == nil {
		t.Error("changing password of unknown user must fail")
	}
	if err := store.ChangePassword("alice", ""); err == nil {
		t.Error("empty new password must fail")
	}
}

func TestDeleteUser(t *testing.T) {
	store := newTestStore(t)
	if err := store.AddUser("alice", "pw", RoleUser); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteUser("alice"); err != nil {
		t.Fatalf("DeleteUser: %v", err)
	}
	if _, err := store.Authenticate("alice", "pw"); err == nil {
		t.Error("deleted user still authenticates")
	}
	if err := store.DeleteUser("alice"); err == nil {
		t.Error("deleting twice must fail")
	}
}

func TestGetUsersOmitsPasswords(t *testing.T) {
	store := newTestStore(t)
	if err := store.AddUser("alice", "secret", RoleAdmin); err != nil {
		t.Fatal(err)
	}

	users := store.GetUsers()
	user, ok := users["alice"]
	if !ok {
		t.Fatal("alice missing")
	}
	if user.Password != "" {
		t.Errorf("password leaked: %q", user.Password)
	}
	if user.Role != RoleAdmin {
		t.Errorf("Role = %q", user.Role)
	}

	raw, err := json.Marshal(users)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "secret") || strings.Contains(string(raw), "password") {
		t.Errorf("JSON must not contain password material: %s", raw)
	}
}

func TestSessionCookieRoundTrip(t *testing.T) {
	store := newTestStore(t)
	cookie := store.CreateSessionCookie("alice", RoleAdmin)

	if cookie.Name != sessionCookieName || !cookie.HttpOnly || cookie.MaxAge != 86400 {
		t.Errorf("cookie = %+v", cookie)
	}

	username, role, err := store.ParseSessionCookie(cookie)
	if err != nil {
		t.Fatalf("ParseSessionCookie: %v", err)
	}
	if username != "alice" || role != RoleAdmin {
		t.Errorf("parsed = (%q, %q)", username, role)
	}
}

func TestParseSessionCookieRejections(t *testing.T) {
	store := newTestStore(t)
	other := newTestStore(t)

	valid := store.CreateSessionCookie("alice", RoleUser).Value

	cases := map[string]string{
		"no dot":            "plaintext",
		"empty":             "",
		"two dots":          "a.b.c",
		"bad payload b64":   "!!!." + valid[strings.Index(valid, ".")+1:],
		"bad signature b64": valid[:strings.Index(valid, ".")] + ".!!!",
		"foreign secret":    other.CreateSessionCookie("alice", RoleUser).Value,
	}

	// tampered payload keeps the original signature
	sigB64 := valid[strings.Index(valid, ".")+1:]
	cases["tampered payload"] = base64.URLEncoding.EncodeToString(
		[]byte("mallory:admin:9999999999")) + "." + sigB64

	// expired session
	expired := store.signSession("alice", RoleUser, time.Now().Add(-time.Minute).Unix())
	cases["expired"] = expired

	for name, value := range cases {
		if _, _, err := store.ParseSessionCookie(&http.Cookie{Value: value}); err == nil {
			t.Errorf("%s: expected rejection", name)
		}
	}

	if _, _, err := store.ParseSessionCookie(nil); err == nil {
		t.Error("nil cookie: expected rejection")
	}
}

func TestRequireAuthPublicPaths(t *testing.T) {
	store := newTestStore(t)
	if err := store.AddUser("alice", "pw", RoleUser); err != nil {
		t.Fatal(err)
	}

	handler := store.RequireAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, _, ok := GetUserFromContext(r); ok {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))

	for _, path := range []string{"/", "/favicon.ico", "/api/auth/login", "/api/auth/check", "/api/version", "/static/css/style.css"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status = %d", path, rec.Code)
		}
	}
}

func TestRequireAuthWithoutUsersGrantsAdmin(t *testing.T) {
	store := newTestStore(t)
	handler := store.RequireAuth(identityProbe())

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, `"role":"admin"`) {
		t.Errorf("body = %s", body)
	}
}

func TestRequireAuthWithSession(t *testing.T) {
	store := newTestStore(t)
	if err := store.AddUser("alice", "pw", RoleUser); err != nil {
		t.Fatal(err)
	}
	handler := store.RequireAuth(identityProbe())

	// no cookie
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("no cookie: status = %d, want 401", rec.Code)
	}

	// valid cookie
	req = httptest.NewRequest(http.MethodGet, "/api/status", nil)
	req.AddCookie(store.CreateSessionCookie("alice", RoleUser))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"username":"alice"`) {
		t.Errorf("valid cookie: status = %d body = %s", rec.Code, rec.Body.String())
	}

	// garbage cookie
	req = httptest.NewRequest(http.MethodGet, "/api/status", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "garbage"})
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("garbage cookie: status = %d, want 401", rec.Code)
	}
}

func TestKioskHeaderTrustedOnlyFromLoopback(t *testing.T) {
	store := newTestStore(t)
	if err := store.AddUser("alice", "pw", RoleUser); err != nil {
		t.Fatal(err)
	}
	handler := store.RequireAuth(identityProbe())

	// loopback peer: kiosk identity
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	req.RemoteAddr = "127.0.0.1:45234"
	req.Header.Set(kioskProxyHeader, "1")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"role":"user"`) || !strings.Contains(rec.Body.String(), `"username":"kiosk"`) {
		t.Errorf("loopback kiosk: status = %d body = %s", rec.Code, rec.Body.String())
	}

	// remote peer cannot use the header to bypass authentication
	req = httptest.NewRequest(http.MethodGet, "/api/status", nil)
	req.RemoteAddr = "203.0.113.7:45234"
	req.Header.Set(kioskProxyHeader, "1")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("remote kiosk header: status = %d, want 401", rec.Code)
	}
}

func TestRequireAdmin(t *testing.T) {
	inner := func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(withIdentity(req, Identity{Username: "a", Role: RoleAdmin}))
	rec := httptest.NewRecorder()
	RequireAdmin(inner)(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("admin: status = %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(withIdentity(req, Identity{Username: "a", Role: RoleUser}))
	rec = httptest.NewRecorder()
	RequireAdmin(inner)(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Errorf("user: status = %d, want 403", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	rec = httptest.NewRecorder()
	RequireAdmin(inner)(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Errorf("anonymous: status = %d, want 403", rec.Code)
	}
}

func TestRequireAuthHandler(t *testing.T) {
	inner := func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	RequireAuthHandler(inner)(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("anonymous: status = %d, want 401", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(withIdentity(req, Identity{Username: "a", Role: RoleUser}))
	rec = httptest.NewRecorder()
	RequireAuthHandler(inner)(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("with identity: status = %d", rec.Code)
	}
}

func TestGetUserFromContextWithoutIdentity(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if _, _, ok := GetUserFromContext(req); ok {
		t.Error("expected no identity")
	}
}

// identityProbe reports the attached identity as JSON.
func identityProbe() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, role, ok := GetUserFromContext(r)
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"username": username, "role": role})
	})
}
