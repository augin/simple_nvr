package api

import (
	"encoding/json"
	"net/http"

	"simple_nvr/internal/auth"
)

func (a *API) HandleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	user, err := a.userStore.Authenticate(req.Username, req.Password)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	cookie := a.userStore.CreateSessionCookie(req.Username, user.Role)
	http.SetCookie(w, cookie)

	writeJSON(w, http.StatusOK, map[string]string{
		"username": req.Username,
		"role":     user.Role,
	})
}

func (a *API) HandleLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     "session_token",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		MaxAge:   -1,
	})

	writeStatus(w, "logged out")
}

func (a *API) HandleMe(w http.ResponseWriter, r *http.Request) {
	username, role, ok := auth.GetUserFromContext(r)
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{"authorized": false})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"authorized": true,
		"username":   username,
		"role":       role,
	})
}

func (a *API) HandleAuthCheck(w http.ResponseWriter, r *http.Request) {
	if !a.userStore.HasUsers() {
		writeJSON(w, http.StatusOK, map[string]any{"auth_required": false})
		return
	}

	username, role, ok := auth.GetUserFromContext(r)
	if !ok {
		if cookie, err := r.Cookie("session_token"); err == nil {
			username, role, err = a.userStore.ParseSessionCookie(cookie)
			if err == nil {
				ok = true
			}
		}
	}

	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{"auth_required": true, "authorized": false})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"auth_required": true,
		"authorized":    true,
		"username":      username,
		"role":          role,
	})
}

func (a *API) HandleGetUsers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.userStore.GetUsers())
}

func (a *API) HandleAddUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// The very first user is the bootstrap admin and needs no credentials.
	if a.userStore.HasUsers() {
		if !requireAdminRole(r) {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	if req.Username == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "username and password required")
		return
	}

	if !a.userStore.HasUsers() && req.Role != "admin" {
		req.Role = "admin"
	}

	if req.Role == "" {
		req.Role = "user"
	}

	if err := a.userStore.AddUser(req.Username, req.Password, req.Role); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeStatus(w, "created")
}

func (a *API) HandleDeleteUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	username := r.URL.Query().Get("username")
	if username == "" {
		writeError(w, http.StatusBadRequest, "username required")
		return
	}

	currentUser, _, _ := auth.GetUserFromContext(r)
	if currentUser == username {
		writeError(w, http.StatusBadRequest, "cannot delete yourself")
		return
	}

	if err := a.userStore.DeleteUser(username); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeStatus(w, "deleted")
}

func (a *API) HandleChangePassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		Username    string `json:"username"`
		OldPassword string `json:"old_password"`
		NewPassword string `json:"new_password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	currentUser, currentRole, _ := auth.GetUserFromContext(r)

	if currentRole != auth.RoleAdmin {
		if currentUser != req.Username {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
		if req.OldPassword == "" {
			writeError(w, http.StatusBadRequest, "old password required")
			return
		}
		if _, err := a.userStore.Authenticate(currentUser, req.OldPassword); err != nil {
			writeError(w, http.StatusUnauthorized, "invalid old password")
			return
		}
	}

	if req.NewPassword == "" {
		writeError(w, http.StatusBadRequest, "new password required")
		return
	}

	if err := a.userStore.ChangePassword(req.Username, req.NewPassword); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeStatus(w, "password changed")
}
