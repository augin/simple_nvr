package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func (a *API) HandleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	user, err := a.userStore.Authenticate(req.Username, req.Password)
	if err != nil {
		http.Error(w, `{"error":"invalid credentials"}`, http.StatusUnauthorized)
		return
	}

	cookie := a.userStore.CreateSessionCookie(req.Username, user.Role)
	http.SetCookie(w, cookie)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
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

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "logged out"})
}

func (a *API) HandleMe(w http.ResponseWriter, r *http.Request) {
	username, role, ok := GetUserFromContext(r)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"authorized": false})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"authorized": true,
		"username":   username,
		"role":       role,
	})
}

func (a *API) HandleAuthCheck(w http.ResponseWriter, r *http.Request) {
	if !a.userStore.HasUsers() {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"auth_required": false})
		return
	}

	username, role, ok := GetUserFromContext(r)
	if !ok {
		if cookie, err := r.Cookie("session_token"); err == nil {
			username, role, err = a.userStore.ParseSessionCookie(cookie)
			if err == nil {
				ok = true
			}
		}
	}

	if !ok {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"auth_required": true, "authorized": false})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"auth_required": true,
		"authorized":    true,
		"username":      username,
		"role":          role,
	})
}

func (a *API) HandleGetUsers(w http.ResponseWriter, r *http.Request) {
	users := a.userStore.GetUsers()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(users)
}

func (a *API) HandleAddUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if a.userStore.HasUsers() {
		_, role, ok := GetUserFromContext(r)
		if !ok || role != "admin" {
			http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
			return
		}
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	if req.Username == "" || req.Password == "" {
		http.Error(w, `{"error":"username and password required"}`, http.StatusBadRequest)
		return
	}

	if !a.userStore.HasUsers() && req.Role != "admin" {
		req.Role = "admin"
	}

	if req.Role == "" {
		req.Role = "user"
	}

	if err := a.userStore.AddUser(req.Username, req.Password, req.Role); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "created"})
}

func (a *API) HandleDeleteUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	username := r.URL.Query().Get("username")
	if username == "" {
		http.Error(w, `{"error":"username required"}`, http.StatusBadRequest)
		return
	}

	currentUser, _, _ := GetUserFromContext(r)
	if currentUser == username {
		http.Error(w, `{"error":"cannot delete yourself"}`, http.StatusBadRequest)
		return
	}

	if err := a.userStore.DeleteUser(username); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
}

func (a *API) HandleChangePassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Username    string `json:"username"`
		OldPassword string `json:"old_password"`
		NewPassword string `json:"new_password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	currentUser, currentRole, _ := GetUserFromContext(r)

	if currentRole != "admin" {
		if currentUser != req.Username {
			http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
			return
		}
		if req.OldPassword == "" {
			http.Error(w, `{"error":"old password required"}`, http.StatusBadRequest)
			return
		}
		if _, err := a.userStore.Authenticate(currentUser, req.OldPassword); err != nil {
			http.Error(w, `{"error":"invalid old password"}`, http.StatusUnauthorized)
			return
		}
	}

	if req.NewPassword == "" {
		http.Error(w, `{"error":"new password required"}`, http.StatusBadRequest)
		return
	}

	if err := a.userStore.ChangePassword(req.Username, req.NewPassword); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "password changed"})
}
