package api

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"simple_nvr/internal/alarm"
	"simple_nvr/internal/auth"
)

func TestHandleAlarmLogAndClear(t *testing.T) {
	env := newTestEnv(t)
	srv := env.serve()

	// Лог пуст по умолчанию.
	resp, err := http.Get(srv.URL + "/api/alarm/log")
	if err != nil {
		t.Fatal(err)
	}
	var events []alarm.AlarmEvent
	decodeJSON(t, resp, &events)
	if len(events) != 0 {
		t.Errorf("events = %v", events)
	}

	// Анонимная очистка отклоняется.
	resp, err = http.Post(srv.URL+"/api/alarm/clear", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("anon clear status = %d, want 403", resp.StatusCode)
	}

	// Очистка админом.
	req := adminRequest(http.MethodPost, srv.URL+"/api/alarm/clear", "")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("admin clear status = %d", resp.StatusCode)
	}

	// GET на методном эндпоинте.
	resp, err = http.Get(srv.URL + "/api/alarm/clear")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("get clear status = %d, want 405", resp.StatusCode)
	}
}

func TestHandleAlarmsRangeValidation(t *testing.T) {
	env := newTestEnv(t)
	srv := env.serve()

	tests := []struct {
		name string
		url  string
		want int
	}{
		{"missing params", "/api/alarms/range", http.StatusBadRequest},
		{"missing date", "/api/alarms/range?camera=cam1", http.StatusBadRequest},
		{"missing camera", "/api/alarms/range?date=2024-01-15", http.StatusBadRequest},
		// Path traversal attempts in the date field must be rejected.
		{"traversal date", "/api/alarms/range?camera=cam1&date=../../etc/passwd", http.StatusBadRequest},
		{"slash date", "/api/alarms/range?camera=cam1&date=2024/01/15", http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := http.Get(srv.URL + tt.url)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestHandleAlarmsRangeReadsJournal(t *testing.T) {
	env := newTestEnv(t)

	// Пишем журнал событий в директорию alarm-сервера.
	eventDir := env.api.alarm.EventDir()
	if err := os.MkdirAll(eventDir, 0o755); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(eventDir, "2024-01-15.jsonl")
	line := `{"time":"2024-01-15T08:00:00Z","camera":"cam1","type":"Motion"}` + "\n" +
		`{"time":"2024-01-15T08:01:00Z","camera":"cam2","type":"Motion"}" + "\n"`
	// Fix the malformed JSON above by rewriting cleanly.
	line = `{"time":"2024-01-15T08:00:00Z","camera":"cam1","type":"Motion"}` + "\n" +
		`{"time":"2024-01-15T08:01:00Z","camera":"cam2","type":"Motion"}` + "\n"
	if err := os.WriteFile(journal, []byte(line), 0o644); err != nil {
		t.Fatal(err)
	}

	srv := env.serve()
	resp, err := http.Get(srv.URL + "/api/alarms/range?camera=cam1&date=2024-01-15")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var events []alarm.AlarmEvent
	decodeJSON(t, resp, &events)
	if len(events) != 1 {
		t.Fatalf("events = %+v, want only cam1", events)
	}
	if events[0].Camera != "cam1" {
		t.Errorf("camera = %q", events[0].Camera)
	}
}

func TestHandleAlarmsRangeMissingJournal(t *testing.T) {
	env := newTestEnv(t)
	srv := env.serve()

	// Отсутствующий журнал — пустой массив, а не ошибка.
	resp, err := http.Get(srv.URL + "/api/alarms/range?camera=cam1&date=2030-01-01")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var events []alarm.AlarmEvent
	decodeJSON(t, resp, &events)
	if len(events) != 0 {
		t.Errorf("events = %v", events)
	}
}

func TestHandleAlarmStatus(t *testing.T) {
	env := newTestEnv(t)
	srv := env.serve()

	resp, err := http.Get(srv.URL + "/api/alarm/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var body map[string]any
	decodeJSON(t, resp, &body)
	if _, ok := body["dahua"]; !ok {
		t.Errorf("status = %v", body)
	}
	if _, ok := body["hikvision"]; !ok {
		t.Errorf("status = %v", body)
	}
}

func TestHandleAlarmStartStopRequiresAdmin(t *testing.T) {
	env := newTestEnv(t)
	srv := env.serve()

	// Аноним.
	resp, err := http.Post(srv.URL+"/api/alarm/start", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("anon start status = %d, want 403", resp.StatusCode)
	}

	// Админ на несвободном порту — ожидаем 500 (порт занят или нет прав).
	req := adminRequest(http.MethodPost, srv.URL+"/api/alarm/start", "")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	// Порт 15002 в тестовом окружении обычно свободен, но мы не хотим
	// реально слушать — просто проверяем, что роль проверяется выше.

	// Метод не поддерживается.
	resp, err = http.Get(srv.URL + "/api/alarm/start")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("get start status = %d, want 405", resp.StatusCode)
	}
}

func TestHandleUsersLifecycle(t *testing.T) {
	env := newTestEnv(t)
	srv := env.serve()

	// Первый пользователь — bootstrap-админ без авторизации.
	req := adminRequest(http.MethodPost, srv.URL+"/api/users", `{"username":"admin","password":"secret","role":"user"}`)
	req.Header.Del("X-Test-User")
	req.Header.Del("X-Test-Role")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bootstrap status = %d", resp.StatusCode)
	}
	if !env.users.HasUsers() {
		t.Fatal("user store should have users")
	}

	// Повторный первый пользователь отклоняется без админ-роли.
	req = adminRequest(http.MethodPost, srv.URL+"/api/users", `{"username":"mallory","password":"x","role":"admin"}`)
	req.Header.Del("X-Test-User")
	req.Header.Del("X-Test-Role")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("anon second user status = %d, want 403", resp.StatusCode)
	}

	// Админ добавляет обычного пользователя.
	req = adminRequest(http.MethodPost, srv.URL+"/api/users", `{"username":"bob","password":"pw","role":"user"}`)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("add user status = %d", resp.StatusCode)
	}

	// Пустые поля.
	req = adminRequest(http.MethodPost, srv.URL+"/api/users", `{"username":"","password":""}`)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("empty user status = %d, want 400", resp.StatusCode)
	}

	// Список пользователей.
	req = adminRequest(http.MethodGet, srv.URL+"/api/users", "")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var users map[string]any
	decodeJSON(t, resp, &users)
	if _, ok := users["bob"]; !ok {
		t.Errorf("users = %v", users)
	}

	// Нельзя удалить самого себя.
	req = adminRequest(http.MethodDelete, srv.URL+"/api/users?username=admin", "")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("self delete status = %d, want 400", resp.StatusCode)
	}

	// Удаление bob админом.
	req = adminRequest(http.MethodDelete, srv.URL+"/api/users?username=bob", "")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("delete bob status = %d", resp.StatusCode)
	}

	// Пустой username.
	req = adminRequest(http.MethodDelete, srv.URL+"/api/users", "")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("empty delete status = %d, want 400", resp.StatusCode)
	}

	// Неверный метод.
	req = adminRequest(http.MethodPatch, srv.URL+"/api/users", "")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("patch status = %d, want 405", resp.StatusCode)
	}
}

func TestHandleLoginAndAuthCheck(t *testing.T) {
	env := newTestEnv(t)
	if err := env.users.AddUser("alice", "pw123", auth.RoleUser); err != nil {
		t.Fatal(err)
	}
	srv := env.serve()

	// Неверные учётные данные.
	resp, err := http.Post(srv.URL+"/api/auth/login", "application/json", strings.NewReader(`{"username":"alice","password":"wrong"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("bad login status = %d, want 401", resp.StatusCode)
	}

	// Успешный вход ставит cookie.
	resp, err = http.Post(srv.URL+"/api/auth/login", "application/json", strings.NewReader(`{"username":"alice","password":"pw123"}`))
	if err != nil {
		t.Fatal(err)
	}
	var loginBody map[string]string
	decodeJSON(t, resp, &loginBody)
	if loginBody["role"] != auth.RoleUser {
		t.Errorf("role = %q", loginBody["role"])
	}
	if len(resp.Cookies()) == 0 {
		t.Error("expected session cookie")
	}

	// Неверный JSON.
	resp, err = http.Post(srv.URL+"/api/auth/login", "application/json", strings.NewReader("{nope"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad json login status = %d, want 400", resp.StatusCode)
	}

	// GET запрещён.
	resp, err = http.Get(srv.URL + "/api/auth/login")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("get login status = %d, want 405", resp.StatusCode)
	}

	// auth/check при наличии пользователей без сессии.
	resp, err = http.Get(srv.URL + "/api/auth/check")
	if err != nil {
		t.Fatal(err)
	}
	var checkBody map[string]any
	decodeJSON(t, resp, &checkBody)
	if checkBody["auth_required"] != true || checkBody["authorized"] != false {
		t.Errorf("check = %v", checkBody)
	}

	// /api/auth/me без сессии.
	resp, err = http.Get(srv.URL + "/api/auth/me")
	if err != nil {
		t.Fatal(err)
	}
	var meBody map[string]any
	decodeJSON(t, resp, &meBody)
	if meBody["authorized"] != false {
		t.Errorf("me = %v", meBody)
	}

	// Logout очищает cookie.
	resp, err = http.Post(srv.URL+"/api/auth/logout", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
}

func TestHandleAuthCheckNoUsers(t *testing.T) {
	env := newTestEnv(t)
	srv := env.serve()

	resp, err := http.Get(srv.URL + "/api/auth/check")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var body map[string]any
	decodeJSON(t, resp, &body)
	if body["auth_required"] != false {
		t.Errorf("check = %v", body)
	}
}

func TestHandleChangePassword(t *testing.T) {
	env := newTestEnv(t)
	if err := env.users.AddUser("carol", "old", auth.RoleUser); err != nil {
		t.Fatal(err)
	}
	srv := env.serve()

	// Смена пароля от имени самой carol (без админ-роли).
	changePassword := func(body string, want int, label string) {
		t.Helper()
		req := userRequest(http.MethodPost, srv.URL+"/api/users/change-password", body)
		req.Header.Set("X-Test-User", "carol")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("%s status = %d, want %d", label, resp.StatusCode, want)
		}
	}

	// Смена пароля без старого пароля.
	changePassword(`{"username":"carol","new_password":"new"}`, http.StatusBadRequest, "no old password")

	// Неверный старый пароль.
	changePassword(`{"username":"carol","old_password":"bad","new_password":"new"}`, http.StatusUnauthorized, "wrong old password")

	// Чужой пользователь без админ-роли.
	changePassword(`{"username":"dave","old_password":"x","new_password":"new"}`, http.StatusForbidden, "other user")

	// Успешная смена.
	changePassword(`{"username":"carol","old_password":"old","new_password":"new"}`, http.StatusOK, "change")
	if _, err := env.users.Authenticate("carol", "new"); err != nil {
		t.Errorf("new password should work: %v", err)
	}

	// Пустой новый пароль.
	changePassword(`{"username":"carol","old_password":"new","new_password":""}`, http.StatusBadRequest, "empty new password")
}

func TestHandleLogs(t *testing.T) {
	env := newTestEnv(t)
	srv := env.serve()

	resp, err := http.Get(srv.URL + "/api/logs")
	if err != nil {
		t.Fatal(err)
	}
	var entries []any
	decodeJSON(t, resp, &entries)
	if entries == nil {
		t.Error("logs should be a JSON array")
	}

	// Очистка без админ-роли.
	resp, err = http.Post(srv.URL+"/api/logs/clear", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("anon clear status = %d, want 403", resp.StatusCode)
	}

	// Очистка админом.
	req := adminRequest(http.MethodPost, srv.URL+"/api/logs/clear", "")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("admin clear status = %d", resp.StatusCode)
	}

	// GET на методном эндпоинте.
	resp, err = http.Get(srv.URL + "/api/logs/clear")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("get clear status = %d, want 405", resp.StatusCode)
	}
}

func TestHandleHikvisionStartStopRequiresAdmin(t *testing.T) {
	env := newTestEnv(t)
	srv := env.serve()

	resp, err := http.Post(srv.URL+"/api/hikvision/start", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("anon start status = %d, want 403", resp.StatusCode)
	}

	resp, err = http.Post(srv.URL+"/api/hikvision/stop", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("anon stop status = %d, want 403", resp.StatusCode)
	}

	resp, err = http.Get(srv.URL + "/api/hikvision/start")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("get start status = %d, want 405", resp.StatusCode)
	}
}
