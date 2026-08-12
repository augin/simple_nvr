---
name: deploy-nvr
description: Use when the user has made code changes to the Simple NVR project and wants to deploy, build, or update the running service. Triggers on keywords like задеплой, собери, обнови, deploy, build, проверь. Handles version bump, git commit, push, and local rebuild. For explicit release requests (релиз, release, новый релиз), also creates and pushes a git tag.
---

# Deploy Simple NVR

Automates the full deploy cycle for the Simple NVR Go project at `/root/simple_nvr`.

## Project context

- Go backend + vanilla HTML/JS/CSS frontend
- Binary: `/usr/bin/simple-nvr`
- Static files: `/usr/share/simple-nvr/` (templates, css, js)
- Systemd service: `simple-nvr`
- Version lives in `main.go` as `var version = "X.Y.Z"`
- Build: `CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=VERSION" -o nvr .`
- GitHub releases triggered by `v*` tags via `.github/workflows/deb-release.yml`

## Deployment workflow

Perform these steps IN ORDER. Do not skip steps.

### Step 1: Determine current version

```bash
cd /root/simple_nvr && grep 'var version' main.go
```

Extract the version string (e.g. `2.5.0`).

### Step 2: Bump minor version

Increment the patch number: `2.5.0` -> `2.5.1`, `2.5.9` -> `2.5.10`.
If the user explicitly asks for a major/minor bump, follow their instruction instead.

### Step 3: Update version in main.go

Edit `main.go` and change the `var version` line to the new version.

### Step 4: Build

```bash
cd /root/simple_nvr && export PATH=$PATH:/usr/local/go/bin && CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=NEW_VERSION" -o nvr .
```

### Step 5: Git commit and push

```bash
cd /root/simple_nvr && git add -A && git commit -m "vNEW_VERSION: <brief description of changes>" && git push
```

Write a meaningful commit message based on what files changed. Use conventional prefixes (feat:, fix:, refactor:, etc.).

### Step 6: Local deploy

```bash
systemctl stop simple-nvr && sleep 1
cp /root/simple_nvr/nvr /usr/bin/simple-nvr
cp /root/simple_nvr/templates/index.html /usr/share/simple-nvr/templates/
cp /root/simple_nvr/static/css/style.css /usr/share/simple-nvr/static/css/
cp /root/simple_nvr/static/js/app.js /usr/share/simple-nvr/static/js/
systemctl start simple-nvr && sleep 2
```

### Step 7: Verify

```bash
curl -s http://localhost:8180/api/version
```

Confirm the response shows the new version. Report success to the user.

## Release workflow

When the user explicitly requests a release (релиз, release, новый релиз, выпусти релиз):

1. Complete deployment steps 1-7 above
2. Create and push a git tag:
   ```bash
   cd /root/simple_nvr && git tag vNEW_VERSION && git push origin vNEW_VERSION
   ```
3. Tell the user the GitHub Actions workflow is building the deb package and point them to the releases page: `https://github.com/augin/simple_nvr/releases`

## Important notes

- Never commit secrets, keys, or credentials.
- If `go build` fails, show the error and stop. Do not deploy a broken build.
- If the port 8180 is already in use after restart, check `fuser -k 8180/tcp` and restart again.
- The deploy.sh script exists but prefer inline commands for clarity and control.
