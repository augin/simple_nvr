let currentCamera = '';
let currentPlaybackSpeed = 1;
let currentConfig = {};
let currentFile = '';
let currentFolder = '';
let archiveCamera = '';
let alarmLogTimer = null;
let alarmRunning = false;

document.addEventListener('DOMContentLoaded', () => {
  loadTheme();
  fetchCameras();
  fetchStatus();
  fetchVersion();
  setInterval(fetchStatus, 5000);
  initVideoZoom('video-player');
  initVideoZoom('archive-video-player');
});

function switchTab(tab) {
  document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.nav-tab').forEach(el => el.classList.remove('active'));
  document.getElementById('tab-' + tab).classList.add('active');
  document.querySelector(`.nav-tab[onclick="switchTab('${tab}')"]`).classList.add('active');

  if (tab === 'settings') {
    loadSettings();
  }
  if (tab === 'archive') {
    fetchArchiveCameras();
  }
  if (tab === 'alarm') {
    loadAlarmStatus();
    loadAlarmLog();
    alarmLogTimer = setInterval(loadAlarmLog, 5000);
  } else {
    if (alarmLogTimer) {
      clearInterval(alarmLogTimer);
      alarmLogTimer = null;
    }
  }
}

function toggleTheme() {
  const html = document.documentElement;
  const isLight = html.classList.contains('light');
  html.classList.toggle('light');
  localStorage.setItem('theme', isLight ? 'dark' : 'light');
}

function loadTheme() {
  const theme = localStorage.getItem('theme');
  if (theme === 'light') {
    document.documentElement.classList.add('light');
  }
}

async function fetchCameras() {
  try {
    const resp = await fetch('/api/cameras');
    const data = await resp.json();
    const container = document.getElementById('camera-list');
    container.innerHTML = '';

    if (data.error) {
      container.innerHTML = `<div class="error-msg">${data.error}</div>`;
      return;
    }

    (data.cameras || []).forEach(camera => {
      const btn = document.createElement('button');
      btn.className = 'camera-btn';
      btn.textContent = camera;
      btn.onclick = () => selectCamera(camera);
      container.appendChild(btn);
    });

    if (!data.cameras || data.cameras.length === 0) {
      container.innerHTML = '<div class="empty-msg">Камеры не найдены</div>';
    }
  } catch (err) {
    console.error('Error fetching cameras:', err);
    document.getElementById('camera-list').innerHTML = `<div class="error-msg">${err.message}</div>`;
  }
}

function selectCamera(camera) {
  currentCamera = camera;
  document.getElementById('current-camera').textContent = camera;

  document.querySelectorAll('.camera-btn').forEach(btn => {
    btn.classList.toggle('active', btn.textContent === camera);
  });

  fetchFiles(camera);
}

async function fetchFiles(camera) {
  try {
    const resp = await fetch(`/api/files?camera=${encodeURIComponent(camera)}`);
    const data = await resp.json();
    await renderFileTree(data, camera);
  } catch (err) {
    console.error('Error fetching files:', err);
  }
}

async function fetchAlarmsForDate(camera, date) {
  try {
    const resp = await fetch(`/api/alarms/range?camera=${encodeURIComponent(camera)}&date=${date}`);
    return await resp.json();
  } catch (err) {
    return [];
  }
}

function fileHasAlarm(file, events) {
  const name = file.replace('.mp4', '');
  const parts = name.split('-');
  if (parts.length < 2) return null;
  const h = parseInt(parts[0], 10);
  const m = parseInt(parts[1], 10);
  if (isNaN(h) || isNaN(m)) return null;

  const startSec = h * 3600 + m * 60;
  const endMin = Math.ceil((m + 1) / 10) * 10;
  const endH = endMin >= 60 ? h + 1 : h;
  const endM = endMin >= 60 ? 0 : endMin;
  const endSec = endH * 3600 + endM * 60;

  const matched = [];
  for (const e of events) {
    const t = new Date(e.time);
    const eSec = t.getHours() * 3600 + t.getMinutes() * 60;
    if (eSec >= startSec && eSec < endSec) {
      matched.push({
        time: t.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
        event: e.event || 'Unknown',
      });
    }
  }
  return matched.length > 0 ? matched : null;
}

const ALARM_ICONS = {
  HumanDetect: {
    svg: '<svg class="alarm-icon-svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>',
    cssClass: 'alarm-icon-human',
  },
  MotionDetect: {
    svg: '<svg class="alarm-icon-svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>',
    cssClass: 'alarm-icon-motion',
  },
  Alarm: {
    svg: '<svg class="alarm-icon-svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>',
    cssClass: 'alarm-icon-alarm',
  },
  HeartBeat: {
    svg: '<svg class="alarm-icon-svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>',
    cssClass: 'alarm-icon-heartbeat',
  },
};

const ALARM_ICON_DEFAULT = {
  svg: '<svg class="alarm-icon-svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>',
  cssClass: 'alarm-icon',
};

function getAlarmIconInfo(eventType) {
  return ALARM_ICONS[eventType] || ALARM_ICON_DEFAULT;
}

async function renderFileTree(data, camera) {
  const container = document.getElementById('file-tree');
  container.innerHTML = '';

  const folders = Object.keys(data).sort().reverse();

  const alarmPromises = folders.map(folder => {
    const date = folder.replace(/\//g, '-');
    return fetchAlarmsForDate(camera, date).then(events => ({ folder, events: events || [] }));
  });

  const alarmResults = await Promise.all(alarmPromises);
  const alarmsByFolder = {};
  for (const { folder, events } of alarmResults) {
    alarmsByFolder[folder] = events;
  }

  const ul = document.createElement('ul');

  folders.forEach((folder, idx) => {
    const li = document.createElement('li');
    const folderSpan = document.createElement('span');
    folderSpan.className = 'folder';
    folderSpan.textContent = folder;
    li.appendChild(folderSpan);

    const fileUl = document.createElement('ul');
    fileUl.className = idx > 0 ? 'collapsed' : '';

    const files = data[folder] || [];
    const events = alarmsByFolder[folder] || [];
    files.sort().reverse().forEach(file => {
      const fileLi = document.createElement('li');
      fileLi.className = 'file';

      const matched = fileHasAlarm(file, events);
      if (matched) {
        const primaryEvent = matched[0].event;
        const iconInfo = getAlarmIconInfo(primaryEvent);
        const iconSpan = document.createElement('span');
        iconSpan.className = 'alarm-icon ' + iconInfo.cssClass;
        iconSpan.innerHTML = iconInfo.svg;
        iconSpan.title = matched.map(m => m.event + ': ' + m.time).join('\n');
        fileLi.appendChild(iconSpan);
      }

      const nameSpan = document.createElement('span');
      nameSpan.textContent = file.replace('.mp4', '');
      nameSpan.style.cursor = 'pointer';
      fileLi.appendChild(nameSpan);

      fileLi.style.cursor = 'pointer';
      fileLi.onclick = (e) => {
        e.stopPropagation();
        playFile(folder, file);
        document.querySelectorAll('.file-tree .file').forEach(el => el.classList.remove('active'));
        fileLi.classList.add('active');
      };

      fileUl.appendChild(fileLi);
    });

    li.appendChild(fileUl);
    ul.appendChild(li);

    folderSpan.onclick = () => {
      fileUl.classList.toggle('collapsed');
    };
  });

  container.appendChild(ul);

  const files = container.querySelectorAll('.file');
  if (files.length > 1) {
    files[1].click();
  } else if (files.length === 1) {
    files[0].click();
  }
}

function parseFileNameTime(file) {
  const name = file.replace('.mp4', '');
  const parts = name.split('-');
  if (parts.length >= 2) {
    const h = parseInt(parts[0], 10);
    const m = parseInt(parts[1], 10);
    if (!isNaN(h) && !isNaN(m)) {
      return h * 3600 + m * 60;
    }
  }
  return 0;
}

function playFile(folder, file) {
  currentFolder = folder;
  currentFile = file;
  const video = document.getElementById('video-player');
  const path = `/api/video/${currentCamera}/${folder}/${file}`;
  video.src = path;
  video.load();
  video.onloadedmetadata = () => {
    video.playbackRate = currentPlaybackSpeed;
    video.play();
    const fileStartSec = parseFileNameTime(file);
    document.getElementById('dl-from').value = formatDuration(fileStartSec);
    document.getElementById('dl-to').value = formatDuration(fileStartSec + video.duration);
  };
}

function formatDuration(seconds) {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  return [h, m, s].map(v => String(v).padStart(2, '0')).join(':');
}

function parseTime(str) {
  const [h, m, s] = str.split(':').map(Number);
  return h * 3600 + m * 60 + s;
}

async function downloadClip() {
  if (!currentCamera || !currentFile) return;
  const fileStartSec = parseFileNameTime(currentFile);
  const from = parseTime(document.getElementById('dl-from').value) - fileStartSec;
  const to = parseTime(document.getElementById('dl-to').value) - fileStartSec;
  if (to <= from || from < 0) return;
  const url = `/api/download?camera=${encodeURIComponent(currentCamera)}&folder=${encodeURIComponent(currentFolder)}&file=${encodeURIComponent(currentFile)}&start=${from}&end=${to}`;
  const a = document.createElement('a');
  a.href = url;
  a.download = '';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

function setSpeed(speed) {
  currentPlaybackSpeed = speed;
  const video = document.getElementById('video-player');
  video.playbackRate = speed;

  document.querySelectorAll('.speed-controls .btn').forEach(btn => {
    btn.classList.toggle('active', parseInt(btn.dataset.speed) === speed);
  });
}

async function fetchArchiveCameras() {
  try {
    const resp = await fetch('/api/cameras');
    const data = await resp.json();
    const container = document.getElementById('archive-camera-list');
    container.innerHTML = '';

    const cameras = data.cameras || [];
    if (cameras.length === 0) {
      container.innerHTML = '<div class="empty-msg">Камеры не найдены</div>';
      return;
    }

    const checks = cameras.map(async (camera) => {
      try {
        const r = await fetch(`/api/archive?camera=${encodeURIComponent(camera)}`);
        const d = await r.json();
        if (Object.keys(d).length > 0) return camera;
      } catch (e) {}
      return null;
    });

    const active = (await Promise.all(checks)).filter(Boolean);

    if (active.length === 0) {
      container.innerHTML = '<div class="empty-msg">Архив пуст</div>';
      return;
    }

    active.forEach(camera => {
      const btn = document.createElement('button');
      btn.className = 'camera-btn';
      btn.textContent = camera;
      btn.onclick = () => selectArchiveCamera(camera);
      container.appendChild(btn);
    });
  } catch (err) {
    console.error('Error fetching archive cameras:', err);
  }
}

function selectArchiveCamera(camera) {
  archiveCamera = camera;
  document.getElementById('archive-current-camera').textContent = camera;
  document.querySelectorAll('#archive-camera-list .camera-btn').forEach(btn => {
    btn.classList.toggle('active', btn.textContent === camera);
  });
  fetchArchiveFiles(camera);
}

async function fetchArchiveFiles(camera) {
  try {
    const resp = await fetch(`/api/archive?camera=${encodeURIComponent(camera)}`);
    const data = await resp.json();
    renderArchiveFileTree(data);
  } catch (err) {
    console.error('Error fetching archive files:', err);
  }
}

function renderArchiveFileTree(data) {
  const container = document.getElementById('archive-file-tree');
  container.innerHTML = '';

  const ul = document.createElement('ul');
  const folders = Object.keys(data).sort().reverse();

  folders.forEach((folder, idx) => {
    const li = document.createElement('li');
    const folderSpan = document.createElement('span');
    folderSpan.className = 'folder';
    folderSpan.textContent = folder;
    li.appendChild(folderSpan);

    const fileUl = document.createElement('ul');
    fileUl.className = idx > 0 ? 'collapsed' : '';

    const files = data[folder] || [];
    files.sort().reverse().forEach(file => {
      const fileLi = document.createElement('li');
      fileLi.className = 'file';

      const nameSpan = document.createElement('span');
      nameSpan.textContent = file.replace('.mp4', '');
      nameSpan.style.cursor = 'pointer';

      const delBtn = document.createElement('button');
      delBtn.className = 'btn-icon';
      delBtn.textContent = '✕';
      delBtn.title = 'Удалить';
      delBtn.onclick = (e) => {
        e.stopPropagation();
        deleteArchiveFile(folder, file, fileLi);
      };

      fileLi.appendChild(nameSpan);
      fileLi.appendChild(delBtn);

      fileLi.style.cursor = 'pointer';
      fileLi.onclick = (e) => {
        e.stopPropagation();
        playArchiveFile(folder, file);
        document.querySelectorAll('#archive-file-tree .file').forEach(el => el.classList.remove('active'));
        fileLi.classList.add('active');
      };

      fileUl.appendChild(fileLi);
    });

    li.appendChild(fileUl);
    ul.appendChild(li);
    folderSpan.onclick = () => {
      fileUl.classList.toggle('collapsed');
    };
  });

  container.appendChild(ul);

  const files = container.querySelectorAll('.file');
  if (files.length > 1) {
    files[1].click();
  } else if (files.length === 1) {
    files[0].click();
  }
}

function playArchiveFile(folder, file) {
  const video = document.getElementById('archive-video-player');
  const path = `/api/archive/video/${archiveCamera}/${folder}/${file}`;
  video.src = path;
  video.load();
  video.onloadedmetadata = () => {
    video.playbackRate = currentPlaybackSpeed;
    video.play();
  };
}

async function deleteArchiveFile(folder, file, element) {
  if (!confirm('Удалить файл?')) return;
  try {
    const resp = await fetch(`/api/archive/delete?camera=${encodeURIComponent(archiveCamera)}&folder=${encodeURIComponent(folder)}&file=${encodeURIComponent(file)}`, { method: 'POST' });
    if (resp.ok) {
      const li = element;
      const ul = li.parentElement;
      li.remove();
      if (ul && ul.children.length === 0) {
        ul.parentElement.remove();
      }
    }
  } catch (err) {
    console.error('Delete error:', err);
  }
}

function setArchiveSpeed(speed) {
  const video = document.getElementById('archive-video-player');
  video.playbackRate = speed;
  document.querySelectorAll('#tab-archive .speed-controls .btn').forEach(btn => {
    btn.classList.toggle('active', parseInt(btn.dataset.speed) === speed);
  });
}

const video = document.getElementById('video-player');
if (video) {
  video.addEventListener('ended', () => {
    const active = document.querySelector('.file-tree .file.active');
    if (active) {
      const prev = active.previousElementSibling;
      if (prev && prev.classList.contains('file')) {
        prev.click();
      } else {
        const prevFolder = active.closest('ul')?.parentElement?.previousElementSibling;
        if (prevFolder) {
          const files = prevFolder.querySelectorAll('.file');
          if (files.length) files[files.length - 1].click();
        }
      }
    }
  });
}

const archiveVideo = document.getElementById('archive-video-player');
if (archiveVideo) {
  archiveVideo.addEventListener('ended', () => {
    const active = document.querySelector('#archive-file-tree .file.active');
    if (active) {
      const prev = active.previousElementSibling;
      if (prev && prev.classList.contains('file')) {
        prev.click();
      } else {
        const prevFolder = active.closest('ul')?.parentElement?.previousElementSibling;
        if (prevFolder) {
          const files = prevFolder.querySelectorAll('.file');
          if (files.length) files[files.length - 1].click();
        }
      }
    }
  });
}

function initVideoZoom(videoId) {
  const wrapper = document.getElementById(videoId)?.closest('.video-wrapper');
  if (!wrapper) return;
  const video = wrapper.querySelector('video');

  let zoom = 1;
  let panX = 0;
  let panY = 0;
  let dragging = false;
  let lastX = 0;
  let lastY = 0;

  function applyTransform() {
    if (zoom <= 1) {
      video.style.transform = '';
      video.classList.remove('zoomed');
    } else {
      video.style.transform = `translate(${panX}px, ${panY}px) scale(${zoom})`;
      video.classList.add('zoomed');
    }
  }

  function clampPan() {
    if (zoom <= 1) { panX = 0; panY = 0; return; }
    const vw = wrapper.clientWidth;
    const vh = wrapper.clientHeight;
    const maxX = (zoom - 1) * vw / 2;
    const maxY = (zoom - 1) * vh / 2;
    panX = Math.max(-maxX, Math.min(maxX, panX));
    panY = Math.max(-maxY, Math.min(maxY, panY));
  }

  wrapper.addEventListener('wheel', (e) => {
    e.preventDefault();
    const rect = wrapper.getBoundingClientRect();
    const cx = e.clientX - rect.left;
    const cy = e.clientY - rect.top;
    const centerX = wrapper.clientWidth / 2;
    const centerY = wrapper.clientHeight / 2;

    const prevZoom = zoom;
    if (e.deltaY < 0) {
      zoom = Math.min(10, zoom * 1.15);
    } else {
      zoom = Math.max(1, zoom / 1.15);
    }

    const ratio = zoom / prevZoom;
    panX = (cx - centerX) * (1 - ratio) + panX * ratio;
    panY = (cy - centerY) * (1 - ratio) + panY * ratio;

    clampPan();
    applyTransform();
  }, { passive: false });

  wrapper.addEventListener('mousedown', (e) => {
    if (zoom <= 1 || e.button !== 0) return;
    dragging = true;
    lastX = e.clientX;
    lastY = e.clientY;
    video.classList.add('dragging');
    e.preventDefault();
  });

  document.addEventListener('mousemove', (e) => {
    if (!dragging) return;
    panX += e.clientX - lastX;
    panY += e.clientY - lastY;
    lastX = e.clientX;
    lastY = e.clientY;
    clampPan();
    applyTransform();
  });

  document.addEventListener('mouseup', () => {
    if (!dragging) return;
    dragging = false;
    video.classList.remove('dragging');
  });

  wrapper.addEventListener('dblclick', (e) => {
    e.preventDefault();
    zoom = 1;
    panX = 0;
    panY = 0;
    applyTransform();
  });
}

async function fetchStatus() {
  try {
    const resp = await fetch('/api/status');
    const data = await resp.json();

    const indicator = document.getElementById('recording-indicator');
    if (indicator) {
      indicator.style.display = data.recording ? 'flex' : 'none';
    }

    const mIndicator = document.getElementById('monitoring-indicator');
    if (mIndicator) {
      mIndicator.style.display = data.recording ? 'flex' : 'none';
    }

    if (data.storage) {
      const storageEl = document.getElementById('storage-info');
      if (storageEl) {
        storageEl.innerHTML = `
          <div>Размер: <span class="value">${data.storage.total_size_gb.toFixed(2)} ГБ</span> / <span class="value">${data.storage.default_camera_limit_gb} ГБ</span></div>
          <div>Файлов: <span class="value">${data.storage.file_count}</span></div>
        `;
      }
      const mStorageEl = document.getElementById('monitoring-storage');
      if (mStorageEl) {
        mStorageEl.innerHTML = `
          <div>Размер: <span class="value">${data.storage.total_size_gb.toFixed(2)} ГБ</span> / <span class="value">${data.storage.default_camera_limit_gb} ГБ</span></div>
          <div>Файлов: <span class="value">${data.storage.file_count}</span></div>
          <div>Директория: <span class="value">${data.storage.base_dir}</span></div>
        `;
      }
    }

    const procEl = document.getElementById('monitoring-processes');
    if (procEl) {
      const procs = data.processes || [];
      if (procs.length === 0) {
        procEl.innerHTML = '<div class="empty-msg">Нет активных процессов записи</div>';
      } else {
        procEl.innerHTML = procs.map(p => `
          <div class="process-item">
            <div>
              <div class="process-header">
                <span class="process-name">${p.name}</span>
                <span class="process-meta">${p.startTime}</span>
                <span class="led led-red"></span>
              </div>
              <div class="process-output">${p.output}</div>
            </div>
          </div>
        `).join('');
      }
    }
  } catch (err) {
    console.error('Error fetching status:', err);
  }
}

async function loadSettings() {
  try {
    const resp = await fetch('/api/config');
    const cfg = await resp.json();
    currentConfig = cfg;

    document.getElementById('base_dir').value = cfg.base_dir || '';
    document.getElementById('archive_dir').value = cfg.archive_dir || '';
    document.getElementById('stream_server').value = cfg.stream_server || '';
    document.getElementById('default_camera_limit_gb').value = cfg.default_camera_limit_gb || 90;
    document.getElementById('global_size_gb').value = cfg.global_size_gb || 0;
    document.getElementById('go2rtc_config_path').value = cfg.go2rtc_config_path || '';
    document.getElementById('http_port').value = cfg.http_port || 8180;
  } catch (err) {
    console.error('Error loading settings:', err);
  }
}

async function saveSettings(e) {
  e.preventDefault();

  const cfg = {
    base_dir: document.getElementById('base_dir').value,
    archive_dir: document.getElementById('archive_dir').value,
    stream_server: document.getElementById('stream_server').value,
    default_camera_limit_gb: parseInt(document.getElementById('default_camera_limit_gb').value),
    global_size_gb: parseInt(document.getElementById('global_size_gb').value) || 0,
    go2rtc_config_path: document.getElementById('go2rtc_config_path').value,
    http_port: parseInt(document.getElementById('http_port').value),
  };

  try {
    await fetch('/api/config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(cfg),
    });
    alert('Настройки сохранены');
  } catch (err) {
    console.error('Error saving settings:', err);
    alert('Ошибка сохранения');
  }
}

function switchSubTab(tab) {
  document.querySelectorAll('#tab-settings .sub-tab').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('#tab-settings .sub-tab-content').forEach(el => el.classList.remove('active'));
  document.querySelector(`#tab-settings .sub-tab[onclick="switchSubTab('${tab}')"]`).classList.add('active');
  document.getElementById('subtab-' + tab).classList.add('active');

  if (tab === 'limits') {
    loadCameraLimits();
  }
}

function formatRecordingTime(fileCount) {
  const totalMinutes = fileCount * 10;
  const days = Math.floor(totalMinutes / 1440);
  const hours = Math.floor((totalMinutes % 1440) / 60);
  if (days > 0) {
    return days + 'д ' + hours + 'ч';
  }
  if (hours > 0) {
    return hours + 'ч';
  }
  return '< 1ч';
}

async function loadCameraLimits() {
  try {
    const [configResp, camerasResp, storageResp] = await Promise.all([
      fetch('/api/config'),
      fetch('/api/cameras'),
      fetch('/api/storage/cameras'),
    ]);

    const cfg = await configResp.json();
    const camerasData = await camerasResp.json();
    const storage = await storageResp.json();

    currentConfig = cfg;
    const globalTarget = cfg.default_camera_limit_gb || 90;
    const limits = cfg.camera_limits || {};
    const dayLimits = cfg.camera_day_limits || {};

    document.getElementById('limits-default-camera').textContent = globalTarget;
    document.getElementById('limits-global-target').textContent = cfg.global_size_gb || 0;

    const cameras = camerasData.cameras || [];
    const container = document.getElementById('camera-limits-list');
    container.innerHTML = '';

    if (cameras.length === 0) {
      container.innerHTML = '<div class="empty-msg">Камеры не найдены</div>';
      return;
    }

    cameras.forEach(camera => {
      const camStorage = storage[camera] || { size_gb: 0, file_count: 0 };
      const limitValue = limits[camera] || globalTarget;
      const dayLimitValue = dayLimits[camera] || 0;

      const row = document.createElement('div');
      row.className = 'camera-limit-row';

      const nameSpan = document.createElement('span');
      nameSpan.className = 'camera-limit-name';
      nameSpan.textContent = camera;

      const sizeSpan = document.createElement('span');
      sizeSpan.className = 'camera-limit-size';
      sizeSpan.textContent = camStorage.size_gb.toFixed(1) + ' ГБ';

      const timeSpan = document.createElement('span');
      timeSpan.className = 'camera-limit-time';
      timeSpan.textContent = formatRecordingTime(camStorage.file_count);

      const input = document.createElement('input');
      input.type = 'number';
      input.className = 'form-input form-input-sm';
      input.value = limitValue;
      input.min = 1;
      input.dataset.camera = camera;
      input.dataset.field = 'size';

      const unitSpan = document.createElement('span');
      unitSpan.className = 'camera-limit-unit';
      unitSpan.textContent = 'ГБ';

      const dayInput = document.createElement('input');
      dayInput.type = 'number';
      dayInput.className = 'form-input form-input-sm';
      dayInput.value = dayLimitValue || '';
      dayInput.min = 1;
      dayInput.placeholder = 'нет';
      dayInput.dataset.camera = camera;
      dayInput.dataset.field = 'days';

      const dayUnitSpan = document.createElement('span');
      dayUnitSpan.className = 'camera-limit-unit';
      dayUnitSpan.textContent = 'дн';

      row.appendChild(nameSpan);
      row.appendChild(sizeSpan);
      row.appendChild(timeSpan);
      row.appendChild(input);
      row.appendChild(unitSpan);
      row.appendChild(dayInput);
      row.appendChild(dayUnitSpan);
      container.appendChild(row);
    });

    let totalSum = 0;
    cameras.forEach(camera => {
      totalSum += limits[camera] || globalTarget;
    });
    document.getElementById('limits-total-sum').textContent = totalSum;

    const globalSize = cfg.global_size_gb || 0;
    const warningEl = document.getElementById('limits-warning');
    if (globalSize > 0 && totalSum > globalSize) {
      warningEl.style.display = 'inline';
      warningEl.textContent = `⚠️ Превышен глобальный лимит на ${totalSum - globalSize} ГБ!`;
    } else {
      warningEl.style.display = 'none';
    }
  } catch (err) {
    console.error('Error loading camera limits:', err);
  }
}

async function saveCameraLimits() {
  const inputs = document.querySelectorAll('#camera-limits-list input[type="number"]');
  const cameraLimits = {};
  const cameraDayLimits = {};

  inputs.forEach(input => {
    const camera = input.dataset.camera;
    const field = input.dataset.field;
    const value = parseInt(input.value);
    if (!camera) return;

    if (field === 'days') {
      if (value > 0) {
        cameraDayLimits[camera] = value;
      }
    } else {
      if (value > 0) {
        cameraLimits[camera] = value;
      }
    }
  });

  try {
    const resp = await fetch('/api/config');
    const cfg = await resp.json();
    cfg.camera_limits = cameraLimits;
    cfg.camera_day_limits = cameraDayLimits;

    await fetch('/api/config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(cfg),
    });
    alert('Лимиты сохранены');
  } catch (err) {
    console.error('Error saving camera limits:', err);
    alert('Ошибка сохранения');
  }
}

async function loadAlarmStatus() {
  try {
    const resp = await fetch('/api/alarm/status');
    const data = await resp.json();
    alarmRunning = data.running;

    const toggle = document.getElementById('alarm-toggle');
    const portInfo = document.getElementById('alarm-port-info');
    const mqttInfo = document.getElementById('alarm-mqtt-info');
    const eventsInfo = document.getElementById('alarm-events-info');

    if (toggle) {
      toggle.checked = data.enabled;
    }
    if (portInfo) {
      portInfo.textContent = 'Порт: ' + (data.port || 15002);
    }
    if (mqttInfo) {
      mqttInfo.textContent = data.mqtt_host ? 'MQTT: ' + data.mqtt_host + ':' + (data.mqtt_port || 1883) : 'MQTT: выкл';
    }
    if (eventsInfo) {
      eventsInfo.textContent = 'Событий: ' + (data.event_count || 0);
    }
  } catch (err) {
    console.error('Error loading alarm status:', err);
  }
}

async function toggleAlarm() {
  const toggle = document.getElementById('alarm-toggle');
  const enabled = toggle.checked;
  const url = enabled ? '/api/alarm/start' : '/api/alarm/stop';
  try {
    await fetch(url, { method: 'POST' });
    loadAlarmStatus();
  } catch (err) {
    toggle.checked = !enabled;
    console.error('Error toggling alarm:', err);
  }
}

async function loadAlarmLog() {
  try {
    const resp = await fetch('/api/alarm/log?limit=100');
    const data = await resp.json();
    const container = document.getElementById('alarm-log');
    if (!container) return;

    if (!data || data.length === 0) {
      container.innerHTML = '<div class="empty-msg">Нет событий</div>';
      return;
    }

    let html = '<table><thead><tr><th>Время</th><th>Камера</th><th>Событие</th><th>Статус</th><th>Описание</th><th>IP</th></tr></thead><tbody>';
    data.forEach(e => {
      const time = new Date(e.time).toLocaleString('ru-RU');
      const camera = e.camera || e.address || '-';
      html += '<tr>';
      html += '<td>' + time + '</td>';
      html += '<td class="alarm-camera">' + camera + '</td>';
      html += '<td>' + (e.event || '-') + '</td>';
      html += '<td class="alarm-status-' + (e.status === 'Start' ? 'start' : 'stop') + '">' + (e.status || '-') + '</td>';
      html += '<td>' + (e.descrip || '-') + '</td>';
      html += '<td>' + (e.address || '-') + '</td>';
      html += '</tr>';
    });
    html += '</tbody></table>';
    container.innerHTML = html;
  } catch (err) {
    console.error('Error loading alarm log:', err);
  }
}

async function clearAlarmLog() {
  try {
    await fetch('/api/alarm/clear', { method: 'POST' });
    loadAlarmLog();
  } catch (err) {
    console.error('Error clearing alarm log:', err);
  }
}

async function fetchVersion() {
  try {
    const resp = await fetch('/api/version');
    const data = await resp.json();
    const pill = document.getElementById('version-pill');
    if (pill && data.version) {
      pill.textContent = data.version;
    }
  } catch (err) {}
}
