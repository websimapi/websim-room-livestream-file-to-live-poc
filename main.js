// Room Livestream POC
// One client becomes "host" by uploading a video -> URL is shared via room state.
// Everyone syncs playback using a shared epoch start and host-controlled play/pause/seek.

const els = {
  video: document.getElementById('video'),
  peers: document.getElementById('peers'),
  liveBadge: document.getElementById('liveBadge'),
  joinOverlay: document.getElementById('joinOverlay'),
  joinBtn: document.getElementById('joinBtn'),
  fileInput: document.getElementById('fileInput'),
  goLiveBtn: document.getElementById('goLiveBtn'),
  startTabBtn: document.getElementById('startTabBtn'),
  takeHostBtn: document.getElementById('takeHostBtn'),
  endBtn: document.getElementById('endBtn'),
  streamTitle: document.getElementById('streamTitle'),
  hostInfo: document.getElementById('hostInfo'),
  roomStatus: document.getElementById('roomStatus'),
  youLabel: document.getElementById('youLabel'),
  diag: document.getElementById('diag'),
  chatLog: document.getElementById('chatLog'),
  chatText: document.getElementById('chatText'),
  chatSend: document.getElementById('chatSend'),
  youAvatar: document.getElementById('youAvatar'),
  youName: document.getElementById('youName'),
};

const room = new WebsimSocket(); // Provided globally
await room.initialize();
/* ensure autoplay-friendly defaults */
els.video.muted = true;
els.video.playsInline = true;

const state = {
  isHost: false,
  ready: false,
  videoReady: false,
  attemptingPlay: false,
  lastAppliedFromState: 0, // last time we applied a sync from room.state (ms)
};

/* add MSE/chunking runtime state */
let mse = null;
let sourceBuffer = null;
let mseReady = false;
/* add guards to avoid rapid reinit loops */
let mseInitInProgress = false;
let lastMSEErrorAt = 0;
/* new: chunk pipeline state */
let expectSeq = 0;
let appendQueue = [];
let appending = false;
let chunkPollTimer = null;
let chunkModeInitialized = false;
/* new: track MSE ObjectURL and stall watchdog */
let mseObjUrl = null;
let stallTimer = null;
let lastTimeSample = 0;
let lastProgressAt = 0;

function getLivestream() {
  return room.roomState.livestream || null;
}

function isYouHost() {
  const ls = getLivestream();
  return !!ls && ls.hostId === room.clientId;
}

function setHostControlsEnabled(enabled) {
  els.goLiveBtn.disabled = !enabled;
  els.takeHostBtn.disabled = false; // allow taking host even when not enabled
  els.endBtn.disabled = !enabled;
  // Host-only seeking control via native controls: we toggle controls visibility
  els.video.controls = enabled;
}

function setViewerControls() {
  els.video.controls = false;
}

// Presence bookkeeping (not strictly needed but useful for display)
room.updatePresence({
  role: 'viewer',
  ts: Date.now(),
});

// Subscribe to peers/presence for UI
room.subscribePresence(() => {
  const list = Object.keys(room.peers).map((id) => {
    const p = room.peers[id];
    const me = id === room.clientId ? ' (you)' : '';
    return `${p.username}${me}`;
  });
  els.peers.textContent = `${list.length} online: ${list.join(', ')}`;
  const me = room.peers[room.clientId];
  els.youLabel.textContent = me ? me.username : '—';
  if (me) {
    els.youName.textContent = me.username;
    els.youAvatar.src = me.avatarUrl || '';
  }
});

// Room state listener
room.subscribeRoomState(() => {
  refreshFromRoomState();
});

// Handle incoming ephemeral events (optional future hooks)
room.onmessage = (event) => {
  const d = event.data || {};
  switch (d.type) {
    case 'connected':
    case 'disconnected':
      // Presence subscription above keeps UI updated
      break;
    case 'chat':
      renderChatMessage(d.clientId, d.text, d.ts);
      break;
    default:
      // console.log('Event:', d);
      break;
  }
};
els.video.addEventListener('error', handleMediaError);

function logDiag(msg) {
  const now = new Date().toLocaleTimeString();
  els.diag.textContent = `[${now}] ${msg}\n` + els.diag.textContent.slice(0, 1200);
}

function ms() {
  return performance.now();
}

function computeLiveCurrentTime(ls) {
  if (!ls || !ls.startedAt) return 0;
  if (ls.paused) {
    if (ls.pauseAt && ls.pauseAt >= ls.startedAt) {
      return Math.max(0, (ls.pauseAt - ls.startedAt) / 1000);
    }
    return 0;
  }
  return Math.max(0, (Date.now() - ls.startedAt) / 1000);
}

async function ensureVideoSrc(url) {
  if (els.video.src !== url) {
    els.video.src = url;
    state.videoReady = false;
    try {
      await els.video.load();
    } catch {
      // ignore
    }
  }
}

// MSE helpers for chunked streaming
function teardownMSE() {
  try { if (els.video) els.video.pause(); } catch {}
  if (mse) {
    try { mse.endOfStream(); } catch {}
  }
  if (stallTimer) { clearInterval(stallTimer); stallTimer = null; }
  if (mseObjUrl) { try { URL.revokeObjectURL(mseObjUrl); } catch {} mseObjUrl = null; }
  mse = null;
  sourceBuffer = null;
  mseReady = false;
  mseInitInProgress = false;
  expectSeq = 0;
  appendQueue = [];
  appending = false;
  if (chunkPollTimer) {
    clearInterval(chunkPollTimer);
    chunkPollTimer = null;
  }
  chunkModeInitialized = false;
  try { els.video.removeAttribute('src'); els.video.load(); } catch {}
}

function ensureMSE(ls) {
  if (chunkModeInitialized || mseInitInProgress) return;
  mseInitInProgress = true;
  teardownMSE(); // reset before init
  mse = new MediaSource();
  els.video.removeAttribute('src'); // fully reset before attaching
  els.video.load();
  const objUrl = URL.createObjectURL(mse);
  mseObjUrl = objUrl;
  els.video.src = objUrl;
  mse.addEventListener('sourceopen', () => {
    try {
      // pick a supported mime (prefer room state, else fallback)
      let mime = ls.mime || 'video/webm; codecs=vp8,opus';
      if (!MediaSource.isTypeSupported(mime)) {
        const fallbacks = ['video/webm;codecs=vp9,opus','video/webm;codecs=vp8,opus','video/webm'];
        mime = fallbacks.find(m => MediaSource.isTypeSupported(m)) || 'video/webm';
      }
      sourceBuffer = mse.addSourceBuffer(mime);
      mse.duration = Infinity;
      sourceBuffer.mode = 'segments';
      // on updateend: dequeue next and trim old buffered data
      sourceBuffer.addEventListener('updateend', () => { tryDequeue(); trimOldBuffer(30); });
      sourceBuffer.addEventListener('error', () => handleMediaError());
      mseReady = true;
      mseInitInProgress = false;
      // Start polling room state for new chunks
      chunkPollTimer = setInterval(() => appendAvailableChunks(ls), 500);
      // Start stall recovery watchdog
      stallTimer = setInterval(maybeRecoverStall, 1000);
    } catch (e) {
      mseInitInProgress = false;
      logDiag('MSE init failed. ' + e.message);
    }
  });
  chunkModeInitialized = true;
}

async function fetchChunk(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error('chunk fetch failed');
  return await res.arrayBuffer();
}

function queueAppend(buf) {
  appendQueue.push(buf);
  tryDequeue();
}

function tryDequeue() {
  if (!sourceBuffer || !mse || mse.readyState !== 'open' || sourceBuffer.updating || !appendQueue.length) return;
  const buf = appendQueue.shift();
  try {
    sourceBuffer.appendBuffer(buf);
  } catch (e) {
    logDiag('appendBuffer error: ' + e.message);
    handleMediaError();
  }
}

/* trim old buffered data to avoid unbounded growth (~30s) */
function trimOldBuffer(maxSec = 30) {
  if (!sourceBuffer) return;
  const buf = els.video.buffered;
  if (!buf || buf.length === 0) return;
  const end = buf.end(buf.length - 1);
  const removeEnd = end - maxSec;
  if (removeEnd <= 0) return;
  if (!sourceBuffer.updating) {
    try { sourceBuffer.remove(0, removeEnd); } catch {}
  }
}

async function appendAvailableChunks(ls) {
  if (!mseReady || !mse || mse.readyState !== 'open' || !sourceBuffer) return;
  if (els.video.error) { handleMediaError(); return; }
  const cur = room.roomState.livestream; // latest
  if (!cur || cur.mode !== 'chunk' || !cur.chunks) return;
  // if we're too far behind, skip ahead to near-live to avoid backlog stalls
  if (typeof cur.latestSeq === 'number' && cur.latestSeq - expectSeq > 10) {
    expectSeq = Math.max(0, cur.latestSeq - 3);
    logDiag('Skipping ahead to near-live to recover.');
  }
  while (cur.chunks[String(expectSeq)]) {
    const meta = cur.chunks[String(expectSeq)];
    try {
      const ab = await fetchChunk(meta.url);
      queueAppend(ab);
      expectSeq++;
      if (els.video.paused && !cur.paused) {
        els.video.play().catch(() => {
          els.joinOverlay.classList.remove('hidden');
        });
      }
    } catch (e) {
      logDiag(`chunk ${expectSeq} fetch failed, retrying later`);
      break;
    }
  }
}

function handleMediaError() {
  const err = els.video.error;
  if (!err) return;
  const code = err.code; // 1:ABORTED 2:NETWORK 3:DECODE 4:SRC_NOT_SUPPORTED
  logDiag(`Video error (${code}). Resetting live buffer…`);
  const ls = getLivestream();
  if (!ls || ls.mode !== 'chunk') return;
  const now = Date.now();
  if (now - lastMSEErrorAt < 1000) return;
  lastMSEErrorAt = now;
  const resumeFrom = (ls.latestSeq || 0);
  teardownMSE();
  expectSeq = resumeFrom;
  setTimeout(() => ensureMSE(ls), 250);
}

/* recover from stalls by jumping to buffered end */
function maybeRecoverStall() {
  const ls = room.roomState.livestream;
  if (!ls || ls.mode !== 'chunk' || ls.paused) return;
  if (!els.video || els.video.paused) return;
  const buf = els.video.buffered;
  const now = performance.now();
  const ct = els.video.currentTime;
  if (ct !== lastTimeSample) {
    lastTimeSample = ct;
    lastProgressAt = now;
  }
  const noProgressMs = now - lastProgressAt;
  const lowReady = els.video.readyState < 3;
  if (buf && buf.length) {
    const end = buf.end(buf.length - 1);
    const behind = end - ct;
    if ((lowReady || noProgressMs > 2000) && behind > 0.2) {
      try { els.video.currentTime = end - 0.1; } catch {}
      els.video.play().catch(() => { els.joinOverlay.classList.remove('hidden'); });
      lastProgressAt = now;
      logDiag('Stall detected: jumped to live edge.');
    }
  }
}

// Sync local video element to room state
async function syncPlayback(toState = true) {
  const ls = getLivestream();
  const haveSrc = !!els.video.src;
  const url = ls && ls.url;

  if (!ls || (!url && ls.mode !== 'chunk')) {
    els.video.removeAttribute('src');
    els.video.load();
    els.liveBadge.textContent = 'OFF AIR';
    els.streamTitle.textContent = 'No stream active';
    els.hostInfo.textContent = 'Host: —';
    setViewerControls();
    teardownMSE();
    return;
  }

  // Show meta
  els.streamTitle.textContent = ls.title || 'Live';
  const hostPeer = room.peers[ls.hostId];
  els.hostInfo.textContent = `Host: ${hostPeer ? hostPeer.username : 'Unknown'}`;
  els.liveBadge.textContent = ls.paused ? 'PAUSED' : 'LIVE';

  // Only host is allowed to control (disabled in chunk mode)
  state.isHost = isYouHost();
  if (state.isHost) {
    setHostControlsEnabled(ls.mode !== 'chunk'); // disable native controls in chunk mode POC
  } else {
    setHostControlsEnabled(false);
    setViewerControls();
  }

  if (ls.mode === 'chunk') {
    ensureMSE(ls);
    // autoplay attempt
    if (!ls.paused && els.video.paused) {
      els.video.play().then(() => els.joinOverlay.classList.add('hidden')).catch(() => els.joinOverlay.classList.remove('hidden'));
    } else if (ls.paused && !els.video.paused) {
      els.video.pause();
    }
    state.lastAppliedFromState = ms();
    return;
  }

  // File URL mode (original)
  await ensureVideoSrc(url);

  const targetT = computeLiveCurrentTime(ls);
  const drift = Math.abs((els.video.currentTime || 0) - targetT);

  // Apply state to video (viewers always follow, host only when this was remote change)
  const shouldPlay = !ls.paused;
  const currentlyPaused = els.video.paused;

  // Adjust time if we're off by > 300ms
  if (drift > 0.3) {
    els.video.currentTime = targetT;
  }

  if (!state.isHost || toState) {
    if (shouldPlay && currentlyPaused && !state.attemptingPlay) {
      state.attemptingPlay = true;
      try {
        await els.video.play();
        state.attemptingPlay = false;
        els.joinOverlay.classList.add('hidden');
      } catch (e) {
        state.attemptingPlay = false;
        // Autoplay blocked: show overlay
        els.joinOverlay.classList.remove('hidden');
      }
    } else if (!shouldPlay && !currentlyPaused) {
      els.video.pause();
    }
  }

  state.lastAppliedFromState = ms();
}

function refreshFromRoomState() {
  syncPlayback(true);
}

// Host actions
async function becomeHostWithFile(file) {
  if (!file) {
    alert('Choose a video file first.');
    return;
  }
  els.goLiveBtn.disabled = true;

  logDiag('Uploading file…');
  let url;
  try {
    url = await window.websim.upload(file);
  } catch (e) {
    logDiag('Upload failed. Switching to chunked live mode.');
    await startChunkedLivestream(file);
    els.goLiveBtn.disabled = false;
    return;
  }
  logDiag('Upload complete. Starting stream.');

  const title = file.name || 'Live';

  // Initialize livestream state
  const liveState = {
    url,
    hostId: room.clientId,
    title,
    startedAt: Date.now(), // epoch when playback time = 0
    paused: false,
    pauseAt: null,
  };

  room.updateRoomState({ livestream: liveState });
  // Host presence hint
  room.updatePresence({ role: 'host', ts: Date.now() });

  await syncPlayback(true);
  try {
    await els.video.play();
    els.joinOverlay.classList.add('hidden');
  } catch {
    els.joinOverlay.classList.remove('hidden');
  }
  els.goLiveBtn.disabled = false;
}

function takeHost() {
  const ls = getLivestream() || {};
  if (!ls.url) {
    alert('No active stream to take over. Upload a file to go live.');
    return;
  }
  // Only take host if there is no host or host not in peers anymore
  const hostMissing = !ls.hostId || !room.peers[ls.hostId];
  if (!hostMissing && ls.hostId !== room.clientId) {
    const ok = confirm('A host is active. Take over hosting?');
    if (!ok) return;
  }
  const now = Date.now();
  const currentTime = computeLiveCurrentTime(ls);
  room.updateRoomState({
    livestream: {
      ...ls,
      hostId: room.clientId,
      // Rebase start to keep continuity
      startedAt: now - Math.floor(currentTime * 1000),
      pauseAt: ls.paused ? now : null,
    },
  });
  room.updatePresence({ role: 'host', ts: now });
}

function endStream() {
  const ls = getLivestream();
  if (!ls || ls.hostId !== room.clientId) {
    alert('You are not the host.');
    return;
  }
  const ok = confirm('End the stream for everyone?');
  if (!ok) return;
  room.updateRoomState({ livestream: null });
  room.updatePresence({ role: 'viewer', ts: Date.now() });
  teardownMSE();
  syncPlayback(true);
}

// Chunked livestream: record local file to webm chunks and upload incrementally
async function startChunkedLivestream(file) {
  // Create offscreen video to feed MediaRecorder
  const off = document.createElement('video');
  off.muted = true;
  off.playsInline = true;
  off.src = URL.createObjectURL(file);
  await off.play().catch(() => {});
  const stream = off.captureStream ? off.captureStream(30) : off.mozCaptureStream?.() ;
  if (!stream) {
    alert('Your browser does not support captureStream for chunked streaming.');
    return;
  }

  // Choose a supported mime for recorder and MSE
  const candidates = [
    'video/webm;codecs=vp9,opus',
    'video/webm;codecs=vp8,opus',
    'video/webm'
  ];
  let mime = '';
  for (const c of candidates) {
    if (MediaRecorder.isTypeSupported(c)) { mime = c; break; }
  }
  if (!mime) {
    alert('No supported recording codec found.');
    return;
  }

  // Initialize room state in chunk mode
  const liveState = {
    mode: 'chunk',
    hostId: room.clientId,
    title: file.name || 'Live',
    startedAt: Date.now(),
    paused: false,
    mime,
    chunks: {}, // object map of seq -> { url }
    latestSeq: 0
  };
  room.updateRoomState({ livestream: liveState });
  room.updatePresence({ role: 'host', ts: Date.now() });

  // Setup MSE playback for host's main player
  ensureMSE(liveState);
  els.joinOverlay.classList.add('hidden');

  // Start recorder
  const rec = new MediaRecorder(stream, { mimeType: mime, videoBitsPerSecond: 1_500_000, audioBitsPerSecond: 96_000 });
  let seq = 0;
  rec.ondataavailable = async (e) => {
    if (!e.data || e.data.size === 0) return;
    try {
      const url = await window.websim.upload(e.data);
      // Update room state with new chunk
      room.updateRoomState({
        livestream: {
          chunks: { [String(seq)]: { url } },
          latestSeq: seq
        }
      });
      // For host playback, append immediately
      try {
        const ab = await e.data.arrayBuffer();
        queueAppend(ab);
      } catch {}
      seq++;
    } catch (err) {
      logDiag('Chunk upload failed, will continue and retry next chunks.');
    }
  };
  rec.onerror = (ev) => logDiag('Recorder error: ' + ev.error?.name || 'unknown');
  rec.start(1000); // 1s chunks

  // Keep offscreen video advancing; sync pause/resume from room state
  const unsub = room.subscribeRoomState((rs) => {
    const ls = getLivestream();
    if (!ls || ls.mode !== 'chunk') return;
    if (ls.paused && !off.paused) off.pause();
    if (!ls.paused && off.paused) off.play().catch(() => {});
  });

  // Clean up when file ends
  off.addEventListener('ended', () => {
    try { rec.stop(); } catch {}
    unsub();
    logDiag('Source ended.');
  });
}

function renderChatMessage(clientId, text, ts) {
  const who = room.peers[clientId] || {};
  const name = who.username || 'Guest';
  const avatar = who.avatarUrl || '';
  const time = ts ? new Date(ts).toLocaleTimeString() : '';
  const wrap = document.createElement('div');
  wrap.className = 'msg';
  const img = document.createElement('img');
  img.className = 'avatar';
  img.src = avatar;
  img.alt = name;
  const bubble = document.createElement('div');
  bubble.className = 'bubble';
  bubble.innerHTML = `<div class="meta">${name} • ${time}</div><div class="text"></div>`;
  bubble.querySelector('.text').textContent = text;
  wrap.appendChild(img);
  wrap.appendChild(bubble);
  els.chatLog.appendChild(wrap);
  els.chatLog.scrollTop = els.chatLog.scrollHeight;
}

async function startScreenLivestream() {
  try {
    const stream = await getScreenStream();
    if (!stream) {
      logDiag('Failed to start tab broadcast: screen capture not available.');
      alert('This browser does not support screen capture. Try Chrome or Edge on desktop.');
      return;
    }
    // If no audio track, try to grab mic and add
    if (!stream.getAudioTracks().length) {
      try {
        const mic = await navigator.mediaDevices.getUserMedia({ audio: true });
        mic.getAudioTracks().forEach(t => stream.addTrack(t));
      } catch {}
    }
    // Choose recorder mime
    const candidates = [
      'video/webm;codecs=vp9,opus',
      'video/webm;codecs=vp8,opus',
      'video/webm'
    ];
    let mime = '';
    for (const c of candidates) {
      if (MediaRecorder.isTypeSupported(c)) { mime = c; break; }
    }
    if (!mime) {
      alert('No supported recording codec found.');
      return;
    }
    const liveState = {
      mode: 'chunk',
      hostId: room.clientId,
      title: 'Tab Broadcast',
      startedAt: Date.now(),
      paused: false,
      mime,
      chunks: {},
      latestSeq: 0
    };
    room.updateRoomState({ livestream: liveState });
    room.updatePresence({ role: 'host', ts: Date.now() });
    ensureMSE(liveState);
    els.joinOverlay.classList.add('hidden');

    const rec = new MediaRecorder(stream, { mimeType: mime, videoBitsPerSecond: 2_000_000, audioBitsPerSecond: 128_000 });
    let seq = 0;
    rec.ondataavailable = async (e) => {
      if (!e.data || e.data.size === 0) return;
      try {
        const url = await window.websim.upload(e.data);
        room.updateRoomState({
          livestream: {
            chunks: { [String(seq)]: { url } },
            latestSeq: seq
          }
        });
        try { queueAppend(await e.data.arrayBuffer()); } catch {}
        seq++;
      } catch (err) {
        logDiag('Chunk upload failed during tab cast.');
      }
    };
    rec.onerror = (ev) => logDiag('Recorder error (tab): ' + (ev.error?.name || 'unknown'));
    rec.start(1000);

    // Stop handling
    const stopAll = () => {
      try { rec.stop(); } catch {}
      stream.getTracks().forEach(t => t.stop());
    };
    screenRecController.stop = stopAll;
    screenRecController.stream = stream;

    // If user stops share from browser UI
    stream.getVideoTracks().forEach(t => {
      t.addEventListener('ended', () => {
        stopAll();
        logDiag('Tab share ended.');
      });
    });
  } catch (e) {
    logDiag('Failed to start tab broadcast: ' + e.message);
  }
}

// Helpers
async function getScreenStream() {
  try {
    if (navigator.mediaDevices?.getDisplayMedia) {
      return await navigator.mediaDevices.getDisplayMedia({
        video: { frameRate: 30, width: { ideal: 1280 }, height: { ideal: 720 } },
        audio: true
      });
    }
    if (navigator.getDisplayMedia) {
      return await navigator.getDisplayMedia({ video: true, audio: true });
    }
    // Firefox fallback
    if (navigator.mediaDevices?.getUserMedia) {
      return await navigator.mediaDevices.getUserMedia({ video: { mediaSource: 'screen' }, audio: true });
    }
  } catch (e) {
    logDiag('getScreenStream error: ' + e.message);
  }
  return null;
}

// Wire UI
els.goLiveBtn.addEventListener('click', async () => {
  const file = els.fileInput.files && els.fileInput.files[0];
  await becomeHostWithFile(file);
});

els.startTabBtn.addEventListener('click', () => {
  startScreenLivestream();
});
/* feature-detect screen share and disable if unsupported */
if (!(navigator.mediaDevices && (navigator.mediaDevices.getDisplayMedia || navigator.getDisplayMedia))) {
  els.startTabBtn.disabled = true;
  els.startTabBtn.title = 'Screen sharing is not supported in this browser.';
  logDiag('Screen share unavailable: getDisplayMedia not supported.');
}

els.takeHostBtn.addEventListener('click', () => {
  takeHost();
});

els.endBtn.addEventListener('click', () => {
  screenRecController.stop?.();
  endStream();
});

els.joinBtn.addEventListener('click', async () => {
  // Attempt to play again
  try {
    els.video.muted = false;
    await els.video.play();
    els.joinOverlay.classList.add('hidden');
  } catch {
    // ignored
  }
});

els.chatSend.addEventListener('click', () => {
  const text = (els.chatText.value || '').trim();
  if (!text) return;
  els.chatText.value = '';
  room.send({ type: 'chat', text, ts: Date.now() });
});
els.chatText.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') els.chatSend.click();
});

// Host control hooks: reflect play/pause/seek to room state
els.video.addEventListener('play', () => {
  if (!isYouHost()) return;
  const ls = getLivestream();
  if (!ls) return;
  if (ls.mode === 'chunk') {
    // Only update if state is actually paused to avoid loops
    if (ls.paused) {
      room.updateRoomState({ livestream: { ...ls, paused: false } });
      logDiag('Host: play (chunk mode)');
    }
    return;
  }
  const now = Date.now();
  const newStartedAt = now - Math.floor(els.video.currentTime * 1000);
  room.updateRoomState({
    livestream: {
      ...ls,
      paused: false,
      startedAt: newStartedAt,
      pauseAt: null,
    },
  });
  logDiag('Host: play');
});

els.video.addEventListener('pause', () => {
  // Ignore pause events triggered by syncing while we just applied state (<200ms)
  if (ms() - state.lastAppliedFromState < 200) return;
  if (!isYouHost()) return;
  const ls = getLivestream();
  if (!ls) return;
  if (ls.mode === 'chunk') {
    room.updateRoomState({ livestream: { ...ls, paused: true } });
    logDiag('Host: pause (chunk mode)');
    return;
  }
  const now = Date.now();
  const newStartedAt = now - Math.floor(els.video.currentTime * 1000);
  room.updateRoomState({
    livestream: {
      ...ls,
      paused: true,
      startedAt: newStartedAt,
      pauseAt: now,
    },
  });
  logDiag('Host: pause');
});

let seeking = false;
els.video.addEventListener('seeking', () => {
  seeking = true;
});
els.video.addEventListener('seeked', () => {
  if (!isYouHost()) {
    // Disallow viewer seeking: snap back to state on next sync
    const ls = getLivestream();
    if (ls) {
      const t = computeLiveCurrentTime(ls);
      if (Math.abs(els.video.currentTime - t) > 0.25) {
        els.video.currentTime = t;
      }
    }
    return;
  }
  const ls = getLivestream();
  if (!ls) return;
  if (ls.mode === 'chunk') {
    // Seeking not supported in chunk mode POC
    logDiag('Seek ignored in chunk mode');
    return;
  }
  const now = Date.now();
  const newStartedAt = now - Math.floor(els.video.currentTime * 1000);
  room.updateRoomState({
    livestream: {
      ...ls,
      startedAt: newStartedAt,
      pauseAt: ls.paused ? now : null,
    },
  });
  seeking = false;
  logDiag(`Host: seek -> ${els.video.currentTime.toFixed(2)}s`);
});

// Passive resync every few seconds to correct drift for viewers
setInterval(() => {
  const ls = getLivestream();
  if (!ls) return;
  if (ls.mode === 'chunk') {
    // Ensure play/pause matches for chunk mode
    if (!isYouHost()) {
      if (!ls.paused && els.video.paused) {
        els.video.play().catch(() => {
          els.joinOverlay.classList.remove('hidden');
        });
      } else if (ls.paused && !els.video.paused) {
        els.video.pause();
      }
    }
    return;
  }
  const targetT = computeLiveCurrentTime(ls);
  const drift = (els.video.currentTime || 0) - targetT;
  if (Math.abs(drift) > 0.4) {
    els.video.currentTime = targetT;
  }
  // Ensure play/pause matches
  const shouldPlay = !ls.paused;
  if (shouldPlay && els.video.paused) {
    els.video.play().catch(() => {
      els.joinOverlay.classList.remove('hidden');
    });
  } else if (!shouldPlay && !els.video.paused) {
    els.video.pause();
  }
}, 2000);

// Initial UI state
els.roomStatus.textContent = 'Connected';
refreshFromRoomState();