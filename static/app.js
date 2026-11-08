// Single-page client. No dependencies; all user text goes through textContent.

const $ = (sel) => document.querySelector(sel);

const PAGE = 50;
const READ_DEBOUNCE_MS = 1000;

const state = {
  me: null,
  my: new Map(),      // chat id -> my_chats entry from /api/home
  home: null,
  chat: null,         // { id, info, firstSeq, lastSeq, hasMore, loadingOlder, token }
  ws: null,
  wsBackoff: 500,
  readTimer: null,
  catchingUp: null,
};
let openToken = 0;

// ---------- API ----------

class ApiError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

async function api(method, path, body) {
  const opts = { method, headers: {}, credentials: "same-origin" };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch("/api" + path, opts);
  const text = await res.text();
  const data = text ? JSON.parse(text) : null;
  if (!res.ok) {
    if (res.status === 401 && state.me && !path.startsWith("/login")) signedOut();
    throw new ApiError(res.status, (data && data.error) || res.statusText);
  }
  return data;
}

// ---------- DOM helpers ----------

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") node.className = v;
    else if (k === "text") node.textContent = v;
    else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v);
  }
  for (const c of children) if (c) node.append(c);
  return node;
}

function notice(text) {
  const n = $("#notice");
  n.textContent = text;
  n.hidden = !text;
  if (text) setTimeout(() => { if (n.textContent === text) n.hidden = true; }, 6000);
}

function plural(n, word) {
  return `${n.toLocaleString()} ${word}${n === 1 ? "" : "s"}`;
}

// ---------- Auth ----------

function showAuth(which = "login") {
  $("#main-view").hidden = true;
  $("#auth-view").hidden = false;
  $("#login-form").hidden = which !== "login";
  $("#register-form").hidden = which !== "register";
  for (const f of document.querySelectorAll("#auth-view form")) f.querySelector("[data-error]").textContent = "";
}

function signedOut() {
  state.me = null;
  state.chat = null;
  if (state.ws) {
    state.ws.onclose = null;
    state.ws.close();
    state.ws = null;
  }
  showAuth("login");
}

function setupAuthForms() {
  for (const btn of document.querySelectorAll("[data-show]")) {
    btn.addEventListener("click", () => showAuth(btn.dataset.show));
  }

  $("#login-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const f = e.target;
    const err = f.querySelector("[data-error]");
    err.textContent = "";
    try {
      const me = await api("POST", "/login", { login: f.login.value.trim(), password: f.password.value });
      f.reset();
      signedIn(me);
    } catch (ex) {
      err.textContent = ex.message;
    }
  });

  $("#register-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const f = e.target;
    const err = f.querySelector("[data-error]");
    err.textContent = "";
    if (f.password.value !== f.repeat.value) {
      err.textContent = "Passwords don't match.";
      return;
    }
    try {
      const me = await api("POST", "/register", { login: f.login.value.trim(), password: f.password.value });
      f.reset();
      signedIn(me);
    } catch (ex) {
      err.textContent = ex.message;
    }
  });

  $("#logout-btn").addEventListener("click", async () => {
    try { await api("POST", "/logout"); } catch { /* signed out either way */ }
    signedOut();
  });
}

function signedIn(me) {
  state.me = me;
  $("#me").textContent = me.login;
  $("#auth-view").hidden = true;
  $("#main-view").hidden = false;
  connectWs();
  loadHome();
  route();
}

// ---------- Home ----------

async function loadHome() {
  try {
    const home = await api("GET", "/home");
    state.home = home;
    state.my = new Map(home.my_chats.map((c) => [c.id, c]));
    renderHome();
  } catch (ex) {
    if (ex.status !== 401) notice("Couldn't load chats: " + ex.message);
  }
}

function unreadOf(c) {
  if (state.chat && state.chat.id === c.id && document.visibilityState === "visible") return 0;
  return Math.max(0, c.last_seq - c.last_read_seq);
}

function chatLink(c, withBadge) {
  const a = el("a", { href: `#/chat/${c.id}` },
    el("span", { class: "title", text: c.title }),
    el("span", { class: "kind", text: c.kind }),
    el("span", { class: "count", text: c.member_count.toLocaleString() }));
  if (state.chat && state.chat.id === c.id) a.classList.add("active");
  if (withBadge) {
    const n = unreadOf(c);
    if (n > 0) a.append(el("span", { class: "badge", text: n > 999 ? "999+" : String(n) }));
  }
  return el("li", {}, a);
}

function fillList(list, chats, withBadge, emptyText) {
  list.replaceChildren(...(chats.length
    ? chats.map((c) => chatLink(c, withBadge))
    : [el("li", { class: "empty-item", text: emptyText })]));
}

function renderHome() {
  if (!state.home) return;
  fillList($("#my-chats"), [...state.my.values()], true, "Join or create a chat.");
  fillList($("#top-groups"), state.home.top_groups, false, "No groups yet.");
  fillList($("#top-channels"), state.home.top_channels, false, "No channels yet.");
}

function setupNewChat() {
  const dialog = $("#new-chat-dialog");
  const form = $("#new-chat-form");
  $("#new-chat-btn").addEventListener("click", () => {
    form.reset();
    form.querySelector("[data-error]").textContent = "";
    dialog.showModal();
  });
  form.querySelector("[data-close]").addEventListener("click", () => dialog.close());
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      const chat = await api("POST", "/chats", { kind: form.kind.value, title: form.title.value });
      dialog.close();
      await loadHome();
      location.hash = `#/chat/${chat.id}`;
    } catch (ex) {
      form.querySelector("[data-error]").textContent = ex.message;
    }
  });
}

// ---------- Routing ----------

function route() {
  if (!state.me) return;
  const m = location.hash.match(/^#\/chat\/(\d+)$/);
  if (m) openChat(Number(m[1]));
  else closeChat();
}

// ---------- Chat view ----------

function closeChat() {
  openToken++;
  state.chat = null;
  wsSend({ t: "close" });
  $("#chat-view").hidden = true;
  $("#chat-empty").hidden = false;
  renderHome();
}

async function openChat(id) {
  const token = ++openToken;
  state.chat = { id, info: null, firstSeq: null, lastSeq: 0, hasMore: false, loadingOlder: false, token };
  wsSend({ t: "open", chat: id });
  $("#chat-empty").hidden = true;
  $("#chat-view").hidden = false;
  $("#message-list").replaceChildren();
  $("#chat-title").textContent = "...";
  $("#chat-meta").textContent = "";
  try {
    const [info, page] = await Promise.all([
      api("GET", `/chats/${id}`),
      api("GET", `/chats/${id}/messages?limit=${PAGE}`),
    ]);
    if (token !== openToken) return;
    state.chat.info = info;
    renderChatHeader();
    const msgs = page.messages.slice().reverse();
    state.chat.hasMore = page.has_more;
    if (msgs.length) {
      state.chat.firstSeq = msgs[0].seq;
      state.chat.lastSeq = msgs[msgs.length - 1].seq;
    }
    $("#message-list").replaceChildren(...msgs.map(renderMessage));
    if (!page.has_more) $("#message-list").prepend(historyStart());
    scrollToBottom();
    // Frames that arrived while loading may be past this page.
    await catchUp();
    markReadSoon(0);
    renderHome();
  } catch (ex) {
    if (token !== openToken) return;
    if (ex.status === 404) {
      notice("That chat doesn't exist anymore.");
      location.hash = "#/";
    } else {
      notice("Couldn't open chat: " + ex.message);
    }
  }
}

function renderChatHeader() {
  const info = state.chat.info;
  $("#chat-title").textContent = info.title;
  $("#chat-meta").textContent =
    `${info.kind} by ${info.owner.login} · ${plural(info.member_count, "member")}`;
  $("#join-btn").hidden = info.is_member;
  $("#leave-btn").hidden = !info.is_member || info.is_owner;
  $("#delete-btn").hidden = !info.is_owner;
  $("#send-form").hidden = !info.can_post;
  $("#read-only").hidden = info.can_post || info.kind !== "channel";
  if (!info.can_post && info.kind === "group") {
    $("#read-only").hidden = false;
    $("#read-only").textContent = "Join the group to post.";
  } else {
    $("#read-only").textContent = "Only the channel owner can post here.";
  }
}

function historyStart() {
  return el("li", { class: "history-start", text: "Start of the conversation" });
}

function renderMessage(m) {
  const d = new Date(m.ts);
  const when = d.toDateString() === new Date().toDateString()
    ? d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
    : d.toLocaleString([], { dateStyle: "short", timeStyle: "short" });
  const li = el("li", { class: "msg", "data-seq": String(m.seq) },
    el("span", { class: "who", text: m.author.login }),
    el("span", { class: "when", text: when }),
    el("div", { class: "body", text: m.body }));
  if (state.me && m.author.id === state.me.id) li.classList.add("mine");
  return li;
}

function nearBottom() {
  const box = $("#messages");
  return box.scrollHeight - box.scrollTop - box.clientHeight < 80;
}

function scrollToBottom() {
  const box = $("#messages");
  box.scrollTop = box.scrollHeight;
}

// Adds a newer message, fetching any gap first. Ignores duplicates.
function ingest(m) {
  const c = state.chat;
  if (!c || m.chat_id !== c.id || c.info === null) return;
  if (m.seq <= c.lastSeq) return;
  if (m.seq > c.lastSeq + 1) {
    catchUp();
    return;
  }
  const stick = nearBottom() || (state.me && m.author.id === state.me.id);
  $("#message-list").append(renderMessage(m));
  c.lastSeq = m.seq;
  if (c.firstSeq === null) c.firstSeq = m.seq;
  if (stick) scrollToBottom();
  bumpMyChat(c.id, m.seq);
  markReadSoon();
}

// Fetches everything after lastSeq for the open chat.
function catchUp() {
  if (state.catchingUp) return state.catchingUp;
  const run = async () => {
    const c = state.chat;
    if (!c || c.info === null) return;
    for (;;) {
      const page = await api("GET", `/chats/${c.id}/messages?after=${c.lastSeq}&limit=100`);
      if (state.chat !== c) return;
      for (const m of page.messages) ingest(m);
      if (!page.has_more) return;
    }
  };
  state.catchingUp = run().catch(() => {}).finally(() => { state.catchingUp = null; });
  return state.catchingUp;
}

async function loadOlder() {
  const c = state.chat;
  if (!c || !c.hasMore || c.loadingOlder || c.firstSeq === null) return;
  c.loadingOlder = true;
  try {
    const page = await api("GET", `/chats/${c.id}/messages?before=${c.firstSeq}&limit=${PAGE}`);
    if (state.chat !== c) return;
    const box = $("#messages");
    const before = box.scrollHeight;
    const nodes = page.messages.slice().reverse().map(renderMessage);
    if (!page.has_more) nodes.unshift(historyStart());
    $("#message-list").prepend(...nodes);
    // Keep what the user was looking at in place.
    box.scrollTop += box.scrollHeight - before;
    if (page.messages.length) c.firstSeq = page.messages[page.messages.length - 1].seq;
    c.hasMore = page.has_more;
  } catch (ex) {
    notice("Couldn't load older messages: " + ex.message);
  } finally {
    c.loadingOlder = false;
  }
}

function bumpMyChat(id, seq) {
  const mine = state.my.get(id);
  if (mine && seq > mine.last_seq) {
    mine.last_seq = seq;
    renderHome();
  }
}

function markReadSoon(delay = READ_DEBOUNCE_MS) {
  clearTimeout(state.readTimer);
  state.readTimer = setTimeout(markRead, delay);
}

async function markRead() {
  const c = state.chat;
  if (!c || !c.info || !c.info.is_member || document.visibilityState !== "visible") return;
  const mine = state.my.get(c.id);
  if (mine && mine.last_read_seq >= c.lastSeq) return;
  const seq = c.lastSeq;
  try {
    await api("POST", `/chats/${c.id}/read`, { seq });
    if (mine) {
      mine.last_read_seq = Math.max(mine.last_read_seq, seq);
      renderHome();
    }
  } catch { /* retried on the next message */ }
}

async function refreshChatInfo() {
  const c = state.chat;
  if (!c) return;
  c.info = await api("GET", `/chats/${c.id}`);
  if (state.chat === c) renderChatHeader();
}

function setupChatView() {
  const send = $("#send-form");
  send.addEventListener("submit", async (e) => {
    e.preventDefault();
    const c = state.chat;
    const body = send.body.value;
    if (!c || !body.trim()) return;
    const btn = send.querySelector("button");
    btn.disabled = true;
    try {
      const msg = await api("POST", `/chats/${c.id}/messages`, { body });
      send.body.value = "";
      if (state.chat === c) {
        ingest(msg);
        const mine = state.my.get(c.id);
        if (mine) mine.last_read_seq = Math.max(mine.last_read_seq, msg.seq);
      }
    } catch (ex) {
      notice(ex.message);
    } finally {
      btn.disabled = false;
      send.body.focus();
    }
  });
  send.body.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey && !e.isComposing) {
      e.preventDefault();
      send.requestSubmit();
    }
  });

  $("#join-btn").addEventListener("click", async () => {
    try {
      await api("POST", `/chats/${state.chat.id}/join`);
      await refreshChatInfo();
      await loadHome();
    } catch (ex) { notice(ex.message); }
  });
  $("#leave-btn").addEventListener("click", async () => {
    try {
      await api("POST", `/chats/${state.chat.id}/leave`);
      await refreshChatInfo();
      await loadHome();
    } catch (ex) { notice(ex.message); }
  });
  $("#delete-btn").addEventListener("click", async () => {
    const c = state.chat;
    if (!confirm(`Delete "${c.info.title}" and all its messages? This can't be undone.`)) return;
    try {
      await api("DELETE", `/chats/${c.id}`);
      location.hash = "#/";
      await loadHome();
    } catch (ex) { notice(ex.message); }
  });

  new IntersectionObserver((entries) => {
    if (entries.some((e) => e.isIntersecting)) loadOlder();
  }, { root: $("#messages"), rootMargin: "200px 0px 0px 0px" }).observe($("#top-sentinel"));

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      markReadSoon(0);
      renderHome();
    }
  });
}

// ---------- WebSocket ----------

function wsSend(frame) {
  if (state.ws && state.ws.readyState === WebSocket.OPEN) state.ws.send(JSON.stringify(frame));
}

function connectWs() {
  if (state.ws) return;
  const url = `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/api/ws`;
  const ws = new WebSocket(url);
  state.ws = ws;
  let opened = false;

  ws.onopen = () => {
    opened = true;
    state.wsBackoff = 500;
    if (state.chat) {
      wsSend({ t: "open", chat: state.chat.id });
      catchUp();
    }
    loadHome();
  };

  ws.onmessage = (e) => {
    let f;
    try { f = JSON.parse(e.data); } catch { return; }
    switch (f.t) {
      case "msg":
        ingest(f);
        break;
      case "seq":
        for (const [id, seq] of Object.entries(f.chats)) {
          const mine = state.my.get(Number(id));
          if (mine) mine.last_seq = Math.max(mine.last_seq, seq);
        }
        renderHome();
        break;
      case "deleted":
        chatDeleted(f.chat);
        break;
      case "resync":
        if (state.chat && state.chat.id === f.chat) catchUp();
        break;
      case "error":
        if (f.code === "not_found" && state.chat && state.chat.id === f.chat) chatDeleted(f.chat);
        break;
    }
  };

  ws.onclose = () => {
    state.ws = null;
    if (!state.me) return;
    if (!opened) {
      // The upgrade may have failed because the session ended.
      api("GET", "/me").catch(() => {});
    }
    const delay = state.wsBackoff * (0.75 + Math.random() * 0.5);
    state.wsBackoff = Math.min(state.wsBackoff * 2, 10000);
    setTimeout(() => { if (state.me) connectWs(); }, delay);
  };
}

function chatDeleted(id) {
  const known = state.my.delete(id);
  if (state.chat && state.chat.id === id) {
    notice("This chat was deleted.");
    location.hash = "#/";
  } else if (known) {
    renderHome();
  }
  loadHome();
}

// ---------- Boot ----------

async function boot() {
  setupAuthForms();
  setupNewChat();
  setupChatView();
  window.addEventListener("hashchange", route);
  try {
    signedIn(await api("GET", "/me"));
  } catch {
    showAuth("login");
  }
}

boot();
