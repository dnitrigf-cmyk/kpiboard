/* =========================
   KPI Board — Online Shared (v13+Firebase)
   - Общая база Firestore: collection 'shared', doc 'board'
   - Email/Password Auth (Firebase)
   - Department chips inline
   - Header menu ≡ forced to far right (+ Logout)
   - Rename User via modal (edit Name + Position)
   - Users store { title, board }
   ========================= */

const STORAGE_KEY = "kpi-multiuser-v7";
const $ = (s) => document.querySelector(s);

// ---------- State ----------
let orgData = { departments: {} }; // { dept: { users: { userName: {title, board} } } }
let currentDepartment = null;
let currentUser = null;
let viewLevel = "month"; // "month" | "week" | "day"
let selectedRow = -1;

let sideUserFilters = {}; // { [dept]: { [user]: null | KPI name } }
let sidebarScope = "dept"; // "dept" | "all"

// Month window (carousel)
const MONTH_WINDOW = 6;
let monthPagerStart = firstOfMonth(new Date());

// Context for week/day
let currentMonth = firstOfMonth(new Date());
let currentISO = isoWeekObj(new Date());

const tbody = $("#tbody");
const theadRow = $("#theadRow");

// ---------- Date helpers ----------
function pad(n){return String(n).padStart(2,"0");}
function ymd(d){return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;}
function firstOfMonth(d){ return new Date(d.getFullYear(), d.getMonth(), 1); }
function addMonths(d, n){ return new Date(d.getFullYear(), d.getMonth()+n, 1); }
function endOfMonth(d){ return new Date(d.getFullYear(), d.getMonth()+1, 0); }
function startOfISOWeek(d){
  const day = d.getDay() === 0 ? 7 : d.getDay();
  const res = new Date(d);
  res.setDate(d.getDate() - (day - 1));
  res.setHours(0,0,0,0);
  return res;
}
function isoWeekObj(d){
  const t = new Date(d.getTime());
  t.setHours(0,0,0,0);
  t.setDate(t.getDate() + 3 - ((t.getDay() + 6) % 7));
  const week1 = new Date(t.getFullYear(),0,4);
  const week = 1 + Math.round(((t.getTime() - week1.getTime())/86400000 - 3 + ((week1.getDay()+6)%7))/7);
  return {week, year:t.getFullYear()};
}
function datesOfISOWeek(week, year){
  const simple = new Date(year,0,1 + (week - 1) * 7);
  const ISOweekStart = startOfISOWeek(simple);
  const days = [];
  for(let i=0;i<5;i++){ const d=new Date(ISOweekStart); d.setDate(ISOweekStart.getDate()+i); days.push(d); }
  return days;
}
function isoWeeksInMonth(monthDate){
  const start = startOfISOWeek(firstOfMonth(monthDate));
  const end = endOfMonth(monthDate);
  const weeks = [];
  let cur = new Date(start);
  while(cur <= end){
    const {week, year} = isoWeekObj(cur);
    const key = `${year}-W${week}`;
    if (!weeks.find(w=>w.key===key)) weeks.push({week, year, key});
    cur.setDate(cur.getDate()+7);
  }
  return weeks;
}
function monthsFrom(startDate, count){
  const list = [];
  for(let i=0;i<count;i++){
    const d = addMonths(startDate, i);
    list.push({date:firstOfMonth(d), label:d.toLocaleString('ru-RU',{month:'long',year:'numeric'})});
  }
  return list;
}

// ---------- Number helpers ----------
function parseNumber(s) {
  if (!s) return NaN;
  s = String(s).trim().replace(/[€$%\u00A0\s]/g, "").replace(",", ".");
  return s === "" ? NaN : Number(s);
}
function normalizePercent(v){
  let n = parseNumber(v);
  if (isNaN(n)) return null;
  n = Math.max(0, Math.min(100, n));
  return Math.round(n * 100) / 100;
}
function averageNumbers(arr){
  const nums = arr.map(parseNumber).filter(v => !isNaN(v));
  if (!nums.length) return null;
  const avg = nums.reduce((a,b)=>a+b,0)/nums.length;
  return Math.round(avg * 100) / 100;
}

// ---------- Storage / migration ----------
function defaultBoard(){
  return {
    rows: [
      { name: "Contract (%)", target: ">= 71 %", entries: {} },
      { name: "Spot (%)",     target: "<= 29 %", entries: {} },
      { name: "Shipments",    target: ">= 9,200", entries: {} }
    ]
  };
}
// ---------- Firestore realtime globals ----------
let __unsubRT = null;
let __isLocalSave = false;

function migrateLegacyBoardFormat(board){
  board.rows.forEach(r=>{
    if (Array.isArray(r.values)){
      const todayISO = isoWeekObj(new Date());
      const days = datesOfISOWeek(todayISO.week, todayISO.year);
      r.entries = r.entries || {};
      r.values.forEach((val, idx)=>{
        if (!val) return;
        const d = days[Math.min(idx,4)];
        r.entries[ymd(d)] = val;
      });
      delete r.values; r.weeks && delete r.weeks;
    } else { r.entries = r.entries || {}; }
  });
}
function migrateIfNeeded(raw){
  if (raw && raw.departments){
    for (const [dept, obj] of Object.entries(raw.departments)){
      obj.users = obj.users || {};
      for (const [uname, maybeNode] of Object.entries(obj.users)){
        if (maybeNode && !("board" in maybeNode)){
          // legacy: user -> board
          migrateLegacyBoardFormat(maybeNode);
          obj.users[uname] = { title:"", board: maybeNode };
        } else {
          migrateLegacyBoardFormat(obj.users[uname].board);
          obj.users[uname].title = obj.users[uname].title || "";
        }
      }
    }
    return raw;
  }
  // very old: root was users map
  const data = {departments:{}};
  const usersMap = raw && typeof raw === "object" ? raw : {};
  const deptName = "General";
  data.departments[deptName] = { users: {} };
  const names = Object.keys(usersMap);
  if (!names.length){
    data.departments[deptName].users["User 1"] = { title:"", board: defaultBoard() };
  } else {
    names.forEach(n=>{
      const b = usersMap[n];
      migrateLegacyBoardFormat(b);
      data.departments[deptName].users[n] = { title:"", board:b };
    });
  }
  return data;
}

/* ========= Firestore SHARED storage (общая база) ========= */
function load(){
  const FB_AUTH = window.__FB_AUTH;
  const FB_DB   = window.__FB_DB;
  const docFn   = window.__fbDoc;
  const getDoc  = window.__fbGetDoc;
  const setDoc  = window.__fbSetDoc;

  const SHARED_COLL = 'shared';
  const SHARED_DOC  = 'board';

  return new Promise((resolve) => {
    async function doLoad(){
      try{
        const ref = docFn(FB_DB, SHARED_COLL, SHARED_DOC);
        const snap = await getDoc(ref);
        if (snap.exists()){
          orgData = migrateIfNeeded(snap.data());
        } else {
          orgData = { departments: { "General": { users: { "User 1": { title:"", board: defaultBoard() } } } } };
          await setDoc(ref, orgData);
        }
      } catch(err){
        console.error("Firestore load error:", err);
        // fallback — локальный кеш
        try {
          const s = localStorage.getItem(STORAGE_KEY);
          if (s) orgData = migrateIfNeeded(JSON.parse(s));
        } catch(e){}
      }

      if (!Object.keys(orgData.departments).length){
        orgData.departments["General"] = { users: { "User 1": { title:"", board: defaultBoard() } } };
      }
      currentDepartment = Object.keys(orgData.departments)[0];
      const users = Object.keys(orgData.departments[currentDepartment].users);
      if (!users.length) orgData.departments[currentDepartment].users["User 1"] = { title:"", board: defaultBoard() };
      currentUser = Object.keys(orgData.departments[currentDepartment].users)[0];
      monthPagerStart = firstOfMonth(new Date());

      // Синхронизируем текущее состояние в базу (единообразие)
      save();
      resolve();
    }

    // ждём авторизацию, если её ещё нет
    if (FB_AUTH && FB_AUTH.currentUser) doLoad();
    else {
      const onAuth = () => { window.removeEventListener('fbAuthChanged', onAuth); doLoad(); };
      window.addEventListener('fbAuthChanged', onAuth);
    }
  });
}
function save(){
  try {
    const FB_DB  = window.__FB_DB;
    const docFn  = window.__fbDoc;
    const setDoc = window.__fbSetDoc;
    const ref = docFn(FB_DB, 'shared', 'board');
    __isLocalSave = true;
    setDoc(ref, orgData);
  } catch(err){
    console.error("Firestore save error:", err);
    localStorage.setItem(STORAGE_KEY, JSON.stringify(orgData));
  }
}
async function ensureUserFromProfile(){
  const prof = window.currentUserProfile;
  if (!prof) return;

  const dept   = prof.department;
  const name   = prof.displayName;
  const title  = prof.position || prof.title || "";
  // гарантия существования департамента
  orgData.departments[dept] = orgData.departments[dept] || { users: {} };

  // если пользователя нет — создаём
  if (!orgData.departments[dept].users[name]) {
    orgData.departments[dept].users[name] = { title, board: defaultBoard() };
    currentDepartment = dept;
    currentUser = name;
    save();
  } else {
    // синхронизация должности, если изменилась
    const node = orgData.departments[dept].users[name];
    if ((node.title || "") !== title) {
      node.title = title;
      save();
    }
  }
}

// ---------- Roles / Permissions ----------
function isSuper(){ return (window.currentUserProfile?.role === "super"); }

function canManageDept(targetDept){
  const prof = window.currentUserProfile;
  if (!prof) return false;
  if (isSuper()) return true;
  if (prof.role === "director") return true;
  if (prof.role === "leader" && prof.department === targetDept) return true;
  return false; // member — нет прав
}

// редактирование значений KPI (дни) — своё, лидер в департаменте, директор/супер везде
function canEditUser(targetDept, targetUserName){
  const prof = window.currentUserProfile;
  if (!prof) return false;
  if (isSuper()) return true;
  if (prof.role === "director") return true;
  if (prof.role === "leader" && prof.department === targetDept) return true;
  if (prof.role === "member" && prof.department === targetDept && targetUserName === prof.displayName) return true;
  return false;
}

// управление структурой KPI (добавить/переименовать/менять Target/удалять)
function canManageKpi(targetDept){
  const prof = window.currentUserProfile;
  if (!prof) return false;
  if (isSuper()) return true;
  if (prof.role === "director") return true;
  if (prof.role === "leader" && prof.department === targetDept) return true;
  return false;
}
function canAddKpi(targetDept){ return canManageKpi(targetDept); }
function canDeleteKpi(targetDept){ return canManageKpi(targetDept); }

function startRealtime(){
  const FB_DB = window.__FB_DB;
  const docFn = window.__fbDoc;
  const onSnap = window.__fbSnap;
  if (!FB_DB || !docFn || !onSnap) return;

  if (__unsubRT) { __unsubRT(); __unsubRT = null; }
  const ref = docFn(FB_DB, 'shared', 'board');

  __unsubRT = onSnap(ref, (snap)=>{
    if (!snap.exists()) return;
    // если это наш локальный save — пропускаем один цикл
    if (__isLocalSave) { __isLocalSave = false; return; }

    const incoming = migrateIfNeeded(snap.data());
    if (JSON.stringify(incoming) !== JSON.stringify(orgData)) {
      orgData = incoming;
      renderUsers();
      renderTable();
      renderSidebar();
    }
  });
}

// ---------- Range helpers ----------
function currentDateRange(){
  if (viewLevel === "month"){
    const months = monthsFrom(monthPagerStart, MONTH_WINDOW);
    return {from: firstOfMonth(months[0].date), to: endOfMonth(months[months.length-1].date)};
  }
  if (viewLevel === "week"){
    return {from: firstOfMonth(currentMonth), to: endOfMonth(currentMonth)};
  }
  const ds = datesOfISOWeek(currentISO.week, currentISO.year);
  return {from: ds[0], to: ds[ds.length-1]};
}
function computeUserAverageInDept(deptName, userName, from, to){
  const node = orgData.departments[deptName]?.users?.[userName];
  if (!node) return null;
  const board = node.board;
  const filter = (sideUserFilters[deptName]?.[userName]) || null;
  const rows = filter ? board.rows.filter(r => r.name === filter) : board.rows;
  const vals = [];
  rows.forEach(r=>{
    for (const [k,v] of Object.entries(r.entries || {})){
      const d = new Date(k+"T00:00:00");
      if (d >= from && d <= to){
        const n = normalizePercent(v);
        if (n != null) vals.push(n);
      }
    }
  });
  if (!vals.length) return null;
  const avg = vals.reduce((a,b)=>a+b,0) / vals.length;
  return Math.round(avg * 100) / 100;
}

// ---------- Header builders ----------
function clearDynamicHeader(){ while (theadRow.children.length > 2) theadRow.removeChild(theadRow.lastChild); }
function buildHeaderMonth(){
  clearDynamicHeader();
  monthsFrom(monthPagerStart, MONTH_WINDOW).forEach(({date,label})=>{
    const th = document.createElement("th");
    th.className = "th-month clickable";
    th.textContent = label;
    th.onclick = ()=>{ viewLevel="week"; currentMonth = date; renderTable(); };
    theadRow.appendChild(th);
  });
}
function buildHeaderWeek(){
  clearDynamicHeader();
  isoWeeksInMonth(currentMonth).forEach(({week,year})=>{
    const th = document.createElement("th");
    th.className="th-week clickable";
    th.textContent = `Week ${week}`;
    th.onclick = ()=>{ viewLevel="day"; currentISO = {week, year}; renderTable(); };
    theadRow.appendChild(th);
  });
}
function buildHeaderDay(){
  clearDynamicHeader();
  const days = datesOfISOWeek(currentISO.week, currentISO.year);
  const labels = ["Mon","Tue","Wed","Thu","Fri"];
  days.forEach((d,i)=>{
    const th = document.createElement("th");
    th.className="th-day";
    th.textContent = `${labels[i]} ${pad(d.getDate())}.${pad(d.getMonth()+1)}`;
    theadRow.appendChild(th);
  });
}

// ---------- Sticky header control bar ----------
function renderHeaderControls(){
  const wrap = document.querySelector(".grid-wrap");
  if (!wrap) return;

  let bar = wrap.querySelector(".head-controls");
  if (!bar) {
    bar = document.createElement("div");
    bar.className = "head-controls";
    bar.innerHTML = `
      <button class="hc-btn" id="hcPrev">←</button>
      <div class="hc-spacer"></div>
      <button class="hc-btn" id="hcNext">→</button>
    `;
    wrap.prepend(bar);
  }
  const prev = bar.querySelector("#hcPrev");
  const next = bar.querySelector("#hcNext");

  if (viewLevel === "month") {
    prev.onclick = () => { monthPagerStart = addMonths(monthPagerStart, -MONTH_WINDOW); renderTable(); };
    next.onclick = () => { monthPagerStart = addMonths(monthPagerStart,  MONTH_WINDOW); renderTable(); };
    bar.style.display = "flex";
  } else if (viewLevel === "week") {
    prev.onclick = () => { currentMonth = addMonths(currentMonth, -1); renderTable(); };
    next.onclick = () => { currentMonth = addMonths(currentMonth,  1); renderTable(); };
    bar.style.display = "flex";
  } else {
    bar.style.display = "none";
  }
}

// ---------- Department chips (inline row near title) ----------
function ensureDeptSelector(){
  const header = document.querySelector("header");
  if (!header) return;

  // make header a flex row if not already
  if (getComputedStyle(header).display !== "flex"){
    header.style.display = "flex";
    header.style.alignItems = "center";
    header.style.gap = "12px";
  }

  let holder = header.querySelector("#deptHolder");
  if (!holder){
    holder = document.createElement("div");
    holder.id = "deptHolder";
    holder.style.display = "flex";
    holder.style.alignItems = "center";
    holder.style.gap = "8px";
    header.appendChild(holder);
  }

  // chips row
  holder.innerHTML = `<span style="color:#a8b6c9;font-size:12px;">Department:</span><div id="deptChips" style="display:flex;gap:6px;flex-wrap:wrap;"></div>`;
  const chips = holder.querySelector("#deptChips");
  Object.keys(orgData.departments).forEach(d=>{
    const chip = document.createElement("button");
    chip.className = "userBtn" + (d===currentDepartment ? " active" : "");
    chip.style.borderRadius = "999px";
    chip.textContent = d;

    // LMB — switch
    chip.addEventListener("click", ()=> switchDepartment(d));

    // RMB — manage
    chip.addEventListener("contextmenu", (e)=>{
      e.preventDefault();
      openContextMenu([
        {label:"Rename Department", action: ()=> canManageDept(d) ? openRenameDeptModal(d) : alert("Недостаточно прав")},
        {label:"Delete Department", action: ()=> canManageDept(d) ? deleteDepartment(d) : alert("Недостаточно прав"), danger:true},
        {label:"Add Department",    action: ()=> canManageDept(d) ? openAddDeptModal() : alert("Недостаточно прав")},
      ], e.pageX, e.pageY);
    });

    chips.appendChild(chip);
  });

  // ensure menu button is far right
  ensureAppMenuButton(header);
}
function switchDepartment(d){
  currentDepartment = d;
  const users = Object.keys(orgData.departments[currentDepartment].users);
  if (!users.length){
    orgData.departments[currentDepartment].users["User 1"] = { title:"", board: defaultBoard() };
  }
  currentUser = Object.keys(orgData.departments[currentDepartment].users)[0];
  save(); renderUsers(); renderTable();
}
function deleteDepartment(deptName){
  if (!canManageDept(deptName)) {
    alert("Недостаточно прав для удаления департамента.");
    return;
  }
  const depts = Object.keys(orgData.departments);
  if (depts.length <= 1){
    alert("You cannot delete the last department.");
    return;
  }
  if (!confirm(`Delete department "${deptName}"? This will remove all its users and data.`)) return;

  const idx = depts.indexOf(deptName);
  delete orgData.departments[deptName];
  const newDept = depts[idx+1] || depts[idx-1] || Object.keys(orgData.departments)[0];
  currentDepartment = newDept;
  if (!Object.keys(orgData.departments[currentDepartment].users).length){
    orgData.departments[currentDepartment].users["User 1"] = { title:"", board: defaultBoard() };
  }
  currentUser = Object.keys(orgData.departments[currentDepartment].users)[0];

  save(); renderUsers(); renderTable(); renderSidebar();
}

// ---------- Render ----------
function renderTable(){
  const dept = orgData.departments[currentDepartment];
  if (!dept) return;
  const node = dept.users[currentUser];
  if (!node) return;
  const board = node.board;

  tbody.innerHTML = "";
  selectedRow = -1;

  if (viewLevel === "month") buildHeaderMonth();
  else if (viewLevel === "week") buildHeaderWeek();
  else buildHeaderDay();

  board.rows.forEach((row, rIdx) => {
    const tr = document.createElement("tr");

    // KPI name — RMB inline rename/delete (только у тех, у кого есть права управлять KPI)
    const thName = document.createElement("th");
    thName.className = "col-kpi editable";
    thName.textContent = row.name;
    thName.addEventListener("contextmenu", (e) => {
      e.preventDefault();
      if (!canManageKpi(currentDepartment)) {
        alert("Недостаточно прав");
        return;
      }
      openContextMenu([
        {label:"Rename", action: () => inlineRenameRowCell(thName, rIdx)},
        {label:"Delete", action: () => deleteRow(rIdx), danger:true},
      ], e.pageX, e.pageY);
    });
    tr.appendChild(thName);

    // Target — RMB edit (только менеджеры KPI)
    const tdTarget = document.createElement("td");
    tdTarget.className = "col-target editable";
    tdTarget.textContent = row.target;
    tdTarget.addEventListener("contextmenu", (e) => {
      e.preventDefault();
      if (!canManageKpi(currentDepartment)) {
        alert("Недостаточно прав");
        return;
      }
      makeEditableField(tdTarget, row, "target");
    });
    tr.appendChild(tdTarget);

    if (viewLevel === "month"){
      monthsFrom(monthPagerStart, MONTH_WINDOW).forEach(({date})=>{
        const from = firstOfMonth(date), to = endOfMonth(date);
        const vals = valuesInRange(row.entries, from, to);
        const avg = averageNumbers(vals);
        const td = document.createElement("td"); td.className = "cell";
        td.appendChild(makeProgress(avg)); tr.appendChild(td);
      });
    } else if (viewLevel === "week"){
      isoWeeksInMonth(currentMonth).forEach(({week,year})=>{
        const ds = datesOfISOWeek(week, year);
        const from = ds[0], to = ds[ds.length-1];
        const vals = valuesInRange(row.entries, from, to);
        const avg = averageNumbers(vals);
        const td = document.createElement("td"); td.className = "cell";
        td.appendChild(makeProgress(avg)); tr.appendChild(td);
      });
    } else { // day
      const days = datesOfISOWeek(currentISO.week, currentISO.year);
      days.forEach((d)=>{
        const key = ymd(d);
        const val = row.entries[key] || "";
        const td = document.createElement("td");
        td.className = "cell editable";
        const wrap = document.createElement("div");
        wrap.className = "rag progress";
        td.appendChild(wrap);
        paintCellProgress(wrap, val);
        td.addEventListener("contextmenu", (e)=>{
          e.preventDefault();
          if (!canEditUser(currentDepartment, currentUser)) {
            alert("У вас нет прав редактировать этот KPI");
            return;
          }
          makeEditableDate(td, row, key);
        });
        tr.appendChild(td);
      });
    }

    tr.addEventListener("click", () => {
      selectedRow = rIdx;
      Array.from(tbody.children).forEach((rowEl, i) =>
        rowEl.style.outline = i === rIdx ? "1px solid #2a334a" : "none"
      );
    });

    tbody.appendChild(tr);
  });

  renderHeaderControls();
  renderNavArrows();
  ensureDeptSelector();   // chips
  hideDeprecatedButtons();
  renderUsers();          // also re-renders sidebar
}

// ---------- Progress bar cells ----------
function makeProgress(valueOrNull){
  const div = document.createElement("div");
  div.className = "rag progress";
  const n = valueOrNull == null ? null : normalizePercent(valueOrNull);
  if (n == null) {
    div.innerHTML = `<div class="bar gray" style="width:0%"></div><span class="pct">—</span>`;
    return div;
  }
  const color = n >= 80 ? "green" : n >= 60 ? "yellow" : "red";
  div.innerHTML = `<div class="bar ${color}" style="width:${n}%"></div><span class="pct">${n}%</span>`;
  return div;
}
function paintCellProgress(container, valueStr){
  const n = normalizePercent(valueStr);
  if (n == null){
    container.innerHTML = `<div class="bar gray" style="width:0%"></div><span class="pct">No data</span>`;
    return;
  }
  const color = n >= 80 ? "green" : n >= 60 ? "yellow" : "red";
  container.innerHTML = `<div class="bar ${color}" style="width:${n}%"></div><span class="pct">${n}%</span>`;
}

// ---------- Values in range ----------
function valuesInRange(entries, fromDate, toDate){
  const out = [];
  const from = new Date(fromDate); from.setHours(0,0,0,0);
  const to = new Date(toDate);     to.setHours(23,59,59,999);
  for (const [k,v] of Object.entries(entries||{})){
    const d = new Date(k+"T00:00:00");
    if (d >= from && d <= to) out.push(v);
  }
  return out;
}

// ---------- Inline edit ----------
function makeEditableField(cell, row, field){
  if (cell.querySelector("input.inline-input")) return;
  const oldVal = row[field] || "";
  const input = document.createElement("input");
  input.type = "text"; input.value = oldVal; input.className = "inline-input";
  cell.innerHTML = ""; cell.appendChild(input);
  input.focus(); input.select();
  const cancel = () => { row[field] = oldVal; renderTable(); };
  const saveAndRerender = () => { row[field] = input.value.trim(); save(); renderTable(); };
  input.addEventListener("keydown", (e)=>{ if (e.key==="Enter") saveAndRerender(); else if (e.key==="Escape") cancel(); });
  input.addEventListener("blur", saveAndRerender);
}
function makeEditableDate(cell, row, dateKey){
  if (cell.querySelector("input.inline-input")) return;
  const oldVal = row.entries[dateKey] || "";
  const input = document.createElement("input");
  input.type = "text"; input.value = oldVal; input.className = "inline-input";
  cell.innerHTML = ""; cell.appendChild(input);
  input.focus(); input.select();
  const cancel = () => { row.entries[dateKey] = oldVal; renderTable(); };
  const saveAndRerender = () => {
    const v = input.value.trim();
    if (v) row.entries[dateKey] = v; else delete row.entries[dateKey];
    save(); renderTable();
  };
  input.addEventListener("keydown", (e)=>{ if (e.key==="Enter") saveAndRerender(); else if (e.key==="Escape") cancel(); });
  input.addEventListener("blur", saveAndRerender);
}

// ---------- Inline rename KPI ----------
function inlineRenameRowCell(cellEl, rIdx){
  const row = orgData.departments[currentDepartment].users[currentUser].board.rows[rIdx];
  if (cellEl.querySelector("input.inline-input")) return;

  const oldVal = row.name;
  const input = document.createElement("input");
  input.type = "text"; input.value = oldVal; input.className = "inline-input";
  cellEl.innerHTML = ""; cellEl.appendChild(input);
  input.focus(); input.select();
  const cancel = () => { row.name = oldVal; renderTable(); };
  const commit = () => { const v = input.value.trim(); if (v) row.name = v; save(); renderTable(); };
  input.addEventListener("keydown", (e)=>{ if (e.key==="Enter") commit(); else if (e.key==="Escape") cancel(); });
  input.addEventListener("blur", commit);
}

// ---------- Context menu ----------
let ctxMenuEl = null;
function ensureCtxMenu(){
  if (ctxMenuEl) return ctxMenuEl;
  ctxMenuEl = document.createElement("div");
  ctxMenuEl.className = "ctx-menu";
  document.body.appendChild(ctxMenuEl);
  ctxMenuEl.addEventListener("click", (e)=> e.stopPropagation());
  document.addEventListener("click", () => ctxMenuEl.style.display="none");
  window.addEventListener("resize", () => ctxMenuEl.style.display="none");
  window.addEventListener("scroll", () => ctxMenuEl.style.display="none", true);
  return ctxMenuEl;
}
function openContextMenu(items, x, y){
  const el = ensureCtxMenu();
  el.innerHTML = "";
  items.forEach(it=>{
    const a = document.createElement("div");
    a.className = "ctx-item" + (it.danger ? " danger" : "");
    a.textContent = it.label;
    a.onclick = (e) => { e.stopPropagation(); el.style.display="none"; it.action?.(); };
    el.appendChild(a);
  });
  el.style.display = "block";
  const {innerWidth, innerHeight} = window;
  const rect = el.getBoundingClientRect();
  const w = rect.width || 180, h = rect.height || (items.length*32);
  el.style.left = Math.min(x, innerWidth - w - 6) + "px";
  el.style.top  = Math.min(y, innerHeight - h - 6) + "px";
}

// ---------- KPI row ops ----------
function deleteRow(rIdx){
  if (!canDeleteKpi(currentDepartment)) {
    alert("У вас нет прав на удаление KPI.");
    return;
  }
  const board = orgData.departments[currentDepartment].users[currentUser].board;
  const row = board.rows[rIdx];
  if (!row) return;
  if (!confirm(`Delete KPI "${row.name}"?`)) return;
  board.rows.splice(rIdx, 1);
  selectedRow = -1;
  save(); renderTable();
}

// ---------- Users bar (bottom) ----------
function renderUsers() {
  const container = $("#users");
  container.innerHTML = "";

  const users = Object.keys(orgData.departments[currentDepartment].users);
  users.forEach(name => {
    const btn = document.createElement("button");
    btn.className = "userBtn" + (name === currentUser ? " active" : "");
    btn.textContent = name;

    btn.onclick = () => { currentUser = name; renderUsers(); renderTable(); };

    // контекстное меню только для управляющих департаментом
    if (canManageDept(currentDepartment)) {
      btn.addEventListener("contextmenu", (e)=>{
        e.preventDefault();
        openContextMenu([
          {label:"Rename", action: ()=>openRenameUserModal(name)},
          {label:"Delete", action: ()=>deleteUser(name), danger:true},
        ], e.pageX, e.pageY);
      });
    }

    container.appendChild(btn);
  });

  renderSidebar();
}
function deleteUser(name){
  if (!canManageDept(currentDepartment)) {
    alert("Недостаточно прав для удаления пользователей.");
    return;
  }
  const users = orgData.departments[currentDepartment].users;
  if (!confirm(`Delete user "${name}" and all his data?`)) return;
  const rest = Object.keys(users).filter(k=>k!==name);
  delete users[name];
  if (!rest.length){ users["User 1"] = { title:"", board: defaultBoard() }; currentUser = "User 1"; }
  else currentUser = rest[0];
  save(); renderUsers(); renderTable();
}

// ---------- Overlay back/home arrows ----------
function renderNavArrows(){
  const wrap = document.querySelector(".grid-wrap"); if (!wrap) return;
  let nav = wrap.querySelector(".nav-arrows");
  if (!nav) {
    nav = document.createElement("div");
    nav.className = "nav-arrows";
    nav.innerHTML = `
      <button class="nav-btn" id="btnBackLevel" title="Back">←</button>
      <button class="nav-btn" id="btnHome" title="Months">⌂</button>
    `;
    wrap.appendChild(nav);
  }
  const btnBack = nav.querySelector("#btnBackLevel");
  const btnHome = nav.querySelector("#btnHome");

  if (viewLevel === "day") {
    btnBack.style.display = "inline-flex";
    btnBack.onclick = () => { viewLevel = "week"; renderTable(); };
  } else if (viewLevel === "week") {
    btnBack.style.display = "inline-flex";
    btnBack.onclick = () => { viewLevel = "month"; renderTable(); };
  } else {
    btnBack.style.display = "none"; btnBack.onclick = null;
  }
  btnHome.onclick = () => { viewLevel = "month"; renderTable(); };
}

// ---------- App menu (≡) forced at far right ----------
function ensureAppMenuButton(headerEl){
  const header = headerEl || document.querySelector("header");

  if (!header) {
    if (document.querySelector("#btnAppMenu")) return;
    const floatBtn = document.createElement("button");
    floatBtn.id = "btnAppMenu";
    floatBtn.className = "app-menu-btn";
    floatBtn.title = "Menu";
    floatBtn.textContent = "≡";
    Object.assign(floatBtn.style, { position:"fixed", top:"10px", right:"12px", zIndex: 10000 });
    document.body.appendChild(floatBtn);
    floatBtn.addEventListener("click", (e)=>{
      e.stopPropagation();
      const r = floatBtn.getBoundingClientRect();
      const menuItems = [
        {label:"Add User",       action: canManageDept(currentDepartment) ? addUserFromMenu : null},
        {label:"Add KPI",        action: canAddKpi(currentDepartment) ? addKpiFromMenu : null},
        {label:"Add Department", action: canManageDept(currentDepartment) ? openAddDeptModal : null},
        {label:"Logout",         action: ()=>window.__fbLogout && window.__fbLogout()},
      ].filter(i => i.action);
      openContextMenu(menuItems, r.right, r.bottom+6);
    });
    return;
  }

  // ensure a flex grow spacer before menu to push it right
  let spacer = header.querySelector("#headSpacerGrow");
  if (!spacer){
    spacer = document.createElement("div");
    spacer.id = "headSpacerGrow";
    spacer.style.flex = "1 1 auto";
    header.appendChild(spacer);
  }

  let btn = header.querySelector("#btnAppMenu");
  if (!btn){
    btn = document.createElement("button");
    btn.id = "btnAppMenu";
    btn.className = "app-menu-btn";
    btn.title = "Menu";
    btn.textContent = "≡";
    header.appendChild(btn);
  } else {
    header.appendChild(btn); // move to the end if structure changed
  }

  btn.addEventListener("click", (e)=>{
    e.stopPropagation();
    const r = btn.getBoundingClientRect();
    const menuItems = [
      {label:"Add User",       action: canManageDept(currentDepartment) ? addUserFromMenu : null},
      {label:"Add KPI",        action: canAddKpi(currentDepartment) ? addKpiFromMenu : null},
      {label:"Add Department", action: canManageDept(currentDepartment) ? openAddDeptModal : null},
      {label:"Logout",         action: ()=>window.__fbLogout && window.__fbLogout()},
    ].filter(i => i.action);
    openContextMenu(menuItems, r.right, r.bottom+6);
  }, { once:false });
}
function addKpiFromMenu(){
  if (!canAddKpi(currentDepartment)) {
    alert("Недостаточно прав для добавления KPI.");
    return;
  }
  const board = orgData.departments[currentDepartment].users[currentUser].board;
  board.rows.push({ name:"New KPI", target:">= 0", entries:{} });
  save(); renderTable();
}

function addUserFromMenu(){
  if (!canManageDept(currentDepartment)) {
    alert("Недостаточно прав для добавления пользователя.");
    return;
  }
  openAddUserModal();
}

// ---------- Sidebar (left) ----------
function renderSidebar(){
  const side = document.querySelector(".side");
  if (!side) return;

  const {from, to} = currentDateRange();

  // scope header with toggle buttons
  let scopeBar = side.querySelector(".users-scope");
  if (!scopeBar){
    scopeBar = document.createElement("div");
    scopeBar.className = "users-scope";
    side.prepend(scopeBar);
  }
  scopeBar.innerHTML = `
    <div class="scope-toggle">
      <button class="scopeBtn ${sidebarScope==='dept'?'active':''}" data-scope="dept">This department</button>
      <button class="scopeBtn ${sidebarScope==='all'?'active':''}"  data-scope="all">All departments</button>
    </div>
  `;
  scopeBar.querySelectorAll(".scopeBtn").forEach(b=>{
    b.onclick = ()=>{ sidebarScope = b.dataset.scope; renderSidebar(); };
  });

  // list block
  let list = side.querySelector(".users-summary");
  if (!list){
    list = document.createElement("div");
    list.className = "users-summary";
    scopeBar.insertAdjacentElement("afterend", list);
  }
  list.innerHTML = "";

  if (sidebarScope === "dept"){
    const deptName = currentDepartment;
    const users = Object.keys(orgData.departments[deptName].users);
    users.forEach(name=>{
      list.appendChild(makeSidebarUserItem(deptName, name, from, to));
    });
  } else {
    Object.keys(orgData.departments).forEach(dept=>{
      const groupTitle = document.createElement("div");
      groupTitle.className = "dept-caption";
      groupTitle.textContent = dept;
      list.appendChild(groupTitle);

      const users = Object.keys(orgData.departments[dept].users);
      if (!users.length){
        const empty = document.createElement("div");
        empty.className = "uitem";
        empty.textContent = "No users";
        list.appendChild(empty);
      } else {
        users.forEach(name=>{
          list.appendChild(makeSidebarUserItem(dept, name, from, to));
        });
      }
    });
  }
}
function makeSidebarUserItem(deptName, name, from, to){
  const node = orgData.departments[deptName].users[name];
  const avg = computeUserAverageInDept(deptName, name, from, to);
  const item = document.createElement("div");
  item.className = "uitem" + ((deptName===currentDepartment && name===currentUser) ? " current":"");

  const title = node.title ? ` (${node.title})` : "";
  const f = sideUserFilters[deptName]?.[name] || null;
  const filterLabel = f ? ` • ${f}` : " • All KPIs";
  const header = document.createElement("div");
  header.className = "uitem-head";
  header.innerHTML = `<span class="uname">${name}${title}</span><span class="ufilter">${filterLabel}</span>`;
  item.appendChild(header);

  const barWrap = document.createElement("div");
  barWrap.className = "rag progress mini";
  if (avg == null){
    barWrap.innerHTML = `<div class="bar gray" style="width:0%"></div><span class="pct">No data</span>`;
  } else {
    const color = avg >= 80 ? "green" : (avg >= 60 ? "yellow" : "red");
    barWrap.innerHTML = `<div class="bar ${color}" style="width:${avg}%"></div><span class="pct">${avg}%</span>`;
  }
  item.appendChild(barWrap);

  // LKM — перейти к пользователю (и к департаменту)
  item.addEventListener("click", ()=>{
    currentDepartment = deptName;
    currentUser = name;
    save(); renderUsers(); renderTable(); renderSidebar();
  });

  // PKM — фильтр KPI
  item.addEventListener("contextmenu", (e)=>{
    e.preventDefault();
    const userBoard = node.board;
    const items = [
      {label:"Show: All KPIs", action: ()=>{
        sideUserFilters[deptName] = sideUserFilters[deptName] || {};
        sideUserFilters[deptName][name] = null; renderSidebar();
      }},
      ...userBoard.rows.map(r => ({
        label: `Show: ${r.name}`,
        action: ()=>{
          sideUserFilters[deptName] = sideUserFilters[deptName] || {};
          sideUserFilters[deptName][name] = r.name; renderSidebar();
        }
      }))
    ];
    openContextMenu(items, e.pageX, e.pageY);
  });

  return item;
}

// ---------- Modals root ----------
function ensureModalRoot(){
  let root = document.querySelector("#modal-root");
  if (!root){
    root = document.createElement("div");
    root.id = "modal-root";
    document.body.appendChild(root);
  }
  return root;
}
function closeModal(){
  const root = ensureModalRoot();
  root.classList.remove("active");
  root.innerHTML = "";
  root.style.display = "none";
  document.body.style.overflow = "";
}

// ---------- Modal: Add User (with Position) ----------
function openAddUserModal(){
  const root = ensureModalRoot();
  root.classList.add("active");
  root.style.display = "flex";
  root.innerHTML = `
    <div class="modal-overlay"></div>
    <div class="modal-card" role="dialog" aria-modal="true">
      <div class="modal-header">
        <h3>Add User</h3>
        <button class="modal-x" title="Close">×</button>
      </div>
      <div class="modal-body">
        <label class="modal-label">User name (in "${currentDepartment}")</label>
        <input type="text" class="modal-input" id="modalUserName" placeholder="e.g. John Doe" />
        <label class="modal-label" style="margin-top:10px;">Position</label>
        <input type="text" class="modal-input" id="modalUserTitle" placeholder="e.g. Sales Manager" />
        <p class="modal-hint">Name must be unique inside the department.</p>
        <p class="modal-error" id="modalUserError" style="display:none;"></p>
      </div>
      <div class="modal-actions">
        <button class="btn ghost" id="btnCancel">Cancel</button>
        <button class="btn primary" id="btnAdd">Add</button>
      </div>
    </div>
  `;
  document.body.style.overflow = "hidden";

  const overlay = root.querySelector(".modal-overlay");
  const card    = root.querySelector(".modal-card");
  const input   = root.querySelector("#modalUserName");
  const inputTitle = root.querySelector("#modalUserTitle");
  const err     = root.querySelector("#modalUserError");
  const btnAdd  = root.querySelector("#btnAdd");
  const btnCancel = root.querySelector("#btnCancel");
  const btnX    = root.querySelector(".modal-x");

  const submit = () => {
    const raw = (input.value || "").trim();
    const title = (inputTitle.value || "").trim();
    if (!raw){ showError("Please enter a name"); return; }
    const users = orgData.departments[currentDepartment].users;
    if (users[raw]){ showError("Name already exists in this department"); return; }
    users[raw] = { title, board: defaultBoard() };
    currentUser = raw;
    save(); renderUsers(); renderTable(); renderSidebar();
    closeModal();
  };
  const showError = (msg) => {
    err.textContent = msg; err.style.display = "block";
    card.classList.remove("shake"); requestAnimationFrame(()=> card.classList.add("shake"));
  };

  overlay.addEventListener("click", closeModal);
  btnCancel.addEventListener("click", closeModal);
  btnX.addEventListener("click", closeModal);
  btnAdd.addEventListener("click", submit);
  input.addEventListener("keydown", (e)=>{ if (e.key==="Enter") submit(); if (e.key==="Escape") closeModal(); });
  inputTitle.addEventListener("keydown", (e)=>{ if (e.key==="Enter") submit(); if (e.key==="Escape") closeModal(); });
  setTimeout(()=> input.focus(), 0);
}

// ---------- Modal: Rename User (Name + Position) ----------
function openRenameUserModal(oldName){
  const node = orgData.departments[currentDepartment].users[oldName];
  const oldTitle = node?.title || "";

  const root = ensureModalRoot();
  root.classList.add("active");
  root.style.display = "flex";
  root.innerHTML = `
    <div class="modal-overlay"></div>
    <div class="modal-card" role="dialog" aria-modal="true">
      <div class="modal-header">
        <h3>Rename User</h3>
        <button class="modal-x" title="Close">×</button>
      </div>
      <div class="modal-body">
        <label class="modal-label">Name</label>
        <input type="text" class="modal-input" id="modalRenameName" value="${oldName}" />
        <label class="modal-label" style="margin-top:10px;">Position</label>
        <input type="text" class="modal-input" id="modalRenameTitle" value="${oldTitle}" />
        <p class="modal-error" id="modalRenameError" style="display:none;"></p>
      </div>
      <div class="modal-actions">
        <button class="btn ghost" id="btnCancel">Cancel</button>
        <button class="btn primary" id="btnSave">Save</button>
      </div>
    </div>
  `;
  document.body.style.overflow = "hidden";

  const overlay = root.querySelector(".modal-overlay");
  const card    = root.querySelector(".modal-card");
  const inputName  = root.querySelector("#modalRenameName");
  const inputTitle = root.querySelector("#modalRenameTitle");
  const err     = root.querySelector("#modalRenameError");
  const btnSave = root.querySelector("#btnSave");
  const btnCancel = root.querySelector("#btnCancel");
  const btnX    = root.querySelector(".modal-x");

  const submit = () => {
    const newName = (inputName.value || "").trim();
    const newTitle = (inputTitle.value || "").trim();
    if (!newName){ showError("Please enter a name"); return; }

    const users = orgData.departments[currentDepartment].users;
    if (newName !== oldName && users[newName]){ showError("Name already exists in this department"); return; }

    // if name changed, rebuild keys preserving order
    if (newName !== oldName){
      const ordered = {};
      Object.keys(users).forEach(k=>{
        if (k === oldName) ordered[newName] = { title:newTitle, board: users[oldName].board };
        else ordered[k] = users[k];
      });
      orgData.departments[currentDepartment].users = ordered;
      if (currentUser === oldName) currentUser = newName;
    } else {
      users[oldName].title = newTitle;
    }

    save(); renderUsers(); renderTable(); renderSidebar();
    closeModal();
  };
  const showError = (msg) => {
    err.textContent = msg; err.style.display = "block";
    card.classList.remove("shake"); requestAnimationFrame(()=> card.classList.add("shake"));
  };

  overlay.addEventListener("click", closeModal);
  btnCancel.addEventListener("click", closeModal);
  btnX.addEventListener("click", closeModal);
  btnSave.addEventListener("click", submit);
  [inputName, inputTitle].forEach(inp=>{
    inp.addEventListener("keydown", (e)=>{ if (e.key==="Enter") submit(); if (e.key==="Escape") closeModal(); });
  });
  setTimeout(()=> inputName.focus(), 0);
}

// ---------- Modal: Add Department ----------
function openAddDeptModal(){
  if (!canManageDept(currentDepartment)) {
    alert("Недостаточно прав для добавления департамента.");
    return;
  }

  const root = ensureModalRoot();
  root.classList.add("active");
  root.style.display = "flex";
  root.innerHTML = `
    <div class="modal-overlay"></div>
    <div class="modal-card" role="dialog" aria-modal="true">
      <div class="modal-header">
        <h3>Add Department</h3>
        <button class="modal-x" title="Close">×</button>
      </div>
      <div class="modal-body">
        <label class="modal-label">Department name</label>
        <input type="text" class="modal-input" id="modalDeptName" placeholder="e.g. Sales" />
        <p class="modal-hint">Name must be unique.</p>
        <p class="modal-error" id="modalDeptError" style="display:none;"></p>
      </div>
      <div class="modal-actions">
        <button class="btn ghost" id="btnCancel">Cancel</button>
        <button class="btn primary" id="btnAdd">Add</button>
      </div>
    </div>
  `;
  document.body.style.overflow = "hidden";

  const overlay = root.querySelector(".modal-overlay");
  const card    = root.querySelector(".modal-card");
  const input   = root.querySelector("#modalDeptName");
  const err     = root.querySelector("#modalDeptError");
  const btnAdd  = root.querySelector("#btnAdd");
  const btnCancel = root.querySelector("#btnCancel");
  const btnX    = root.querySelector(".modal-x");

  const submit = () => {
    const raw = (input.value || "").trim();
    if (!raw){ showError("Please enter a department name"); return; }
    if (orgData.departments[raw]){ showError("Department already exists"); return; }
    orgData.departments[raw] = { users: {} };
    if (!Object.keys(orgData.departments[raw].users).length){
      orgData.departments[raw].users["User 1"] = { title:"", board: defaultBoard() };
    }
    currentDepartment = raw;
    currentUser = Object.keys(orgData.departments[raw].users)[0];
    save(); renderUsers(); renderTable(); renderSidebar();
    closeModal();
  };
  const showError = (msg) => {
    err.textContent = msg; err.style.display = "block";
    card.classList.remove("shake"); requestAnimationFrame(()=> card.classList.add("shake"));
  };

  overlay.addEventListener("click", closeModal);
  btnCancel.addEventListener("click", closeModal);
  btnX.addEventListener("click", closeModal);
  btnAdd.addEventListener("click", submit);
  input.addEventListener("keydown", (e)=>{ if (e.key==="Enter") submit(); if (e.key==="Escape") closeModal(); });
  setTimeout(()=> input.focus(), 0);
}

// ---------- Modal: Rename Department ----------
function openRenameDeptModal(initialName){
  if (!canManageDept(currentDepartment)) {
    alert("Недостаточно прав для переименования департамента.");
    return;
  }

  const dept = initialName || currentDepartment;
  const root = ensureModalRoot();
  root.classList.add("active");
  root.style.display = "flex";
  root.innerHTML = `
    <div class="modal-overlay"></div>
    <div class="modal-card" role="dialog" aria-modal="true">
      <div class="modal-header">
        <h3>Rename Department</h3>
        <button class="modal-x" title="Close">×</button>
      </div>
      <div class="modal-body">
        <label class="modal-label">New name for "${dept}"</label>
        <input type="text" class="modal-input" id="modalDeptNewName" value="${dept}" />
        <p class="modal-error" id="modalDeptError" style="display:none;"></p>
      </div>
      <div class="modal-actions">
        <button class="btn ghost" id="btnCancel">Cancel</button>
        <button class="btn primary" id="btnSave">Save</button>
      </div>
    </div>
  `;
  document.body.style.overflow = "hidden";

  const overlay = root.querySelector(".modal-overlay");
  const card    = root.querySelector(".modal-card");
  const input   = root.querySelector("#modalDeptNewName");
  const err     = root.querySelector("#modalDeptError");
  const btnSave = root.querySelector("#btnSave");
  const btnCancel = root.querySelector("#btnCancel");
  const btnX    = root.querySelector(".modal-x");

  const submit = () => {
    const raw = (input.value || "").trim();
    if (!raw){ showError("Please enter a name"); return; }
    if (raw === dept){ closeModal(); return; }
    if (orgData.departments[raw]){ showError("Department already exists"); return; }
    // rename key while preserving order
    const ordered = {};
    for (const k of Object.keys(orgData.departments)){
      if (k === dept) ordered[raw] = orgData.departments[k];
      else ordered[k] = orgData.departments[k];
    }
    orgData.departments = ordered;

    // move filters
    if (sideUserFilters[dept]){
      sideUserFilters[raw] = sideUserFilters[dept];
      delete sideUserFilters[dept];
    }

    if (currentDepartment === dept) currentDepartment = raw;
    save(); renderUsers(); renderTable(); renderSidebar();
    closeModal();
  };
  const showError = (msg) => {
    err.textContent = msg; err.style.display = "block";
    card.classList.remove("shake"); requestAnimationFrame(()=> card.classList.add("shake"));
  };

  overlay.addEventListener("click", closeModal);
  btnCancel.addEventListener("click", closeModal);
  btnX.addEventListener("click", closeModal);
  btnSave.addEventListener("click", submit);
  input.addEventListener("keydown", (e)=>{ if (e.key==="Enter") submit(); if (e.key==="Escape") closeModal(); });
  setTimeout(()=> input.focus(), 0);
}

// ---------- Hide deprecated controls ----------
function hideDeprecatedButtons(){
  ["addWeek","delRow","addUser","addKpi","newUserName"].forEach(id=>{
    const el = document.getElementById(id);
    if (el) el.style.display = "none";
  });
  document.querySelectorAll(".addUser").forEach(el=> el.style.display="none");
}

// ---------- Init ----------
window.addEventListener('fbAuthChanged', async (e) => {
  const user = e.detail;
  if (user) {
    window.hideLoginScreen && window.hideLoginScreen();
    await load();               // загрузили общую доску из Firestore
    await ensureUserFromProfile(); // гарантировали карточку и переключились
    startRealtime();            // подписка на live-обновления
    renderUsers();
    renderTable();
    renderSidebar();
  } else {
    if (typeof __unsubRT === 'function') { __unsubRT(); __unsubRT = null; }
    window.showLoginScreen && window.showLoginScreen();
  }
});

if (window.__FB_AUTH && window.__FB_AUTH.currentUser) {
  window.dispatchEvent(new CustomEvent('fbAuthChanged', { detail: window.__FB_AUTH.currentUser }));
} else {
  window.showLoginScreen && window.showLoginScreen();
}

