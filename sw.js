// sw.js — PWA cache with network-first for navigations
const CACHE_NAME = "kpi-board-v3"; // ← повышай версию при любых правках
const STATIC_ASSETS = [
  "./",               // важно для GitHub Pages
  "./index.html",
  "./styles.css",
  "./app.js",
  "./manifest.webmanifest",
  // добавь сюда свои иконки, если они лежат в /icons
  // "./icons/icon-192.png",
  // "./icons/icon-512.png",
];

// Установка: кладём статические файлы
self.addEventListener("install", (evt) => {
  evt.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(STATIC_ASSETS))
  );
  self.skipWaiting();
});

// Активация: чистим старые кеши и захватываем клиентов
self.addEventListener("activate", (evt) => {
  evt.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.map((k) => (k === CACHE_NAME ? null : caches.delete(k))))
    )
  );
  self.clients.claim();
});

// Универсальный fetch:
// - Для навигации (HTML, Ctrl+R) -> NETWORK-FIRST с фолбэком на кэш index.html
// - Для статики (css/js/иконки)   -> CACHE-FIRST с фолбэком в сеть
self.addEventListener("fetch", (evt) => {
  const req = evt.request;

  // 1) Навигация/HTML: network-first
  if (req.mode === "navigate" || (req.destination === "" && req.headers.get("accept")?.includes("text/html"))) {
    evt.respondWith(
      (async () => {
        try {
          // Пробуем сеть
          const fresh = await fetch(req);
          // Обновим кэш копией ответа
          const cache = await caches.open(CACHE_NAME);
          cache.put("./index.html", fresh.clone());
          return fresh;
        } catch (e) {
          // Оффлайн/ошибка — отдаём закэшированный index.html
          const cached = await caches.match("./index.html");
          if (cached) return cached;
          // как крайний случай — пустой ответ
          return new Response("Offline", { status: 503, statusText: "Offline" });
        }
      })()
    );
    return;
  }

  // 2) Остальное (css/js/img/manifest): cache-first
  evt.respondWith(
    (async () => {
      const cached = await caches.match(req);
      if (cached) return cached;

      try {
        const fresh = await fetch(req);
        const cache = await caches.open(CACHE_NAME);
        cache.put(req, fresh.clone());
        return fresh;
      } catch (e) {
        // нет в кэше и сеть недоступна
        return new Response("", { status: 504, statusText: "Gateway Timeout" });
      }
    })()
  );
});
