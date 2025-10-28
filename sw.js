// sw.js — PWA cache (GET only)
// ВАЖНО: кэшируем только статику и HTML. Никаких POST/Firestore в кэше.

const CACHE_NAME = 'kpi-cache-v5';

// Что положить в предзагрузку (по нужде дополни своими файлами/иконками)
const PRECACHE = [
  './',
  './index.html',
  './styles.css',
  './app.js',
  './manifest.webmanifest',
  // './icons/icon-192.png',
  // './icons/icon-512.png',
];

self.addEventListener('install', (event) => {
  // Сразу активируем новую версию
  self.skipWaiting();
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) =>
      cache.addAll(
        PRECACHE.filter(Boolean).map(
          // просим не лезть в HTTP-кэш браузера
          (url) => new Request(url, { cache: 'no-cache', credentials: 'same-origin' })
        )
      )
    )
  );
});

self.addEventListener('activate', (event) => {
  // Чистим старые версии
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.map((k) => (k === CACHE_NAME ? null : caches.delete(k))))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const req = event.request;

  // 1) Никогда не кэшируем не-GET (POST/PUT/PATCH/DELETE) — сразу в сеть
  if (req.method !== 'GET') {
    return; // не ставим respondWith => пройдёт по умолчанию в fetch()
  }

  const url = new URL(req.url);

  // 2) Никогда не перехватываем Firestore/внешние каналы — они часто long-polling
  const isFirestore = url.origin === 'https://firestore.googleapis.com';
  if (isFirestore) {
    return; // пропускаем в сеть без кэширования
  }

  // 3) Для HTML (навигация) — стратегия "сеть в приоритете, кэш — запасной"
  const isHTML =
    req.mode === 'navigate' ||
    (req.destination === 'document') ||
    (req.headers.get('accept') || '').includes('text/html');

  if (isHTML) {
    event.respondWith(
      fetch(req)
        .then((resp) => {
          // Только GET и только успешные ответы кладём в кэш
          const respClone = resp.clone();
          caches.open(CACHE_NAME).then((cache) => {
            cache.put(req, respClone).catch(() => {});
          });
          return resp;
        })
        .catch(async () => {
          const cache = await caches.open(CACHE_NAME);
          const cached = await cache.match('./'); // запасной — корень/предкэш
          return cached || new Response('Offline', { status: 503, statusText: 'Offline' });
        })
    );
    return;
  }

  // 4) Для статических GET того же источника — "кэш в приоритете, сеть — для обновления"
  const isSameOrigin = url.origin === self.location.origin;

  if (isSameOrigin) {
    event.respondWith(
      caches.match(req).then((cached) => {
        const networkFetch = fetch(req)
          .then((resp) => {
            // Кладём только 200/OK и типы, которые разумно кэшировать
            if (resp && resp.status === 200) {
              const clone = resp.clone();
              caches.open(CACHE_NAME).then((cache) => {
                cache.put(req, clone).catch(() => {});
              });
            }
            return resp;
          })
          .catch(() => cached); // если сеть упала — отдаём кэш, если он есть

        // Если в кэше есть — отдаём сразу; параллельно обновляем из сети
        return cached || networkFetch;
      })
    );
    return;
  }

  // 5) Для чужих GET (CDN/gstatic и т.д.) — просто сеть (можно расширить при желании)
  // Хотим — можно сделать "stale-while-revalidate" аналогично пункту (4).
  // По умолчанию: не перехватываем.
});

