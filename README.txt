Val0x04 - Bot Discord (Rust) - untuk hosting di Railway

ARSITEKTUR
Bot ini berperan sebagai WebSocket SERVER. Mod Fabric di server Minecraft (Octavia) yang akan connect KELUAR ke bot ini. Dengan begini, tidak perlu buka port tambahan di server Minecraft (Octavia) sama sekali, karena Octavia hanya perlu bisa melakukan koneksi keluar (outbound), yang selalu diizinkan oleh hosting mana pun.

Railway akan otomatis kasih domain publik untuk bot ini (contoh: nama-app-lo.railway.app) dan menangani TLS/HTTPS secara otomatis. Bot cukup listen di protokol WebSocket biasa (ws://) pada port yang Railway tentukan lewat environment variable PORT; Railway sendiri yang membungkusnya jadi wss:// ke publik.

DEPLOY KE RAILWAY
1. Push folder ini ke repository GitHub (atau upload manual sesuai opsi yang Railway sediakan).
2. Di Railway, buat project baru dari repo ini. Railway akan otomatis mendeteksi Dockerfile dan build dari situ.
3. Di tab Settings > Networking, klik Generate Domain untuk dapat domain publik (misal nama-app-lo.railway.app).
4. Set environment variables di tab Variables (lihat daftar di bawah).
5. Deploy. Setelah selesai, bot langsung berjalan dan mendengarkan koneksi WebSocket di domain itu.

ENVIRONMENT VARIABLES (set di Railway, tab Variables)

DISCORD_TOKEN
Token bot Discord dari Discord Developer Portal.

DISCORD_CHANNEL_ID
ID channel Discord tempat chat bridge berjalan.

BRIDGE_WEBSOCKET_AUTH_TOKEN
Token bebas yang lo tentukan sendiri (boleh string acak apa saja, semakin panjang semakin aman). Nilai ini HARUS SAMA PERSIS dengan yang nanti diisi di config mod Fabric (config/discordbridge.properties, field websocket-auth-token). Karena sekarang bot yang menentukan token (bukan mod yang generate otomatis), edit dulu file config mod itu dan ganti nilai websocket-auth-token dengan token yang sama seperti di sini.

PORT
TIDAK PERLU DI-SET MANUAL. Railway otomatis mengisi variable ini sendiri. Bot membaca env var ini untuk tahu di port mana harus listen.

CARA MOD FABRIC CONNECT KE BOT INI
Di config/discordbridge.properties milik mod, set:
websocket-url=wss://nama-app-lo.railway.app
websocket-auth-token=(harus sama persis dengan BRIDGE_WEBSOCKET_AUTH_TOKEN di Railway)

Ganti nama-app-lo.railway.app dengan domain asli yang di-generate Railway untuk project ini.

IZIN BOT DISCORD
Di Discord Developer Portal, aktifkan Privileged Gateway Intents berikut:
	SERVER MEMBERS INTENT
	MESSAGE CONTENT INTENT

PERILAKU BOT
Bot menerima koneksi WebSocket dari mod Fabric dan memvalidasi header X-Auth-Token saat handshake; kalau tidak cocok, koneksi ditolak dengan status 401.
Kalau mod terputus, bot tetap berjalan menunggu koneksi baru (tidak perlu restart bot).
Pesan chat biasa dari mod ditampilkan sebagai teks biasa di Discord. Semua event lain (join, leave, death, advancement, bridge status, server start/stop) ditampilkan sebagai embed dengan emoji sesuai konteks.
Pesan dari Discord ke Minecraft menyertakan role tertinggi pengirim (kalau ada), diambil dari cache Discord.
