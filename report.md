# Laporan UAS Sistem Terdistribusi

### Nama: Daniel Belawa Koten
### NIM: 11231019

## 1. Bagian Teori 

### T1 - Bab 1: Karakteristik Sistem Terdistribusi dan Trade-off Pub-Sub Aggregator

Sistem terdistribusi adalah sistem yang komponennya berjalan pada proses atau node berbeda, berkomunikasi melalui jaringan, dan tetap terlihat sebagai satu sistem bagi pengguna. Karakteristik pentingnya adalah konkurensi, tidak adanya clock global yang benar-benar sempurna, kemungkinan kegagalan parsial, heterogenitas komponen, serta kebutuhan koordinasi antar proses. Pada project ini, karakteristik tersebut muncul melalui pemisahan `publisher` dan `aggregator` sebagai service berbeda dalam Docker Compose. Publisher mengirim event lewat HTTP, sedangkan aggregator menerima event, memasukkannya ke antrean, lalu worker paralel memproses data ke SQLite.

Trade-off desain Pub-Sub aggregator adalah antara decoupling dan kompleksitas konsistensi. Publisher menjadi sederhana karena hanya perlu mengirim event ke endpoint `POST /publish` tanpa mengetahui jumlah worker atau detail database. Namun, aggregator harus menangani masalah khas sistem terdistribusi seperti event duplikat, retry, event yang datang tidak berurutan, dan worker yang berjalan bersamaan. Karena itu, sistem memakai idempotent consumer, dedup store persisten, dan transaksi database. Dengan desain ini, sistem menerima kenyataan bahwa delivery tidak selalu sempurna, tetapi menjaga hasil akhir tetap benar melalui mekanisme deduplication (Coulouris dkk., 2012).

### T2 - Bab 2: Kapan Memilih Publish-Subscribe Dibanding Client-Server

Arsitektur publish-subscribe cocok dipilih ketika pengirim data tidak perlu mengetahui siapa penerima data, berapa banyak consumer yang aktif, atau bagaimana data diproses setelah diterima. Pada log aggregator, publisher hanya bertugas menghasilkan event log. Setelah event dikirim, aggregator yang bertanggung jawab melakukan validasi, antrean, deduplication, statistik, audit log, dan penyimpanan. Pola ini lebih fleksibel daripada client-server biasa karena aliran log biasanya bersifat terus-menerus, dapat dikirim dalam batch, dan tidak selalu membutuhkan respons hasil pemrosesan secara langsung.

Client-server lebih cocok untuk interaksi sinkron, misalnya client meminta profil pengguna dan server langsung mengembalikan hasilnya. Sebaliknya, publish-subscribe lebih cocok ketika sistem membutuhkan decoupling dan pemrosesan asynchronous. Pada project ini, `POST /publish` hanya mengembalikan bahwa event diterima, sedangkan pemrosesan akhir dilakukan oleh worker di belakang layar. Trade-off-nya adalah sistem harus siap menghadapi eventual consistency, retry, dan duplikasi. Karena itu, Pub-Sub perlu dipasangkan dengan idempotency dan deduplication agar hasil akhir tetap konsisten walaupun event dikirim lebih dari sekali (Coulouris dkk., 2012).

### T3 - Bab 3: At-Least-Once, Exactly-Once Delivery, dan Idempotent Consumer

At-least-once delivery berarti event dijamin dikirim minimal satu kali, tetapi bisa saja terkirim lebih dari satu kali. Model ini umum dipakai karena lebih realistis dalam sistem terdistribusi. Jika terjadi timeout, koneksi putus, atau respons server tidak diterima publisher, publisher dapat melakukan retry. Akibatnya, event yang sama mungkin masuk ke aggregator lebih dari sekali. Exactly-once delivery terlihat ideal karena seolah-olah event selalu dikirim dan diproses tepat satu kali. Namun, exactly-once secara end-to-end sulit dicapai karena ada banyak titik kegagalan, misalnya request berhasil diterima tetapi respons gagal sampai ke publisher.

Project ini memilih at-least-once delivery dan membuat consumer idempotent. Artinya, event yang sama boleh diterima ulang, tetapi efek pemrosesannya hanya terjadi sekali. Implementasinya memakai pasangan `(topic, event_id)` sebagai identitas event. Worker mencoba menyimpan event dengan `INSERT OR IGNORE` pada tabel `processed_events`. Jika event sudah ada, sistem tidak memproses ulang event tersebut dan hanya menaikkan `duplicate_dropped`. Dengan cara ini, sistem tidak harus menjamin jaringan bebas duplikasi, tetapi tetap menghasilkan efek akhir yang konsisten (Coulouris dkk., 2012).

### T4 - Bab 4: Skema Penamaan Topic dan Event ID untuk Deduplication

Penamaan penting dalam sistem terdistribusi karena setiap entitas harus dapat dikenali secara stabil dan tidak ambigu. Pada project ini, identitas event tidak hanya memakai `event_id`, tetapi pasangan `(topic, event_id)`. `topic` menunjukkan kategori log, misalnya `app`, `auth`, `payment`, atau `system`. `event_id` menunjukkan identitas unik event yang dibuat oleh publisher. Dengan kombinasi ini, event dengan `event_id` yang sama masih dapat dianggap berbeda jika berada pada topic berbeda.

Skema ini dibuat collision-resistant untuk kebutuhan tugas. Publisher membentuk `event_id` dari kombinasi source, urutan event, dan potongan UUID, misalnya `publisher-1-123-abcdef123456`. Bagian source membantu mengetahui asal event, urutan membantu melacak event dari publisher yang sama, dan UUID mengurangi risiko tabrakan nama. Pada storage, uniqueness dijaga oleh constraint `UNIQUE(topic, event_id)`. Jadi, walaupun dua worker memproses event yang sama secara bersamaan, database tetap menolak duplikasi. Untuk sistem produksi, event id bisa diperkuat dengan UUID penuh atau ULID. Namun, untuk project ini, kombinasi topic dan event id sudah cukup kuat untuk mendukung deduplication yang aman (Coulouris dkk., 2012).

### T5 - Bab 5: Ordering Praktis dengan Timestamp dan Monotonic Counter

Ordering dalam sistem terdistribusi tidak sederhana karena setiap node dapat memiliki clock berbeda dan event bisa mengalami delay jaringan. Jika aggregator memaksa total ordering global, sistem akan menjadi lebih kompleks dan mahal. Pada project ini, total ordering global tidak diterapkan karena deduplication tidak bergantung pada urutan kedatangan event. Correctness hanya bergantung pada identitas `(topic, event_id)`. Event tetap aman diproses walaupun datang out-of-order.

Sebagai strategi praktis, event menyimpan `timestamp` dalam format ISO8601 dan publisher menambahkan `monotonic_counter` pada payload. Timestamp membantu observasi waktu kejadian, sedangkan monotonic counter membantu mengurutkan event dari source yang sama. Batasannya, timestamp bisa terkena clock skew jika beberapa mesin memiliki jam berbeda. Monotonic counter juga hanya bermakna pada satu source, bukan secara global. Dampaknya, laporan event dapat diurutkan secara praktis untuk analisis, tetapi sistem tidak menjanjikan urutan absolut seluruh event dari semua publisher. Desain ini sesuai dengan prinsip bahwa ordering dalam sistem terdistribusi sebaiknya disesuaikan dengan kebutuhan aplikasi, bukan selalu dipaksakan secara global (Coulouris dkk., 2012).

### T6 - Bab 6: Failure Modes dan Mitigasinya

Failure modes utama pada project ini adalah duplikasi event, request timeout, retry publisher, database lock, worker error, dan restart container. Duplikasi event terjadi karena model at-least-once delivery mengizinkan publisher mengirim event yang sama lebih dari sekali. Mitigasinya adalah idempotent consumer dan dedup store persisten. Request timeout dimitigasi dengan retry dan exponential backoff pada publisher. Backoff penting agar publisher tidak langsung membanjiri aggregator ketika terjadi gangguan sementara.

Pada sisi database, SQLite dapat mengalami kondisi `database is locked` ketika beberapa worker melakukan write bersamaan. Mitigasinya adalah `PRAGMA busy_timeout=5000`, transaksi `BEGIN IMMEDIATE`, dan retry singkat pada error locked. Untuk restart container, mitigasinya adalah named volume `aggregator_data` yang menyimpan file SQLite di `/var/lib/aggregator/aggregator.db`. Dengan begitu, event yang sudah diproses tetap dikenali setelah container dibuat ulang. Batasan sistem adalah antrean masih in-memory. Jika aggregator mati setelah event diterima tetapi sebelum worker memprosesnya, event di antrean bisa hilang. Untuk produksi, antrean dapat diganti dengan broker durable seperti Redis Streams, NATS JetStream, atau tabel inbox (Coulouris dkk., 2012).

### T7 - Bab 7: Eventual Consistency, Idempotency, dan Deduplication

Aggregator menerapkan pemrosesan asynchronous. Ketika endpoint `POST /publish` mengembalikan respons sukses, artinya event sudah diterima dan dimasukkan ke antrean, tetapi belum tentu langsung selesai diproses oleh worker. Karena itu, jika client langsung memanggil `GET /events`, event baru mungkin belum muncul. Setelah worker selesai memproses antrean, data pada endpoint read menjadi konsisten. Pola ini menunjukkan eventual consistency dalam skala kecil.

Idempotency dan deduplication membuat eventual consistency tetap aman. Selama jeda antara event diterima dan event selesai diproses, publisher mungkin melakukan retry karena mengira pengiriman gagal. Jika sistem tidak idempotent, retry dapat membuat event tersimpan berkali-kali. Pada project ini, setiap worker mencoba insert ke tabel `processed_events` dengan constraint `UNIQUE(topic, event_id)`. Jika event sudah pernah diproses, percobaan berikutnya dihitung sebagai duplikat. Endpoint `/admin/drain` disediakan untuk demo dan test agar sistem menunggu antrean kosong sebelum membaca hasil final. Dengan desain ini, read mungkin tertunda, tetapi hasil akhirnya tetap benar setelah semua worker selesai (Coulouris dkk., 2012).

### T8 - Bab 8: Desain Transaksi, ACID, Isolation Level, dan Lost Update

Desain transaksi menjadi bagian paling penting pada project ini. Setiap event diproses dalam satu transaction boundary. Di SQLite, worker menjalankan `BEGIN IMMEDIATE`, lalu melakukan `INSERT OR IGNORE` ke `processed_events`, memperbarui statistik, menulis audit log, dan melakukan `COMMIT`. Atomicity memastikan perubahan tidak masuk setengah-setengah. Consistency dijaga oleh constraint `UNIQUE(topic, event_id)`. Isolation diperkuat oleh `BEGIN IMMEDIATE`, karena SQLite mengambil write lock sejak awal transaksi. Durability didukung oleh file SQLite yang disimpan pada Docker named volume.

Strategi menghindari lost update dilakukan dengan increment langsung di database:

```sql
UPDATE stats SET count = count + 1 WHERE name = ?
```

Sistem tidak membaca nilai counter ke aplikasi lalu menulis ulang nilai baru. Jika banyak worker menaikkan counter bersamaan, database yang mengatur serialisasi update. Pada SQLite, isolation praktisnya cocok untuk single-writer transactional storage. Jika memakai PostgreSQL, rancangan setara dapat menggunakan `READ COMMITTED` dengan `INSERT ... ON CONFLICT DO NOTHING`, karena konflik deduplication tetap diselesaikan secara atomik oleh unique constraint (Coulouris dkk., 2012).

### T9 - Bab 9: Kontrol Konkurensi dengan Locking, Unique Constraints, Upsert, dan Idempotent Write

Kontrol konkurensi pada project ini menggabungkan locking database, unique constraint, retry, dan pola idempotent write. Worker berjalan paralel dan bisa saja memproses event dengan `(topic, event_id)` yang sama pada waktu hampir bersamaan. Jika sistem memakai pola read-then-write, dua worker dapat sama-sama membaca bahwa event belum ada, lalu keduanya mencoba menyimpan event. Pola itu rawan race condition.

Project ini menghindarinya dengan langsung mencoba write secara atomik. Worker memakai `INSERT OR IGNORE` ke tabel yang memiliki `UNIQUE(topic, event_id)`. Jika insert berhasil, event dihitung sebagai `unique_processed`. Jika insert diabaikan karena constraint sudah terisi, event dihitung sebagai `duplicate_dropped`. Ini adalah pola idempotent write: worker tidak perlu memastikan lebih dulu apakah event ada; database yang menentukan hasilnya. Pada PostgreSQL, pola yang sama dapat ditulis sebagai `INSERT ... ON CONFLICT DO NOTHING`. Uji konkurensi membuktikan 40 thread yang memproses event sama hanya menghasilkan 1 event unik dan 39 duplikat. Dengan demikian, correctness tidak bergantung pada urutan thread, tetapi pada constraint dan transaksi database (Coulouris dkk., 2012).

### T10 - Bab 10-13: Compose, Keamanan Lokal, Persistensi Volume, dan Observability

Bab 10-13 terlihat pada aspek keamanan, penyimpanan, sistem web, dan koordinasi. Docker Compose digunakan untuk mengorkestrasi service `aggregator` dan `publisher` dalam network lokal `uas_net`. Publisher mengakses aggregator melalui hostname internal `aggregator`, bukan layanan eksternal publik. Port yang diekspos ke host hanya `8080` untuk demo lokal. Dari sisi keamanan, desain ini membatasi komunikasi pada jaringan Compose dan tidak membuka storage ke luar.

Persistensi menggunakan named volume `aggregator_data`, yang menyimpan SQLite pada path `/var/lib/aggregator/aggregator.db`. Karena data berada di volume, dedup store tetap ada meskipun container aggregator dihapus dan dibuat ulang. Dari sisi sistem berbasis web, FastAPI menyediakan endpoint `POST /publish`, `GET /events`, `GET /stats`, `GET /health`, dan `POST /admin/drain`. Dari sisi koordinasi, Compose mengatur healthcheck, environment variable, network, dan volume, sedangkan aggregator mengatur worker melalui antrean internal. Observability disediakan melalui log worker dan endpoint `/stats`, yang menampilkan `received`, `unique_processed`, `duplicate_dropped`, `topics`, `uptime`, `worker_count`, dan `queue_size` (Coulouris dkk., 2012).

## 2. Identitas dan Ringkasan Sistem

Project ini adalah implementasi **Pub-Sub Log Aggregator Terdistribusi** menggunakan Python, FastAPI, worker paralel, Docker Compose, dan SQLite sebagai penyimpanan persisten. Sistem menerima event log dari publisher melalui endpoint `POST /publish`, memasukkan event ke antrean internal, lalu beberapa worker consumer memproses event secara paralel. Event unik disimpan ke tabel `processed_events`, sedangkan event duplikat dibuang secara idempotent berdasarkan pasangan `(topic, event_id)`.

Model pengiriman yang dipilih adalah **at-least-once delivery**. Artinya, publisher boleh mengirim event yang sama lebih dari satu kali, terutama ketika terjadi retry, timeout, atau kegagalan sementara. Karena itu, correctness tidak bergantung pada asumsi bahwa jaringan selalu mengirim tepat satu kali. Correctness diletakkan di sisi consumer melalui idempotency, deduplication, dan transaksi database. Pendekatan ini sesuai dengan prinsip sistem terdistribusi, yaitu sistem harus dirancang untuk menghadapi kegagalan parsial, konkurensi, dan komunikasi yang tidak selalu sempurna (Coulouris dkk., 2012).

Komponen utama sistem:

| Komponen | Peran |
|---|---|
| `aggregator` | Service FastAPI yang menerima event, menjalankan worker, menyediakan endpoint observability, dan menyimpan data ke SQLite. |
| `publisher` | Simulator pengirim event yang menghasilkan event unik dan duplikat dengan rasio tertentu. |
| `worker consumer` | Thread paralel di dalam aggregator yang memproses antrean event. |
| `SQLite dedup store` | Penyimpanan persisten untuk event unik, statistik, dan audit log. |
| `Docker Compose` | Orkestrasi service, jaringan lokal, dan named volume. |



## 3. Arsitektur Sistem

Arsitektur sistem dapat diringkas sebagai berikut:

```text
publisher
   |
   | HTTP POST /publish
   v
aggregator API
   |
   | in-memory queue
   v
multi-worker consumer
   |
   | transactional write
   v
SQLite persistent dedup store
```

Alur kerja sistem:

1. Publisher membuat event sesuai model JSON yang ditentukan.
2. Publisher mengirim event ke aggregator melalui HTTP.
3. Aggregator memvalidasi schema event menggunakan model FastAPI/Pydantic.
4. Jika valid, aggregator menaikkan counter `received` dan memasukkan event ke antrean.
5. Worker mengambil event dari antrean secara paralel.
6. Worker mencoba menyimpan event dengan `INSERT OR IGNORE` pada tabel yang memiliki `UNIQUE(topic, event_id)`.
7. Jika insert berhasil, event dihitung sebagai `unique_processed`.
8. Jika insert diabaikan karena sudah ada, event dihitung sebagai `duplicate_dropped`.
9. Semua keputusan pemrosesan dicatat juga di tabel `audit_log`.

Endpoint yang disediakan:

| Endpoint | Fungsi |
|---|---|
| `GET /health` | Mengecek status hidup service. |
| `POST /publish` | Menerima satu event atau batch event. |
| `GET /events?topic=...&limit=...` | Menampilkan event unik yang sudah diproses. |
| `GET /stats` | Menampilkan metrik `received`, `unique_processed`, `duplicate_dropped`, `topics`, `uptime`, `worker_count`, dan `queue_size`. |
| `POST /admin/drain` | Menunggu antrean selesai diproses, berguna untuk test dan demo. |

Dalam Docker Compose, `publisher` berkomunikasi ke aggregator memakai alamat internal `http://aggregator:8080/publish`. Host hanya mengakses aggregator melalui `localhost:8080`. Dengan begitu, sistem tetap berjalan dalam jaringan lokal Compose dan tidak bergantung pada layanan eksternal publik. Penyimpanan SQLite ditempatkan di named volume `aggregator_data` pada path container `/var/lib/aggregator/aggregator.db`.



## 4. Model Event dan API

Model event minimal yang dipakai:

```json
{
  "topic": "app",
  "event_id": "publisher-1-123-abcdef123456",
  "timestamp": "2026-06-17T22:00:00+08:00",
  "source": "publisher-1",
  "payload": {
    "level": "INFO",
    "message": "log event 123",
    "monotonic_counter": 123
  }
}
```

Makna field:

| Field | Keterangan |
|---|---|
| `topic` | Kategori event, misalnya `app`, `auth`, `payment`, atau `system`. |
| `event_id` | Identitas unik event dari publisher. |
| `timestamp` | Waktu event dibuat dalam format ISO8601. |
| `source` | Nama sumber event, misalnya `publisher-1`. |
| `payload` | Isi log yang fleksibel, tetapi harus berbentuk object. |

Validasi schema dilakukan sebelum event masuk antrean. Jika batch berisi item tidak valid, request ditolak oleh validasi API dan counter `received` tidak bertambah. Hal ini dibuktikan oleh test `test_batch_with_invalid_item_is_rejected_atomically_by_validation`. Dengan kebijakan ini, sistem menghindari kondisi sebagian batch diterima dan sebagian gagal pada tahap validasi.

 
## 5. Keputusan Desain

### 5.1 Idempotency

Idempotency berarti operasi yang sama dapat dijalankan lebih dari sekali tanpa mengubah hasil akhir setelah eksekusi pertama. Pada sistem ini, idempotency diterapkan di consumer. Jika event dengan `(topic, event_id)` yang sama diterima berulang kali, hanya percobaan pertama yang menghasilkan event unik. Percobaan berikutnya tetap dicatat sebagai penerimaan event, tetapi tidak menghasilkan baris baru di `processed_events`.

Contoh perilaku:

| Percobaan | `(topic, event_id)` | Hasil |
|---|---|---|
| Pertama | `("app", "event-001")` | Diproses sebagai event unik. |
| Kedua | `("app", "event-001")` | Dihitung sebagai duplikat. |
| Ketiga | `("app", "event-001")` | Dihitung sebagai duplikat. |

Keputusan ini penting karena publisher memakai model at-least-once. Jika request gagal atau timeout, publisher boleh retry. Tanpa idempotent consumer, retry dapat menyebabkan efek samping ganda, misalnya statistik salah, event tersimpan berkali-kali, atau audit log tidak mewakili kondisi sebenarnya. Dengan idempotency, sistem tetap benar meskipun event masuk lebih dari sekali (Coulouris dkk., 2012).





### 5.2 Dedup Store

Dedup store adalah tempat sistem menyimpan identitas event yang sudah pernah diproses. Project ini memakai SQLite dengan tabel `processed_events`. Constraint utama:

```sql
UNIQUE(topic, event_id)
```

Worker tidak melakukan pola "cek dulu lalu insert". Pola seperti itu rawan race condition karena dua worker dapat membaca bahwa event belum ada pada waktu yang hampir sama, lalu keduanya mencoba insert. Sebagai gantinya, sistem langsung melakukan operasi atomik:

```sql
INSERT OR IGNORE INTO processed_events
    (topic, event_id, timestamp, source, payload, payload_hash, worker_id)
VALUES (?, ?, ?, ?, ?, ?, ?)
```

Jika `rowcount == 1`, event berarti baru dan unik. Jika `rowcount == 0`, event berarti sudah pernah diproses. Dengan meletakkan keputusan deduplication pada database, sistem memanfaatkan constraint yang lebih kuat daripada pengecekan manual di kode aplikasi.

SQLite disimpan pada named volume Docker:

```text
aggregator_data:/var/lib/aggregator/aggregator.db
```

Karena berada di volume, dedup store tetap ada meskipun container aggregator dihapus dan dibuat ulang, selama volume tidak ikut dihapus. Ini mendukung crash recovery pada sisi deduplication.




### 5.3 Transaksi dan Konkurensi

Pemrosesan event dilakukan dalam transaksi SQLite. Bagian pentingnya:

```sql
BEGIN IMMEDIATE;
INSERT OR IGNORE INTO processed_events ...;
UPDATE stats SET count = count + 1 WHERE name = ?;
INSERT INTO audit_log ...;
COMMIT;
```

`BEGIN IMMEDIATE` dipilih agar SQLite mengambil write lock sejak awal transaksi. Karena SQLite hanya mengizinkan satu writer aktif pada satu waktu, strategi ini membuat konflik writer dikelola secara eksplisit oleh database. Sistem juga mengaktifkan `PRAGMA busy_timeout=5000` dan retry singkat ketika terjadi error `database is locked`. Dengan demikian, worker paralel tetap dapat berjalan tanpa menghasilkan double processing.

Statistik diperbarui dengan operasi increment langsung:

```sql
UPDATE stats SET count = count + 1 WHERE name = ?
```

Strategi ini menghindari lost update. Sistem tidak membaca nilai counter ke aplikasi lalu menulis ulang nilai baru. Jika dua worker memperbarui counter dalam waktu berdekatan, database akan menserialisasi update tersebut.

Untuk database server seperti PostgreSQL, desain yang setara dapat memakai `READ COMMITTED` dengan:

```sql
INSERT ... ON CONFLICT DO NOTHING
```

Pada project ini, SQLite dengan `BEGIN IMMEDIATE`, unique constraint, dan retry sudah cukup karena targetnya adalah demo lokal, correctness, dan pengujian konkurensi.




### 5.4 Ordering

Sistem tidak menerapkan total ordering global. Alasannya, total ordering mahal dan tidak diperlukan untuk deduplication. Event log dapat berasal dari banyak source, memiliki delay jaringan berbeda, dan bisa datang out-of-order. Correctness sistem hanya bergantung pada identitas `(topic, event_id)`, bukan urutan kedatangan.

Walaupun total ordering tidak dipaksakan, sistem tetap menyimpan informasi ordering praktis:

| Mekanisme | Fungsi | Batasan |
|---|---|---|
| `timestamp` | Menunjukkan waktu event dibuat. | Bisa terkena clock skew antar mesin. |
| `monotonic_counter` pada payload | Mengurutkan event dari source yang sama. | Hanya bermakna pada satu source. |
| `id` autoincrement di database | Menunjukkan urutan event unik diproses oleh storage. | Bukan urutan kejadian sebenarnya di seluruh sistem. |

Dengan desain ini, analisis log masih bisa dilakukan berdasarkan timestamp atau counter per source, tetapi sistem tidak menjanjikan urutan global yang mutlak. Ini sesuai dengan prinsip sistem terdistribusi bahwa ordering harus dipilih sesuai kebutuhan, bukan selalu dipaksakan secara global (Coulouris dkk., 2012).




### 5.5 Retry dan Reliability

Publisher memiliki retry dengan exponential backoff sederhana. Jika pengiriman batch gagal karena timeout atau error HTTP, publisher mencoba ulang sampai empat kali. Delay awal adalah 0,25 detik dan bertambah dua kali lipat pada percobaan berikutnya.

Ringkasan reliability:

| Risiko | Mitigasi |
|---|---|
| Request timeout | Publisher retry dengan backoff. |
| Event terkirim lebih dari sekali | Consumer idempotent dan dedup store persisten. |
| Worker paralel memproses event sama | Unique constraint dan transaksi database. |
| SQLite lock sementara | `busy_timeout` dan retry pada error `locked`. |
| Container aggregator dibuat ulang | Database disimpan pada named volume. |
| Event sudah diterima tetapi belum diproses lalu proses mati | Batasan sistem karena antrean masih in-memory. |

Batasan paling penting adalah antrean aggregator masih in-memory. Jika aggregator mati setelah endpoint `/publish` menerima event tetapi sebelum worker memproses event tersebut, event di antrean dapat hilang. Untuk versi produksi, antrean bisa dipindahkan ke broker durable seperti Redis Streams, NATS JetStream, PostgreSQL inbox table, atau message broker lain. Namun untuk scope tugas ini, fokus utama yaitu idempotency, deduplication persisten, transaksi, dan konkurensi sudah terpenuhi.




## 6. Analisis Performa dan Metrik

Benchmark lokal dilakukan pada 17 Juni 2026 dengan konfigurasi:

| Parameter | Nilai |
|---|---:|
| Total event | 20.000 |
| Duplicate rate target | 30% |
| Event unik target | 14.000 |
| Event duplikat target | 6.000 |
| Worker | 6 worker pada benchmark lokal |
| Storage | SQLite |
| Mode uji | TestClient lokal |

Hasil benchmark:

| Metrik | Nilai |
|---|---:|
| Total diterima (`received`) | 20.000 |
| Event unik (`unique_processed`) | 14.000 |
| Duplikat dibuang (`duplicate_dropped`) | 6.000 |
| Duplicate rate aktual | 30% |
| Elapsed time | 94,959 detik |
| Throughput | 210,62 event/detik |
| Latency rata-rata | 4,748 ms/event |

Interpretasi hasil:

1. Sistem memenuhi requirement minimal 20.000 event.
2. Rasio duplikat memenuhi requirement minimal 30%.
3. Jumlah event unik dan duplikat konsisten dengan total event yang diterima.
4. Throughput dipengaruhi oleh SQLite, jumlah worker, mode logging, spesifikasi laptop, dan apakah sistem berjalan langsung atau melalui Docker.
5. SQLite memberikan correctness yang baik untuk tugas ini, tetapi throughput writer dibatasi karena SQLite menserialisasi transaksi tulis.

Metrik yang paling penting untuk correctness bukan hanya throughput, tetapi konsistensi antara `received`, `unique_processed`, dan `duplicate_dropped`. Untuk benchmark 20.000 event, relasinya adalah:

```text
received = unique_processed + duplicate_dropped
20.000  = 14.000 + 6.000
```

Relasi tersebut menunjukkan bahwa tidak ada event yang hilang pada proses deduplication setelah event berhasil masuk ke antrean dan worker menyelesaikan pemrosesan.





## 7. Hasil Uji Konkurensi

Uji konkurensi utama adalah `test_database_dedup_is_safe_under_concurrent_workers`. Test ini membuat 40 thread yang semuanya mencoba memproses event dengan pasangan `(topic, event_id)` yang sama. Jika deduplication tidak aman, lebih dari satu thread dapat berhasil memproses event tersebut sebagai event unik.

Skenario test:

| Item | Nilai |
|---|---:|
| Jumlah thread | 40 |
| Jumlah worker executor | 16 |
| Event yang diproses | Sama untuk semua thread |
| Dedup key | `(topic, event_id)` |
| Ekspektasi event unik | 1 |
| Ekspektasi duplikat | 39 |

Hasil yang diharapkan dan sudah lolos:

```text
results.count(True) == 1
unique_processed == 1
duplicate_dropped == 39
```

Maknanya, hanya satu transaksi yang berhasil melakukan insert event unik. Tiga puluh sembilan transaksi lain tetap berjalan, tetapi insert-nya diabaikan oleh `INSERT OR IGNORE` karena unique constraint sudah terisi. Ini membuktikan bahwa deduplication tidak bergantung pada urutan thread atau pengecekan manual di aplikasi. Keputusan akhir ada pada database.

Selain test konkurensi, terdapat 16 test yang mencakup:

| Area | Contoh test |
|---|---|
| Health check | `test_health_endpoint` |
| Publish single event | `test_publish_single_event_is_accepted` |
| Publish batch | `test_publish_batch_is_accepted` |
| Validasi batch kosong | `test_empty_batch_is_rejected` |
| Validasi schema | `test_missing_event_id_is_rejected_without_incrementing_received` |
| Validasi payload | `test_payload_must_be_object` |
| Deduplication | `test_duplicate_event_is_processed_once` |
| Topic berbeda | `test_same_event_id_on_different_topics_is_unique_per_topic` |
| Filter event | `test_events_can_be_filtered_by_topic` |
| Statistik | `test_stats_contains_topic_counts_and_worker_count` |
| Konkurensi | `test_database_dedup_is_safe_under_concurrent_workers` |
| Persistensi | `test_persistence_blocks_reprocessing_after_restart` |
| Batch invalid | `test_batch_with_invalid_item_is_rejected_atomically_by_validation` |
| Stress test kecil | `test_small_stress_run_keeps_consistent_counts` |
| Limit event | `test_event_limit_is_respected` |
| Audit log | `test_audit_log_records_processed_and_duplicate_attempts` |

Perintah menjalankan test:

```powershell
python -m pytest -q
```

Hasil verifikasi terakhir pada repository:

```text
16 passed
```





## 8. Keterkaitan Implementasi dengan Bab 1-13

### Bab 1: Karakteristik Sistem Terdistribusi

Sistem terdistribusi terdiri dari beberapa komponen yang berkomunikasi melalui jaringan dan harus tetap bekerja walaupun ada konkurensi, delay, atau kegagalan parsial. Pada project ini, `publisher` dan `aggregator` dipisahkan sebagai service berbeda dalam Docker Compose. Walaupun berjalan di satu laptop, pola komunikasinya tetap menyerupai sistem terdistribusi karena service berinteraksi melalui HTTP, memiliki proses berbeda, dan dapat mengalami failure secara terpisah.

Trade-off utama desain Pub-Sub aggregator adalah decoupling melawan kompleksitas konsistensi. Publisher tidak perlu mengetahui detail worker, storage, atau deduplication. Namun, aggregator harus siap menghadapi duplikasi dan event out-of-order. Karena itu, sistem memakai idempotent consumer dan dedup store persisten. Pendekatan ini sesuai dengan gagasan bahwa sistem terdistribusi harus dirancang dengan asumsi failure mungkin terjadi, bukan dengan asumsi semua komponen selalu sukses (Coulouris dkk., 2012).


### Bab 2: Arsitektur Sistem dan Publish-Subscribe

Arsitektur publish-subscribe cocok ketika pengirim data tidak perlu mengetahui siapa consumer-nya. Pada project ini, publisher hanya mengirim event log ke aggregator. Setelah itu, aggregator mengatur antrean, worker, deduplication, statistik, dan penyimpanan. Dibanding client-server biasa, desain Pub-Sub lebih sesuai untuk log aggregator karena event log biasanya bersifat streaming, dapat dikirim dalam batch, dan dapat diproses secara asynchronous.

Client-server cocok untuk request-response langsung, misalnya mengambil profil pengguna. Namun untuk log, publisher tidak perlu menunggu semua proses penyimpanan selesai secara sinkron. Aggregator cukup menerima event, memasukkannya ke antrean, lalu worker memprosesnya. Konsekuensinya, hasil pada `/events` dan `/stats` bersifat eventual setelah worker selesai. Pola ini menunjukkan trade-off arsitektur antara responsivitas, decoupling, dan kebutuhan mekanisme konsistensi tambahan (Coulouris dkk., 2012).


### Bab 3: Komunikasi, At-Least-Once, dan Idempotent Consumer

Komunikasi antar service dilakukan melalui HTTP. Publisher mengirim event dengan `POST /publish`, dan aggregator mengembalikan respons bahwa event diterima. Namun, di sistem terdistribusi, respons HTTP tidak selalu cukup untuk menjamin bahwa seluruh proses downstream selesai. Request bisa timeout, koneksi bisa gagal, atau publisher bisa mengulang pengiriman.

Karena itu, project ini memilih model at-least-once delivery. Dalam model ini, event minimal dikirim sekali, tetapi boleh terkirim lebih dari sekali. Exactly-once delivery secara end-to-end sulit dicapai karena ada banyak titik kegagalan antara pengiriman, penerimaan, pemrosesan, dan commit storage. Solusi yang lebih realistis adalah membuat consumer idempotent. Dengan idempotency, event yang sama dapat dikirim ulang tanpa menyebabkan efek samping ganda (Coulouris dkk., 2012).


### Bab 4: Naming, Topic, dan Event ID

Penamaan sangat penting karena deduplication membutuhkan identitas event yang stabil. Project ini memakai pasangan `(topic, event_id)` sebagai dedup key. `topic` menyatakan domain log, sedangkan `event_id` menyatakan identitas event dari publisher. Contoh topic adalah `app`, `auth`, `payment`, dan `system`.

Constraint dibuat pada pasangan `(topic, event_id)`, bukan hanya pada `event_id`. Dengan begitu, event id yang sama masih bisa dianggap berbeda jika berada pada topic berbeda. Test `test_same_event_id_on_different_topics_is_unique_per_topic` membuktikan keputusan ini. Untuk skala produksi, `event_id` dapat dibuat dengan UUID penuh, ULID, atau kombinasi source dan sequence yang lebih kuat. Pada project ini, publisher memakai kombinasi source, index, dan potongan UUID sehingga cukup collision-resistant untuk kebutuhan tugas (Coulouris dkk., 2012).


### Bab 5: Waktu dan Ordering

Ordering dalam sistem terdistribusi sulit karena setiap node dapat memiliki jam berbeda. Event dari satu source bisa datang lebih lambat daripada event dari source lain. Karena itu, project ini tidak memakai total ordering global. Deduplication tetap benar walaupun event datang tidak berurutan, karena keputusan unik atau duplikat hanya bergantung pada `(topic, event_id)`.

Sistem tetap menyimpan `timestamp` ISO8601 dan `monotonic_counter` pada payload. Timestamp membantu observasi waktu, sedangkan monotonic counter membantu mengurutkan event dari source yang sama. Batasannya, timestamp bisa terkena clock skew dan monotonic counter tidak berlaku global. Dengan kata lain, ordering pada project ini bersifat praktis, bukan jaminan urutan total. Ini sesuai dengan teori waktu dalam sistem terdistribusi, yaitu ordering harus dipahami berdasarkan batasan clock dan kebutuhan aplikasi (Coulouris dkk., 2012).

### Bab 6: Failure Modes dan Mitigasi

Failure mode utama pada project ini adalah duplikasi event, retry publisher, database lock, worker failure, dan restart container. Duplikasi event dimitigasi dengan dedup store. Retry publisher dimitigasi dengan backoff agar publisher tidak langsung membanjiri aggregator ketika terjadi error sementara. Database lock dimitigasi dengan `busy_timeout` dan retry singkat. Restart container dimitigasi dengan penyimpanan SQLite pada named volume.

Satu batasan yang sengaja dijelaskan adalah antrean masih in-memory. Jika aggregator mati setelah event diterima tetapi sebelum diproses worker, event di antrean dapat hilang. Ini adalah risiko desain tanpa durable broker. Namun, setelah event berhasil diproses dan masuk dedup store, event lama tetap dikenali sebagai duplikat setelah restart. Dengan demikian, crash recovery yang dijamin project ini adalah pada dedup store dan hasil pemrosesan, bukan pada antrean in-memory (Coulouris dkk., 2012).


### Bab 7: Konsistensi, Eventual Consistency, dan Deduplication

Aggregator memproses event secara asynchronous. Ketika `POST /publish` berhasil, event sudah diterima dan dimasukkan ke antrean, tetapi belum tentu langsung muncul pada `GET /events`. Setelah worker selesai, barulah data terlihat pada endpoint read. Ini adalah bentuk eventual consistency dalam skala kecil.

Endpoint `/admin/drain` dipakai pada test dan demo untuk menunggu antrean selesai sebelum membaca hasil final. Dalam penggunaan nyata, client dapat membaca `/stats` dan melihat `queue_size` untuk mengetahui apakah masih ada event menunggu proses. Idempotency berperan penting karena selama proses asynchronous, publisher bisa melakukan retry. Tanpa deduplication, retry dapat membuat hasil akhir tidak konsisten. Dengan unique constraint, hasil akhir tetap benar: event unik hanya disimpan sekali, sementara duplikat dihitung sebagai `duplicate_dropped` (Coulouris dkk., 2012).

Referensi bagian: Coulouris dkk. (2012), Bab 7. Referensi implementasi: `aggregator/app/main.py`, `aggregator/app/processor.py`, dan `tests/test_aggregator.py`.

### Bab 8: Transaksi, ACID, dan Isolation

Bab 8 menjadi bagian paling penting dalam project ini. Setiap event diproses dalam transaction boundary. Insert event, update statistik, dan insert audit log dilakukan dalam satu transaksi. Atomicity memastikan perubahan tidak setengah masuk. Consistency dijaga oleh unique constraint. Isolation dibantu oleh `BEGIN IMMEDIATE`, sehingga konflik writer diatur oleh SQLite. Durability dijaga oleh file database pada volume.

Lost update dihindari dengan operasi increment langsung di database. Sistem tidak melakukan read-modify-write di aplikasi untuk counter statistik. Jika banyak worker menaikkan counter secara bersamaan, database mengatur serialisasi update. Untuk SQLite, pendekatan ini cocok karena writer lock eksplisit membuat perilaku transaksi lebih mudah dipahami. Untuk PostgreSQL, desain yang setara dapat menggunakan `READ COMMITTED` dan `INSERT ... ON CONFLICT DO NOTHING`, karena konflik deduplication diselesaikan oleh constraint unik (Coulouris dkk., 2012).


### Bab 9: Kontrol Konkurensi, Locking, Unique Constraint, dan Upsert

Kontrol konkurensi project ini menggabungkan locking database, unique constraint, dan pola idempotent write. Worker boleh berjalan paralel, tetapi mereka tidak boleh menghasilkan pemrosesan ganda untuk event yang sama. Karena itu, sistem tidak memakai pola read-then-write. Sistem langsung mencoba insert, lalu database menentukan apakah insert berhasil atau diabaikan.

Pada SQLite, sintaks yang digunakan adalah `INSERT OR IGNORE`. Pada PostgreSQL, padanannya adalah `INSERT ... ON CONFLICT DO NOTHING`. Keuntungan pola ini adalah operasi deduplication menjadi atomik. Test konkurensi dengan 40 thread membuktikan hanya satu thread yang menghasilkan event unik dan 39 lainnya masuk sebagai duplikat. Trade-off-nya, SQLite membatasi writer sehingga throughput tidak setinggi database server. Namun untuk tugas ini, correctness lebih penting daripada throughput maksimum (Coulouris dkk., 2012).


### Bab 10: Keamanan Lokal

Keamanan pada project ini disesuaikan dengan scope tugas lokal. Semua service berjalan dalam network Docker Compose `uas_net`. Publisher mengakses aggregator menggunakan hostname internal `aggregator`. Tidak ada koneksi ke layanan eksternal publik. Port yang diekspos ke host hanya `8080` untuk keperluan demo dan akses API lokal.

Desain ini mengurangi permukaan akses karena storage tidak diekspos sebagai service publik. SQLite berada di volume yang hanya dipakai container aggregator. Untuk produksi, sistem masih perlu autentikasi, authorization, rate limiting, validasi source, dan transport security. Namun untuk tugas ini, isolasi jaringan Compose dan tidak adanya dependensi eksternal sudah sesuai dengan requirement keamanan lokal (Coulouris dkk., 2012).

Referensi bagian: Coulouris dkk. (2012), Bab 10. Referensi implementasi: `docker-compose.yml`.

### Bab 11: Penyimpanan dan Persistensi

Bab penyimpanan berkaitan langsung dengan dedup store. SQLite menyimpan tiga informasi utama: event unik pada `processed_events`, counter pada `stats`, dan jejak pemrosesan pada `audit_log`. Database ditempatkan di named volume Docker, sehingga data tidak hilang ketika container aggregator dihapus dan dibuat ulang.

Persistensi dibuktikan oleh test `test_persistence_blocks_reprocessing_after_restart`. Test tersebut menjalankan aplikasi dengan file database yang sama, mengirim event, menghentikan client pertama, lalu menjalankan client kedua dengan database yang sama. Ketika event yang sama dikirim lagi, sistem mengenalinya sebagai duplikat. Ini menunjukkan bahwa identitas event yang sudah diproses tidak hanya disimpan di memori, tetapi benar-benar persisten (Coulouris dkk., 2012).


### Bab 12: Sistem Berbasis Web

Aggregator adalah web service berbasis FastAPI. API menyediakan endpoint publish, read events, stats, health check, dan drain. Endpoint `/health` digunakan oleh Docker Compose healthcheck agar publisher hanya dijalankan setelah aggregator sehat. Endpoint `/stats` dan `/events` memberi cara sederhana untuk mengamati hasil sistem.

Pemakaian HTTP membuat sistem mudah diuji dan didemonstrasikan. Test menggunakan `TestClient` untuk memanggil endpoint secara langsung. Dalam konteks sistem terdistribusi, API menjadi kontrak antar komponen. Publisher tidak perlu mengetahui detail database atau worker. Ia hanya perlu memahami kontrak `POST /publish` dan format event JSON (Coulouris dkk., 2012).


### Bab 13: Koordinasi, Orkestrasi, dan Observability

Koordinasi dilakukan pada dua level. Pertama, Docker Compose mengoordinasikan service, network, environment variable, healthcheck, dan volume. Kedua, aggregator mengoordinasikan worker internal melalui antrean. Worker mengambil event dari queue dan memprosesnya secara paralel.

Observability disediakan melalui log dan endpoint `/stats`. Log worker menampilkan event yang diproses dan duplikat yang dibuang. Endpoint `/stats` menampilkan total penerimaan, event unik, duplikat, distribusi topic, uptime, jumlah worker, dan ukuran antrean. Informasi ini penting untuk membuktikan bahwa sistem bukan hanya berjalan, tetapi juga dapat diamati saat menerima beban dan duplikasi (Coulouris dkk., 2012).


## 9. Ringkasan Pemenuhan Requirement

| Requirement | Status | Bukti |
|---|---|---|
| Multi-service Docker Compose | Terpenuhi | `aggregator` dan `publisher` pada `docker-compose.yml`. |
| API `POST /publish` | Terpenuhi | `aggregator/app/main.py`. |
| API `GET /events` | Terpenuhi | Endpoint `/events`. |
| API `GET /stats` | Terpenuhi | Endpoint `/stats`. |
| Idempotency | Terpenuhi | `(topic, event_id)` diproses sekali. |
| Dedup store persisten | Terpenuhi | SQLite pada named volume `aggregator_data`. |
| Transaksi | Terpenuhi | `BEGIN IMMEDIATE`, insert, update stats, audit log, commit. |
| Konkurensi | Terpenuhi | Multi-worker dan test 40 thread. |
| Ordering dijelaskan | Terpenuhi | Timestamp dan monotonic counter, tanpa total ordering global. |
| Retry | Terpenuhi | Publisher retry dengan exponential backoff. |
| Performa 20.000 event | Terpenuhi | Benchmark 20.000 event, 30% duplikat. |
| Tests 12-20 | Terpenuhi | 16 test. |
| Observability | Terpenuhi | Log worker, `/stats`, `/health`, `/admin/drain`. |
| Persistensi setelah restart | Terpenuhi | Test persistensi dan Docker volume. |




## 10. Asumsi dan Batasan

Asumsi:

1. Sistem berjalan pada jaringan lokal Docker Compose.
2. Tidak ada layanan eksternal publik yang dipakai.
3. Event id dibuat oleh publisher dan dianggap cukup unik untuk scope tugas.
4. Clock antar source tidak dijamin sinkron sempurna.
5. Deduplication dianggap benar jika pasangan `(topic, event_id)` sama.

Batasan:

1. Antrean masih in-memory, sehingga event yang sudah diterima tetapi belum diproses dapat hilang jika aggregator mati mendadak.
2. SQLite cocok untuk demo dan correctness lokal, tetapi bukan pilihan terbaik untuk throughput writer tinggi.
3. Tidak ada autentikasi API karena scope tugas adalah demo lokal.
4. Tidak ada broker durable seperti Redis Streams atau NATS JetStream.
5. Total ordering global tidak diterapkan.

Rencana pengembangan:

1. Mengganti antrean in-memory dengan durable broker.
2. Memindahkan storage ke PostgreSQL untuk concurrency writer yang lebih baik.
3. Menambahkan autentikasi API dan validasi source.
4. Menambahkan metrics endpoint format Prometheus.
5. Menambahkan outbox pattern untuk side-effect lanjutan.




## 11. Kesimpulan

Project ini membangun Pub-Sub Log Aggregator yang memenuhi fokus utama UAS Sistem Terdistribusi: idempotent consumer, deduplication persisten, transaksi, kontrol konkurensi, Docker Compose, pengujian, dan observability. Sistem sengaja memakai model at-least-once delivery karena lebih realistis untuk sistem terdistribusi. Kemungkinan duplikasi tidak dihindari di jaringan, tetapi dikendalikan di consumer melalui unique constraint dan transaksi database.

Hasil uji menunjukkan bahwa event duplikat tidak diproses ulang, data tetap persisten setelah restart, dan konkurensi multi-thread tidak menyebabkan double processing. Benchmark 20.000 event dengan 30% duplikat menunjukkan sistem memenuhi requirement performa minimum. Secara teori, implementasi ini menghubungkan Bab 1-13 dari buku utama, dengan penekanan kuat pada Bab 8-9 tentang transaksi dan kontrol konkurensi.



## Referensi

Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). *Distributed systems: Concepts and design* (ed. ke-5). Addison-Wesley.
