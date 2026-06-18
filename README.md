# Pub-Sub Log Aggregator Terdistribusi

Implementasi UAS Sistem Terdistribusi menggunakan Python, FastAPI, worker paralel, dan SQLite sebagai dedup store persisten. Sistem ini menerima event log, memprosesnya secara idempotent, membuang duplikat berdasarkan pasangan `(topic, event_id)`, dan menyimpan statistik pemrosesan.

## Arsitektur

```text
publisher -> HTTP POST /publish -> aggregator API -> in-memory queue
                                           |
                                           v
                                  multi-worker consumer
                                           |
                                           v
                            SQLite persistent dedup store
```

Komponen repository:

- `aggregator/`: FastAPI service, worker consumer, SQLite transaction layer.
- `publisher/`: simulator pengirim event, termasuk duplikat minimal 30%.
- `tests/`: 16 pengujian menggunakan `pytest`.
- `docker-compose.yml`: menjalankan `aggregator` dan `publisher` dalam jaringan Compose lokal.
- `report.md`: laporan teori, desain, metrik, dan referensi.

## Endpoint

- `POST /publish`: menerima satu event atau batch event.
- `GET /events?topic=...`: menampilkan event unik yang sudah diproses.
- `GET /stats`: menampilkan `received`, `unique_processed`, `duplicate_dropped`, `topics`, `uptime`, `worker_count`, dan `queue_size`.
- `GET /health`: health check.
- `POST /admin/drain`: menunggu antrean selesai diproses, berguna untuk demo dan test.

Contoh event:

```json
{
  "topic": "app",
  "event_id": "event-001",
  "timestamp": "2026-06-17T22:00:00+08:00",
  "source": "publisher-1",
  "payload": {
    "level": "INFO",
    "message": "user login"
  }
}
```

## Menjalankan dengan Docker

```bash
docker compose up --build
```

Aggregator dapat diakses dari host:

```text
http://localhost:8080
```

Menjalankan publisher simulator dari Compose:

```bash
docker compose --profile tools run --rm publisher
```

Data SQLite disimpan di named volume:

```text
aggregator_data:/var/lib/aggregator/aggregator.db
```

Volume ini menjaga data tetap ada meskipun container dihapus, selama volume tidak dihapus.

## Menjalankan Test

```powershell
python -m pytest -q
```

Hasil verifikasi terakhir:

```text
16 passed
```

## Keputusan Desain Penting

- Deduplication memakai `UNIQUE(topic, event_id)` pada tabel `processed_events`.
- Pemrosesan event memakai transaksi SQLite `BEGIN IMMEDIATE`.
- Insert event memakai `INSERT OR IGNORE`, sehingga duplikat aman walau diproses worker paralel.
- Statistik diperbarui dengan operasi `UPDATE stats SET count = count + 1`, sehingga tidak terjadi lost update.
- Delivery dianggap at-least-once: publisher boleh mengirim duplikat, consumer wajib idempotent.
- Ordering tidak memakai total ordering global. Event disimpan bersama timestamp dan payload `monotonic_counter` dari publisher untuk ordering praktis per sumber.

## Metrik Benchmark Lokal

Benchmark lokal menggunakan TestClient, SQLite, 6 worker, 20.000 event, dan 30% duplikat:

| Metrik | Nilai |
|---|---:|
| received | 20.000 |
| unique_processed | 14.000 |
| duplicate_dropped | 6.000 |
| duplicate rate | 30% |
| elapsed | 94,959 detik |
| throughput | 210,62 event/detik |
| latency rata-rata | 4,748 ms/event |

Angka bisa berbeda tergantung spesifikasi laptop, mode logging, dan apakah dijalankan lewat Docker atau lokal.

## Video Demo

Tautan video YouTube unlisted/publik:

```text

```
