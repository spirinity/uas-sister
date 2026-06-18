# UAS Pub-Sub Log Aggregator

Sistem ini adalah implementasi awal untuk UAS Sistem Terdistribusi bertema
Pub-Sub Log Aggregator dengan idempotent consumer, deduplication persisten,
transaksi, dan kontrol konkurensi.

## Arsitektur

Service Docker Compose:

- `aggregator`: FastAPI API dengan 4 Uvicorn worker. Publish hanya menulis message ke Redis Stream.
- `consumer`: consumer group dengan 4 worker yang membaca Redis Stream dan memproses transaksi Postgres.
- `publisher`: simulator pengirim event dengan duplikasi minimal 30%.
- `broker`: Redis Stream persisten dengan append-only file dan consumer group.
- `storage`: Postgres 16 dengan named volume `pg_data`.

Semua service berjalan di network Compose lokal `pubsub_net`. Hanya aggregator
yang diekspos ke host melalui `127.0.0.1:8080`.

## Model Event

```json
{
  "topic": "payment",
  "event_id": "payment-123",
  "timestamp": "2026-06-16T10:00:00Z",
  "source": "publisher-simulator",
  "payload": {
    "message": "example log"
  }
}
```

## Endpoint

### `POST /publish`

Menerima single event atau batch event.

```bash
curl -X POST http://127.0.0.1:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"topic":"auth","event_id":"evt-1","timestamp":"2026-06-16T10:00:00Z","source":"manual","payload":{"ok":true}}'
```

Response:

```json
{
  "received": 1,
  "unique_processed": 1,
  "duplicate_dropped": 0
}
```

### `GET /events?topic=auth`

Menampilkan event unik yang sudah diproses.

```bash
curl http://127.0.0.1:8080/events?topic=auth
```

### `GET /stats`

Menampilkan statistik global.

```bash
curl http://127.0.0.1:8080/stats
```

### `GET /health`

Health check memverifikasi koneksi Postgres dan Redis.

```bash
curl http://127.0.0.1:8080/health
```

## Idempotency dan Deduplication

Alur publish adalah:

```text
Publisher/Swagger -> Aggregator API -> Redis Stream -> Consumer -> Postgres
```

API menambahkan satu message batch ke stream `log-events`. Consumer group
`log-aggregators` membagi message ke empat worker. Consumer baru menjalankan
transaksi Postgres, menyimpan hasil sementara di Redis, lalu melakukan `XACK`.
Jika consumer crash sebelum `XACK`, message tetap pending dan diklaim ulang
dengan `XAUTOCLAIM`, sehingga delivery bersifat at-least-once.

Deduplication dilakukan secara persisten di Postgres melalui tabel
`processed_events` dengan constraint:

```sql
UNIQUE (topic, event_id)
```

Saat event diproses, aggregator menjalankan:

```sql
INSERT INTO processed_events (topic, event_id)
VALUES ($1, $2)
ON CONFLICT (topic, event_id) DO NOTHING
RETURNING id;
```

Jika `RETURNING id` menghasilkan nilai, event adalah event baru dan disimpan ke
tabel `events`. Jika tidak, event dianggap duplikat dan tidak diproses ulang.

## Transaksi dan Kontrol Konkurensi

Setiap batch diproses di dalam transaksi Postgres dengan isolation level
`READ COMMITTED`. Race condition dicegah oleh kombinasi:

- transaksi database,
- unique constraint `(topic, event_id)`,
- atomic upsert `ON CONFLICT DO NOTHING`,
- update statistik dengan SQL increment, misalnya `count = count + 1`.

Dengan pola ini, dua worker paralel yang memproses event sama tidak dapat
menghasilkan double-processing karena hanya satu insert dedup yang akan menang.

## Menjalankan Sistem

```bash
docker compose up --build
```

Aggregator tersedia di:

```text
http://127.0.0.1:8080
```

Gunakan `127.0.0.1`, bukan `localhost`, jika Swagger `Execute` menampilkan `Failed to fetch`. Pada beberapa konfigurasi Windows, `localhost` bisa resolve ke IPv6 `::1` dan mengenai listener yang berbeda dari service Docker.


## Demo Use Cases di Swagger

Buka:

```text
http://127.0.0.1:8080/docs
```

Gunakan tag `Demo Use Cases` untuk menjalankan skenario presentasi langsung dari Swagger:

- `POST /demo/publish-single`: publish satu event contoh.
- `POST /demo/publish-duplicate`: kirim event yang sama beberapa kali untuk membuktikan deduplication.
- `POST /demo/publish-batch`: kirim batch event dengan `duplicate_rate`; pakai `total=20000` untuk demo performa.
- `POST /demo/concurrency`: kirim request paralel dengan event yang sama untuk membuktikan transaksi dan unique constraint mencegah race condition.

Setelah setiap demo, response sudah menyertakan `result` dan `stats`, jadi kamu bisa menjelaskan received, unique_processed, dan duplicate_dropped tanpa pindah ke terminal.
## Menjalankan Publisher Simulator

Jalankan setelah aggregator sehat:

```bash
docker compose --profile tools run --rm publisher
```

Default simulator:

- total event: `20000`
- duplicate rate: `0.30`
- batch size: `250`

Variabel dapat diubah:

```bash
docker compose --profile tools run --rm \
  -e TOTAL_EVENTS=5000 \
  -e DUPLICATE_RATE=0.40 \
  -e BATCH_SIZE=100 \
  publisher
```

## Demo Konkurensi

Script ini mengirim event dengan `(topic, event_id)` yang sama secara paralel.
Hasil yang diharapkan: `unique_processed` hanya bertambah `1`, sisanya masuk
ke `duplicate_dropped`.

```bash
python scripts/concurrency_demo.py --requests 50
curl http://127.0.0.1:8080/stats
```

## Persistensi

Postgres menggunakan named volume:

```yaml
volumes:
  pg_data:
```

Untuk demo persistensi:

```bash
docker compose up --build
curl http://127.0.0.1:8080/stats
docker compose down
docker compose up
curl http://127.0.0.1:8080/stats
```

Data tetap ada selama volume `pg_data` tidak dihapus.

## Menjalankan Tests

Install dependency test secara lokal:

```bash
pip install -r aggregator/requirements.txt -r publisher/requirements.txt -r tests/requirements.txt
```

Jalankan:

```bash
PYTHONPATH=aggregator:publisher pytest tests
```

Di Windows PowerShell:

```powershell
$env:PYTHONPATH = "aggregator;publisher"
pytest tests
```

## Cakupan Tests Saat Ini

Ada 20 test yang mencakup:

- validasi request event,
- single publish,
- batch publish,
- duplicate event,
- duplicate dalam batch,
- seluruh endpoint Swagger demo: single, duplicate, batch, dan concurrency,
- `GET /events`,
- filter topic,
- validasi limit,
- `GET /stats`,
- daftar topic,
- schema dedup unique constraint,
- publisher duplicate generation,
- chunking batch.

## Catatan Untuk Laporan

Keputusan desain utama:

- Delivery diasumsikan at-least-once.
- Redis Stream dan consumer group menjadi jalur Pub-Sub internal.
- Empat Uvicorn worker melayani API dan empat consumer worker memproses queue.
- Idempotency dijamin oleh `(topic, event_id)`.
- Dedup store persisten memakai Postgres volume.
- Isolation level yang dipilih adalah `READ COMMITTED`.
- Correctness dedup tidak bergantung pada pengecekan aplikasi, tetapi pada
  unique constraint dan atomic upsert di database.
