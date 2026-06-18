# Laporan UAS Pub-Sub Log Aggregator

Dokumen ini adalah kerangka awal laporan. Isi teori T1-T10 dan metrik final
perlu dilengkapi setelah implementasi dijalankan dan video demo direkam.

## Ringkasan Sistem

Sistem yang dibangun adalah Pub-Sub Log Aggregator multi-service menggunakan
FastAPI, Postgres, Redis, dan Docker Compose. Aggregator menerima event melalui
`POST /publish`, menyimpan event unik, menolak duplikasi secara idempotent, dan
menyediakan observability melalui `GET /events` dan `GET /stats`.

## Arsitektur

- `aggregator`: API dan consumer logic.
- `publisher`: simulator pengirim event duplikat.
- `broker`: Redis internal Compose.
- `storage`: Postgres dengan volume persisten.

## Keputusan Desain

### Idempotency

Idempotency didefinisikan berdasarkan pasangan `(topic, event_id)`. Event dengan
pasangan yang sama hanya boleh diproses satu kali.

### Dedup Store

Dedup store menggunakan tabel `processed_events` di Postgres dengan constraint
unik `(topic, event_id)`.

### Transaksi dan Konkurensi

Pemrosesan batch dilakukan dalam transaksi. Insert ke dedup store menggunakan
`INSERT ... ON CONFLICT DO NOTHING RETURNING id`. Jika insert berhasil, event
baru diproses. Jika konflik, event dianggap duplikat.

Isolation level yang digunakan adalah `READ COMMITTED`. Strategi ini cukup
karena correctness dedup dijaga oleh unique constraint dan operasi upsert atomik
di database.

### Ordering

Sistem tidak menjanjikan total ordering global. Ordering praktis menggunakan
`timestamp` dari event dan `processed_at` dari database untuk audit.

### Reliability

Sistem mengasumsikan at-least-once delivery. Publisher boleh mengirim event
duplikat, sementara aggregator memastikan hasil akhir tetap konsisten.

## Metrik Awal

Isi setelah menjalankan:

```bash
docker compose --profile tools run --rm publisher
curl http://localhost:8080/stats
```

## Kerangka Teori T1-T10

### T1 - Karakteristik Sistem Terdistribusi

TODO.

### T2 - Publish-Subscribe vs Client-Server

TODO.

### T3 - At-Least-Once vs Exactly-Once Delivery

TODO.

### T4 - Penamaan Topic dan Event ID

TODO.

### T5 - Ordering Praktis

TODO.

### T6 - Failure Modes dan Mitigasi

TODO.

### T7 - Eventual Consistency

TODO.

### T8 - Desain Transaksi

TODO.

### T9 - Kontrol Konkurensi

TODO.

### T10 - Orkestrasi, Keamanan Lokal, Persistensi, Observability

TODO.

## Referensi

TODO: lengkapi metadata buku utama dalam format APA 7th.
