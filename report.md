# Laporan UAS Pub-Sub Log Aggregator

## Identitas

| Keterangan  | Detail                                            |
| ----------- | ------------------------------------------------- |
| Mata Kuliah | Sistem Paralel dan Terdistribusi A                |
| Nama        | Mahardika Arka                                    |
| NIM         | 11231037                                          |
| Tema        | Pub-Sub Log Aggregator Terdistribusi              |
| Bahasa      | Python                                            |
| Stack       | FastAPI, Redis Stream, PostgreSQL, Docker Compose |

## Ringkasan Sistem

Sistem yang dibangun adalah **Pub-Sub Log Aggregator** multi-service yang
berjalan dengan Docker Compose. Sistem menerima event log dari publisher atau
Swagger UI melalui API `POST /publish`, memasukkan event ke Redis Stream, lalu
consumer memproses event tersebut ke PostgreSQL.

Fokus utama sistem adalah **idempotent consumer**, **deduplication persisten**,
serta **transaksi dan kontrol konkurensi**. Event yang memiliki pasangan
`(topic, event_id)` yang sama hanya boleh diproses satu kali. Untuk menjamin
hal tersebut, sistem menggunakan tabel `processed_events` dengan unique
constraint `(topic, event_id)` dan query `INSERT ... ON CONFLICT DO NOTHING`.

Sistem juga menyediakan observability melalui endpoint `GET /health`,
`GET /events`, dan `GET /stats`. Seluruh service berjalan dalam jaringan lokal
Docker Compose, dengan hanya service `aggregator` yang diekspos ke host melalui
port `8080`.

## Arsitektur Sistem

![Arsitektur Pub-Sub Log Aggregator](images/aristektur.png)

Service yang digunakan:

| Service      | Peran                                                                                      |
| ------------ | ------------------------------------------------------------------------------------------ |
| `aggregator` | FastAPI API untuk menerima publish request, Swagger demo, health check, events, dan stats. |
| `consumer`   | Worker async yang membaca Redis Stream dan memproses event ke PostgreSQL.                  |
| `publisher`  | Simulator pengirim 20.000 event dengan 30% duplikasi.                                      |
| `broker`     | Redis Stream internal sebagai message broker.                                              |
| `storage`    | PostgreSQL 16 sebagai database persisten.                                                  |

Alur utama:

```text
Publisher / Swagger -> Aggregator API -> Redis Stream -> Consumer -> PostgreSQL
```

Pendekatan ini mengikuti gaya publish-subscribe karena pengirim event tidak
perlu mengetahui detail pemrosesan consumer. Redis Stream menjadi pemisah
antara penerimaan event dan pemrosesan event, sehingga sistem lebih mudah
menghadapi retry, duplicate delivery, dan worker paralel.

## Model Event dan API

Model event minimal:

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

Endpoint utama:

| Method | Endpoint                  | Fungsi                                         |
| ------ | ------------------------- | ---------------------------------------------- |
| `GET`  | `/health`                 | Mengecek API, PostgreSQL, dan Redis.           |
| `POST` | `/publish`                | Menerima single atau batch event.              |
| `GET`  | `/events?topic=...`       | Menampilkan event unik yang sudah diproses.    |
| `GET`  | `/stats`                  | Menampilkan statistik global.                  |
| `POST` | `/demo/publish-single`    | Demo satu event.                               |
| `POST` | `/demo/publish-duplicate` | Demo idempotency dan deduplication.            |
| `POST` | `/demo/publish-batch`     | Demo performa batch, termasuk 20.000 event.    |
| `POST` | `/demo/concurrency`       | Demo request paralel untuk uji race condition. |

## Keputusan Desain

### Idempotency

Idempotency didefinisikan berdasarkan pasangan `(topic, event_id)`. Jika dua
event memiliki `topic` dan `event_id` yang sama, maka event kedua dan seterusnya
dianggap duplikat. Pola ini cocok untuk at-least-once delivery karena event
dapat dikirim ulang tanpa mengubah hasil akhir sistem.

### Dedup Store Persisten

Deduplication disimpan di PostgreSQL pada tabel `processed_events` dengan
constraint:

```sql
UNIQUE (topic, event_id)
```

Dedup store ini persisten karena PostgreSQL menggunakan named volume `pg_data`.
Artinya, data dedup tetap ada walaupun container dihentikan atau dibuat ulang,
selama volume tidak dihapus.

### Transaksi dan Kontrol Konkurensi

Pemrosesan batch dilakukan dalam transaksi PostgreSQL dengan isolation level
`READ COMMITTED`. Consumer mencoba mencatat event ke dedup store dengan query:

```sql
INSERT INTO processed_events (topic, event_id)
VALUES ($1, $2)
ON CONFLICT (topic, event_id) DO NOTHING
RETURNING id;
```

Jika `RETURNING id` menghasilkan nilai, event dianggap baru dan disimpan ke
tabel `events`. Jika tidak ada nilai, event dianggap duplicate dan tidak
diproses ulang. Correctness dedup tidak bergantung pada pengecekan manual di
aplikasi, tetapi dijaga oleh unique constraint dan atomic upsert di database.

### Ordering

Sistem tidak menjanjikan total ordering global. Hal ini disengaja karena total
ordering dalam sistem terdistribusi dapat menambah kompleksitas dan overhead.
Ordering praktis dilakukan menggunakan `timestamp` dari event dan `processed_at`
dari database. Kedua field ini cukup untuk audit dan observasi urutan pemrosesan
secara operasional.

### Reliability

Sistem mengasumsikan delivery bersifat **at-least-once**. Redis Stream dan
consumer group memungkinkan message yang belum di-acknowledge untuk diklaim
ulang dengan `XAUTOCLAIM`. Jika consumer crash sebelum `XACK`, event tetap bisa
diproses kembali. Karena consumer idempotent, retry tidak membuat data ganda.

## Metrik dan Hasil Pengujian

Pengujian dilakukan pada 19 Juni 2026 di lingkungan lokal Docker Compose.

### Health Check

Endpoint:

```powershell
curl http://127.0.0.1:8080/health
```

Hasil:

```json
{
  "status": "ok",
  "database": "ok",
  "broker": "ok"
}
```

Hasil ini menunjukkan API, PostgreSQL, dan Redis terhubung dengan benar.

### Demo Deduplication

Skenario:

```text
POST /demo/publish-duplicate?copies=5
```

Hasil:

| Metrik              | Nilai |
| ------------------- | ----: |
| `received`          |     5 |
| `unique_processed`  |     1 |
| `duplicate_dropped` |     4 |

Interpretasi: lima event diterima, tetapi hanya satu event diproses sebagai
event unik. Empat event lain dikenali sebagai duplicate.

### Demo Konkurensi

Skenario:

```text
POST /demo/concurrency?requests=25
```

Hasil:

| Metrik              | Nilai |
| ------------------- | ----: |
| `received`          |    25 |
| `unique_processed`  |     1 |
| `duplicate_dropped` |    24 |

Interpretasi: request paralel tidak menghasilkan double-processing karena
PostgreSQL hanya mengizinkan satu insert untuk `(topic, event_id)` yang sama.

### Demo Performa 20.000 Event

Skenario publisher simulator:

```powershell
docker compose --profile tools run --rm publisher
```

Konfigurasi default:

| Parameter        |  Nilai |
| ---------------- | -----: |
| `TOTAL_EVENTS`   | 20.000 |
| `DUPLICATE_RATE` |   0,30 |
| `BATCH_SIZE`     |    250 |

Hasil pengujian:

| Metrik                         |              Nilai |
| ------------------------------ | -----------------: |
| `received`                     |             20.000 |
| `unique_processed`             |             14.000 |
| `duplicate_dropped`            |              6.000 |
| `elapsed_seconds`              |       21,305 detik |
| `throughput_events_per_second` | 938,75 event/detik |

Interpretasi: sistem memenuhi requirement performa minimum karena berhasil
memproses 20.000 event dengan 30% duplikasi dan tetap menghasilkan dedup yang
konsisten.

### Demo Persistensi

Skenario:

```powershell
curl http://127.0.0.1:8080/stats
docker compose down
docker compose up -d
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/stats
```

Hasil sebelum dan sesudah container recreate:

| Metrik              | Sebelum Recreate | Sesudah Recreate |
| ------------------- | ---------------: | ---------------: |
| `received`          |           80.253 |           80.253 |
| `unique_processed`  |           56.109 |           56.109 |
| `duplicate_dropped` |           24.144 |           24.144 |

Interpretasi: data tetap ada setelah container dihentikan dan dibuat ulang
karena PostgreSQL menggunakan named volume `pg_data`.

### Automated Tests

Perintah:

```powershell
$env:PYTHONPATH = "aggregator;publisher"
pytest tests
```

Hasil:

```text
20 passed
```

Cakupan test meliputi validasi schema event, single publish, batch publish,
duplicate event, endpoint demo Swagger, stats, unique constraint, publisher
simulator, dan chunking batch.

## Keterkaitan Teori T1-T10

### T1 - Karakteristik Sistem Terdistribusi

Sistem terdistribusi terdiri dari beberapa komponen yang berjalan pada proses
atau node berbeda, berkomunikasi melalui jaringan, dan terlihat sebagai satu
sistem terpadu bagi pengguna. Project ini memenuhi karakteristik tersebut
karena terdiri dari `aggregator`, `consumer`, `publisher`, `broker`, dan
`storage` yang saling berkomunikasi melalui network Docker Compose. Pengguna
berinteraksi melalui API, sementara detail Redis Stream dan PostgreSQL
tersembunyi di belakang service internal.

Trade-off utama desain ini adalah pemisahan antara penerimaan event dan
pemrosesan event. Pemisahan tersebut membuat sistem lebih fleksibel dan tahan
terhadap retry, tetapi menambah kompleksitas karena harus menangani duplicate
delivery, ordering, dan konsistensi statistik. Menurut Coulouris et al. (2012),
sistem terdistribusi perlu memperhatikan heterogeneity, concurrency,
independent failures, dan transparency. Implementasi ini menanggapi aspek
tersebut melalui Docker Compose untuk isolasi service, Redis Stream untuk
komunikasi, serta PostgreSQL untuk konsistensi data.

### T2 - Publish-Subscribe vs Client-Server

Arsitektur client-server cocok ketika client membutuhkan response langsung dari
server yang memegang seluruh logika. Namun, untuk kasus log aggregator,
publish-subscribe lebih sesuai karena producer event tidak perlu mengetahui
consumer mana yang memproses event. Publisher cukup mengirim event ke
aggregator, aggregator memasukkan event ke broker, lalu consumer mengambil
event dari Redis Stream.

Keuntungan publish-subscribe pada project ini adalah decoupling. Publisher
tidak bergantung langsung pada database, dan consumer dapat ditambah atau
dijalankan paralel tanpa mengubah kontrak publisher. Model ini juga lebih cocok
untuk beban event tinggi, retry, dan pemrosesan asynchronous. Kekurangannya
adalah sistem harus menangani kemungkinan duplicate delivery dan ordering yang
tidak selalu deterministik. Karena itu, desain ini dilengkapi idempotent
consumer dan dedup store persisten. Coulouris et al. (2012) menekankan bahwa
arsitektur sistem terdistribusi harus dipilih berdasarkan pola komunikasi dan
kebutuhan koordinasi antar komponen.

### T3 - At-Least-Once vs Exactly-Once Delivery

At-least-once delivery berarti event dijamin dapat dikirim minimal satu kali,
tetapi event yang sama mungkin diterima lebih dari sekali. Exactly-once delivery
berarti event diproses tepat satu kali secara end-to-end. Dalam praktik sistem
terdistribusi, exactly-once sulit dan mahal karena membutuhkan koordinasi kuat
antara broker, consumer, dan storage. Project ini memilih asumsi at-least-once
karena lebih realistis untuk sistem berbasis queue atau stream.

Konsekuensi at-least-once adalah duplicate event harus dianggap kondisi normal,
bukan error. Karena itu, consumer dibuat idempotent. Jika event yang sama
diterima berkali-kali, hasil akhirnya tetap sama seperti event diterima satu
kali. Pada implementasi ini, idempotency dijamin oleh pasangan `(topic,
event_id)` dan unique constraint PostgreSQL. Dengan demikian, sistem tidak
bergantung pada ilusi exactly-once, tetapi membangun correctness melalui
deduplication persisten. Pendekatan ini sesuai dengan pembahasan reliability
dan failure handling pada sistem terdistribusi (Coulouris et al., 2012).

### T4 - Penamaan Topic dan Event ID

Penamaan adalah bagian penting dalam sistem terdistribusi karena komponen yang
berbeda harus sepakat tentang identitas resource atau message. Pada project ini,
identitas event ditentukan oleh pasangan `topic` dan `event_id`. `topic`
mengelompokkan event berdasarkan domain, misalnya `auth`, `payment`,
`inventory`, atau `shipping`. `event_id` menjadi identitas unik event dalam
topic tersebut.

Kombinasi `(topic, event_id)` dipilih karena satu `event_id` mungkin saja
digunakan oleh sistem berbeda, tetapi pasangan dengan topic membuat identitas
lebih spesifik. Desain ini juga lebih fleksibel dibanding menjadikan
`event_id` saja sebagai kunci global. Untuk mengurangi collision, publisher
simulator menggunakan UUID ketika membuat event baru. Pada database, pasangan
tersebut dijaga dengan unique constraint sehingga collision atau duplicate
tidak menghasilkan pemrosesan ganda. Coulouris et al. (2012) menjelaskan bahwa
skema penamaan harus mendukung identifikasi objek secara konsisten di antara
komponen terdistribusi.

### T5 - Ordering Praktis

Sistem ini tidak menjanjikan total ordering global. Keputusan ini disengaja
karena total ordering membutuhkan koordinasi yang lebih mahal, terutama ketika
event datang dari banyak sumber dan diproses oleh beberapa worker. Untuk log
aggregator, kebutuhan utamanya adalah deduplication dan audit, bukan urutan
global sempurna.

Ordering praktis dilakukan dengan dua field. Pertama, `timestamp` berasal dari
event dan merepresentasikan waktu saat event dibuat oleh source. Kedua,
`processed_at` berasal dari database dan menunjukkan waktu saat event berhasil
diproses. Kombinasi ini cukup untuk analisis operasional: pengguna dapat
melihat kapan event dibuat dan kapan event diproses. Jika ada event out-of-order,
sistem tetap konsisten karena deduplication tidak bergantung pada urutan
kedatangan, melainkan pada `(topic, event_id)`. Coulouris et al. (2012)
menjelaskan bahwa waktu dan ordering dalam sistem terdistribusi sering kali
bersifat parsial dan dipengaruhi keterlambatan komunikasi.

### T6 - Failure Modes dan Mitigasi

Failure mode utama dalam project ini adalah duplicate delivery, consumer crash,
broker restart, database restart, dan request timeout. Duplicate delivery
ditangani dengan idempotent consumer. Consumer crash ditangani dengan Redis
Stream consumer group: message yang belum di-acknowledge dapat diklaim ulang
menggunakan `XAUTOCLAIM`. Broker Redis dikonfigurasi dengan append-only file,
sedangkan PostgreSQL menggunakan named volume agar data tetap persisten.

Jika consumer gagal sebelum `XACK`, message tetap berada dalam pending entries
list dan dapat diproses ulang. Karena dedup store berada di PostgreSQL,
reprocessing tidak menyebabkan data ganda. Jika API menunggu terlalu lama untuk
hasil consumer, endpoint mengembalikan timeout, tetapi event tetap aman di
stream selama belum diproses. Mitigasi ini menunjukkan bahwa sistem menerima
kemungkinan partial failure, bukan mengasumsikan semua komponen selalu sehat.
Menurut Coulouris et al. (2012), independent failure adalah salah satu tantangan
utama sistem terdistribusi.

### T7 - Eventual Consistency

Sistem ini menggunakan pola eventual consistency antara saat event diterima oleh
API dan saat event tersedia di PostgreSQL. Ketika request masuk, event
dimasukkan ke Redis Stream terlebih dahulu. Consumer kemudian membaca stream dan
menulis hasil ke database. Ada jeda kecil antara publish dan data benar-benar
tersimpan, tetapi selama consumer berjalan, sistem akan mencapai state akhir
yang konsisten.

Idempotency dan deduplication menjadi kunci agar eventual consistency tetap
benar. Jika event dikirim ulang karena retry atau crash recovery, hasil akhir
tidak berubah: event unik hanya tersimpan sekali dan duplicate dihitung sebagai
`duplicate_dropped`. Statistik juga di-update secara transaksional sehingga
angka `received`, `unique_processed`, dan `duplicate_dropped` tetap konsisten.
Pendekatan ini sesuai dengan prinsip sistem terdistribusi bahwa konsistensi
sering kali harus dipertimbangkan bersama availability, latency, dan fault
tolerance (Coulouris et al., 2012).

### T8 - Desain Transaksi

Transaksi digunakan pada bagian paling kritis, yaitu pemrosesan batch event oleh
consumer. Dalam satu transaksi, sistem menambah statistik `received`, mencoba
insert ke dedup store, menyimpan event unik ke tabel `events`, lalu memperbarui
`unique_processed` dan `duplicate_dropped`. Dengan transaction boundary ini,
perubahan data penting tidak dilakukan secara terpisah tanpa kontrol.

Isolation level yang dipilih adalah `READ COMMITTED`. Level ini cukup karena
correctness dedup dijaga oleh unique constraint dan atomic upsert, bukan oleh
read-before-write yang rawan race condition. Jika dua transaksi mencoba
memproses event sama, PostgreSQL akan memastikan hanya satu insert
`(topic, event_id)` yang berhasil. Lost update pada statistik dicegah dengan
SQL increment seperti `received = received + $1`, bukan dengan membaca nilai
lama lalu menulis ulang dari aplikasi. Coulouris et al. (2012) membahas bahwa
transaksi ACID penting untuk menjaga konsistensi pada operasi konkuren.

### T9 - Kontrol Konkurensi

Kontrol konkurensi utama pada project ini adalah kombinasi unique constraint,
transaksi, dan `INSERT ... ON CONFLICT DO NOTHING`. Strategi ini lebih aman
daripada mengecek terlebih dahulu apakah event ada, lalu melakukan insert.
Pola check-then-insert dapat mengalami race condition ketika dua worker membaca
data kosong secara bersamaan dan sama-sama mencoba insert.

Dengan atomic upsert, database menjadi arbiter konkurensi. Jika banyak worker
memproses event dengan `(topic, event_id)` yang sama, hanya satu worker yang
berhasil mendapatkan `RETURNING id`. Worker lain menerima hasil kosong dan
menandai event sebagai duplicate. Uji `/demo/concurrency` membuktikan hal ini:
25 request paralel menghasilkan `unique_processed = 1` dan
`duplicate_dropped = 24`. Ini menunjukkan bahwa sistem bebas dari
double-processing untuk event yang sama. Menurut Coulouris et al. (2012),
kontrol konkurensi diperlukan agar operasi simultan tidak menghasilkan state
yang inkonsisten.

### T10 - Orkestrasi, Keamanan Lokal, Persistensi, Observability

Docker Compose digunakan sebagai mekanisme orkestrasi lokal. Compose
menjalankan `aggregator`, `consumer`, `publisher`, `broker`, dan `storage` dalam
satu network internal. Hanya `aggregator` yang diekspos ke host melalui port
`8080`, sedangkan Redis dan PostgreSQL tetap internal. Ini memenuhi kebutuhan
keamanan lokal karena tidak ada akses ke layanan eksternal publik untuk jalur
runtime utama.

Persistensi dijaga dengan named volume `pg_data` untuk PostgreSQL dan
`broker_data` untuk Redis. Demo recreate membuktikan bahwa statistik dan data
tetap ada setelah container dihentikan dan dibuat ulang. Observability
disediakan melalui `/health`, `/events`, `/stats`, serta logging consumer yang
mencatat jumlah event diterima, unik, dan duplicate. Web API FastAPI dan
Swagger UI juga memudahkan demonstrasi dan validasi sistem. Aspek orkestrasi,
storage, keamanan, dan observability ini terkait dengan pembahasan sistem
berbasis web dan koordinasi komponen pada sistem terdistribusi (Coulouris et
al., 2012).

## Kesimpulan

Project ini memenuhi tujuan utama UAS Pub-Sub Log Aggregator. Sistem berjalan
sebagai multi-service Docker Compose, menerima event melalui API, menggunakan
Redis Stream sebagai broker internal, dan menyimpan event unik ke PostgreSQL.
Deduplication dilakukan secara persisten dengan unique constraint
`(topic, event_id)`, sedangkan transaksi dan atomic upsert mencegah race
condition saat worker paralel memproses event yang sama.

Hasil pengujian menunjukkan sistem mampu memproses 20.000 event dengan 30%
duplikasi, menghasilkan 14.000 event unik dan 6.000 duplicate dropped, dengan
throughput sekitar 938,75 event/detik. Uji persistensi juga menunjukkan data
tetap ada setelah container recreate. Automated tests sebanyak 20 test berjalan
berhasil. Dengan demikian, implementasi ini sudah mendemonstrasikan konsep
idempotent consumer, persistent deduplication, transaksi, kontrol konkurensi,
reliability, dan observability pada sistem terdistribusi.

## Link Repository dan Video

Repository GitHub:

```text
https://github.com/spirinity/uas-sister
```

Video demo:

```text
https://youtu.be/HZQJCX7mDVI
```

## Referensi

Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). _Distributed
systems: Concepts and design_ (5th ed.). Addison-Wesley.

PostgreSQL Global Development Group. (2026). _PostgreSQL documentation: INSERT_.
https://www.postgresql.org/docs/current/sql-insert.html

Redis Ltd. (2026). _Redis documentation: Streams_. https://redis.io/docs/latest/develop/data-types/streams/
