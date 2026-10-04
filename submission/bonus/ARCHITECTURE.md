# Architecture Brief — LLM Observability ở quy mô 1B requests/ngày

**Topic A** · Phan Van Nghi · 2A202602632 · K4-Track02-Day18

> Các số liệu quy mô là đầu vào của đề bài. Đơn giá cloud dùng mức tham chiếu AWS us-east-1 on-demand (S3 Standard $0,023/GB-tháng, S3 Standard-IA $0,0125/GB-tháng với thời gian lưu tối thiểu 30 ngày, EBS gp3 $0,08/GB-tháng, PUT $0,005/1K, GET $0,0004/1K, Graviton ≈ $0,04/vCPU-giờ). Tỷ lệ nén và tốc độ redaction là **giả định** phải đo lại trong MVP (mục 6).

---

## 1. Problem statement

Một foundation-model API log mọi request/response: **1B req/ngày × ~5 KB = 5 TB/ngày raw**. Trung bình 11,6K req/s; tôi giả định peak gấp 3, tức **~35K req/s ≈ 175 MB/s**.

Bốn yêu cầu:

1. Dashboard cost & latency theo **tenant**, refresh **mỗi 5 phút**.
2. Prompt/response đầy đủ giữ **7 ngày** để incident review. Sau đó **chỉ còn aggregates trong 1 năm**.
3. **PII phải được redact trước khi bất kỳ ai đọc.**
4. Tổng chi phí storage **≤ $5K/tháng**.

Bài toán khó vì ba lực kéo ngược nhau:
- **Freshness 5 phút** kéo về phía commit dày, dẫn tới small files và metadata phình.
- **PII + retention 7 ngày** đòi xóa *vật lý*. Nhưng như NB6 và NB8 đã đo, time travel, tombstone và snapshot cũ vẫn giữ byte trên đĩa sau khi "xóa".
- **Budget**: giữ 1 năm raw là 1.825 TB × $23 ≈ **$42K/tháng**, gấp 8,4 lần cap. Retention và tiering là quyết định gánh tải chính, không phải tối ưu phụ.

---

## 2. Architecture diagram

```text
                 ┌────────────────────── CONTROL PLANE ──────────────────────┐
                 │ Iceberg REST catalog (Apache Polaris)                       │
                 │  • RBAC: role `incident_reviewer` mới đọc được bronze.*     │
                 │  • vend credential S3 ngắn hạn theo table (không IAM rộng)  │
                 │  • snapshot summary: flink checkpoint-id, redactor_version  │
                 └──────────────▲───────────────────────────▲──────────────────┘
                                │ commit/plan               │ plan + creds
 API gateways                   │                           │
 (1B req/day) ──► Kafka ──► [Redactor + Bronze writer] ──► BRONZE  (Iceberg, S3 Standard)
   5 TB/ngày      zstd,       Flink, 24 vCPU autoscale       llm_calls_redacted
                  RF=3,       • PII → HMAC token (vault)     partition: day(ts)   ← hidden partitioning
                  TTL 24h     • commit mỗi 60 s, 32 writer   sort/cluster: tenant_id (compaction hằng ngày) ← Z-order
                              exactly-once (Flink ckpt)      TTL 7 ngày: drop partition → expire_snapshots
                                                             → orphan sweep (age guard 24 h) ← maintenance pair
                                       │ incremental snapshot read (60 s)
                                       ▼
                               SILVER (Iceberg) llm_calls_flat: 1 dòng/request_id (dedup),
                               cột có kiểu, KHÔNG có prompt/response; TTL 8 ngày ← medallion + dedup
                                       │
                                       ▼
                               GOLD (Iceberg) tenant_model_5min: count, tokens, cost_usd,
                               error/rate_limited, KLL sketch latency; partition month(bucket_ts),
                               sort tenant_id; giữ 13 tháng ← time travel cho rollback
                                       │
       Dashboards (Trino, 16 vCPU) ◄───┘   query p95 < 2 s, chỉ đọc Gold
       Incident review (Trino, role riêng) ──► Bronze, filter tenant_id + ts → prune 1/N file
```

**Day18 concepts trong diagram, mỗi cái gắn với một lựa chọn cụ thể:**
- (1) Medallion: Bronze đã redact, Silver dedup không chứa payload, Gold aggregate.
- (2) Catalog là control plane: RBAC và credential vending tách người đọc payload khỏi người đọc metrics.
- (3) Hidden partitioning + clustering theo tenant cho hot path "filter by tenant".
- (4) Maintenance pair: expiry + orphan sweep để xóa vật lý PII và kiểm soát bill.
- (5) Time travel và snapshot rollback cho Gold khi tính sai.
- (6) FinOps: phép tính chi phí ở mục 5 quyết định tier, retention và trigger interval.

---

## 3. Quyết định chính và alternatives đã loại

### D1 — Table format: **Apache Iceberg**
- **Chọn Iceberg** vì ba điểm hệ thống này dùng trực tiếp:
  - *Hidden partitioning* `day(ts)`: incident reviewer lọc `ts BETWEEN …` và tự được prune, không phải nhớ cột `dt`. NB5 đo được 10 → 1 file.
  - *Partition evolution*: khi số tenant tăng, có thể thêm `bucket(64, tenant_id)` mà không rewrite 7 TB. NB5 đã cho thấy 2 spec cùng tồn tại.
  - *REST catalog* chuẩn cho Trino, Flink và DuckDB cùng lúc.
- **Loại Delta Lake.**
  - Delta partition theo cột vật lý. Đổi partition scheme phải rewrite toàn bộ bảng, và người dùng phải tự lọc cột partition dẫn xuất. Đúng loại lỗi "quên `WHERE dt=`" mà NB5 ước tính ~$220/ngày ở 10K query.
  - CDF của Delta là ưu điểm thật, nhưng hệ thống này không cần propagate delete ra index ngoài.
- **Loại Parquet/JSON thô trên S3 + Athena (Hive-style).**
  - Không có ACID: dashboard có thể đọc file đang ghi dở.
  - Không có schema enforcement.
  - Không xóa theo predicate được nếu không tự viết lại file.
  - Không có snapshot để rollback khi tính sai.

### D2 — Catalog: **Apache Polaris (Iceberg REST)**
- **Chọn Polaris.** Nó vend credential S3 ngắn hạn theo table và role. Vì vậy chỉ role `incident_reviewer` (≈ 10 người, có audit) chạm được `bronze.*`, còn 200+ người dùng dashboard chỉ có quyền trên `gold.*`. Yêu cầu 3 được thực thi bởi control plane, không bởi quy ước.
- **Loại Hive Metastore.** Không vend credential, nên engine nào cũng cần IAM đọc toàn bucket. Một notebook bị lộ key là đọc được mọi payload. HMS cũng không hiểu commit Iceberg nguyên tử nếu không có lock bổ sung.
- **Loại AWS Glue + Lake Formation.**
  - Làm được RBAC, nhưng khóa vào một cloud.
  - DuckDB/Flink ngoài AWS phải đi đường riêng.
  - Quota API `GetTable`/`UpdateTable` dễ chạm trần khi writer commit mỗi 60 s và 500 panel refresh mỗi 5 phút.
  - Đây là đánh đổi chấp nhận được nếu tổ chức đã all-in AWS. Tôi giả định không phải vậy.

### D3 — PII: **tokenize/redact trong stream trước khi chạm Bronze**
- **Chọn redact ngay trong stream.** Flink job chạy detector (regex + từ điển cho email, số điện thoại VN `(\+84|0)\d{9,10}`, CCCD 12 số, số thẻ có kiểm Luhn, API key pattern):
  - Thay giá trị bằng `HMAC-SHA256(key_v, value)`, nên vẫn join hoặc đếm được theo token.
  - Mapping ngược nằm trong vault với TTL 7 ngày.
  - Commit ghi `redactor_version` vào snapshot properties.
  - Raw chỉ tồn tại trong Kafka (mã hóa, TTL 24 h, không ai có quyền consume ngoài redactor).
- **Loại "land raw ở Bronze rồi redact ở Silver".**
  - Vi phạm yêu cầu 3: file raw đã nằm trên S3 và đọc được qua storage credential.
  - Kể cả khi xóa, snapshot cũ vẫn giữ byte cho tới khi expire + sweep (NB6, NB8 đo đúng hiện tượng này).
- **Loại chỉ mã hóa cột (column-level encryption).** Ai có key vẫn đọc được plaintext. Key thường được cấp rộng cho job ETL, và một lần `decrypt()` trong notebook là lộ. Mã hóa bảo vệ khi bị đánh cắp file, không bảo vệ khỏi người dùng hợp lệ đọc quá quyền.
- **Loại bỏ hẳn prompt/response.** Không đáp ứng incident review 7 ngày, vốn là lý do tồn tại của hệ thống.

### D4 — Layout: **partition `day(ts)` + cluster theo `tenant_id` khi compaction**
- **Chọn layout này.**
  - Writer streaming ghi theo thời gian.
  - Job compaction hằng ngày (cho ngày D-1) rewrite thành file ~256 MB, sắp theo `tenant_id, ts`.
  - Với ~1 TB/ngày nén, mỗi ngày có ~4.000 file. Query một tenant-ngày chạm ~1–3 file nhờ min/max `tenant_id`, tương tự NB2: 1/55 file chứa `user_id` mục tiêu.
- **Loại partition theo `tenant_id`.**
  - 10K tenant × 1.440 commit/ngày tạo ra hàng triệu file tí hon mỗi ngày (small-file anti-pattern).
  - Tenant phân bố lệch (top 1% tenant chiếm phần lớn traffic) nên partition lệch nặng.
- **Loại partition theo giờ không clustering.**
  - Mỗi file giờ chứa mọi tenant, nên min/max `tenant_id` của mọi file xấp xỉ toàn dải.
  - Query 1 tenant phải đọc hết. Đây là đúng trạng thái "trước Z-order" của NB2 (đọc 200/200 file).

### D5 — Retention & tiering: **TTL 7 ngày trên S3 Standard, xóa qua metadata rồi sweep**
- **Chọn quy trình xóa 3 bước, chạy hằng ngày lúc 02:00 UTC:**
  1. `DELETE WHERE ts < now() - 7d` (drop nguyên partition ngày, thao tác metadata).
  2. `expire_snapshots(older_than = 24h)`.
  3. Orphan sweep: file trên S3 − file mọi snapshot còn sống tham chiếu, với age guard 24 h. NB6 cho thấy expiry một mình **không** xóa file vật lý.
- **Loại S3 lifecycle rule xóa object theo tuổi ngay dưới prefix bảng.**
  - S3 xóa file mà metadata vẫn tham chiếu, nên query time-travel hoặc snapshot chưa expire gặp `FileNotFound`.
  - Manifest cũng phình mãi vì catalog không biết file đã mất.
- **Loại tier Standard-IA/Glacier cho Bronze.** IA tính tối thiểu 30 ngày:
  - Mỗi TB nằm 7 ngày trên IA vẫn trả 30 × $12,5/30 = **$12,5/TB**.
  - Trên Standard chỉ trả $23 × 7/30 = **$5,4/TB**.
  - Glacier còn có phí retrieval và độ trễ phút–giờ, không hợp với incident review.
  - Điểm hòa vốn: $23 × d/30 = $12,5, tức d ≈ 16 ngày, chưa tính phí retrieval IA $0,01/GB. Bronze sống 7 ngày nên Standard rẻ hơn 2,3 lần.
  - Chỉ Gold (sống 13 tháng) mới qua ngưỡng hòa vốn, nhưng Gold chỉ ~0,67 TB: IA tiết kiệm (23 − 12,5) × 0,67 ≈ $7/tháng. Không đáng thêm một lifecycle rule phải đồng bộ với metadata.

### D6 — Percentile ở Gold: **lưu KLL sketch theo (tenant, model, 5 phút)**
- **Chọn KLL sketch.** Mỗi dòng Gold có `count, sum_tokens, cost_usd, n_error, n_rate_limited` và một **KLL sketch** latency khoảng 300 B. Dashboard roll-up 5 phút → giờ → ngày → tháng bằng cách merge sketch, nên p50/p95 theo năm vẫn tính được khi Silver đã bị xóa sau 7 ngày.
- **Loại lưu sẵn p50/p95 dạng số.** Percentile không cộng được: trung bình các p95 5 phút không phải p95 của ngày. Dashboard 1 năm sẽ sai một cách có hệ thống.
- **Loại tính từ Silver mỗi lần refresh.** Silver chỉ sống 8 ngày. Quét 60 GB/ngày mỗi 5 phút (288 lần/ngày) là ~17 TB scan/ngày cho một con số đã biết trước.
- **Bài học từ NB4.**
  - `error_rate` tách riêng `rate_limited` khỏi `error`: quota khác lỗi model.
  - Bucket thời gian tính theo **UTC** (`ts AT TIME ZONE 'UTC'`), không phụ thuộc TimeZone của session. Trong NB4, timezone UTC+7 của máy đã biến 7 ngày dữ liệu thành 8 ngày lẻ.

### D7 — Ingestion & freshness: **Kafka → Flink, commit Iceberg mỗi 60 s, exactly-once**
- **Chọn kiến trúc này.**
  - Flink checkpoint mỗi 60 s. Iceberg sink commit đúng một snapshot cho mỗi checkpoint thành công và ghi `flink.job-id` + `flink.max-committed-checkpoint-id` vào snapshot summary.
  - Offset Kafka nằm trong checkpoint state của Flink, không nằm trong bảng. Khi restart từ checkpoint N, committer thấy checkpoint ≤ N đã có trong bảng và bỏ qua, nên không ghi trùng. Khi khởi động lại, Kafka source tua về offset của checkpoint N.
  - Job tự thêm `redactor_version` vào snapshot summary để phục vụ F1.
  - Bronze có 32 writer subtask (song song theo partition Kafka). Mỗi subtask ghi ≥ 1 file mỗi checkpoint, nên mỗi ngày có 32 × 1.440 ≈ **46K file** trước compaction. Con số này dùng ở F4 và trong phép tính PUT.
  - Gold aggregator đọc incremental snapshot mỗi 60 s.
  - Độ trễ cuối-cuối ≈ 60 s (Bronze) + 60 s (Gold) + ≤ 5 phút (refresh dashboard), đáp ứng yêu cầu 5 phút với p95 lag dữ liệu ≤ 3 phút.
- **Loại API server ghi trực tiếp vào bảng.** Hàng nghìn writer cùng commit gây xung đột optimistic concurrency liên tục (retry storm). Mỗi writer còn ghi file nhỏ riêng, và không có back-pressure khi S3 chậm.
- **Loại batch 5 phút từ file dump.** Batch 5 phút + aggregate + refresh 5 phút cho lag tới ~12 phút, vượt SLA. Commit 60 s là điểm cân bằng: 1.440 commit/ngày vẫn quản lý được bằng compaction và manifest rewrite hằng ngày.

---

## 4. Failure modes (03:00 sáng)

| # | Sự cố | Phát hiện | Rollback / khắc phục |
|---|---|---|---|
| F1 | **Redactor regression**: bản deploy mới bỏ sót số điện thoại dạng `+84 9x xxx xxxx` có khoảng trắng, nên PII lọt vào Bronze | **Canary**: mỗi phút bơm 100 request tổng hợp chứa PII đã biết vào Kafka. Một job quét Bronze tìm đúng các giá trị đó; ≥ 1 hit thì page on-call. Thêm scanner mẫu 0,1% dòng với detector chặt hơn. | (a) Dừng writer và pin `redactor_version` cũ. (b) Từ snapshot properties, xác định dải snapshot có `redactor_version = bad`. (c) `rewrite` các file trong dải ts đó qua redactor đã sửa. (d) **Bắt buộc** `expire_snapshots` ngay (bỏ qua retention 24 h) và orphan sweep. Nếu không, time travel vẫn trả PII. Position/deletion-vector delete **không** đủ vì file gốc vẫn nằm trên đĩa. (e) Kiểm chứng bằng cách quét **file vật lý** trên S3, không qua table API. |
| F2 | **Retention job xóa sai ngày**: lỗi timezone khiến `now() - 7d` tính theo UTC+7, xóa nhầm 7 giờ của ngày D-6 | Pre-check trước DELETE: số dòng dự kiến xóa phải ≈ 1/7 bảng (±20%) và `max(ts)` của phần xóa < `now() - 7d`, nếu không thì abort. Sau xóa, so số dòng mỗi ngày với Gold `count`. | Vì expiry chạy sau DELETE **24 h**, snapshot trước DELETE vẫn còn: `rollback_to_snapshot(id_before_delete)`. Đây là lý do không đặt `older_than = 0` (NB6: retention 0 làm mất time travel ngay). |
| F3 | **Upstream đổi schema**: gateway đổi `usage.input` thành `usage.input_tokens` | Writer Bronze vẫn chạy vì payload redacted là cột JSON, nhưng Silver parse ra NULL. Data-quality check: tỷ lệ NULL của `prompt_tokens` > 1% trong 5 phút, hoặc `cost_usd` của tenant lớn giảm > 50% so với cùng giờ tuần trước, thì alert. | Rollback Gold về snapshot trước sự cố (time travel). Sửa parser và chuyển schema evolution sang opt-in qua data contract (NB1: enforcement mặc định, evolution có chủ đích). Recompute Gold cho cửa sổ lỗi từ Silver/Bronze, vẫn còn vì < 7 ngày. |
| F4 | **Compaction chết 2 ngày**, small files tích tụ (~46K file/ngày) | Metric `avg_file_size(bronze, day) < 64 MB` và `planning_time_p95 > 2 s` thì alert. Theo dõi số manifest. | Compaction idempotent: chạy catch-up cho từng ngày bị thiếu. Dashboard không bị ảnh hưởng vì chỉ đọc Gold. Nếu quá tải, tạm tăng trigger lên 120 s (đánh đổi freshness lấy số file). |

---

## 5. Chi phí back-of-envelope

**Giả định nén:**
- Payload JSON văn bản → Parquet zstd **5:1**, nên Bronze ≈ 1 TB/ngày.
- Kafka producer zstd **4:1**, nên Kafka ≈ 1,25 TB/ngày.
- Silver ≈ 60 B/dòng nén, tức 60 GB/ngày.
- Gold: 10K tenant × 2 model hoạt động × 288 bucket/ngày = 5,76M dòng × 300 B ≈ 1,7 GB/ngày.

### Storage (yêu cầu ≤ $5K/tháng)

| Thành phần | Phép tính | $/tháng |
|---|---|---:|
| Kafka (EBS gp3) | 1,25 TB/ngày × 1 ngày × RF 3 = 3,75 TB, cấp dư 50% → 5,6 TB × $80/TB | 450 |
| Bronze (S3 Standard) | 7 ngày live + 1 ngày file tombstone chờ expiry + 1 ngày trễ sweep = 9 TB × $23 | 207 |
| Silver | 60 GB × 8 ngày = 0,48 TB × $23 | 11 |
| Gold (13 tháng) | 1,7 GB × 395 ngày ≈ 0,67 TB × $23 | 15 |
| Metadata Iceberg (~1%) | ~0,1 TB × $23 | 3 |
| S3 PUT | (46K stream = 32 writer × 1.440 commit + 4K compaction + 10K Silver/Gold)/ngày × 30 = 1,8M × $0,005/1K | 9 |
| S3 GET | 500 panel × 288 refresh × ~20 GET = 2,9M/ngày × 30 = 86M × $0,0004/1K | 35 |
| **Tổng storage** | | **≈ $730** |

Tổng ≈ **15% cap**, còn dư 6,8 lần. Kiểm tra độ nhạy:
- Nếu nén chỉ đạt **2:1** thì Bronze = 2,5 TB/ngày × 9 = 22,5 TB → $518 và tổng ≈ $1.040, vẫn dưới cap.
- Cap chỉ bị phá nếu đổi retention: giữ Bronze nén 1 năm = 365 TB × $23 = **$8.400**. Tức là TTL 7 ngày + orphan sweep mới là thứ giữ ngân sách.

### Compute (ngoài cap storage, liệt kê để đánh giá khả thi)

| Thành phần | Phép tính | $/tháng |
|---|---|---:|
| Kafka brokers | 3 × 4 vCPU × 730 h × $0,04 | 350 |
| Redactor + Bronze writer | Peak 175 MB/s ÷ ~10 MB/s/vCPU ≈ 18 vCPU; autoscale trung bình 12 vCPU × 730 × $0,04 | 350 |
| Silver/Gold streaming | 8 vCPU × 730 × $0,04 | 234 |
| Compaction + expiry + sweep | Đọc + ghi 2 TB/ngày ở ~50 MB/s/vCPU ≈ 11 vCPU-h, ×2 cho sort ≈ 22 vCPU-h/ngày × 30 × $0,04 | 27 |
| Trino (dashboard + incident review) | 16 vCPU × 730 × $0,04 | 467 |
| **Tổng compute** | | **≈ $1.430** |

**Tổng hệ thống ≈ $2,2K/tháng.** Chi phí cho một query incident review: một tenant-ngày, nhờ clustering chỉ đọc ~1–3 file × 256 MB ≈ 0,5 GB thay vì 1 TB nếu không prune.

---

## 6. MVP một tuần

**Slice:** pipeline end-to-end ở **1% quy mô** (10M req/ngày ≈ 116 req/s). Chạy trên 1 máy: Redpanda, Flink local, MinIO, Polaris, Trino, cùng generator request tổng hợp có PII được cài sẵn.

| Ngày | Việc |
|---|---|
| 1 | Generator: 10K tenant (phân bố Zipf), 3 model, payload 5 KB, cài **10.000 giá trị PII** đã biết (email, SĐT VN có và không khoảng trắng, CCCD, số thẻ). Dựng Kafka, MinIO, Polaris. |
| 2 | Redactor + writer Bronze (checkpoint/commit 60 s, `redactor_version` trong snapshot summary). Kill job giữa chừng để kiểm tra không có dòng trùng `request_id`. Đo MB/s/vCPU thật để thay giả định 10 MB/s. |
| 3 | Silver dedup + Gold 5 phút với KLL sketch; bucket theo UTC. Dashboard query p95 trong Trino. |
| 4 | Compaction + clustering theo tenant; job retention 3 bước; RBAC Polaris (2 role). |
| 5 | Chạy các bài kiểm chứng nghiệm thu, đo tỷ lệ nén thật, cập nhật bảng chi phí. |

**Tiêu chí nghiệm thu**

1. **0/10.000** giá trị PII cài sẵn xuất hiện khi quét **file Parquet vật lý** trong bucket Bronze (quét byte trực tiếp, không qua table API).
2. Lag event → Gold p95 ≤ 5 phút trong 24 h chạy liên tục.
3. Query 1 tenant-ngày: số file đọc / tổng file ≤ 1/10 (đo bằng `plan_files()`, như NB5).
4. Sau retention: dữ liệu ngày D-8 có **0 byte** trên MinIO (so file listing với manifest), Gold vẫn đọc đủ 8 ngày. Role `dashboard` bị từ chối khi đọc `bronze.*`.
5. Mô phỏng F1: deploy redactor lỗi trong 10 phút, chạy runbook. Kết quả là 0 hit PII trong file vật lý **và** trong mọi snapshot còn sống.

**Cơ chế khó nhất và cách kiểm chứng:** xóa vật lý PII xuyên qua time travel. Đó là F1 cộng tiêu chí 1 và 4.
- Rủi ro thật không phải redact sai một lần mà là tin rằng đã xóa. NB6 cho thấy `expire_snapshots` của PyIceberg để lại file; NB8 cho thấy version cũ vẫn chứa subject đã xóa.
- Vì vậy bài kiểm tra phải đọc **storage**, không đọc **table**: liệt kê mọi object trong prefix và quét byte tìm 10.000 giá trị PII cài sẵn.

---

## Nguồn tham khảo

- Lab Day18 (repo này): NB1 enforcement/evolution; NB2 Z-order pruning; NB4 medallion và bài học timezone; NB5 hidden partitioning, partition evolution, catalog; NB6 maintenance (expiry ≠ xóa vật lý, orphan sweep); NB8 time travel và xóa subject.
- Apache Iceberg spec: partition transforms, partition evolution, snapshot expiry, `remove_orphan_files`. https://iceberg.apache.org/spec/
- Apache Polaris (Iceberg REST catalog, credential vending). https://polaris.apache.org/
- Apache DataSketches, KLL sketch. https://datasketches.apache.org/
- AWS S3 pricing và minimum storage duration của Standard-IA. https://aws.amazon.com/s3/pricing/
