# Thông tin bài nộp

| Mục | Giá trị |
|---|---|
| Họ tên | Phan Van Nghi |
| MSSV | 2A202602632 |
| Mã bài | K4-Track02-Day18 — Lakehouse Lab |
| Repo | https://github.com/vannghj/K4-Track02-Day18-PhanVanNghi-2A202602632-Lakehouse-Lab |
| Đường chạy | **Lightweight** cho cả 8 notebook (không dùng Spark cho NB1–NB4) |
| Python | 3.12.14 (`.venv`) |
| Hệ điều hành | macOS 27.0, Apple silicon (arm64) |
| Thư viện chính | deltalake 1.6.6 · pyiceberg 0.12.0 · duckdb 1.5.6 · polars 1.44.2 · pyarrow 25.0.1 · numpy 2.5.3 |
| Bonus | Có — `submission/bonus/ARCHITECTURE.md` (topic A: LLM observability 1B req/ngày) |

## Kết quả kiểm tra

| Lệnh | Kết quả |
|---|---|
| `make smoke` | 9/9 PASS |
| `make test` | 24/24 PASS |
| `make run-all` | 8/8 PASS |

## Nội dung bài nộp

- `notebooks/` — 8 notebook đã thực thi trong Jupyter kernel `.venv`, giữ output. Mỗi notebook có cell bằng chứng bổ sung và phần **📝 Kết quả & giải thích** ở cuối.
- `screenshots/` — ảnh render từ output đã lưu của từng notebook:

| Notebook | Ảnh |
|---|---|
| NB1 | `nb01_delta_log.png` (commit JSON + schema), `nb01_schema_enforcement.png` (bad write bị chặn, `tier`) |
| NB2 | `nb02_optimize.png` (trước/sau, speedup, pruning), `nb02_file_stats.png` (min/max `user_id`) |
| NB3 | `nb03_history_restore.png` |
| NB4 | `nb04_gold.png` |
| NB5 | `nb05_iceberg.png` |
| NB6 | `nb06_delta_jobs.png`, `nb06_iceberg_jobs.png` |
| NB7 | `nb07_vectors.png` |
| NB8 | `nb08_agents.png` |

- `REFLECTION.md` — anti-pattern lakehouse.
- `bonus/ARCHITECTURE.md` — architecture brief.

## Thay đổi so với notebook gốc

Các cell dưới đây chỉ được **thêm** vào bản nộp trong `submission/notebooks/`, không sửa logic hay hạ ngưỡng nào của notebook gốc. Mã nguồn đề bài (`notebooks/*.py`, `scripts/`, `tests/`) giữ nguyên, nên `make test` và `make run-all` chạy trên đề gốc.

- NB1: in schema sau evolution và nội dung 2 commit JSON.
- NB3: đếm `score < 0` ở v2, v3 và version hiện tại.
- NB4: in vị trí 3 bảng, số dòng theo ngày, giá trị `status`, Gold đầy đủ 24 dòng, và assert mọi điều kiện Gold. Notebook gốc chưa assert các điều kiện này.
- NB6: in danh sách checkpoint, `_last_checkpoint`, số data file trên đĩa so với trong log, và số dòng còn đọc được.
- NB8: so sánh số dòng của subject ở từng version bằng time travel.
