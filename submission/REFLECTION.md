# Reflection — Small files không có maintenance

**Anti-pattern:** ingest streaming thành vô số file nhỏ nhưng không lên lịch compaction, expiry và orphan cleanup.

**Vì sao hệ thống tôi quan tâm dễ vướng?**
- Tôi quan tâm hệ thống LLM observability: log mọi request/response để theo dõi latency, chi phí và lỗi theo tenant.
- Để dashboard tươi, người ta hay commit mỗi vài giây, nên mỗi ngày sinh hàng chục nghìn file nhỏ.
- Lab cho thấy cái giá:
  - NB2: point query trên 200 file nhỏ chậm hơn 10,8× so với sau OPTIMIZE + Z-order.
  - NB5: metadata lớn hơn data (290%).
- Tệ hơn, xóa prompt nhạy cảm không tự giải phóng storage:
  - VACUUM của delta-rs bỏ sót orphan.
  - `expire_snapshots` của PyIceberg chỉ sửa metadata.
  - Version cũ vẫn giữ dữ liệu (NB6, NB8).

**Cách phòng tránh**
- Trigger interval ≥ 1 phút, target file 128–512 MB.
- Compaction + clustering theo `tenant_id` hằng ngày.
- Ghép expiry với orphan sweep có age guard.
- Checkpoint log định kỳ.
- Giám sát kích thước file trung bình như một SLO.
