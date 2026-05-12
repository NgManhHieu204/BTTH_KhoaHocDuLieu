import pandas as pd
import time
import glob
import os

# Ghi nhận thời gian bắt đầu
start_time = time.time()

try:
    # ĐỌC DỮ LIỆU (Eager Execution)

    # Xác định đường dẫn thư mục hiện tại của file
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Lùi lại rồi đi vào thư mục 'data'
    data_path = os.path.join(script_dir, "..", "data", "*.parquet")

    # Tìm các file parquet
    files = glob.glob(data_path)
    print(f"Đang đọc {len(files)} file dữ liệu từ data...")

    # Đọc và gộp
    pdf_list = [pd.read_parquet(f) for f in files]
    pdf = pd.concat(pdf_list, ignore_index=True)

    # LÀM SẠCH VÀ CHUYỂN ĐỔI (Feature Engineering)
    # Tính thời gian di chuyển bằng phút
    pdf['trip_duration_mins'] = (pdf['tpep_dropoff_datetime'] - pdf['tpep_pickup_datetime']).dt.total_seconds() / 60

    # Trích xuất giờ đón khách
    pdf['pickup_hour'] = pdf['tpep_pickup_datetime'].dt.hour

    # TỔNG HỢP DỮ LIỆU (Aggregation)
    # Nhóm theo giờ, tính trung bình doanh thu và thời gian
    hourly_stats = pdf.groupby('pickup_hour').agg(
        avg_revenue=('total_amount', 'mean'),
        avg_duration=('trip_duration_mins', 'mean'),
        total_trips=('VendorID', 'count')
    ).reset_index()

    # Sắp xếp kết quả
    hourly_stats = hourly_stats.sort_values(by='pickup_hour')

    # Ghi nhận thời gian kết thúc
    end_time = time.time()

    print("\nKết quả tổng hợp (Hiển thị 5 dòng đầu):")
    print(hourly_stats.head())
    
    print(f"\nThời gian xử lý bằng Pandas: {end_time - start_time:.2f} giây")

except FileNotFoundError:
    print(f"Lỗi: Không tìm thấy file {file_path}. Vui lòng tải dữ liệu trước.")
except Exception as e:
    print(f"Lỗi trong quá trình xử lý: {e}")