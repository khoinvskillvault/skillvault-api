#!/bin/bash
# 1. Tải trình cài đặt từ Vnstock
wget -q https://vnstocks.com/files/vnstock-cli-installer.run -O installer.run
chmod +x installer.run

# 2. Chạy cài đặt không tương tác bằng API Key đã thiết lập ở Bước 1
./installer.run -- --non-interactive --api-key "$VNSTOCK_API_KEY"

# 3. Tiếp tục cài đặt các thư viện còn lại
pip install -r requirements.txt
