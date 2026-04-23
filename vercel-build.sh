#!/bin/bash
# 1. Tải trình cài đặt
wget -q https://vnstocks.com/files/vnstock-cli-installer.run -O installer.run
chmod +x installer.run

# 2. ÉP cài đặt vào thư mục tên là 'vnstock_lib' ngay tại dự án
./installer.run -- --non-interactive --api-key "$VNSTOCK_API_KEY" --venv-path "./vnstock_lib"

# 3. Cài các thứ còn lại
pip install -r requirements.txt
