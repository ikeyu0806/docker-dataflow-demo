# data-platform
ローカルでdockerからGCPの開発用プロジェクトに接続して開発する手順です。

```
# setup
# 事前にサービスアカウントの認証キーをダウンロードしてルート直下にcredentials.jsonという名前で保存する必要があります。
cp .env.example .env
docker-compose build
```

```
# jobsの実行
docker-compose run data_platform jobs/write_to_pubsub_demo/main.py
```

## 認証設定
GCP管理画面でサービスアカウントのjsonをダウンロードしてルート直下にcredentials.jsonとして配置してください。
