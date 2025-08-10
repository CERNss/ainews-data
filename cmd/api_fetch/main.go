package main

import (
	"context"
	"net/http"
	"time"

	"api-fetch/internal/api_fetch/api"
	"api-fetch/internal/api_fetch/helper"
	"api-fetch/internal/api_fetch/scheduler"
	"api-fetch/pkg/mongodb"
)

func main() {
	ctx := context.Background()

	if err := helper.ConfigureTimeLocation("Asia/Shanghai"); err != nil {
		panic(err) // 或者日志+退出
	}

	cfg, err := mongodb.LoadConfig("config/1-config.yaml")
	if err != nil {
		panic(err)
	}

	stores := helper.MustMongo(
		ctx,
		cfg.Mongo.Host,
		cfg.Mongo.DBName,
		cfg.Mongo.Username,
		cfg.Mongo.Password,
		cfg.Mongo.AuthSource,
	)

	// 2) 启动最小定时任务（写死：每 1 分钟跑一次）
	worker := &scheduler.Worker{
		Stores:     stores,
		HTTPClient: &http.Client{Timeout: 10 * time.Second},
	}
	go worker.Run(context.Background())

	// 3) 起 HTTP API
	srv := &api.Server{Stores: stores}
	r := srv.Router()
	_ = r.SetTrustedProxies(nil)
	_ = r.Run(":8080")
}
