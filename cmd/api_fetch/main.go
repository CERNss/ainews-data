package main

import (
	"api-fetch/internal/api_fetch/api"
	"api-fetch/internal/api_fetch/helper"
	"api-fetch/internal/api_fetch/scheduler"
	"api-fetch/internal/middleware/logger"
	"api-fetch/pkg/mongodb"
	"context"
	"go.uber.org/zap"
	"net/http"
	"time"
)

func main() {

	log, err := logger.NewLogger()
	if err != nil {
		panic(err)
	}

	defer func(log *zap.Logger) {
		err := log.Sync()
		if err != nil {

		}
	}(log)

	ctx := context.Background()

	log.Info("Starting API Fetch Service...")
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

	worker := scheduler.NewScheduler(
		log,
		stores,
		&http.Client{Timeout: 10 * time.Second},
	)
	worker.Run(ctx)

	//dp := processor.NewDataProcessor(log, stores)
	//dp.Run(ctx, []processor.DataProcessorConfig{
	//	{
	//		Source:   "澎湃",
	//		Category: "general",
	//		InfoType: "daily",
	//		Enabled:  true,
	//	},
	//})

	srv := &api.Server{Stores: stores}
	r := srv.Router()
	_ = r.SetTrustedProxies(nil)
	log.Info("API Fetch Service is running", zap.String("address", ":8080"))
	_ = r.Run(":8080")
}
