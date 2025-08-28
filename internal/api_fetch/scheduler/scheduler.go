package scheduler

import (
	"api-fetch/internal/api_fetch/helper"
	"api-fetch/internal/api_fetch/model"
	"api-fetch/internal/api_fetch/processor"
	"context"
	"net/http"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
)

type Scheduler struct {
	Log           *zap.Logger
	Stores        *helper.Stores
	HTTPClient    *http.Client
	processor     *processor.Processor     // API数据获取处理器
	dataProcessor *processor.DataProcessor // 数据后处理器
	retryWg       sync.WaitGroup
}

// NewScheduler 创建新的调度器
func NewScheduler(log *zap.Logger, stores *helper.Stores, httpClient *http.Client) *Scheduler {
	scheduler := &Scheduler{
		Log:        log,
		Stores:     stores,
		HTTPClient: httpClient,
	}
	// 创建API处理器实例
	scheduler.processor = processor.NewProcessor(log, stores, httpClient)
	// 创建数据后处理器实例
	scheduler.dataProcessor = processor.NewDataProcessor(log, stores)
	return scheduler
}

// next4x 计算下一个执行时间点（0, 3, 6, 9, 12, 15, 18, 21点）
func next4x(now time.Time, loc *time.Location) time.Time {
	anchors := []int{0, 3, 6, 9, 12, 15, 18, 21}
	local := now.In(loc)
	for _, h := range anchors {
		t := time.Date(local.Year(), local.Month(), local.Day(), h, 0, 0, 0, loc)
		if !t.Before(local) {
			return t.UTC()
		}
	}
	// 都过了 -> 明天 00:00
	next := time.Date(local.Year(), local.Month(), local.Day()+1, 0, 0, 0, 0, loc)
	return next.UTC()
}

// next4xPlus15 计算数据处理时间点（next4x + 15分钟）
func next4xPlus15(now time.Time, loc *time.Location) time.Time {
	return next4x(now, loc).Add(15 * time.Minute)
}

func every5minutes(now time.Time) time.Time {
	rounded := now.Truncate(5 * time.Minute) // 将当前时间向下取整到最近的 5 分钟
	if rounded.Before(now) {
		// 如果当前时间已经超过了这个 5 分钟整点，计算下一个 5 分钟整点
		rounded = rounded.Add(5 * time.Minute)
	}
	return rounded
}

// Run 启动调度器主循环
func (s *Scheduler) Run(ctx context.Context) {
	s.Log.Info("Scheduler starting...")

	// 立即执行一次（可选）
	s.runOnce(ctx)

	// 启动数据处理调度器
	go s.runDataProcessorScheduler(ctx)

	// 主循环：API抓取调度
	shanghai, _ := time.LoadLocation("Asia/Shanghai")
	for {
		select {
		case <-ctx.Done():
			s.Log.Info("Scheduler stopping, waiting for retry goroutines to complete...")
			s.retryWg.Wait()
			s.Log.Info("Scheduler stopped")
			return
		default:
			next := next4x(time.Now(), shanghai)
			sleep := time.Until(next)
			if sleep < 0 {
				sleep = 0
			}

			s.Log.Info("API Scheduler sleeping until next execution",
				zap.Time("nextExecution", next),
				zap.Duration("sleepDuration", sleep),
			)

			timer := time.NewTimer(sleep)
			select {
			case <-ctx.Done():
				timer.Stop()
				s.Log.Info("Scheduler stopping, waiting for retry goroutines to complete...")
				s.retryWg.Wait()
				s.Log.Info("Scheduler stopped")
				return
			case <-timer.C:
				s.runOnce(ctx)
			}
		}
	}
}

// runDataProcessorScheduler 数据处理调度器
func (s *Scheduler) runDataProcessorScheduler(ctx context.Context) {
	shanghai, _ := time.LoadLocation("Asia/Shanghai")

	for {
		select {
		case <-ctx.Done():
			s.Log.Info("Data Processor Scheduler stopping")
			return
		default:
			next := next4xPlus15(time.Now(), shanghai)
			sleep := time.Until(next)
			if sleep < 0 {
				sleep = 0
			}

			s.Log.Info("Data Processor Scheduler sleeping until next execution",
				zap.Time("nextExecution", next),
				zap.Duration("sleepDuration", sleep),
			)

			timer := time.NewTimer(sleep)
			select {
			case <-ctx.Done():
				timer.Stop()
				s.Log.Info("Data Processor Scheduler stopping")
				return
			case <-timer.C:
				s.runDataProcessor(ctx)
			}
		}
	}
}

// runOnce 执行一次完整的API数据抓取流程
func (s *Scheduler) runOnce(ctx context.Context) {
	now := time.Now()
	s.Log.Info("Starting scheduled API fetch execution", zap.Time("executionTime", now))

	// 1) 读取启用的 API 配置
	cur, err := s.Stores.APIs.Find(ctx, bson.M{"enabled": true})
	if err != nil {
		s.Log.Error("Failed to find enabled APIs", zap.Error(err))
		return
	}
	defer func(cur *mongo.Cursor, ctx context.Context) {
		if err := cur.Close(ctx); err != nil {
			s.Log.Warn("Failed to close cursor", zap.Error(err))
		}
	}(cur, ctx)

	// 2) 确保当天分表存在并创建索引
	collName := helper.RawDataCollName(now)
	helper.EnsureRawDataIndexes(ctx, s.Stores.DB, collName)
	contentColl := s.Stores.DB.Collection(collName)

	// 3) 处理每个 API
	apiCount := 0
	for cur.Next(ctx) {
		var api model.APIInfo
		if err := cur.Decode(&api); err != nil {
			s.Log.Error("Failed to decode API config", zap.Error(err))
			continue
		}

		s.Log.Info("Processing API",
			zap.String("source", api.Source),
			zap.String("category", api.Category),
			zap.String("url", api.URL),
		)

		// 使用处理器进行数据抓取和保存（包含重试机制）
		s.processor.ProcessAPIWithRetry(ctx, &api, contentColl, now, &s.retryWg)
		apiCount++
	}

	s.Log.Info("Scheduled API fetch execution completed",
		zap.Time("executionTime", now),
		zap.Int("processedAPIs", apiCount),
	)
}

// runDataProcessor 执行数据后处理
func (s *Scheduler) runDataProcessor(ctx context.Context) {
	now := time.Now()
	s.Log.Info("Starting scheduled data processing execution", zap.Time("executionTime", now))

	// 配置数据处理器
	configs := []processor.DataProcessorConfig{
		{
			Source:   "澎湃",
			Category: "general",
			InfoType: "daily",
			Enabled:  true,
		},
		// 可以在这里添加更多配置
	}

	// 运行数据处理器
	s.dataProcessor.Run(ctx, configs)

	s.Log.Info("Scheduled data processing execution completed", zap.Time("executionTime", now))
}
