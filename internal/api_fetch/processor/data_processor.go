package processor

import (
	"api-fetch/internal/api_fetch/helper"
	"api-fetch/internal/api_fetch/model"
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"time"
)

// DataProcessorConfig 处理器配置
type DataProcessorConfig struct {
	Source   string `json:"source"`
	Category string `json:"category"`
	InfoType string `json:"info_type"`
	Enabled  bool   `json:"enabled"`
}

// DataProcessor 数据处理器
type DataProcessor struct {
	Log    *zap.Logger
	Stores *helper.Stores

	// 处理函数映射
	processors map[string]DataProcessorFunc
}

// DataProcessorFunc 处理函数类型
type DataProcessorFunc func(ctx context.Context, doc *model.CrawlResult) (*model.ProcessedData, error)

// NewDataProcessor 创建数据处理器
func NewDataProcessor(log *zap.Logger, stores *helper.Stores) *DataProcessor {
	dp := &DataProcessor{
		Log:        log,
		Stores:     stores,
		processors: make(map[string]DataProcessorFunc),
	}

	// 注册处理函数
	dp.registerProcessors()

	return dp
}

// registerProcessors 注册所有处理函数
func (dp *DataProcessor) registerProcessors() {
	// 抖音热搜处理器
	// dp.processors["抖音_general_trending"] = dp.processDouyinTrending
	// dp.processors["微博_general_trending"] = dp.processWeiboTrending
	// dp.processors["百度_general_trending"] = dp.processBaiduTrending
	//dp.processors["知乎_general_trending"] = dp.processZhihuTrending
	dp.processors["澎湃_general_daily"] = dp.processPengpaiDaily
}

// Run 启动数据处理器
func (dp *DataProcessor) Run(ctx context.Context, configs []DataProcessorConfig) {
	for _, cfg := range configs {
		if !cfg.Enabled {
			continue
		}
		// 为每个配置启动一个独立协程，协程内部只负责 **一次** 处理（外部计时器决定何时再次调用）。
		go func(c DataProcessorConfig) {
			// 这里不再使用 ticker，直接调用一次
			dp.processData(ctx, c)
		}(cfg)
	}
}

// processData 处理数据
func (dp *DataProcessor) processData(ctx context.Context, config DataProcessorConfig) {
	processorKey := fmt.Sprintf("%s_%s_%s", config.Source, config.Category, config.InfoType)

	processor, exists := dp.processors[processorKey]
	if !exists {
		dp.Log.Warn("No processor found for config",
			zap.String("processorKey", processorKey),
		)
		return
	}

	// 查询未处理的数据
	filter := bson.M{
		"source":    config.Source,
		"category":  config.Category,
		"info_type": config.InfoType,
		"processed": true,
	}

	// 获取今天的集合名
	today := time.Now()
	collName := helper.RawDataCollName(today)
	collection := dp.Stores.DB.Collection(collName)

	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		dp.Log.Error("Failed to query data",
			zap.String("collection", collName),
			zap.Any("filter", filter),
			zap.Error(err),
		)
		return
	}
	defer cursor.Close(ctx)

	processedCount := 0
	for cursor.Next(ctx) {
		var doc model.CrawlResult
		if err := cursor.Decode(&doc); err != nil {
			dp.Log.Error("Failed to decode document", zap.Error(err))
			continue
		}

		// 处理数据
		processedData, err := processor(ctx, &doc)
		if err != nil {
			dp.Log.Error("Failed to process document",
				zap.String("docId", doc.ID.Hex()),
				zap.Error(err),
			)
			continue
		}

		// 保存处理后的数据
		if err := dp.saveProcessedData(ctx, processedData); err != nil {
			dp.Log.Error("Failed to save processed data",
				zap.String("docId", doc.ID.Hex()),
				zap.Error(err),
			)
			continue
		}

		// 标记原始数据为已处理
		if err := dp.markAsProcessed(ctx, collection, doc.ID); err != nil {
			dp.Log.Error("Failed to mark as processed",
				zap.String("docId", doc.ID.Hex()),
				zap.Error(err),
			)
			continue
		}

		processedCount++
	}

	if processedCount > 0 {
		dp.Log.Info("Data processing completed",
			zap.String("processorKey", processorKey),
			zap.Int("processedCount", processedCount),
		)
	}
}

// saveProcessedData 保存处理后的数据
func (dp *DataProcessor) saveProcessedData(ctx context.Context, data *model.ProcessedData) error {
	// 使用专门的处理后数据集合
	collection := dp.Stores.DB.Collection("processed_data")

	_, err := collection.InsertOne(ctx, data)
	return err
}

// markAsProcessed 标记为已处理
func (dp *DataProcessor) markAsProcessed(ctx context.Context, collection *mongo.Collection, docID interface{}) error {
	filter := bson.M{"_id": docID}
	update := bson.M{"$set": bson.M{"processed": true, "processed_at": time.Now()}}

	_, err := collection.UpdateOne(ctx, filter, update)
	return err
}
