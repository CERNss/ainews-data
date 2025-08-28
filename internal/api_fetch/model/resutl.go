package model

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type CrawlResult struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	Date      string             `bson:"date" json:"date"` // YYYY-MM-DD（按 Asia/Shanghai 计算）
	Source    string             `bson:"source" json:"source"`
	Category  string             `bson:"category" json:"category"`
	InfoType  string             `bson:"info_type" json:"info_type"`  // 信息类型
	Data      bson.M             `bson:"data" json:"data"`            // 原始/解析后的内容
	Processed bool               `bson:"processed" json:"processed"`  // 是否已处理
	CreatedAt time.Time          `bson:"createdAt" json:"created_at"` // UTC
}

// ProcessedData 处理后的数据
type ProcessedData struct {
	Source      string                 `bson:"source"`
	Category    string                 `bson:"category"`
	InfoType    string                 `bson:"info_type"`
	Date        string                 `bson:"date"`
	ProcessedAt time.Time              `bson:"processed_at"`
	Data        map[string]interface{} `bson:"data"`
	Processed   bool                   `bson:"processed" json:"processed"`
	RawDocID    string                 `bson:"raw_doc_id"` // 原始文档ID
}
