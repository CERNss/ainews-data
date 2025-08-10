package model

import (
	"go.mongodb.org/mongo-driver/bson"
	"time"
)

type CrawlResult struct {
	ID        string    `bson:"_id,omitempty" json:"id"`
	Date      string    `bson:"date" json:"date"` // YYYY-MM-DD（按 Asia/Tokyo 计算）
	Source    string    `bson:"source" json:"source"`
	Category  string    `bson:"category" json:"category"`
	Data      bson.M    `bson:"data" json:"data"`            // 原始/解析后的内容
	CreatedAt time.Time `bson:"createdAt" json:"created_at"` // UTC
}
