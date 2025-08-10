package helper

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Stores struct {
	DB   *mongo.Database
	APIs *mongo.Collection // 固定集合：apis
}

func MustMongo(ctx context.Context, host, dbname, username, password, authSource string) *Stores {
	clientOpts := options.Client().
		ApplyURI("mongodb://" + host).
		SetAuth(options.Credential{
			Username:   username,
			Password:   password,
			AuthSource: authSource,
		})

	cli, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		panic(err)
	}
	if err = cli.Ping(ctx, nil); err != nil {
		panic(err)
	}

	db := cli.Database(dbname)
	s := &Stores{
		DB:   db,
		APIs: db.Collection("apis"),
	}
	ensureIndexes(ctx, s)
	return s
}

func ensureIndexes(ctx context.Context, s *Stores) {
	// apis: 常用查询索引
	_, _ = s.APIs.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "enabled", Value: 1}}},
		{Keys: bson.D{{Key: "source", Value: 1}}},
		{Keys: bson.D{{Key: "category", Value: 1}}},
	})
}

// -------- 按日期分表（collection）工具 --------

// 替换原来的 tokyo 变量为 shanghai
var shanghai *time.Location

// ConfigureTimeLocation 设置时区，默认 Asia/Shanghai
func ConfigureTimeLocation(name string) error {
	loc, err := time.LoadLocation(name)
	if err != nil {
		// 可选兜底：固定到 UTC+8，避免因加载失败直接崩
		loc = time.FixedZone("CST", 8*3600)
		// 如果你更倾向报错而不是兜底，改成：return err
	}
	shanghai = loc
	return nil
}

// ContentCollName 当天（或指定时间）对应的 collection 名称：content_YYYY_MM_DD
func ContentCollName(t time.Time) string {
	loc := shanghai
	if loc == nil {
		// 防御：若忘记初始化，仍使用 UTC+8 兜底
		loc = time.FixedZone("CST", 8*3600)
	}
	day := t.In(loc).Format("2006_01_02")
	return fmt.Sprintf("content_%s", day)
}

// EnsureContentIndexes 确保当天分表有索引（source、category、createdAt）
func EnsureContentIndexes(ctx context.Context, db *mongo.Database, collName string) {
	c := db.Collection(collName)
	_, _ = c.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "source", Value: 1}}},
		{Keys: bson.D{{Key: "category", Value: 1}}},
		{Keys: bson.D{{Key: "createdAt", Value: 1}}},
	})
}
