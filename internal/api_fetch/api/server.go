package api

import (
	"api-fetch/internal/api_fetch/model"
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"

	"api-fetch/internal/api_fetch/helper"
)

type Server struct {
	Stores *helper.Stores
}

func (s *Server) Router() *gin.Engine {
	r := gin.Default()
	r.GET("/apis", s.listAPIs)
	r.GET("/contents", s.listContents) // ?date=YYYY-MM-DD&source=&category=&page=1&limit=20
	return r
}

func (s *Server) listAPIs(c *gin.Context) {
	cur, err := s.Stores.APIs.Find(c, bson.M{})
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	defer func(cur *mongo.Cursor, ctx context.Context) {
		err := cur.Close(ctx)
		if err != nil {

		}
	}(cur, c)
	var out []model.APIInfo
	for cur.Next(c) {
		var a model.APIInfo
		_ = cur.Decode(&a)
		out = append(out, a)
	}
	c.JSON(http.StatusOK, gin.H{"data": out})
}

func (s *Server) listContents(c *gin.Context) {
	date := c.Query("date")
	if date == "" {
		loc, _ := time.LoadLocation("Asia/Shanghai")
		date = time.Now().In(loc).Format("2006-01-02")
	}
	collName := "content_" + replace(date, "-", "_")
	coll := s.Stores.DB.Collection(collName)

	filter := bson.M{}
	if v := c.Query("source"); v != "" {
		filter["source"] = v
	}
	if v := c.Query("category"); v != "" {
		filter["category"] = v
	}

	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if page <= 0 {
		page = 1
	}
	if limit <= 0 || limit > 200 {
		limit = 20
	}
	skip := int64((page - 1) * limit)

	cur, err := coll.Find(c, filter, nil)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	defer func(cur *mongo.Cursor, ctx context.Context) {
		err := cur.Close(ctx)
		if err != nil {

		}
	}(cur, c)

	// 手动分页（最简化；生产建议用 FindOptions 设置 Skip/Limit + 排序）
	var all []bson.M
	for cur.Next(c) {
		var m bson.M
		_ = cur.Decode(&m)
		all = append(all, m)
	}
	end := skip + int64(limit)
	if end > int64(len(all)) {
		end = int64(len(all))
	}
	start := skip
	if start > end {
		start = end
	}
	c.JSON(200, gin.H{
		"date":  date,
		"total": len(all),
		"data":  all[start:end],
		"page":  page,
		"limit": limit,
	})
}

func replace(s, old, new string) string {
	out := []rune(s)
	for i, r := range out {
		if string(r) == old {
			out[i] = []rune(new)[0]
		}
	}
	return string(out)
}
