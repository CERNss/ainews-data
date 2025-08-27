package scheduler

import (
	"api-fetch/internal/api_fetch/helper"
	"api-fetch/internal/api_fetch/model"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type Worker struct {
	Log        *zap.Logger
	Stores     *helper.Stores
	HTTPClient *http.Client
}

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

func (w *Worker) Run(ctx context.Context) {
	// 立即跑一次（如果你更想严格对齐到点，也可以去掉这行）
	w.runOnce(ctx)

	// 主循环：每次睡到下一个点位
	shanghai, _ := time.LoadLocation("Asia/Shanghai")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			next := next4x(time.Now(), shanghai)
			sleep := time.Until(next)
			if sleep < 0 {
				sleep = 0
			}
			timer := time.NewTimer(sleep)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				w.runOnce(ctx)
			}
		}
	}
}

func (w *Worker) runOnce(ctx context.Context) {
	now := time.Now()
	// 1) 读启用的 API
	cur, err := w.Stores.APIs.Find(ctx, bson.M{"enabled": true})
	if err != nil {
		return
	}
	defer func(cur *mongo.Cursor, ctx context.Context) {
		err := cur.Close(ctx)
		if err != nil {

		}
	}(cur, ctx)

	// 2) 确保当天分表 & 写入
	collName := helper.RawDataCollName(now)
	helper.EnsureRawDataIndexes(ctx, w.Stores.DB, collName)
	contentColl := w.Stores.DB.Collection(collName)

	for cur.Next(ctx) {
		var api model.APIInfo
		w.Log.Info("Processing API", zap.String("api", cur.Current.String()))
		if err := cur.Decode(&api); err != nil {
			w.Log.Error("Failed to decode API", zap.Error(err))
			continue
		}
		w.fetchAndSaveWithRetry(ctx, &api, contentColl, now)
	}
}

// calculateRetryDelay 计算重试延迟时间：15s * 2^(n-1)
func (w *Worker) calculateRetryDelay(retryCount int) time.Duration {
	if retryCount <= 0 {
		return 15 * time.Second
	}
	// 15s * 2^(n-1)
	delay := 15 * time.Second
	for i := 1; i < retryCount; i++ {
		delay *= 2
	}
	return delay
}

// fetchAndSaveWithRetry 带重试机制的抓取和保存
func (w *Worker) fetchAndSaveWithRetry(ctx context.Context, api *model.APIInfo, contentColl *mongo.Collection, now time.Time) {
	const maxRetries = 5

	for attempt := 1; attempt <= maxRetries; attempt++ {
		success := w.fetchAndSave(ctx, api, contentColl, now, attempt)
		if success {
			// 成功，退出重试循环
			return
		}

		if attempt < maxRetries {
			// 计算重试延迟：15s * 2^(n-1)
			retryDelay := w.calculateRetryDelay(attempt)
			w.Log.Info("Retrying after delay",
				zap.String("source", api.Source),
				zap.String("category", api.Category),
				zap.Int("attempt", attempt),
				zap.Int("maxRetries", maxRetries),
				zap.Duration("delay", retryDelay),
			)

			// 等待重试延迟
			timer := time.NewTimer(retryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				w.Log.Info("Context cancelled, stopping retries",
					zap.String("source", api.Source),
					zap.String("category", api.Category),
				)
				return
			case <-timer.C:
				// 继续重试
			}
		} else {
			// 达到最大重试次数
			w.Log.Error("Max retries exceeded, giving up",
				zap.String("source", api.Source),
				zap.String("category", api.Category),
				zap.Int("maxRetries", maxRetries),
			)
		}
	}
}

func (w *Worker) fetchAndSave(ctx context.Context, api *model.APIInfo, contentColl *mongo.Collection, now time.Time, attempt int) bool {
	var req *http.Request
	var err error

	if strings.ToUpper(api.Method) == "GET" {
		u, _ := url.Parse(api.URL)
		q := u.Query()
		for k, v := range api.Params {
			q.Set(k, v)
		}
		u.RawQuery = q.Encode()
		req, err = http.NewRequestWithContext(ctx, api.Method, u.String(), nil)
	} else if strings.ToUpper(api.Method) == "POST/JSON" {
		jsonData, err := json.Marshal(api.Params)
		if err != nil {
			w.Log.Error("Failed to marshal JSON params",
				zap.String("source", api.Source),
				zap.String("category", api.Category),
				zap.Int("attempt", attempt),
				zap.Error(err),
			)
			return false // 序列化失败，不重试
		}

		// 创建 POST 请求，body 是 JSON
		req, err = http.NewRequestWithContext(ctx, api.Method, api.URL, bytes.NewReader(jsonData))
		if err != nil {
			w.Log.Error("Failed to create POST/JSON request",
				zap.String("source", api.Source),
				zap.String("category", api.Category),
				zap.Int("attempt", attempt),
				zap.Error(err),
			)
			return false
		}
		req.Header.Set("Content-Type", "application/json")
	} else if strings.ToUpper(api.Method) == "POST/FORM" {
		form := url.Values{}
		for k, v := range api.Params {
			form.Set(k, v)
		}
		req, err = http.NewRequestWithContext(ctx, api.Method, api.URL, strings.NewReader(form.Encode()))
		if err != nil {
			w.Log.Error("Failed to create POST/FORM request",
				zap.String("source", api.Source),
				zap.String("category", api.Category),
				zap.Int("attempt", attempt),
				zap.Error(err),
			)
			return false
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	if req == nil {
		w.Log.Error("Failed to create request - unsupported method",
			zap.String("method", api.Method),
			zap.String("source", api.Source),
			zap.String("category", api.Category),
			zap.Int("attempt", attempt),
		)
		return false
	}

	for k, v := range api.Headers {
		req.Header.Set(k, v)
	}

	resp, err := w.HTTPClient.Do(req)
	if err != nil {
		w.Log.Error("Failed to fetch API",
			zap.String("url", api.URL),
			zap.String("source", api.Source),
			zap.String("category", api.Category),
			zap.Int("attempt", attempt),
			zap.Error(err),
		)
		return false // 网络错误，触发重试
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			w.Log.Warn("Failed to close response body", zap.Error(err))
		}
	}(resp.Body)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		w.Log.Error("Failed to read response body",
			zap.String("source", api.Source),
			zap.String("category", api.Category),
			zap.Int("attempt", attempt),
			zap.Error(err),
		)
		return false
	}

	w.Log.Info("Fetched API",
		zap.String("source", api.Source),
		zap.String("category", api.Category),
		zap.Int("attempt", attempt),
		zap.String("body", string(body)),
	)

	var parsed any
	if err := json.Unmarshal(body, &parsed); err != nil {
		w.Log.Warn("Invalid JSON, skip",
			zap.String("source", api.Source),
			zap.String("category", api.Category),
			zap.Int("attempt", attempt),
			zap.Error(err),
		)
		return false // JSON解析失败，不重试
	}

	// 顶层必须是对象
	parsedObj, ok := parsed.(map[string]any)
	if !ok {
		w.Log.Warn("Top-level is not JSON object, skip",
			zap.String("source", api.Source),
			zap.String("category", api.Category),
			zap.Int("attempt", attempt),
		)
		return false // 格式错误，不重试
	}

	// Required 校验（全转 string 比较）
	for key, want := range api.Required {
		gotVal, exists := parsedObj[key]
		if !exists {
			w.Log.Warn("skip: missing required field",
				zap.String("field", key),
				zap.String("source", api.Source),
				zap.String("category", api.Category),
				zap.Int("attempt", attempt),
			)
			return false // 字段缺失，触发重试
		}
		// 转成 string
		wantStr := fmt.Sprint(want)
		gotStr := fmt.Sprint(gotVal)
		if gotStr != wantStr {
			w.Log.Warn("skip: field value mismatch",
				zap.String("field", key),
				zap.String("want", wantStr),
				zap.String("got", gotStr),
				zap.String("source", api.Source),
				zap.String("category", api.Category),
				zap.Int("attempt", attempt),
			)
			return false // 字段值不匹配，触发重试
		}
	}

	// 拆 data，只保留顶层对象
	data := bson.M(parsedObj)

	// 使用 Asia/Shanghai 生成 date 字段（YYYY-MM-DD）
	shanghai, _ := time.LoadLocation("Asia/Shanghai")

	doc := model.CrawlResult{
		Date:      now.In(shanghai).Format("2006-01-02"),
		Source:    api.Source,
		Category:  api.Category,
		InfoType:  api.InfoType,
		Data:      data,
		Processed: false, // 新增的默认值
		CreatedAt: time.Now().UTC(),
	}

	_, err = contentColl.InsertOne(ctx, doc)
	if err != nil {
		w.Log.Error("Failed to insert document",
			zap.String("source", api.Source),
			zap.String("category", api.Category),
			zap.Int("attempt", attempt),
			zap.Error(err),
		)
		return false // 数据库插入失败，可以重试
	}

	w.Log.Info("Successfully processed API",
		zap.String("source", api.Source),
		zap.String("category", api.Category),
		zap.Int("attempt", attempt),
	)

	return true // 成功
}
