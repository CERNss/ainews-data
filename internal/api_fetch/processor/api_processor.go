package processor

import (
	"api-fetch/internal/api_fetch/helper"
	"api-fetch/internal/api_fetch/model"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
)

type Processor struct {
	Log        *zap.Logger
	Stores     *helper.Stores
	HTTPClient *http.Client
}

// NewProcessor 创建新的数据处理器
func NewProcessor(log *zap.Logger, stores *helper.Stores, httpClient *http.Client) *Processor {
	return &Processor{
		Log:        log,
		Stores:     stores,
		HTTPClient: httpClient,
	}
}

// ProcessAPIWithRetry 处理单个API，包含异步重试机制
func (p *Processor) ProcessAPIWithRetry(ctx context.Context, api *model.APIInfo, contentColl *mongo.Collection, now time.Time, retryWg *sync.WaitGroup) {
	// 第一次尝试同步执行
	success := p.fetchAndSave(ctx, api, contentColl, now, 1)
	if success {
		return
	}

	// 第一次失败，启动异步重试goroutine
	retryWg.Add(1)
	go func() {
		defer retryWg.Done()
		p.asyncRetryLoop(ctx, api, contentColl, now)
	}()
}

// calculateRetryDelay 计算重试延迟时间：15s * 2^(n-1)
func (p *Processor) calculateRetryDelay(retryCount int) time.Duration {
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

// asyncRetryLoop 异步重试循环
func (p *Processor) asyncRetryLoop(ctx context.Context, api *model.APIInfo, contentColl *mongo.Collection, now time.Time) {
	const maxRetries = 5

	for attempt := 2; attempt <= maxRetries; attempt++ {
		// 计算重试延迟：15s * 2^(n-1)
		retryDelay := p.calculateRetryDelay(attempt - 1) // attempt-1 因为这是第2次开始

		p.Log.Info("Async retry scheduled",
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
			p.Log.Info("Context cancelled, stopping async retries",
				zap.String("source", api.Source),
				zap.String("category", api.Category),
				zap.Int("attempt", attempt),
			)
			return
		case <-timer.C:
			// 执行重试
			success := p.fetchAndSave(ctx, api, contentColl, now, attempt)
			if success {
				p.Log.Info("Async retry succeeded",
					zap.String("source", api.Source),
					zap.String("category", api.Category),
					zap.Int("attempt", attempt),
				)
				return // 成功，退出重试循环
			}
		}
	}

	// 达到最大重试次数
	p.Log.Error("Async retry max attempts exceeded, giving up",
		zap.String("source", api.Source),
		zap.String("category", api.Category),
		zap.Int("maxRetries", maxRetries),
	)
}

// fetchAndSave 获取API数据并保存到数据库
func (p *Processor) fetchAndSave(ctx context.Context, api *model.APIInfo, contentColl *mongo.Collection, now time.Time, attempt int) bool {
	// 1. 构建HTTP请求
	req, err := p.buildHTTPRequest(ctx, api, attempt)
	if err != nil {
		return false
	}

	// 2. 执行HTTP请求
	resp, err := p.HTTPClient.Do(req)
	if err != nil {
		p.Log.Error("Failed to fetch API",
			zap.String("url", api.URL),
			zap.String("source", api.Source),
			zap.String("category", api.Category),
			zap.Int("attempt", attempt),
			zap.Error(err),
		)
		return false // 网络错误，触发重试
	}
	defer func(Body io.ReadCloser) {
		if err := Body.Close(); err != nil {
			p.Log.Warn("Failed to close response body", zap.Error(err))
		}
	}(resp.Body)

	// 3. 读取响应体
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		p.Log.Error("Failed to read response body",
			zap.String("source", api.Source),
			zap.String("category", api.Category),
			zap.Int("attempt", attempt),
			zap.Error(err),
		)
		return false
	}

	p.Log.Debug("Fetched API response",
		zap.String("source", api.Source),
		zap.String("category", api.Category),
		zap.Int("attempt", attempt),
		zap.Int("bodySize", len(body)),
	)

	// 4. 解析和验证JSON
	parsedObj, err := p.parseAndValidateJSON(body, api, attempt)
	if err != nil {
		return false
	}

	// 5. 提取数据
	data, extractionStrategy, err := p.extractData(parsedObj, api, attempt)
	if err != nil {
		return false
	}

	// 6. 保存到数据库
	return p.saveToDatabase(ctx, data, api, contentColl, now, attempt, extractionStrategy)
}

// buildHTTPRequest 构建HTTP请求
func (p *Processor) buildHTTPRequest(ctx context.Context, api *model.APIInfo, attempt int) (*http.Request, error) {
	var req *http.Request
	var err error

	switch strings.ToUpper(api.Method) {
	case "GET":
		u, _ := url.Parse(api.URL)
		q := u.Query()
		for k, v := range api.Params {
			q.Set(k, v)
		}
		u.RawQuery = q.Encode()
		req, err = http.NewRequestWithContext(ctx, "GET", u.String(), nil)

	case "POST/JSON":
		jsonData, err := json.Marshal(api.Params)
		if err != nil {
			p.Log.Error("Failed to marshal JSON params",
				zap.String("source", api.Source),
				zap.String("category", api.Category),
				zap.Int("attempt", attempt),
				zap.Error(err),
			)
			return nil, err
		}
		req, err = http.NewRequestWithContext(ctx, "POST", api.URL, bytes.NewReader(jsonData))
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
		}

	case "POST/FORM":
		form := url.Values{}
		for k, v := range api.Params {
			form.Set(k, v)
		}
		req, err = http.NewRequestWithContext(ctx, "POST", api.URL, strings.NewReader(form.Encode()))
		if err == nil {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}

	default:
		p.Log.Error("Unsupported HTTP method",
			zap.String("method", api.Method),
			zap.String("source", api.Source),
			zap.String("category", api.Category),
			zap.Int("attempt", attempt),
		)
		return nil, fmt.Errorf("unsupported method: %s", api.Method)
	}

	if err != nil {
		p.Log.Error("Failed to create HTTP request",
			zap.String("method", api.Method),
			zap.String("source", api.Source),
			zap.String("category", api.Category),
			zap.Int("attempt", attempt),
			zap.Error(err),
		)
		return nil, err
	}

	// 设置请求头
	for k, v := range api.Headers {
		req.Header.Set(k, v)
	}

	return req, nil
}

// parseAndValidateJSON 解析和验证JSON响应
func (p *Processor) parseAndValidateJSON(body []byte, api *model.APIInfo, attempt int) (map[string]any, error) {
	var parsed any
	if err := json.Unmarshal(body, &parsed); err != nil {
		p.Log.Warn("Invalid JSON response",
			zap.String("source", api.Source),
			zap.String("category", api.Category),
			zap.Int("attempt", attempt),
			zap.Error(err),
		)
		return nil, err
	}

	// 顶层必须是对象
	parsedObj, ok := parsed.(map[string]any)
	if !ok {
		p.Log.Warn("Top-level is not JSON object",
			zap.String("source", api.Source),
			zap.String("category", api.Category),
			zap.Int("attempt", attempt),
		)
		return nil, fmt.Errorf("top-level is not JSON object")
	}

	// Required 字段校验
	for key, want := range api.Required {
		gotVal, exists := parsedObj[key]
		if !exists {
			p.Log.Warn("Missing required field",
				zap.String("field", key),
				zap.String("source", api.Source),
				zap.String("category", api.Category),
				zap.Int("attempt", attempt),
			)
			return nil, fmt.Errorf("missing required field: %s", key)
		}

		// 转成 string 比较
		wantStr := fmt.Sprint(want)
		gotStr := fmt.Sprint(gotVal)
		if gotStr != wantStr {
			p.Log.Warn("Required field value mismatch",
				zap.String("field", key),
				zap.String("want", wantStr),
				zap.String("got", gotStr),
				zap.String("source", api.Source),
				zap.String("category", api.Category),
				zap.Int("attempt", attempt),
			)
			return nil, fmt.Errorf("field value mismatch: %s", key)
		}
	}

	return parsedObj, nil
}

// extractData 根据配置提取数据
func (p *Processor) extractData(parsedObj map[string]any, api *model.APIInfo, attempt int) (bson.M, string, error) {
	var data bson.M
	var extractionStrategy string

	// 检查是否配置了自定义数据字段
	if api.DataField != "" {
		// 使用自定义字段名
		if dataVal, exists := parsedObj[api.DataField]; exists {
			data = p.convertToDataBson(dataVal)
			extractionStrategy = fmt.Sprintf("custom field: %s", api.DataField)
		} else {
			p.Log.Warn("Missing custom data field in response",
				zap.String("dataField", api.DataField),
				zap.String("source", api.Source),
				zap.String("category", api.Category),
				zap.Int("attempt", attempt),
			)
			return nil, "", fmt.Errorf("missing custom data field: %s", api.DataField)
		}
	} else if api.UseFullResponse {
		// 使用完整响应
		data = bson.M(parsedObj)
		extractionStrategy = "full response"
	} else {
		// 默认使用 "data" 字段
		if dataVal, exists := parsedObj["data"]; exists {
			data = p.convertToDataBson(dataVal)
			extractionStrategy = "default data field"
		} else {
			p.Log.Warn("Missing 'data' field in response",
				zap.String("source", api.Source),
				zap.String("category", api.Category),
				zap.Int("attempt", attempt),
			)
			return nil, "", fmt.Errorf("missing 'data' field in response")
		}
	}

	return data, extractionStrategy, nil
}

// saveToDatabase 保存数据到数据库
func (p *Processor) saveToDatabase(ctx context.Context, data bson.M, api *model.APIInfo, contentColl *mongo.Collection, now time.Time, attempt int, extractionStrategy string) bool {
	// 使用 Asia/Shanghai 生成 date 字段（YYYY-MM-DD）
	shanghai, _ := time.LoadLocation("Asia/Shanghai")

	doc := model.CrawlResult{
		Date:      now.In(shanghai).Format("2006-01-02"),
		Source:    api.Source,
		Category:  api.Category,
		InfoType:  api.InfoType,
		Data:      data,
		Processed: false,
		CreatedAt: time.Now().UTC(),
	}

	_, err := contentColl.InsertOne(ctx, doc)
	if err != nil {
		p.Log.Error("Failed to insert document",
			zap.String("source", api.Source),
			zap.String("category", api.Category),
			zap.Int("attempt", attempt),
			zap.Error(err),
		)
		return false // 数据库插入失败，可以重试
	}

	p.Log.Info("Successfully processed API",
		zap.String("source", api.Source),
		zap.String("category", api.Category),
		zap.Int("attempt", attempt),
		zap.String("extractionStrategy", extractionStrategy),
	)

	return true // 成功
}

// convertToDataBson 将不同类型的数据转换为 bson.M
func (p *Processor) convertToDataBson(dataVal any) bson.M {
	switch v := dataVal.(type) {
	case map[string]any:
		return bson.M(v) // data是对象，直接使用
	case []any:
		return bson.M{"items": v} // data是数组，包装为 {"items": [...]}
	default:
		return bson.M{"value": v} // data是基本类型，包装为 {"value": ...}
	}
}
