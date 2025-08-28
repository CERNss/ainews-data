package processor

import (
	"api-fetch/internal/api_fetch/model"
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	"reflect"
	"time"
)

func (dp *DataProcessor) processPengpaiDaily(ctx context.Context, doc *model.CrawlResult) (*model.ProcessedData, error) {
	if doc.Data == nil {
		dp.Log.Warn("doc.Data is nil", zap.String("rawDocId", doc.ID.Hex()))
		return nil, fmt.Errorf("empty data")
	}

	categories := map[string]string{
		"morningEveningNews":       "早晚报",
		"financialInformationNews": "财经资讯",
		"hotNews":                  "热点新闻",
		"editorHandpicked":         "编辑精选",
	}

	var allResults []interface{}

	for categoryKey, seriesTypeName := range categories {
		rawData := doc.Data[categoryKey]
		if rawData == nil {
			continue
		}

		arr := convertToSlice(rawData, dp.Log)
		if arr == nil {
			dp.Log.Warn("failed to convert to slice",
				zap.String("category", categoryKey),
				zap.String("dataType", reflect.TypeOf(rawData).String()))
			continue
		}

		for i, item := range arr {
			if transformed, ok := transformItem(item, seriesTypeName, dp.Log); ok {
				allResults = append(allResults, transformed)
			} else {
				dp.Log.Debug("item filtered out",
					zap.String("category", categoryKey),
					zap.Int("index", i))
			}
		}
	}

	finalData := map[string]interface{}{
		"articles": allResults,
	}

	processed := &model.ProcessedData{
		Source:      doc.Source,
		Category:    doc.Category,
		InfoType:    doc.InfoType,
		Date:        doc.Date,
		ProcessedAt: time.Now(),
		Data:        finalData,
		RawDocID:    doc.ID.Hex(),
	}

	return processed, nil
}

// 🔥 关键修正：处理 MongoDB primitive.A 类型
func convertToSlice(rawData interface{}, logger *zap.Logger) []interface{} {
	if rawData == nil {
		return nil
	}

	// 🔥 处理 MongoDB primitive.A 类型
	if primitiveA, ok := rawData.(primitive.A); ok {
		result := make([]interface{}, len(primitiveA))
		for i, v := range primitiveA {
			result[i] = v
		}
		return result
	}

	// 处理标准的 []interface{}
	if arr, ok := rawData.([]interface{}); ok {
		return arr
	}

	// 使用反射处理其他切片类型
	rv := reflect.ValueOf(rawData)
	if rv.Kind() != reflect.Slice {
		logger.Debug("rawData is not a slice",
			zap.String("actualType", reflect.TypeOf(rawData).String()),
			zap.String("kind", rv.Kind().String()))
		return nil
	}

	result := make([]interface{}, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		result[i] = rv.Index(i).Interface()
	}
	return result
}

func transformItem(item interface{}, seriesType string, logger *zap.Logger) (map[string]interface{}, bool) {
	// 🔥 处理 MongoDB primitive.M 类型
	var m map[string]interface{}

	if primitiveM, ok := item.(primitive.M); ok {
		// 将 primitive.M 转换为 map[string]interface{}
		m = make(map[string]interface{})
		for k, v := range primitiveM {
			m[k] = v
		}
	} else if mapItem, ok := item.(map[string]interface{}); ok {
		m = mapItem
	} else {
		logger.Debug("item is not map or primitive.M",
			zap.String("actualType", reflect.TypeOf(item).String()),
			zap.String("seriesType", seriesType))
		return nil, false
	}

	// 放宽 contType 条件
	if ctRaw, exists := m["contType"]; exists {
		var contType int64
		switch v := ctRaw.(type) {
		case int:
			contType = int64(v)
		case int32:
			contType = int64(v)
		case int64:
			contType = v
		case float64:
			contType = int64(v)
		case string:
			if _, err := fmt.Sscanf(v, "%d", &contType); err != nil {
				logger.Debug("invalid contType format",
					zap.String("contType", v),
					zap.String("seriesType", seriesType))
				return nil, false
			}
		default:
			logger.Debug("unsupported contType type",
				zap.String("contType", fmt.Sprintf("%v", ctRaw)),
				zap.String("contTypeType", reflect.TypeOf(ctRaw).String()),
				zap.String("seriesType", seriesType))
			return nil, false
		}

		// 允许常见的内容类型：0(文章), 1(其他), 9(视频), 15(快讯)
		allowedTypes := []int64{0, 1, 9, 15}
		isAllowed := false
		for _, allowedType := range allowedTypes {
			if contType == allowedType {
				isAllowed = true
				break
			}
		}

		if !isAllowed {
			logger.Debug("contType not allowed",
				zap.Int64("contType", contType),
				zap.String("seriesType", seriesType))
			return nil, false
		}
	} else {
		logger.Debug("no contType field", zap.String("seriesType", seriesType))
		return nil, false
	}

	// 检查 ID 字段
	contID, hasContID := m["contId"]
	origID, hasOrigID := m["originalContId"]

	if (!hasContID || isEmpty(contID)) && (!hasOrigID || isEmpty(origID)) {
		logger.Debug("no valid ID found", zap.String("seriesType", seriesType))
		return nil, false
	}

	// 构造新的结构体
	result := make(map[string]interface{})

	var articleID string
	if hasContID && !isEmpty(contID) {
		articleID = fmt.Sprintf("%v", contID)
	} else if hasOrigID && !isEmpty(origID) {
		articleID = fmt.Sprintf("%v", origID)
	}
	result["articleID"] = articleID

	if seriesTag, exists := m["seriesTagRecType"]; exists && !isEmpty(seriesTag) {
		result["partition"] = seriesTag
	}

	if pubTime, exists := m["pubTimeLong"]; exists && !isEmpty(pubTime) {
		result["timestamp"] = pubTime
	}

	result["seriesType"] = seriesType
	result["origin_url"] = fmt.Sprintf("https://www.thepaper.cn/detail/%s", articleID)

	return result, true
}

func isEmpty(v interface{}) bool {
	if v == nil {
		return true
	}
	switch t := v.(type) {
	case string:
		return t == ""
	case []byte:
		return len(t) == 0
	case []interface{}:
		return len(t) == 0
	case map[string]interface{}:
		return len(t) == 0
	case primitive.A:
		return len(t) == 0
	case primitive.M:
		return len(t) == 0
	default:
		return false
	}
}
