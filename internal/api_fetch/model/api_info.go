package model

type APIInfo struct {
	ID       string            `bson:"_id,omitempty" json:"id"`
	Name     string            `bson:"name" json:"name"`
	Method   string            `bson:"method" json:"method"` // "GET"|"POST"
	URL      string            `bson:"url" json:"url"`
	Headers  map[string]string `bson:"headers,omitempty" json:"headers,omitempty"`
	Params   map[string]string `bson:"params,omitempty" json:"params,omitempty"` // ✅ 新增请求参数
	Source   string            `bson:"source" json:"source"`                     // 来源
	Category string            `bson:"category" json:"category"`                 // 信息分类
	InfoType string            `bson:"info_type" json:"info_type"`               // ✅ 信息类型
	Enabled  bool              `bson:"enabled" json:"enabled"`
}
