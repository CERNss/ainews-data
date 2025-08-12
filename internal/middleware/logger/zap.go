package logger

import (
	"go.uber.org/zap"
)

// NewLogger 创建一个新的 zap.Logger 实例
// 这个函数会被 fx 用来提供 logger 依赖
func NewLogger() (*zap.Logger, error) {
	// 在实际应用中，你可能需要根据配置来初始化 logger
	// 例如：开发环境用 zap.NewDevelopment(), 生产环境用 zap.NewProduction()
	// 也可以从配置文件读取日志级别、输出格式等

	// 示例：使用开发版 logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		return nil, err
	}

	// 示例：使用生产版 logger (输出为 JSON)
	// logger, err := zap.NewProduction()
	// if err != nil {
	// 	return nil, err
	// }

	// 你可以在这里添加全局的初始字段
	// logger = logger.With(zap.String("service", "my-app"))

	return logger, nil
}
