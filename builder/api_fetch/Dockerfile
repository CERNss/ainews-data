# 使用官方 Go 镜像作为构建阶段
FROM golang:1.23 AS builder

# 设置工作目录
WORKDIR /app

# 将 go.mod 和 go.sum 复制到工作目录
COPY go.mod go.sum ./

# 下载依赖
RUN go mod download

# 将项目代码复制到工作目录
COPY . .

# 构建可执行文件
RUN go build -o api_fetch ./cmd/api_fetch

# 使用更小的基础镜像运行应用程序
FROM debian:bullseye-slim

# 设置工作目录
WORKDIR /app

# 将构建阶段的可执行文件复制到运行阶段
COPY --from=builder /app/api_fetch .

# 暴露应用程序的端口（根据你的项目设置）
EXPOSE 8080

# 启动应用程序
CMD ["./api_fetch"]
