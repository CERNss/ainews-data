FROM alpine:3

RUN apk update --no-cache && apk add --no-cache ca-certificates tzdata

ENV TZ Asia/Shanghai

COPY ./auth_server /auth_server

EXPOSE 8080

CMD ["./api-fetch"]