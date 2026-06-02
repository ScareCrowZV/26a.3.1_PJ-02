FROM golang:latest AS builder

WORKDIR /build
COPY . .
ENV GO111MODULE=on
ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64
RUN go work sync
RUN go mod download
RUN go build -o /app/main .

FROM alpine:latest

LABEL version="1.0.0"
LABEL maintainer="Golubev Ivan"
WORKDIR /app
COPY --from=builder /app/main .
CMD ["./main"]