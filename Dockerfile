FROM golang:1.22-alpine AS builder

ARG COMPONENT

WORKDIR /app

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/app ./cmd/${COMPONENT}

FROM alpine:3.19

RUN apk --no-cache add ca-certificates wget

WORKDIR /app

COPY --from=builder /bin/app .

RUN mkdir -p /data

ENTRYPOINT ["./app"]