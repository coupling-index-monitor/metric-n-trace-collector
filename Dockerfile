FROM golang:1.22.4 as builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .
COPY .env .env

RUN go build -o coupling-monitor .

FROM gcr.io/distroless/base-debian12

WORKDIR /root/

COPY --from=builder /app/coupling-monitor .
COPY --from=0 /app/.env .

EXPOSE 8001

CMD ["./coupling-monitor"]
