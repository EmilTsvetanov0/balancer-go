FROM golang:1.24

WORKDIR /app
COPY mock_server.go .

RUN go build -o server mock_server.go

CMD ["./server"]
