FROM golang:1.22.5 AS builder

COPY . /go/src/app
WORKDIR /go/src/app

ENV GO111MODULE=on

RUN CGO_ENABLED=0 GOOS=linux go build -o app

RUN git log -1 --oneline > version.txt

FROM alpine:latest
RUN apk add postgresql16-client
WORKDIR /root/
COPY --from=builder /go/src/app/app .
COPY --from=builder /go/src/app/version.txt .

ENTRYPOINT ["./app"]
