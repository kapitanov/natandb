FROM golang:latest as build
WORKDIR /src
ADD go.mod /src/go.mod
ADD go.sum /src/go.sum
RUN go mod download
ADD . /src
ENV CGO_ENABLED=0
RUN go build -o /out/natandb -v ./cmd/natandb

FROM alpine:latest
RUN apk --no-cache add ca-certificates curl
COPY --from=build /out/natandb /opt/natandb/natandb
WORKDIR /opt/natandb
VOLUME /var/lib/natandb
EXPOSE 18081
HEALTHCHECK --start-period=30s CMD curl -f http://localhost:18081/api || exit 1
CMD [ "/opt/natandb/natandb", "run", "--data", "/var/lib/natandb", "--listen", "0.0.0.0:18081" ]
