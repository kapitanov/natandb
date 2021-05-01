FROM golang:latest as build
ADD . /src
WORKDIR /src
RUN go get
RUN CGO_ENABLED=0 go build -o natandb .

FROM alpine:latest
RUN apk --no-cache add ca-certificates curl

COPY --from=build /src/natandb /opt/natandb/natandb
WORKDIR /opt/natandb

VOLUME /var/lib/natandb
EXPOSE 18081
HEALTHCHECK --start-period=30s CMD curl -f http://localhost:18081/api || exit 1

CMD [ "/opt/natandb/natandb", "run", "--data", "/var/lib/natandb", "--listen", "0.0.0.0:18081" ]
