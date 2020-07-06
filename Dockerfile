FROM golang AS build-env
ENV CGO_ENABLED=0
ADD . /src
RUN cd /src && \
    go get -d -v ./... && \
    go build -o yarn-exporter yarn-exporter.go && \
    chmod a+x yarn-exporter

FROM golang:alpine
WORKDIR /yarn-exporter
COPY --from=build-env /src/yarn-exporter /usr/bin/
EXPOSE 9113/tcp
ENTRYPOINT [ "/usr/bin/yarn-exporter" ]