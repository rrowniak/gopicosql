FROM golang:alpine AS build

MAINTAINER rrowniak_at_gmail.com

RUN apk update && apk upgrade && apk add --no-cache make

WORKDIR /go/src/github.com/rrowniak/gopicosql/
COPY ./ ./

RUN make build-static


FROM alpine:latest
COPY --from=build /go/src/github.com/rrowniak/gopicosql/build/dbserver_static /bin/dbserver
ENTRYPOINT ["/bin/dbserver"]