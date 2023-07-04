FROM golang:1.18-buster AS build
#RUN go env -w GOPROXY=direct

WORKDIR /app
COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY main.go ./
RUN go build -o /app

FROM gcr.io/distroless/base-debian10
WORKDIR /
COPY --from=build /app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]