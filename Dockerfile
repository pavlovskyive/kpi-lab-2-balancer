FROM golang:1.15 as build

RUN apt-get update && apt-get install -y ninja-build

WORKDIR /go/src
RUN git clone https://github.com/pavlovskyive/kpi-lab-1
WORKDIR /go/src/kpi-lab-1
RUN go get -u ./build/cmd/bood

WORKDIR /go/src/kpi-lab-2
COPY . .

RUN CGO_ENABLED=0 bood

# ==== Final image ====
FROM alpine:3.11
WORKDIR /opt/kpi-lab-2
COPY entry.sh ./
COPY --from=build /go/src/kpi-lab-2/out/bin/* ./
ENTRYPOINT ["/opt/kpi-lab-2/entry.sh"]
CMD ["server"]
