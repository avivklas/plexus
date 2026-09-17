FROM golang:1.27

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN go build -o /usr/local/bin/bench .

# Benchmark data lives on the container's own filesystem, never on a bind mount
# from the host: virtiofs does not honour O_DIRECT the way ext4 does.
RUN mkdir -p /data

COPY sweep.sh ab.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/sweep.sh /usr/local/bin/ab.sh

ENTRYPOINT ["/usr/local/bin/sweep.sh"]
