# Stage 1
FROM cgr.dev/chainguard/go AS builder
WORKDIR /app
COPY . .
RUN go build -o statsbot main.go

# Stage 2
FROM cgr.dev/chainguard/wolfi-base:latest
WORKDIR /app
COPY --from=builder /app/statsbot .
CMD ["./statsbot"]