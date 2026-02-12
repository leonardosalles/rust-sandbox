FROM rust:1.76 as builder

WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim

WORKDIR /app
COPY --from=builder /app/target/release/pdf-summary-task /app/task

EXPOSE 8080

CMD ["./task"]
