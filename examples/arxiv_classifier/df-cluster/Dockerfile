FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential git ca-certificates && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.cargo/bin:$PATH"

RUN pip install --no-cache-dir \
    "strata @ git+https://github.com/forge-labs-dev/strata.git@4ec2eb9" \
    "datafusion>=42" \
    "pandas>=2" \
    "pyarrow>=18"

FROM python:3.12-slim

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

EXPOSE 8080
CMD ["python", "-m", "strata.notebook.remote_executor", "--host", "0.0.0.0", "--port", "8080"]
