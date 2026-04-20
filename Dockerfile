FROM ghcr.io/astral-sh/uv:python3.12-bookworm

WORKDIR /

RUN git clone https://github.com/rocky-d/binance-connector-python.git

WORKDIR /bnufsa

COPY . .

RUN uv sync

CMD ["uv", "run", "main.py"]
