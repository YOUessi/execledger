FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .
RUN useradd --create-home --uid 10001 execledger && mkdir -p /state && chown -R execledger:execledger /state
USER execledger
EXPOSE 8080
CMD ["execledger", "serve", "--host", "0.0.0.0", "--port", "8080", "--root", "/state"]
