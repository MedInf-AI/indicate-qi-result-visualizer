FROM docker.io/library/python:3.12-slim

RUN pip install --no-cache-dir "duckdb>=1.1" "pyyaml>=6"

WORKDIR /app
COPY qi_report.py style.css behavior.js ./
COPY data/ data/
RUN mkdir /results && chown nobody: /results

# Result bundles live here; mount a volume to keep uploads.
VOLUME /results
EXPOSE 8000
# Python as PID 1 ignores SIGTERM; SIGINT ends the server cleanly.
STOPSIGNAL SIGINT

USER nobody
ENTRYPOINT ["python", "qi_report.py", "--host", "0.0.0.0", "--port", "8000"]
CMD ["--results-dir", "/results"]
