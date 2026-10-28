ARG BUILD_FROM
FROM $BUILD_FROM
# Python και aiohttp από τα πακέτα του Alpine (σταθερές εκδόσεις της διανομής).
RUN apk add --no-cache python3 py3-aiohttp
COPY agent.py /agent.py
CMD ["python3", "-u", "/agent.py"]
