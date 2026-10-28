ARG BUILD_FROM
FROM $BUILD_FROM
# Python και aiohttp από τα πακέτα του Alpine (σταθερές εκδόσεις της διανομής).
RUN apk add --no-cache python3 py3-aiohttp
COPY agent.py /agent.py
# Χωρίς το s6 της βασικής εικόνας: το s6 «καθαρίζει» το περιβάλλον και χάνεται το SUPERVISOR_TOKEN.
# Ο πράκτορας τρέχει απευθείας, με όλο το περιβάλλον που δίνει το Home Assistant.
ENTRYPOINT []
CMD ["python3", "-u", "/agent.py"]
