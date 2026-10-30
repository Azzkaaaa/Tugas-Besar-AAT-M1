# Build context: repository root.
#   docker build -f services/auth-bridge/Dockerfile .
FROM node:24-alpine
WORKDIR /app
COPY services/auth-bridge/package.json services/auth-bridge/package-lock.json ./
RUN npm ci --omit=dev \
    && rm -rf /usr/local/lib/node_modules /opt/yarn-* \
       /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack /usr/local/bin/yarn /usr/local/bin/yarnpkg
COPY services/auth-bridge/src ./src
RUN mkdir -p /data && chown node:node /data
USER node
EXPOSE 3001
CMD ["node", "src/server.mjs"]
